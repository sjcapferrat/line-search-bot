# app.py
from __future__ import annotations

import os
import uuid
import logging
import traceback
import re
import unicodedata
from typing import Dict, Any, Optional, List

from fastapi import FastAPI, Request, Body
from fastapi.responses import PlainTextResponse, Response
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

# ==============================
# 基本セットアップ
# ==============================
app = FastAPI()
logger = logging.getLogger("app")

def _rid() -> str:
    return uuid.uuid4().hex[:8]

def _norm(s: str) -> str:
    """全角/半角・濁点結合などを統一して比較しやすくする"""
    return unicodedata.normalize("NFKC", s or "")

# ユーザーごとの Clarify 選択待ち状態（揮発）
# 例: _PENDING[sender_id] = {"clarify": dict, "query": dict, "raw": str}
_PENDING: Dict[str, Dict[str, Any]] = {}

# Feature flags
ALLOW_DEV = os.environ.get("ALLOW_DEV", "1") == "1"  # 本番は 0 推奨
FORCE_SUBSTRATE_FALLBACK = os.environ.get("FORCE_SUBSTRATE_FALLBACK", "0") == "1"

# ==============================
# 文字化け対策（JSON に charset 付与）
# ==============================
@app.middleware("http")
async def add_json_charset(request: Request, call_next):
    resp = await call_next(request)
    ctype = resp.headers.get("content-type", "")
    if ctype.startswith("application/json") and "charset=" not in ctype:
        resp.headers["content-type"] = "application/json; charset=utf-8"
    return resp

# ==============================
# ヘルスチェック
# ==============================
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

# ==============================
# LINE 資格情報
# ==============================
CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN) if CHANNEL_ACCESS_TOKEN else None
parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None

# ==============================
# Clarify: nlp_extract の _needs_choice → Clarify オブジェクトへ
# ==============================
def _clarify_from_needs_choice(filters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    nlp_extract.extract_query() が返す filters["_needs_choice"]["下地の状況"]
    を Clarify オブジェクトに変換。
    """
    needs = (filters.get("_needs_choice") or {}).get("下地の状況")
    if not needs:
        return None
    cands = needs.get("candidates") or []
    if not cands:
        return None
    term = str(needs.get("term") or "下地")
    return {
        "trigger": "substrate",
        "question": f"「{term}」はどれですか？（複数可）",
        "choices": [{"id": str(i + 1), "label": lab} for i, lab in enumerate(cands)],
        "column": "下地の状況",
    }

# ==============================
# Natural language choice utils
# ==============================
def _score_label_by_keywords(fragment: str, label: str) -> int:
    """
    fragment と label の語彙一致スコア。
    - 直包含（ラベル丸ごと一致）は最強
    - 括弧内の特徴語（例: 水性/硬質）も拾う
    """
    f = _norm(fragment)
    f = re.sub(r"(のほう|の方)\s*$", "", f)
    lb = _norm(label)

    if lb and lb in f:
        return 100

    base = re.sub(r"（.*?）", "", lb)
    inside = "".join(re.findall(r"（(.*?)）", lb))
    keywords = set()

    for w in ["防塵塗料", "厚膜塗料", "エポキシ", "アクリル", "ウレタン", "塗り重ね"]:
        if w in base or w in inside:
            keywords.add(w)

    # 括弧内の特徴語
    for token in re.findall(r"[A-Za-z]+|[\u4E00-\u9FFF]+|[\u3040-\u30FF]+", inside):
        for sub in ["水性", "硬質", "無黄変", "速乾"]:
            if sub in token:
                keywords.add(sub)

    synonyms = {
        "防塵塗料": {"防塵"},
        "厚膜塗料": {"厚膜"},
        "塗り重ね": {"重ね", "重ね塗り"},
        "水性": {"水性"},
        "硬質": {"硬質"},
    }

    score = 0
    for kw in keywords:
        if kw in f:
            score += 5
        for alt in synonyms.get(kw, set()):
            if alt in f:
                score += 3

    if ("水性" in f) and (("水性" in inside) or ("水性" in base)):
        score += 15
    if ("硬質" in f) and (("硬質" in inside) or ("硬質" in base)):
        score += 8
    return score

def _parse_chosen_text(chosen_text: str, choices: List[Dict[str, str]]) -> List[str]:
    """
    自然文/番号/ラベル混在の chosen_text から、選ばれた“ラベル配列”を返す。
    - "all/全部/すべて" は全選択
    - "unknown/わからない/任せる" は空のまま（おすすめ選定は呼び出し側）
    - 数字(1,2) なら id マッチ
    - それ以外はキーワードスコアで最尤ラベルを1件
    """
    if not chosen_text:
        return []

    t = _norm(chosen_text).lower().strip()

    if t in {"all", "全部", "すべて", "全て"}:
        return [str(c.get("label", "")) for c in choices if c.get("label")]
    if t in {"unknown", "わからない", "任せる"}:
        return []

    t2 = t.replace("，", ",")
    parts = [p.strip() for p in re.split(r"[,\s]+", t2) if p.strip()]
    id2label = {str(c.get("id", "")).strip(): str(c.get("label", "")).strip() for c in choices}
    labels = [str(c.get("label", "")).strip() for c in choices]

    # まず番号で拾う
    picked: List[str] = []
    for p in parts:
        if p in id2label and id2label[p]:
            picked.append(id2label[p])
    if picked:
        return picked

    # ラベルそのもの一致
    for p in parts:
        for lab in labels:
            if p and (p == _norm(lab)):
                return [lab]

    # スコアで最尤を1件
    best = None
    best_score = -1
    for lab in labels:
        sc = _score_label_by_keywords(chosen_text, lab)
        if sc > best_score:
            best_score = sc
            best = lab
    return [best] if best else []

# ==============================
# 共通メッセージ・ヒント
# ==============================
RESET_HINT = "\n\n---\n新しい検索を行う場合はゼロ、０、またはリセット指示をお願いします。"
NORESULT_MSG = "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。"

def _with_reset_hint(text: str) -> str:
    text = (text or "").rstrip()
    return f"{text}{RESET_HINT}"

# ==============================
# Webhook（LINE） — 安定版
# ==============================
@app.post("/callback")
async def callback(request: Request):
    if not parser:
        logger.error("LINE credentials not set")
        # 502 を避けるため 200 を返す
        return PlainTextResponse("OK", status_code=200)

    signature = request.headers.get("X-Line-Signature") or request.headers.get("x-line-signature", "")
    try:
        body_text = (await request.body()).decode("utf-8")
    except Exception as e:
        logger.error("read body failed: %r", e)
        return PlainTextResponse("OK", status_code=200)

    logger.info("==> /callback hit, bytes=%s", len(body_text))

    try:
        events = parser.parse(body_text, signature)
    except InvalidSignatureError:
        return PlainTextResponse("Invalid signature", status_code=400)
    except Exception as e:
        logger.error("parser.parse failed: %r\n%s", e, traceback.format_exc())
        return PlainTextResponse("OK", status_code=200)

    if not events:
        logger.info("no events (verify?) -> 200")
        return PlainTextResponse("OK", status_code=200)

    # 遅延インポート
    try:
        from nlp_extract import extract_query
        from disambiguator import detect, apply_choice_to_query
        from search_core import run_query_system
        from formatters import to_plain_text
        from postprocess import reorder_and_pair
    except Exception as e:
        logger.error("delayed import failed: %r\n%s", e, traceback.format_exc())
        return PlainTextResponse("OK", status_code=200)

    def _sender_id(ev):
        src = getattr(ev, "source", None)
        return (getattr(src, "user_id", None)
                or getattr(src, "group_id", None)
                or getattr(src, "room_id", None)
                or "unknown")

    for event in events:
        try:
            if not (isinstance(event, MessageEvent) and isinstance(event.message, TextMessage)):
                continue

            rid = _rid()
            user_text = (event.message.text or "").strip()
            sender_id = _sender_id(event)
            logger.info("[%s] from=%s text=%r", rid, sender_id, user_text)

            # グローバルコマンド（どの状態でも有効）
            t = _norm(user_text)
            if t in {"0", "０", "ゼロ", "ﾘｾｯﾄ", "リセット"}:
                _PENDING.pop(sender_id, None)
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="新しい検索を始めます。条件を入力してください。")
                )
                continue
            if t in {"1", "１", "終わり", "終了"}:
                _PENDING.pop(sender_id, None)
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="終了しました。またどうぞ！")
                )
                continue

            # ❶ Clarify の回答待ちだった場合
            if sender_id in _PENDING:
                try:
                    pend = _PENDING.pop(sender_id)
                    clarify = pend["clarify"]
                    choices = clarify.get("choices") or []
                    chosen_labels = _parse_chosen_text(user_text, choices)

                    if not chosen_labels:
                        # 再度案内
                        lines = ["選択肢が認識できませんでした。番号（例：1,3）またはラベル名で返信してください。", ""]
                        for ch in choices:
                            lines.append(f'  {ch.get("id")}) {ch.get("label")}')
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n".join(lines)))
                        continue

                    query_after = apply_choice_to_query(pend["query"], chosen_labels, clarify)
                    results = run_query_system(query_after)
                    results = reorder_and_pair(results, pend["raw"], query_after)

                    text_msg = to_plain_text(results, query_after, "(clarified)")
                    if not results:
                        text_msg = NORESULT_MSG
                    else:
                        text_msg = _with_reset_hint(text_msg)
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=text_msg[:4900]))
                    continue

                except Exception as e:
                    logger.error("[%s] clarify handling failed: %r\n%s", rid, e, traceback.format_exc())
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=f"選択の処理でエラーが発生しました。最初から入力し直してください。（Error ID: {rid}）")
                    )
                    continue

            # ❷ 抽出 → Clarify 判定
            try:
                query, explain = extract_query(user_text)
            except Exception as e:
                logger.error("[%s] extract_query failed: %r\n%s", rid, e, traceback.format_exc())
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）")
                )
                continue

            c_from_needs = _clarify_from_needs_choice(query)
            clarifies = [c_from_needs] if c_from_needs else (detect(user_text) or [])
            if clarifies:
                c = clarifies[0]
                _PENDING[sender_id] = {"clarify": c, "query": query, "raw": user_text}

                lines = []
                lines.append(str(c.get("question") or "条件をもう少し具体化してください。"))
                lines.append("")
                lines.append("次から選んで返信してください（複数可）：")
                for ch in c.get("choices", []):
                    lines.append(f'  {ch.get("id")}) {ch.get("label")}')
                lines += ["", "ヒント:", "・番号だけでもOK（例：1,3）", "・ラベルでもOK（例：厚膜塗料（エポキシ））", "・全て = all / わからない = unknown"]
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n".join(lines)))
                continue

            # ❸ 検索 → 並べ替え → 返信
            try:
                results = run_query_system(query)
                results = reorder_and_pair(results, user_text, query)
            except Exception as e:
                logger.error("[%s] search failed: %r\n%s", rid, e, traceback.format_exc())
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）")
                )
                continue

            if not results:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=NORESULT_MSG)
                )
                continue

            text_msg = to_plain_text(results, query, explain)
            text_msg = _with_reset_hint(text_msg)
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=text_msg[:4900]))

        except Exception as e:
            logger.error("event handling failed: %r\n%s", e, traceback.format_exc())
            # ここで握りつぶして次イベントへ
            continue

    return PlainTextResponse("OK", status_code=200)

# ==============================
# 開発用 API（ALLOW_DEV=1 のときだけ）
# ==============================
if ALLOW_DEV:
    @app.post("/dev/run")
    async def dev_run(payload: dict = Body(...)):
        """
        本文(text)を投げると、抽出→Clarify 判定（まず _needs_choice）→
        - Clarify あり: clarify を返す
        - なし: そのまま検索→整形テキストを返す

        debug=true を渡すと内部状態（query など）を debug に入れて返す。
        """
        rid = _rid()
        try:
            text = (payload.get("text") or "").strip()
            debug = bool(payload.get("debug"))
            if not text:
                return {"status": "error", "message": "text を入れてください"}

            # 遅延インポート
            from nlp_extract import extract_query
            from disambiguator import detect
            from search_core import run_query_system
            from postprocess import reorder_and_pair
            from formatters import to_plain_text

            query, explain = extract_query(text)

            # _needs_choice → Clarify
            clarify = _clarify_from_needs_choice(query)
            clarifies_detect = []
            if not clarify:
                try:
                    clarifies_detect = detect(text) or []
                except Exception:
                    clarifies_detect = []
                if clarifies_detect:
                    clarify = clarifies_detect[0]

            if clarify:
                res = {
                    "status": "clarify",
                    "question": clarify.get("question"),
                    "column": clarify.get("column"),
                    "choices": clarify.get("choices", []),
                    "hint": "番号やラベルを chosen に入れて /dev/choose へPOSTしてください。",
                    "text": text,
                }
                if debug:
                    import nlp_extract as _ne
                    import disambiguator as _da
                    res["debug"] = {
                        "text": text.encode("utf-8", "replace").decode("utf-8", "replace"),
                        "explain": explain,
                        "env": {"FORCE_SUBSTRATE_FALLBACK": FORCE_SUBSTRATE_FALLBACK},
                        "mods": {"nlp_extract_file": getattr(_ne, "__file__", None),
                                 "disambiguator_file": getattr(_da, "__file__", None)},
                        "query": query,
                        "needs_choice": (query.get("_needs_choice") or {}).get("下地の状況"),
                        "clarify_from_needs": clarify if clarify and clarify.get("column") == "下地の状況" else None,
                        "clarifies_detect": clarifies_detect,
                        "clarify_final": clarify,
                    }
                return res

            # Clarify なし → 検索
            results = run_query_system(query)
            results = reorder_and_pair(results, text, query)
            rendered = to_plain_text(results, query, "(dev)")
            if not results:
                rendered = NORESULT_MSG

            res = {"status": "ok", "result_text": _with_reset_hint(rendered), "query": query}
            if debug:
                res["debug"] = {
                    "text": text.encode("utf-8", "replace").decode("utf-8", "replace"),
                    "explain": explain,
                    "env": {"FORCE_SUBSTRATE_FALLBACK": FORCE_SUBSTRATE_FALLBACK},
                    "mods": {},
                    "query": query,
                    "needs_choice": (query.get("_needs_choice") or {}).get("下地の状況"),
                    "clarify_from_needs": None,
                    "clarifies_detect": [],
                    "clarify_final": None,
                    "fb_used": False,
                }
            return res

        except Exception as e:
            logger.error("[%s] dev_run failed: %r\n%s", rid, e, traceback.format_exc())
            return {"status": "error", "message": str(e), "error_id": rid, "trace": traceback.format_exc()}

    @app.post("/dev/choose")
    async def dev_choose(payload: dict = Body(...)):
        """
        /dev/run で clarify が出たときの 2段目。
        { "text": "...", "chosen": ["2"] } でも、
        { "text": "...", "chosen_text": "水性のほう" } でもOK。
        """
        rid = _rid()
        try:
            text = _norm((payload.get("text") or "").strip())
            chosen = payload.get("chosen") or []
            chosen_text = payload.get("chosen_text") or ""
            debug_flag = bool(payload.get("debug"))

            if not text:
                return {"status": "error", "message": "text を入れてください", "error_id": rid}

            from nlp_extract import extract_query
            from disambiguator import detect, apply_choice_to_query
            from search_core import run_query_system
            from postprocess import reorder_and_pair
            from formatters import to_plain_text

            query, _ = extract_query(text)

            # Clarify 再構築（まず _needs_choice）
            c = _clarify_from_needs_choice(query)
            if not c:
                try:
                    detected = detect(text) or []
                except Exception:
                    detected = []
                if detected:
                    c = detected[0]

            if not c:
                return {"status": "error", "message": "clarify は不要でした（/dev/run を先に）", "error_id": rid}

            chs = c.get("choices", []) or []
            parsed_chosen_labels: List[str] = []

            # 自然文優先
            if chosen_text:
                parsed_chosen_labels = _parse_chosen_text(chosen_text, chs)

            # 明示配列（["1","3"] or ["厚膜塗料（〜）"]）も許容
            if not parsed_chosen_labels and chosen:
                id2label = {str(x.get("id", "")).strip(): str(x.get("label", "")).strip() for x in chs}
                labels_set = {str(x.get("label", "")).strip() for x in chs}
                tmp: List[str] = []
                for it in chosen:
                    s = str(it).strip()
                    if s in id2label and id2label[s]:
                        tmp.append(id2label[s])
                    elif s in labels_set:
                        tmp.append(s)
                parsed_chosen_labels = tmp

            # unknown の場合はおすすめ（先頭）に寄せる
            if not parsed_chosen_labels and chosen_text and _norm(chosen_text) in {"unknown", "わからない", "任せる"}:
                if chs:
                    parsed_chosen_labels = [str(chs[0].get("label", "")).strip()]

            # all の場合は全件
            if parsed_chosen_labels == ["all"]:
                parsed_chosen_labels = [str(x.get("label", "")).strip() for x in chs if x.get("label")]

            if not parsed_chosen_labels:
                dbg = {"choices": chs, "chosen": chosen, "chosen_text": chosen_text}
                return {
                    "status": "error",
                    "message": "選択肢を解釈できませんでした。番号（例: 2）かラベル、または『水性のほう』のように書いてください。",
                    "error_id": rid,
                    "debug": dbg if debug_flag else None,
                }

            # 反映 → 検索
            query_after = apply_choice_to_query(query, parsed_chosen_labels, c)
            results = run_query_system(query_after)
            results = reorder_and_pair(results, text, query_after)
            rendered = to_plain_text(results, query_after, "(dev clarified)")
            if not results:
                rendered = NORESULT_MSG

            resp = {"status": "ok", "result_text": _with_reset_hint(rendered), "query": query_after}
            if debug_flag:
                resp["debug"] = {
                    "parsed_chosen": parsed_chosen_labels,
                    "clarify_question": c.get("question"),
                    "choices_labels": [x.get("label") for x in chs],
                }
            return resp

        except Exception as e:
            logger.error("[%s] dev_choose failed: %r\n%s", rid, e, traceback.format_exc())
            return {"status": "error", "message": str(e), "error_id": rid, "trace": traceback.format_exc()}
