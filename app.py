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
from linebot.models import (
    MessageEvent, TextMessage,
    TextSendMessage, FlexSendMessage,
    QuickReply, QuickReplyButton, MessageAction
)

# ==============================
# 基本セットアップ
# ==============================
app = FastAPI()
logger = logging.getLogger("app")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

def _rid() -> str:
    return uuid.uuid4().hex[:8]

def _norm(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")

# 揮発セッション
_PENDING: Dict[str, Dict[str, Any]] = {}   # Clarify待ち（従来の仕組みを温存）
_STATE: Dict[str, str] = {}                # user_id -> state
_LAST_HITS: Dict[str, List[Dict[str, Any]]] = {}  # 直前ヒットの生データ（need_refine用）

def get_session_state(uid: str) -> str:
    return _STATE.get(uid, "")

def set_session_state(uid: str, state: str) -> None:
    _STATE[uid] = state

def reset_session_state(uid: str) -> None:
    _STATE.pop(uid, None)

def reset_session(uid: str) -> None:
    _PENDING.pop(uid, None)
    _LAST_HITS.pop(uid, None)
    reset_session_state(uid)

def set_last_hits(uid: str, rows: List[Dict[str, Any]] | None) -> None:
    if rows is not None:
        _LAST_HITS[uid] = rows

def last_hits(uid: str) -> List[Dict[str, Any]]:
    return _LAST_HITS.get(uid, [])

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
# 共通返信ユーティリティ
# ==============================
def _to_quick_reply(qr_spec: List[Dict[str, Any]] | None) -> Optional[QuickReply]:
    """
    formatters.py の Quick Reply仕様（list of {"type":"action","action":{"type":"message","label":...,"text":...}}）
    を LINE SDK の QuickReply に変換
    """
    if not qr_spec:
        return None
    buttons: List[QuickReplyButton] = []
    for item in qr_spec:
        try:
            action = item.get("action", {})
            label = str(action.get("label") or "").strip()
            text = str(action.get("text") or "").strip()
            if label and text:
                buttons.append(QuickReplyButton(action=MessageAction(label=label, text=text)))
        except Exception:
            continue
    return QuickReply(items=buttons) if buttons else None

def reply_text(reply_token: str, text: str, quick_reply: List[Dict[str, Any]] | None = None) -> None:
    if not line_bot_api:
        return
    qr = _to_quick_reply(quick_reply)
    line_bot_api.reply_message(reply_token, TextSendMessage(text=text[:4900], quick_reply=qr))

def reply_flex(reply_token: str, header_text: str, flex_json: Dict[str, Any],
               quick_reply: List[Dict[str, Any]] | None = None) -> None:
    """
    header_text はテキストも一緒に出したいときの先頭メッセージ
    """
    if not line_bot_api:
        return
    msgs = []
    qr = _to_quick_reply(quick_reply)
    if header_text:
        msgs.append(TextSendMessage(text=header_text[:4900]))
    msgs.append(FlexSendMessage(alt_text="検索結果", contents=flex_json))
    # QuickReply は同時に1つのメッセージにしか付けられないため、最後のテキストに付与
    if isinstance(msgs[0], TextSendMessage) and qr:
        msgs[0].quick_reply = qr
    line_bot_api.reply_message(reply_token, msgs)

def _sender_id(event: MessageEvent) -> str:
    src = getattr(event, "source", None)
    if not src:
        return "unknown"
    return getattr(src, "user_id", None) or getattr(src, "group_id", None) or getattr(src, "room_id", None) or "unknown"

# ==============================
# Clarify: nlp_extract の _needs_choice → Clarify オブジェクトへ
# ==============================
def _clarify_from_needs_choice(filters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
    picked: List[str] = []
    for p in parts:
        if p in id2label and id2label[p]:
            picked.append(id2label[p])
    if picked:
        return picked
    for p in parts:
        for lab in labels:
            if p and (p == _norm(lab)):
                return [lab]
    best = None
    best_score = -1
    for lab in labels:
        sc = _score_label_by_keywords(chosen_text, lab)
        if sc > best_score:
            best_score = sc
            best = lab
    return [best] if best else []

# ==============================
# Webhook（LINE）
# ==============================
@app.post("/callback")
async def callback(request: Request):
    if not parser:
        logger.error("LINE credentials not set")
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

    # 遅延インポート（新パイプライン）
    try:
        from nlp_extract import extract_query
        from disambiguator import detect, apply_choice_to_query
        from search_core import run_query, sort_by_eval
        from formatters import (
            build_flex_from_rows,
            qr_reset_and_exit, qr_refine_or_rank,
            tail_reset_hint, msg_invalid_conditions, msg_no_results
        )
        from postprocess import reorder_and_pair  # 既存Clarify経路で利用
    except Exception as e:
        logger.error("delayed import failed: %r\n%s", e, traceback.format_exc())
        return PlainTextResponse("OK", status_code=200)

    for event in events:
        try:
            if not (isinstance(event, MessageEvent) and isinstance(event.message, TextMessage)):
                continue

            user_text = (event.message.text or "").strip()
            user_id = _sender_id(event)

            # 1) グローバルコマンド（どの状態でも有効）
            if user_text in ("0", "０", "ゼロ", "ﾘｾｯﾄ", "リセット"):
                reset_session(user_id)
                reply_text(event.reply_token, "新しい検索を始めます。条件を入力してください。")
                continue
            if user_text in ("1", "１", "終わり", "終了"):
                reset_session(user_id)
                reply_text(event.reply_token, "終了しました。またどうぞ！")
                continue

            # 2) 「上位5／絞り込む／全件」待ちの分岐（run_queryの前）
            state = get_session_state(user_id)
            if state == "await_refine_choice":
                ut = _norm(user_text)
                if ut in ("上位5", "評価順", "評価順(上位5)"):
                    rows = sort_by_eval(last_hits(user_id))[:5]
                    flex = build_flex_from_rows(rows, [])
                    reply_flex(event.reply_token, tail_reset_hint("評価順の上位5件を表示します。"),
                               flex, quick_reply=qr_reset_and_exit())
                    reset_session_state(user_id)
                    continue
                if ut.startswith("絞り込む"):
                    reply_text(event.reply_token, "絞り込み条件を入力してください（例：材質、機種、刃種など）")
                    set_session_state(user_id, "await_more_filters")
                    continue
                if ut.startswith("全件"):
                    rows = sort_by_eval(last_hits(user_id))
                    flex = build_flex_from_rows(rows, [])
                    reply_flex(event.reply_token, tail_reset_hint("全件を表示します。"),
                               flex, quick_reply=qr_reset_and_exit())
                    reset_session_state(user_id)
                    continue
                # 想定外 → 再提示
                reply_text(event.reply_token, "「絞り込む」「上位5」「全件」から選んでください。",
                           quick_reply=qr_refine_or_rank())
                continue

            # 3) まず従来Clarify（_needs_choice / detect）を優先的にハンドリング
            #    ※ ここは既存実装資産を活かす（Clarify後は to_plain_text）
            try:
                query, explain = extract_query(user_text)
            except Exception as e:
                rid = _rid()
                logger.error("[/%s] extract_query failed: %r\ntext=%r\ntrace=\n%s",
                             "callback", e, user_text, traceback.format_exc())
                reply_text(event.reply_token, f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）")
                continue

            clarifies = []
            c_from_needs = _clarify_from_needs_choice(query)
            if c_from_needs:
                clarifies = [c_from_needs]
            else:
                try:
                    clarifies = detect(user_text) or []
                except Exception:
                    clarifies = []

            if clarifies:
                c = clarifies[0]
                _PENDING[user_id] = {"clarify": c, "query": query, "raw": user_text}

                lines: List[str] = []
                qtxt = str(c.get("question") or "条件をもう少し具体化してください。")
                lines.append(qtxt)
                lines.append("")
                lines.append("次から選んで返信してください（複数可）：")
                for ch in c.get("choices", []):
                    cid = str(ch.get("id", "")).strip()
                    label = str(ch.get("label", "")).strip()
                    if cid and label:
                        lines.append(f"  {cid}) {label}")
                lines += [
                    "",
                    "ヒント:",
                    "・番号だけでもOK（例：1,3）",
                    "・ラベルそのものでもOK（例：厚膜塗料（エポキシ））",
                    "・全て = all / わからない = unknown も可",
                ]
                reply_text(event.reply_token, "\n".join(lines))
                continue

            # 4) 新パイプライン：構造化→run_query（UX強化仕様）
            try:
                # run_query は SearchOutcome を返す想定（9.2.4の実装）
                outcome = run_query(query)
            except Exception as e:
                rid = _rid()
                logger.error("[%s] run_query failed: %r\ntext=%r\nquery=%r\ntrace=\n%s",
                             rid, e, user_text, query, traceback.format_exc())
                reply_text(event.reply_token, f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）")
                continue

            if outcome.status == "invalid_conditions":
                reply_text(event.reply_token, msg_invalid_conditions())
                continue

            if outcome.status == "no_results":
                reply_text(event.reply_token, msg_no_results(), quick_reply=qr_reset_and_exit())
                continue

            if outcome.status == "range_out":
                qrs = qr_reset_and_exit()
                # 再検索の提案ボタン（suggest_depth があれば先頭に追加）
                if outcome.total_hits and outcome.suggest_depth is not None:
                    qrs.insert(0, {
                        "type": "action",
                        "action": {"type": "message",
                                   "label": f"{outcome.suggest_depth:.1f}mmで再検索",
                                   "text": f"{outcome.suggest_depth:.1f}mm"}
                    })
                reply_text(event.reply_token, outcome.message or "処理する深さ・厚さが推奨する幅を超えているようです。", quick_reply=qrs)
                continue

            if outcome.status == "need_refine":
                # 上位5／絞り込む／全件 の選択待ちへ
                # raw hits があれば保存、無ければ singles を保存
                raw = getattr(outcome, "raw_hits", None)
                set_last_hits(user_id, raw if raw is not None else (outcome.singles or []))
                set_session_state(user_id, "await_refine_choice")
                reply_text(event.reply_token, outcome.message or "検索結果が多いです。どうしますか？", quick_reply=qr_refine_or_rank())
                continue

            # ok（結果表示）
            flex = build_flex_from_rows(outcome.singles or [], outcome.pairs or [])
            reply_flex(event.reply_token, tail_reset_hint("検索結果を表示します。"), flex, quick_reply=qr_reset_and_exit())

        except Exception as e:
            logger.error("event handling failed: %r\n%s", e, traceback.format_exc())
            # セーフガードとして200を返す
            continue

    return PlainTextResponse("OK", status_code=200)

# ==============================
# 開発用 API（ALLOW_DEV=1 のときだけ）
# ==============================
ALLOW_DEV = os.environ.get("ALLOW_DEV", "1") == "1"  # 本番は 0 推奨
FORCE_SUBSTRATE_FALLBACK = os.environ.get("FORCE_SUBSTRATE_FALLBACK", "0") == "1"

if ALLOW_DEV:
    @app.post("/dev/run")
    async def dev_run(payload: dict = Body(...)):
        """
        本文(text)を投げると、抽出→Clarify 判定（まず _needs_choice）→
        - Clarify あり: clarify を返す
        - なし: 新パイプライン run_query → SearchOutcome を返す（テキスト整形は formatters 側に委譲）
        """
        rid = _rid()
        try:
            text = (payload.get("text") or "").strip()
            debug = bool(payload.get("debug"))
            if not text:
                return {"status": "error", "message": "text を入れてください"}

            from nlp_extract import extract_query
            from disambiguator import detect
            from search_core import run_query
            from formatters import to_plain_text  # ある場合のみ利用（互換目的）

            query, explain = extract_query(text)

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
                        "text": text,
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

            # Clarify なし → 新パイプライン
            outcome = run_query(query)
            # renderテキスト（任意）
            rendered = None
            try:
                rendered = to_plain_text(outcome.singles or [], query, "(dev)")  # 存在しない場合は無視
            except Exception:
                pass

            res = {
                "status": outcome.status,
                "message": outcome.message,
                "total_hits": outcome.total_hits,
                "suggest_depth": outcome.suggest_depth,
                "singles": outcome.singles,
                "pairs": outcome.pairs,
                "render": rendered,
                "query": query,
            }
            if debug:
                res["debug"] = {"query": query, "explain": explain}
            return res

        except Exception as e:
            rid = _rid()
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
            from search_core import run_query
            from formatters import to_plain_text, build_flex_from_rows  # 互換

            query, _ = extract_query(text)

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

            if chosen_text:
                parsed_chosen_labels = _parse_chosen_text(chosen_text, chs)

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

            if not parsed_chosen_labels and chosen_text and _norm(chosen_text) in {"unknown", "わからない", "任せる"}:
                if chs:
                    parsed_chosen_labels = [str(chs[0].get("label", "")).strip()]

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

            query_after = apply_choice_to_query(query, parsed_chosen_labels, c)
            outcome = run_query(query_after)
            rendered = None
            try:
                rendered = to_plain_text(outcome.singles or [], query_after, "(dev clarified)")
            except Exception:
                pass

            resp = {
                "status": outcome.status,
                "message": outcome.message,
                "total_hits": outcome.total_hits,
                "suggest_depth": outcome.suggest_depth,
                "singles": outcome.singles,
                "pairs": outcome.pairs,
                "render": rendered,
                "query": query_after,
            }
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
