# app.py
from __future__ import annotations

import os
import uuid
import logging
import traceback
import re
import unicodedata
from copy import deepcopy
from typing import Dict, Any, Optional, List, Tuple

from fastapi import FastAPI, Request, Body
from fastapi.responses import PlainTextResponse, Response

from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
    QuickReply, QuickReplyButton, MessageAction
)

# ==============================
# 基本セットアップ
# ==============================
app = FastAPI()
logger = logging.getLogger("app")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

def _rid() -> str:
    return uuid.uuid4().hex[:8]

def _norm(s: str) -> str:
    """全角/半角・濁点結合などを統一して比較しやすくする"""
    return unicodedata.normalize("NFKC", s or "")

# セッション状態
_PENDING: Dict[str, Dict[str, Any]] = {}   # Clarify待ち
_REFINE:  Dict[str, Dict[str, Any]] = {}   # 絞り込みセッション（継続）

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
# 共通ユーティリティ（返信／クイックリプライ）
# ==============================
RESET_HINT = "新しい検索を行う場合はゼロ、０、またはリセット指示をお願いします。"

def _tail_hint(text: str) -> str:
    text = (text or "").rstrip()
    return f"{text}\n\n{RESET_HINT}"

def _reply_text(token: str, text: str, quick: Optional[List[Dict[str, Any]]] = None):
    if not line_bot_api:
        return
    qr = None
    if quick:
        items = []
        for q in quick[:13]:  # LINEの上限
            act = q.get("action", {})
            items.append(QuickReplyButton(action=MessageAction(label=act.get("label",""), text=act.get("text",""))))
        qr = QuickReply(items=items)
    line_bot_api.reply_message(token, TextSendMessage(text=text[:4900], quick_reply=qr))

def _qr_reset_and_exit() -> List[Dict[str, Any]]:
    return [
        {"action": {"type": "message", "label": "リセット(0)", "text": "0"}},
        {"action": {"type": "message", "label": "終了(1)", "text": "1"}},
    ]

def _get_sender_id(event: Any) -> str:
    src = getattr(event, "source", None)
    sender_id = None
    if src:
        sender_id = getattr(src, "user_id", None) or getattr(src, "group_id", None) or getattr(src, "room_id", None)
    return sender_id or "unknown"

def reset_session(user_id: str):
    _PENDING.pop(user_id, None)
    _REFINE.pop(user_id, None)

# ==============================
# Clarify: nlp_extract の _needs_choice → Clarify 変換
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
# ファセット（候補一覧） / データセットサイズ
# ==============================
_DATASET_SIZE: Optional[int] = None
_FACET_COLS = [
    "下地の状況", "作業名", "機械カテゴリー", "ライナックス機種名",
    "使用カッター名", "工程数", "作業効率評価", "処理する深さ・厚さ"
]

def _dataset_size() -> int:
    """CSV 全行数（初回だけ計算しキャッシュ）"""
    global _DATASET_SIZE
    if _DATASET_SIZE is None:
        from search_core import run_query_system
        _DATASET_SIZE = len(run_query_system({}))  # フィルタ無し＝全件
    return _DATASET_SIZE

def _compute_facets(rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    vals: Dict[str, set] = {c: set() for c in _FACET_COLS}
    for r in rows:
        for c in _FACET_COLS:
            v = str(r.get(c, "") or "")
            # 読点・カンマ区切りを分解
            parts = [p.strip() for p in re.split(r"[、,]", v) if p.strip()]
            if not parts:
                if v:
                    vals[c].add(v)
            else:
                for p in parts:
                    vals[c].add(p)
    return {k: sorted(list(v)) for k, v in vals.items() if v}

def _format_facets_text(facets: Dict[str, List[str]], max_chars: int = 3800) -> str:
    head = "絞り込み可能な候補一覧です。列名: 候補\n例）機械カテゴリー: ハンドグラインダ\n"
    budget = max_chars - len(head)
    chunks = []
    for col, vals in facets.items():
        if not vals:
            continue
        line = f"\n【{col}】\n" + "、".join(vals)
        if len(line) > budget:
            kept = []
            cur = len(f"\n【{col}】\n")
            for v in vals:
                add = len(v) + (0 if not kept else 1)
                if cur + add > budget - 8:
                    break
                kept.append(v)
                cur += add
            rest = max(len(vals) - len(kept), 0)
            if kept:
                line = f"\n【{col}】\n" + "、".join(kept)
                if rest:
                    line += f" …（他{rest}件）"
            else:
                line = f"\n【{col}】\n…"
        chunks.append(line)
        budget -= len(line)
        if budget <= 0:
            break
    return head + "".join(chunks)

def _quick_from_facets(facets: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    qr: List[Dict[str, Any]] = []
    # 各列 2〜3 件だけQRに（上限対策）
    for col, vals in facets.items():
        for v in vals[:3]:
            qr.append({"action": {"type": "message", "label": f"{col}:{v}", "text": f"{col}:{v}"}})
    return qr

# ==============================
# 絞り込みセッション（最優先で処理）
# ==============================
def _start_refine(user_id: str, base_query: Dict[str, Any], facets: Dict[str, List[str]], base_hits: List[Dict[str, Any]]):
    _REFINE[user_id] = {
        "base_query": deepcopy(base_query),
        "terms": {},               # {col: set(values)}
        "facets": facets,          # 現在表示している候補
        "last_hits": base_hits,    # 直近のヒット集合
    }

def _stop_refine(user_id: str):
    _REFINE.pop(user_id, None)

def _apply_terms_to_query(base_q: Dict[str, Any], terms: Dict[str, set]) -> Dict[str, Any]:
    q = deepcopy(base_q)
    for col, vs in terms.items():
        lst = sorted({str(v) for v in vs})
        if col == "処理する深さ・厚さ":
            # 旧キー互換で文字列値をそのまま追加
            q.setdefault(col, [])
            q[col] = sorted(set(q[col]) | set(lst))
        else:
            q[col] = lst
    return q

def _parse_refine_input(text: str, facets: Dict[str, List[str]]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    return (mode, column, value)
      mode: "top5" / "add" / "invalid"
    """
    t = _norm(text).strip()
    if t in {"上位5", "トップ5", "top5"}:
        return ("top5", None, None)

    # "列=値" / "列: 値"
    m = re.match(r"^(.+?)[=:：]\s*(.+)$", t)
    if m:
        col = m.group(1).strip()
        val = m.group(2).strip()
        if col in facets:
            return ("add", col, val)
        # 列名うろ覚え対策（前方一致）
        for c in facets.keys():
            if _norm(c).startswith(_norm(col)):
                return ("add", c, val)
        return ("invalid", None, None)

    # 値だけ来た場合：どの列に属するか単一に決められれば採用
    candidates = []
    for col, vals in facets.items():
        if t in vals:
            candidates.append((col, t))
    if len(candidates) == 1:
        return ("add", candidates[0][0], candidates[0][1])

    return ("invalid", None, None)

def _sort_by_eval(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rank = {"◎": 0, "○": 1, "〇": 1, "△": 2}
    def key(r):
        eng = r.get("工程数","")
        k_eng = 0 if "単一" in eng else 1
        eff = r.get("作業効率評価","")
        k_eff = rank.get(eff, 9)
        return (k_eng, k_eff)
    return sorted(rows, key=key)

# ==============================
# Webhook（最優先で絞り込みセッションを処理）
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

    try:
        events = parser.parse(body_text, signature)
    except InvalidSignatureError:
        return PlainTextResponse("Invalid signature", status_code=400)
    except Exception as e:
        logger.error("parser.parse failed: %r\n%s", e, traceback.format_exc())
        return PlainTextResponse("OK", status_code=200)

    if not events:
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

    for event in events:
        try:
            if not (isinstance(event, MessageEvent) and isinstance(event.message, TextMessage)):
                continue

            user_text = (event.message.text or "").strip()
            user_id = _get_sender_id(event)
            raw_lower = _norm(user_text).lower().strip()

            # ---- グローバルコマンド（どの状態でも有効） ----
            if raw_lower in {"0","０","ゼロ","ﾘｾｯﾄ","リセット"}:
                reset_session(user_id)
                _reply_text(event.reply_token, "新しい検索を始めます。条件を入力してください。")
                continue
            if raw_lower in {"1","１","終わり","終了"}:
                reset_session(user_id)
                _reply_text(event.reply_token, "終了しました。またどうぞ！")
                continue

            # ---- 1) 絞り込みセッションがあれば最優先で処理 ----
            if user_id in _REFINE:
                rf = _REFINE[user_id]
                mode, col, val = _parse_refine_input(user_text, rf["facets"])

                # 無効入力ならガイド再表示
                if mode is None or mode == "invalid":
                    facets_text = _format_facets_text(rf["facets"])
                    qr = _quick_from_facets(rf["facets"])
                    qr.insert(0, {"action":{"type":"message","label":"評価順 上位5","text":"上位5"}})
                    qr += _qr_reset_and_exit()
                    _reply_text(
                        event.reply_token,
                        _tail_hint(f"うまく認識できませんでした。『列名=値』（例：機械カテゴリー=ハンドグラインダ）で入力してください。\n\n{facets_text}"),
                        qr
                    )
                    continue

                # 上位5
                if mode == "top5":
                    top5 = _sort_by_eval(rf["last_hits"])[:5]
                    rendered = to_plain_text(top5, rf["base_query"], "(上位5)")
                    qr = _quick_from_facets(rf["facets"])[:10]
                    qr.insert(0, {"action":{"type":"message","label":"さらに絞り込む","text":"工程数=単一工程"}})
                    qr += _qr_reset_and_exit()
                    _reply_text(event.reply_token, _tail_hint(rendered), qr)
                    continue

                # 追加条件（AND：列ごとに OR）
                if mode == "add" and col:
                    terms = rf["terms"]
                    terms.setdefault(col, set()).add(val)

                    q2 = _apply_terms_to_query(rf["base_query"], terms)
                    hits = run_query_system(q2)
                    rf["last_hits"] = hits

                    # 0件
                    if not hits:
                        _reply_text(
                            event.reply_token,
                            _tail_hint("該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。"),
                            _qr_reset_and_exit()
                        )
                        continue

                    # 少数 → 結果表示（セッションは継続：更に絞れる）
                    if len(hits) < 10:
                        hits2 = reorder_and_pair(hits, user_text, q2)
                        rendered = to_plain_text(hits2, q2, "(refined)")
                        qr = _quick_from_facets(_compute_facets(hits))[:10]
                        qr.insert(0, {"action":{"type":"message","label":"評価順 上位5","text":"上位5"}})
                        qr += _qr_reset_and_exit()
                        _reply_text(event.reply_token, _tail_hint(rendered), qr)
                        continue

                    # まだ多い → 候補更新して続行
                    facets = _compute_facets(hits)
                    rf["facets"] = facets
                    facets_text = _format_facets_text(facets)
                    qr = _quick_from_facets(facets)
                    qr.insert(0, {"action":{"type":"message","label":"評価順 上位5","text":"上位5"}})
                    qr += _qr_reset_and_exit()
                    _reply_text(
                        event.reply_token,
                        _tail_hint(f"まだ件数が多いです（{len(hits)}件）。さらに条件を追加してください。\n\n{facets_text}"),
                        qr
                    )
                    continue

            # ---- 2) Clarify待ち（下地の状況など） ----
            if user_id in _PENDING:
                rid = _rid()
                try:
                    pending = _PENDING.pop(user_id)
                    clarify = pending["clarify"]

                    if raw_lower in {"all", "すべて", "全部", "全て"}:
                        chosen = ["all"]
                    elif raw_lower in {"unknown", "わからない", "任せる"}:
                        chosen = ["unknown"]
                    else:
                        raw_norm = raw_lower.replace("，", ",")
                        chosen = [x.strip() for x in re.split(r"[,\s]+", raw_norm) if x.strip()]

                    query_after = apply_choice_to_query(pending["query"], chosen, clarify)
                    results = run_query_system(query_after)
                    results = reorder_and_pair(results, pending["raw"], query_after)

                    text_msg = to_plain_text(results, query_after, "(clarified)")
                    if not results:
                        text_msg = "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。"

                    _reply_text(event.reply_token, _tail_hint(text_msg), _qr_reset_and_exit())
                    continue

                except Exception as e:
                    text_for_log = pending["raw"] if "pending" in locals() and isinstance(pending, dict) else None
                    logger.error(
                        "[%s] clarify handling failed: %r\ntext=%r\nclarify=%r\ntrace=\n%s",
                        rid, e, text_for_log, locals().get("clarify"), traceback.format_exc()
                    )
                    _reply_text(event.reply_token, f"選択の処理でエラーが発生しました。最初から入力し直してください。（Error ID: {rid}）")
                    continue

            # ---- 3) 新規の自然文入力 → 抽出 ----
            try:
                query, explain = extract_query(user_text)
            except Exception as e:
                rid = _rid()
                logger.error("[/%s] extract_query failed: %r\ntext=%r\ntrace=\n%s",
                             "callback", e, user_text, traceback.format_exc())
                _reply_text(event.reply_token, f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）")
                continue

            # Clarify 判定（_needs_choice 優先 → detect()）
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

                # 質問メッセージ
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
                _reply_text(event.reply_token, "\n".join(lines))
                continue

            # ---- 4) 検索 → 結果分岐（大量ヒット時は絞り込みセッションへ） ----
            rid = _rid()
            try:
                hits = run_query_system(query)
            except Exception as e:
                logger.error("[%s] search failed: %r\ntext=%r\nquery=%r\ntrace=\n%s",
                             rid, e, user_text, query, traceback.format_exc())
                _reply_text(event.reply_token, f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）")
                continue

            # 条件が効いておらず全件ヒット
            if hits and len(hits) == _dataset_size():
                _reply_text(
                    event.reply_token,
                    _tail_hint("検索条件が認識されませんでした。他の入力をお願いします。"),
                    _qr_reset_and_exit()
                )
                continue

            # 0件
            if not hits:
                _reply_text(
                    event.reply_token,
                    _tail_hint("該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。"),
                    _qr_reset_and_exit()
                )
                continue

            # 大量ヒット → 全候補提示して絞り込みセッション開始
            if len(hits) >= 10:
                facets = _compute_facets(hits)
                _start_refine(user_id, query, facets, hits)
                facets_text = _format_facets_text(facets)
                qr = _quick_from_facets(facets)
                qr.insert(0, {"action":{"type":"message","label":"評価順 上位5","text":"上位5"}})
                qr += _qr_reset_and_exit()
                _reply_text(
                    event.reply_token,
                    _tail_hint(f"検索結果が多いです（{len(hits)}件）。以下から追加条件で絞り込めます。\n"
                               f"（例：機械カテゴリー=ハンドグラインダ / 使用カッター名: Pg600 / 上位5）\n\n{facets_text}"),
                    qr
                )
                continue

            # 少数 → そのまま整形して回答
            hits2 = reorder_and_pair(hits, user_text, query)
            text_msg = to_plain_text(hits2, query, explain)
            _reply_text(event.reply_token, _tail_hint(text_msg), _qr_reset_and_exit())

        except Exception as e:
            logger.error("event handling failed: %r\n%s", e, traceback.format_exc())
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
        """
        rid = _rid()
        try:
            text = (payload.get("text") or "").strip()
            debug = bool(payload.get("debug"))
            if not text:
                return {"status": "error", "message": "text を入れてください"}

            from nlp_extract import extract_query
            from disambiguator import detect
            from search_core import run_query_system
            from postprocess import reorder_and_pair
            from formatters import to_plain_text

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

            results = run_query_system(query)
            results = reorder_and_pair(results, text, query)
            rendered = to_plain_text(results, query, "(dev)")
            if not results:
                rendered = "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。"

            res = {"status": "ok", "result_text": rendered, "query": query}
            if debug:
                res["debug"] = {
                    "text": text,
                    "explain": explain,
                    "env": {"FORCE_SUBSTRATE_FALLBACK": FORCE_SUBSTRATE_FALLBACK},
                    "query": query,
                    "needs_choice": (query.get("_needs_choice") or {}).get("下地の状況"),
                }
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
            from search_core import run_query_system
            from postprocess import reorder_and_pair
            from formatters import to_plain_text

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
            results = run_query_system(query_after)
            results = reorder_and_pair(results, text, query_after)
            rendered = to_plain_text(results, query_after, "(dev clarified)")
            if not results:
                rendered = "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。"

            resp = {"status": "ok", "result_text": rendered, "query": query_after}
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
