# app.py
from __future__ import annotations

import os
import uuid
import logging
import traceback
import re
import unicodedata
from typing import Dict, Any, Optional, List, Tuple
from collections import Counter

from fastapi import FastAPI, Request, Body
from fastapi.responses import PlainTextResponse, Response
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
    MessageAction, QuickReply, QuickReplyButton
)

# ==============================
# 基本セットアップ
# ==============================
app = FastAPI()
logger = logging.getLogger("app")

def _rid() -> str:
    return uuid.uuid4().hex[:8]

def _norm(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")

# Clarify 待ち
_PENDING: Dict[str, Dict[str, Any]] = {}
# フロー状態（連続絞り込みなど）
_STATE: Dict[str, Dict[str, Any]] = {}  # { user_id: {"mode": "...", "base_query": dict, "raw_first": str} }

def set_session_state(user_id: str, mode: str, **kw):
    _STATE[user_id] = {"mode": mode, **kw}

def get_session_state(user_id: str) -> Optional[Dict[str, Any]]:
    return _STATE.get(user_id)

def reset_session_state(user_id: str):
    _STATE.pop(user_id, None)

ALLOW_DEV = os.environ.get("ALLOW_DEV", "1") == "1"  # 本番は 0 推奨
FORCE_SUBSTRATE_FALLBACK = os.environ.get("FORCE_SUBSTRATE_FALLBACK", "0") == "1"

# ==============================
# 文字化け対策
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
# メッセージ共通部品
# ==============================
def tail_reset_hint(msg: str) -> str:
    return (
        (msg or "")
        + "\n\n―――\n新しい検索を行う場合は「0」「０」または『リセット』、"
          "終了は「1」「１」または『終わり』『終了』と入力してください。"
    )

def qr_reset_and_exit() -> QuickReply:
    return QuickReply(items=[
        QuickReplyButton(action=MessageAction(label="新規検索（0）", text="0")),
        QuickReplyButton(action=MessageAction(label="終了（1）", text="1")),
    ])

def qr_refine_or_rank() -> QuickReply:
    return QuickReply(items=[
        QuickReplyButton(action=MessageAction(label="評価順 上位5を表示", text="上位5")),
        QuickReplyButton(action=MessageAction(label="他の条件で絞り込む", text="絞り込み")),
        QuickReplyButton(action=MessageAction(label="全件を見る", text="全件")),
    ])

# ==============================
# Clarify: _needs_choice → Clarify
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
    synonyms = {"防塵塗料":{"防塵"}, "厚膜塗料":{"厚膜"}, "塗り重ね":{"重ね","重ね塗り"}, "水性":{"水性"}, "硬質":{"硬質"}}
    score = 0
    for kw in keywords:
        if kw in f: score += 5
        for alt in synonyms.get(kw, set()):
            if alt in f: score += 3
    if ("水性" in f) and (("水性" in inside) or ("水性" in base)): score += 15
    if ("硬質" in f) and (("硬質" in inside) or ("硬質" in base)): score += 8
    return score

def _parse_chosen_text(chosen_text: str, choices: List[Dict[str, str]]) -> List[str]:
    if not chosen_text: return []
    t = _norm(chosen_text).lower().strip()
    if t in {"all","全部","すべて","全て"}: return [str(c.get("label","")) for c in choices if c.get("label")]
    if t in {"unknown","わからない","任せる"}: return []
    t2 = t.replace("，", ",")
    parts = [p.strip() for p in re.split(r"[,\s]+", t2) if p.strip()]
    id2label = {str(c.get("id","")).strip(): str(c.get("label","")).strip() for c in choices}
    labels = [str(c.get("label","")).strip() for c in choices]
    picked: List[str] = []
    for p in parts:
        if p in id2label and id2label[p]:
            picked.append(id2label[p])
    if picked: return picked
    for p in parts:
        for lab in labels:
            if p and (p == _norm(lab)):
                return [lab]
    best, best_score = None, -1
    for lab in labels:
        sc = _score_label_by_keywords(chosen_text, lab)
        if sc > best_score: best_score, best = sc, lab
    return [best] if best else []

# ==============================
# 絞り込みヘルパ：ファセット抽出＆QR生成
# ==============================
_FACET_COLUMNS = [
    "作業名", "機械カテゴリー", "ライナックス機種名", "使用カッター名", "工程数", "作業効率評価", "下地の状況"
]

def _split_cell_values(cell: str) -> List[str]:
    parts = [p.strip() for p in re.split(r"[,\s、]+", str(cell or "")) if p.strip()]
    return parts

def build_facet_suggestions(rows: List[Dict[str, str]], base_query: Dict[str, Any], limit: int = 8
) -> Tuple[str, QuickReply]:
    """
    rows から列別の出現頻度を集計し、未指定の列を優先に候補を作る。
    戻り値: (テキスト説明, QuickReply)
    """
    # 現在の選択をメモ
    chosen = {col: tuple(base_query.get(col, []) or []) for col in _FACET_COLUMNS}

    # 集計
    per_col_counts: Dict[str, Counter] = {}
    for col in _FACET_COLUMNS:
        cnt = Counter()
        for r in rows:
            for v in _split_cell_values(r.get(col, "")):
                cnt[v] += 1
        per_col_counts[col] = cnt

    # 候補を詰める（既に選択されている値は除外）
    suggestions: List[Tuple[str, str, int]] = []  # (col, val, count)
    # 未指定列を優先→指定済み列（より絞れる）
    cols_order = [c for c in _FACET_COLUMNS if not chosen.get(c)] + [c for c in _FACET_COLUMNS if chosen.get(c)]
    for col in cols_order:
        if len(suggestions) >= limit:
            break
        for val, cnt in per_col_counts[col].most_common():
            if chosen.get(col) and val in chosen[col]:
                continue
            suggestions.append((col, val, cnt))
            if len(suggestions) >= limit:
                break

    # 説明テキスト
    lines = []
    lines.append("検索結果が多いため、次の条件で絞り込めます。押すだけで適用されます。")
    if any(base_query.get(c) for c in _FACET_COLUMNS):
        active = []
        for c in _FACET_COLUMNS:
            if base_query.get(c):
                active.append(f"{c}={ ' / '.join(base_query[c]) }")
        if active:
            lines.append("【現在の条件】" + "、".join(active))
    if not suggestions:
        lines.append("（提示できる追加候補はありません。自由入力で追加条件をどうぞ）")
    else:
        lines.append("【候補の例】")
        for col, val, cnt in suggestions[:limit]:
            lines.append(f"・{col}={val}（{cnt}件）")

    # QR
    items = []
    for col, val, cnt in suggestions[:limit]:
        # extract_query が自然文も理解する前提で、「{col}は{val}」の形にする
        items.append(QuickReplyButton(action=MessageAction(
            label=f"{col}={val}（{cnt}）", text=f"{col}は{val}"
        )))
    # 汎用操作
    items += [
        QuickReplyButton(action=MessageAction(label="評価順 上位5を表示", text="上位5")),
        QuickReplyButton(action=MessageAction(label="全件を見る", text="全件")),
        QuickReplyButton(action=MessageAction(label="新規検索（0）", text="0")),
        QuickReplyButton(action=MessageAction(label="終了（1）", text="1")),
    ]
    return "\n".join(lines), QuickReply(items=items[:13])  # LINE 制限ガード

def render_active_filters(q: Dict[str, Any]) -> str:
    parts = []
    for c in _FACET_COLUMNS:
        if q.get(c):
            parts.append(f"{c}={ ' / '.join(q[c]) }")
    return "、".join(parts) if parts else "（未指定）"

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
        from search_core import run_query, run_query_system, sort_by_eval
        from formatters import to_plain_text
    except Exception as e:
        logger.error("delayed import failed: %r\n%s", e, traceback.format_exc())
        return PlainTextResponse("OK", status_code=200)

    # --- マージヘルパ（初回条件を保持して追加条件を乗せる） ---
    def merge_structured_query(base: Dict[str, Any], delta: Dict[str, Any]) -> Dict[str, Any]:
        if not delta:
            return base
        merged = dict(base)
        # リスト列は「指定があれば置き換え」＝明示入力を優先
        for col in _FACET_COLUMNS:
            if delta.get(col):
                merged[col] = list(delta[col])
        # 深さ系（あれば上書き）
        for k in ("depth_value", "depth_range", "処理する深さ・厚さ"):
            if delta.get(k) not in (None, [], ""):
                merged[k] = delta[k]
        return merged

    for event in events:
        try:
            if not (isinstance(event, MessageEvent) and isinstance(event.message, TextMessage)):
                continue

            # 送信者ID
            src = getattr(event, "source", None)
            user_id = None
            if src:
                user_id = getattr(src, "user_id", None) or getattr(src, "group_id", None) or getattr(src, "room_id", None)
            user_id = user_id or "unknown"

            user_text = (event.message.text or "").strip()

            # === グローバルコマンド ===
            if user_text in ("0","０","ゼロ","ﾘｾｯﾄ","リセット"):
                _PENDING.pop(user_id, None)
                reset_session_state(user_id)
                if line_bot_api:
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="新しい検索を始めます。条件を入力してください。"))
                continue
            if user_text in ("1","１","終わり","終了"):
                _PENDING.pop(user_id, None)
                reset_session_state(user_id)
                if line_bot_api:
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="終了しました。またどうぞ！"))
                continue

            # === need_refine / refining の分岐待ち ===
            flow = get_session_state(user_id)
            if flow and flow.get("mode") in {"await_refine_choice", "refining"}:
                base_query = flow.get("base_query") or flow.get("query") or {}
                choice = _norm(user_text)

                # 上位5／全件
                if choice in {"上位5","上位５","5","５","評価順","評価順上位5","評価順の上位5件"}:
                    all_hits = run_query_system(base_query)
                    top5 = sort_by_eval(all_hits)[:5]
                    txt = to_plain_text(top5, base_query, "（評価順 上位5）")
                    # 次の絞り込み候補も出す（連続絞り込み可能）
                    sugg_text, qr = build_facet_suggestions(all_hits, base_query)
                    reply = tail_reset_hint(txt + "\n\n" + sugg_text)
                    if line_bot_api:
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply[:4900], quick_reply=qr))
                    # refining を維持
                    set_session_state(user_id, "refining", base_query=base_query, raw_first=flow.get("raw_first"))
                    continue

                if choice in {"全件","全部","全件表示","全部表示","すべて表示","すべて"}:
                    all_hits = run_query_system(base_query)
                    txt = to_plain_text(all_hits, base_query, "（全件）")
                    sugg_text, qr = build_facet_suggestions(all_hits, base_query)
                    reply = tail_reset_hint(txt + "\n\n" + sugg_text)
                    if line_bot_api:
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply[:4900], quick_reply=qr))
                    set_session_state(user_id, "refining", base_query=base_query, raw_first=flow.get("raw_first"))
                    continue

                # 「絞り込み」→ 追加条件の入力を促すだけ（候補QRはすでに出ている想定）
                if choice in {"絞り込み","絞込","絞る"}:
                    msg = "追加の条件を入力してください。（例：機械カテゴリーはグラインダー、作業効率は◎ など）"
                    all_hits = run_query_system(base_query)
                    sugg_text, qr = build_facet_suggestions(all_hits, base_query)
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text=tail_reset_hint(msg + "\n\n" + sugg_text), quick_reply=qr)
                        )
                    set_session_state(user_id, "refining", base_query=base_query, raw_first=flow.get("raw_first"))
                    continue

                # それ以外の入力は「追加条件」として解釈 → base にマージ
                try:
                    delta_q, _ = extract_query(user_text)
                except Exception:
                    delta_q = {}

                merged_q = merge_structured_query(base_query, delta_q)
                outcome = run_query(merged_q)

                if outcome.status == "invalid_conditions":
                    msg = "検索条件が認識されませんでした。他の入力をお願いします。"
                    if line_bot_api:
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=tail_reset_hint(msg)))
                    # 状態は維持
                    set_session_state(user_id, "refining", base_query=base_query, raw_first=flow.get("raw_first"))
                    continue

                if outcome.status == "no_results":
                    msg = "該当なしでした。条件を変えるかリセットしてください。"
                    all_hits = run_query_system(base_query)
                    sugg_text, qr = build_facet_suggestions(all_hits, base_query)
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text=tail_reset_hint(msg + "\n\n直前の条件: " + render_active_filters(merged_q) + "\n\n" + sugg_text),
                                            quick_reply=qr)
                        )
                    # 直前の条件は採用せず（base 維持）
                    set_session_state(user_id, "refining", base_query=base_query, raw_first=flow.get("raw_first"))
                    continue

                if outcome.status == "range_out":
                    qrp = qr_reset_and_exit()
                    if outcome.total_hits and outcome.suggest_depth is not None:
                        qrp.items.insert(0, QuickReplyButton(
                            action=MessageAction(label=f"{outcome.suggest_depth:.1f}mmで再検索", text=f"{outcome.suggest_depth:.1f}mm")
                        ))
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text=tail_reset_hint(outcome.message or "範囲外です。"), quick_reply=qrp)
                        )
                    # base は維持
                    set_session_state(user_id, "refining", base_query=base_query, raw_first=flow.get("raw_first"))
                    continue

                if outcome.status == "need_refine":
                    # マージ結果を新たな base に更新して、候補提示ループへ
                    all_hits = run_query_system(merged_q)
                    sugg_text, qr = build_facet_suggestions(all_hits, merged_q)
                    msg = outcome.message or f"検索結果数が多いです（{outcome.total_hits}件）。"
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text=tail_reset_hint(msg + "\n\n" + sugg_text), quick_reply=qr)
                        )
                    set_session_state(user_id, "refining", base_query=merged_q, raw_first=flow.get("raw_first"))
                    continue

                # OK（10件未満）でも refining は続けられるように候補を提示
                txt = to_plain_text(outcome.singles or [], merged_q, "(現在の条件で表示)")
                all_hits = run_query_system(merged_q)
                sugg_text, qr = build_facet_suggestions(all_hits, merged_q)
                reply = tail_reset_hint(txt + "\n\n" + sugg_text)
                if line_bot_api:
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply[:4900], quick_reply=qr))
                set_session_state(user_id, "refining", base_query=merged_q, raw_first=flow.get("raw_first"))
                continue

            # === Clarify の回答待ち ===
            if user_id in _PENDING:
                rid = _rid()
                try:
                    pending = _PENDING.pop(user_id)
                    clarify = pending["clarify"]
                    raw_lower = _norm(user_text).lower().strip()
                    if raw_lower in {"all","すべて","全部","全て"}:
                        chosen = ["all"]
                    elif raw_lower in {"unknown","わからない","任せる"}:
                        chosen = ["unknown"]
                    else:
                        raw_norm = raw_lower.replace("，", ",")
                        chosen = [x.strip() for x in re.split(r"[,\s]+", raw_norm) if x.strip()]
                    query_after = apply_choice_to_query(pending["query"], chosen, clarify)

                    # Clarify 後は通常フロー → 以降で need_refine になれば refining へ
                    from search_core import run_query as _rq
                    outcome = _rq(query_after)

                    if outcome.status == "invalid_conditions":
                        txt = "検索条件が認識されませんでした。他の入力をお願いします。"
                        if line_bot_api:
                            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=tail_reset_hint(txt)))
                        continue

                    if outcome.status == "no_results":
                        txt = "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。"
                        if line_bot_api:
                            line_bot_api.reply_message(
                                event.reply_token,
                                TextSendMessage(text=tail_reset_hint(txt), quick_reply=qr_reset_and_exit())
                            )
                        continue

                    if outcome.status == "range_out":
                        qrp = qr_reset_and_exit()
                        if outcome.total_hits and outcome.suggest_depth is not None:
                            qrp.items.insert(0, QuickReplyButton(
                                action=MessageAction(label=f"{outcome.suggest_depth:.1f}mmで再検索", text=f"{outcome.suggest_depth:.1f}mm")
                            ))
                        if line_bot_api:
                            line_bot_api.reply_message(
                                event.reply_token,
                                TextSendMessage(text=tail_reset_hint(outcome.message or "範囲外です。"), quick_reply=qrp)
                            )
                        continue

                    if outcome.status == "need_refine":
                        # Clarify 後すぐ refining へ
                        all_hits = run_query_system(query_after)
                        sugg_text, qr = build_facet_suggestions(all_hits, query_after)
                        msg = outcome.message or f"検索結果数が多いです（{outcome.total_hits}件）。"
                        if line_bot_api:
                            line_bot_api.reply_message(
                                event.reply_token,
                                TextSendMessage(text=tail_reset_hint(msg + "\n\n" + sugg_text), quick_reply=qr)
                            )
                        set_session_state(user_id, "refining", base_query=query_after, raw_first=pending["raw"])
                        continue

                    # OK
                    txt = to_plain_text(outcome.singles or [], query_after, "(clarified)")
                    # 続けて絞り込みできるよう候補も出す
                    all_hits = run_query_system(query_after)
                    sugg_text, qr = build_facet_suggestions(all_hits, query_after)
                    reply = tail_reset_hint(txt + "\n\n" + sugg_text)
                    if line_bot_api:
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply[:4900], quick_reply=qr))
                    set_session_state(user_id, "refining", base_query=query_after, raw_first=pending["raw"])
                    continue

                except Exception as e:
                    text_for_log = pending["raw"] if "pending" in locals() and isinstance(pending, dict) else None
                    logger.error("[%s] clarify handling failed: %r\ntext=%r\nclarify=%r\ntrace=\n%s",
                                 rid, e, text_for_log, locals().get("clarify"), traceback.format_exc())
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text=f"選択の処理でエラーが発生しました。最初から入力し直してください。（Error ID: {rid}）")
                        )
                    continue

            # === 通常フロー（抽出→Clarify判定→検索） ===
            try:
                structured_query, explain = extract_query(user_text)
            except Exception as e:
                rid = _rid()
                logger.error("[/%s] extract_query failed: %r\ntext=%r\ntrace=\n%s",
                             "callback", e, user_text, traceback.format_exc())
                if line_bot_api:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）")
                    )
                continue

            # Clarify 判定
            clarifies = []
            c_from_needs = _clarify_from_needs_choice(structured_query)
            if c_from_needs:
                clarifies = [c_from_needs]
            else:
                try:
                    clarifies = detect(user_text) or []
                except Exception:
                    clarifies = []

            if clarifies:
                c = clarifies[0]
                _PENDING[user_id] = {"clarify": c, "query": structured_query, "raw": user_text}
                lines = []
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
                if line_bot_api:
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n".join(lines)))
                continue

            # 検索実行
            outcome = run_query(structured_query)

            if outcome.status == "invalid_conditions":
                txt = "検索条件が認識されませんでした。他の入力をお願いします。"
                if line_bot_api:
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=tail_reset_hint(txt)))
                continue

            if outcome.status == "no_results":
                txt = "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。"
                if line_bot_api:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=tail_reset_hint(txt), quick_reply=qr_reset_and_exit())
                    )
                continue

            if outcome.status == "range_out":
                qrp = qr_reset_and_exit()
                if outcome.total_hits and outcome.suggest_depth is not None:
                    qrp.items.insert(0, QuickReplyButton(
                        action=MessageAction(label=f"{outcome.suggest_depth:.1f}mmで再検索", text=f"{outcome.suggest_depth:.1f}mm")
                    ))
                if line_bot_api:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=tail_reset_hint(outcome.message or "範囲外です。"), quick_reply=qrp)
                    )
                continue

            if outcome.status == "need_refine":
                # ここから連続絞り込みモードへ
                all_hits = run_query_system(structured_query)
                sugg_text, qr = build_facet_suggestions(all_hits, structured_query)
                msg = outcome.message or f"検索結果数が多いです（{outcome.total_hits}件）。"
                if line_bot_api:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=tail_reset_hint(msg + "\n\n" + sugg_text), quick_reply=qr)
                    )
                set_session_state(user_id, "refining", base_query=structured_query, raw_first=user_text)
                continue

            # OK（初回検索で10件未満）
            txt = to_plain_text(outcome.singles or [], structured_query, explain)
            # OK でも連続絞り込みを許可：候補を添付
            all_hits = run_query_system(structured_query)
            sugg_text, qr = build_facet_suggestions(all_hits, structured_query)
            reply = tail_reset_hint(txt + "\n\n" + sugg_text)
            if line_bot_api:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=reply[:4900], quick_reply=qr)
                )
            set_session_state(user_id, "refining", base_query=structured_query, raw_first=user_text)

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
        rid = _rid()
        try:
            text = (payload.get("text") or "").strip()
            debug = bool(payload.get("debug"))
            if not text:
                return {"status": "error", "message": "text を入れてください"}

            from nlp_extract import extract_query
            from disambiguator import detect
            from search_core import run_query, run_query_system
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
                    import nlp_extract as _ne, disambiguator as _da
                    res["debug"] = {
                        "text": text.encode("utf-8","replace").decode("utf-8","replace"),
                        "explain": explain,
                        "env": {"FORCE_SUBSTRATE_FALLBACK": FORCE_SUBSTRATE_FALLBACK},
                        "mods": {"nlp_extract_file": getattr(_ne,"__file__",None),
                                 "disambiguator_file": getattr(_da,"__file__",None)},
                        "query": query,
                        "needs_choice": (query.get("_needs_choice") or {}).get("下地の状況"),
                        "clarify_from_needs": clarify if clarify and clarify.get("column") == "下地の状況" else None,
                        "clarifies_detect": clarifies_detect,
                        "clarify_final": clarify,
                    }
                return res

            outcome = run_query(query)
            if outcome.status == "ok":
                rendered = to_plain_text(outcome.singles or [], query, "(dev)")
            else:
                rendered = outcome.message or outcome.status

            res = {"status": outcome.status, "result_text": rendered, "query": query}
            if debug: res["debug"] = {"text": text, "explain": explain, "query": query}
            return res

        except Exception as e:
            rid = _rid()
            logger.error("[%s] dev_run failed: %r\n%s", rid, e, traceback.format_exc())
            return {"status": "error", "message": str(e), "error_id": rid, "trace": traceback.format_exc()}

    @app.post("/dev/choose")
    async def dev_choose(payload: dict = Body(...)):
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
            parsed: List[str] = []

            if chosen_text:
                parsed = _parse_chosen_text(chosen_text, chs)
            if not parsed and chosen:
                id2label = {str(x.get("id","")).strip(): str(x.get("label","")).strip() for x in chs}
                labels_set = {str(x.get("label","")).strip() for x in chs}
                tmp: List[str] = []
                for it in chosen:
                    s = str(it).strip()
                    if s in id2label and id2label[s]:
                        tmp.append(id2label[s])
                    elif s in labels_set:
                        tmp.append(s)
                parsed = tmp
            if not parsed and chosen_text and _norm(chosen_text) in {"unknown","わからない","任せる"}:
                if chs:
                    parsed = [str(chs[0].get("label","")).strip()]
            if parsed == ["all"]:
                parsed = [str(x.get("label","")).strip() for x in chs if x.get("label")]
            if not parsed:
                dbg = {"choices": chs, "chosen": chosen, "chosen_text": chosen_text}
                return {"status": "error", "message": "選択肢を解釈できませんでした。", "error_id": rid, "debug": dbg if debug_flag else None}

            query_after = apply_choice_to_query(query, parsed, c)
            outcome = run_query(query_after)
            rendered = to_plain_text(outcome.singles or [], query_after, "(dev clarified)") if outcome.status=="ok" else (outcome.message or outcome.status)

            resp = {"status": outcome.status, "result_text": rendered, "query": query_after}
            if debug_flag:
                resp["debug"] = {"parsed_chosen": parsed, "clarify_question": c.get("question"),
                                 "choices_labels": [x.get("label") for x in chs]}
            return resp

        except Exception as e:
            logger.error("[%s] dev_choose failed: %r\n%s", rid, e, traceback.format_exc())
            return {"status": "error", "message": str(e), "error_id": rid, "trace": traceback.format_exc()}
