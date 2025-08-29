# app.py
from __future__ import annotations

import os
import uuid
import logging
import traceback
import re
import unicodedata
from typing import Dict, Any, Optional, List, Tuple, Set

from fastapi import FastAPI, Request, Body
from fastapi.responses import PlainTextResponse, Response

from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import (
    MessageEvent,
    TextMessage,
    TextSendMessage,
    QuickReply,
    QuickReplyButton,
    MessageAction,
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
    """全角/半角・濁点結合などを統一して比較しやすくする"""
    return unicodedata.normalize("NFKC", s or "")

# 揮発セッション（メモリ）
# user_id -> {
#   "mode": "idle" | "await_clarify" | "await_refine",
#   "base_query": dict,
#   "active_filters": Dict[col,List],
#   "last_facets": Dict[col,List],
#   "clarify": Dict,
#   "refine_stack": [
#       {"rows":[...], "query":{...}, "facets":{...}}
#   ]
# }
_SESS: Dict[str, Dict[str, Any]] = {}

ALLOW_DEV = os.environ.get("ALLOW_DEV", "1") == "1"  # 本番は 0 推奨

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
# QuickReply: ラベル短縮 & 上限対策
# ==============================
ALIAS = {
    "ライナックス機種名": "機種",
    "使用カッター名": "カッター",
    "機械カテゴリー": "機械",
    "作業効率評価": "評価",
    "処理する深さ・厚さ": "深さ",
    "下地の状況": "下地",
    "工程数": "工程",
    "作業名": "作業",
}

def _ellipsize(s: str, limit: int = 20) -> str:
    s = s or ""
    return s if len(s) <= limit else (s[: max(0, limit - 1)] + "…")

def _make_quick(items: List[Tuple[str, str]]) -> Optional[QuickReply]:
    """[(label, text)] -> QuickReply（labelは20文字に丸め、最大13件）"""
    if not items:
        return None
    btns: List[QuickReplyButton] = []
    for label, text in items[:13]:  # LINE 制約
        btns.append(
            QuickReplyButton(
                action=MessageAction(label=_ellipsize(label, 20), text=text)
            )
        )
    return QuickReply(items=btns)

def _qr_reset_and_exit_items() -> List[Tuple[str, str]]:
    return [("0 リセット", "0"), ("1 終了", "1")]

def _qr_exit_only_items() -> List[Tuple[str, str]]:
    return [("1 終了", "1")]

def _qr_from_facets(facets: Dict[str, List[str]], per_col: int = 3) -> List[Tuple[str, str]]:
    """検索結果から得たファセットをQRに（各列最大 per_col 件）"""
    qr: List[Tuple[str, str]] = []
    for col, vals in facets.items():
        alias = ALIAS.get(col, col)
        for v in vals[:per_col]:
            qr.append((_ellipsize(f"{alias}:{v}", 20), f"{col}:{v}"))
    return qr

def _qr_for_clarify(clarify: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Clarify回答用のQR（候補＋全部＋わからない）
    ※ここでは 0/1 を出さない（数値 1/0 の誤解釈で終了/リセットを防ぐ）
    """
    items: List[Tuple[str, str]] = []
    labels = [str(c.get("label","")).strip() for c in (clarify.get("choices") or []) if c.get("label")]
    for lab in labels[:11]:  # 11件 + 2件（全部/わからない）= 13
        items.append((_ellipsize(lab, 20), lab))
    items.append(("全部", "all"))
    items.append(("わからない", "unknown"))
    return items[:13]

def _has_any_condition(query: Dict[str, Any]) -> bool:
    """実質“条件なし（=全件）”かどうかの判別用"""
    if not isinstance(query, dict):
        return True
    keys = {
        "作業名","機械カテゴリー","ライナックス機種名","使用カッター名",
        "作業効率評価","工程数","処理する深さ・厚さ","下地の状況",
        "depth_value","depth_range"
    }
    for k in keys:
        v = query.get(k)
        if v:
            if isinstance(v, (list, tuple, set, dict)):
                if len(v) > 0:
                    return True
            else:
                return True
    return False

def _make_qr_for_refine(user_id: str, facets: Dict[str, List[str]], allow_show_all: bool = False) -> List[Tuple[str, str]]:
    """絞り込み用のQR（←戻る / 全件表示 / 候補 / 0/1）"""
    head: List[Tuple[str, str]] = []
    sess = _SESS.get(user_id) or {}
    if len(sess.get("refine_stack") or []) >= 2:
        head.append(("← 戻る", "戻る"))
    if allow_show_all:
        head.append(("全件表示", "全件表示"))
    mid = _qr_from_facets(facets, per_col=3)
    tail = _qr_reset_and_exit_items()
    room_for_mid = max(0, 13 - len(head) - len(tail))
    return head + mid[:room_for_mid] + tail

def _reply_text(token: str, text: str, quick_items: Optional[List[Tuple[str, str]]] = None):
    if not line_bot_api:
        return
    qr = _make_quick(quick_items) if quick_items else None
    try:
        line_bot_api.reply_message(
            token, TextSendMessage(text=text[:4900], quick_reply=qr)
        )
    except LineBotApiError as e:
        logger.error("LINE reply failed: %r", e)
        try:
            line_bot_api.reply_message(token, TextSendMessage(text=text[:4900]))
        except Exception:
            pass

# ==============================
# Clarify（nlp_extract の _needs_choice → Clarify へ）
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
        "term": term,
    }

def _parse_clarify_answer(text: str, choices: List[Dict[str, Any]]) -> List[str]:
    """Clarify回答（自然文/番号/ラベル/all/unknown）→ ラベル配列"""
    t = _norm(text).strip().lower()
    labels = [str(c.get("label","")).strip() for c in choices if c.get("label")]
    id2label = {str(c.get("id","")).strip(): str(c.get("label","")).strip() for c in choices}

    if not t:
        return []

    if t in {"all", "全部", "全て", "すべて"}:
        return labels[:]
    if t in {"unknown", "わからない", "任せる"}:
        return [labels[0]] if labels else []

    parts = re.split(r"[,\s，、]+", t)
    picked: List[str] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if p in id2label and id2label[p]:
            picked.append(id2label[p])
            continue
        for lab in labels:
            if p == _norm(lab).lower() or p in _norm(lab).lower():
                picked.append(lab)
                break
    dedup = []
    for x in picked:
        if x and x not in dedup:
            dedup.append(x)
    return dedup

# ==============================
# ファセット作成（結果から候補提示）
# ==============================
_FACET_COLS = [
    "作業名",
    "機械カテゴリー",
    "ライナックス機種名",
    "使用カッター名",
    "作業効率評価",
    "工程数",
    "処理する深さ・厚さ",
    "下地の状況",
]

def _build_facets(rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    facets: Dict[str, Set[str]] = {c: set() for c in _FACET_COLS}
    for r in rows or []:
        for c in _FACET_COLS:
            v = (r.get(c) or "").strip()
            if not v:
                continue
            parts = [p.strip() for p in re.split(r"[,\s、]+", v) if p.strip()]
            if parts:
                facets[c].update(parts)
    out: Dict[str, List[str]] = {}
    for c, s in facets.items():
        if s:
            out[c] = sorted(s)
    return out

def _apply_refine(base_query: Dict[str, Any], active_filters: Dict[str, List[str]]) -> Dict[str, Any]:
    q = dict(base_query)
    for col, vals in active_filters.items():
        q[col] = list(vals)
    return q

def _parse_colon_filter(text: str) -> Optional[Tuple[str, str]]:
    t = _norm(text).strip()
    m = re.match(r"^([^:：=＝]+)\s*[:：=＝]\s*(.+)$", t)
    if not m:
        return None
    col = m.group(1).strip()
    val = m.group(2).strip()
    return (col, val) if col and val else None

def _sender_id_from_event(event: MessageEvent) -> str:
    src = getattr(event, "source", None)
    return (getattr(src, "user_id", None)
            or getattr(src, "group_id", None)
            or getattr(src, "room_id", None)
            or "unknown")

def _reset_session(user_id: str):
    if user_id in _SESS:
        _SESS.pop(user_id, None)

# ==============================
# 絞り込み “戻る” 用スナップショット
# ==============================
def _S(uid: str) -> Dict[str, Any]:
    s = _SESS.get(uid)
    if not s:
        s = {"mode": "idle", "base_query": {}, "active_filters": {}, "last_facets": {}, "clarify": None, "refine_stack": []}
        _SESS[uid] = s
    if "refine_stack" not in s:
        s["refine_stack"] = []
    return s

def _push_snapshot(uid: str, rows: List[Dict[str, Any]], query: Dict[str, Any], facets: Dict[str, List[str]]) -> None:
    _S(uid)["refine_stack"].append({"rows": rows, "query": query, "facets": facets})

def _current_snapshot(uid: str) -> Optional[Dict[str, Any]]:
    st = _S(uid)["refine_stack"]
    return st[-1] if st else None

def _undo_snapshot(uid: str) -> Optional[Dict[str, Any]]:
    st = _S(uid)["refine_stack"]
    if len(st) <= 1:
        return None
    st.pop()
    return st[-1]

# ==============================
# “前の結果をくっつけない”描画
# ==============================
def _render_refined_simple(rows: List[Dict[str, Any]], header: Optional[str] = None) -> str:
    cols = [
        "作業名", "機械カテゴリー", "ライナックス機種名", "使用カッター名",
        "処理する深さ・厚さ", "作業効率評価", "工程数", "下地の状況"
    ]
    lines: List[str] = []
    if header:
        lines.append(header)
    if not rows:
        lines.append("（該当なし）")
        return "\n".join(lines)
    for i, r in enumerate(rows, 1):
        lines.append(f"{i}. {r.get('作業名','')}")
        for c in cols[1:]:
            v = r.get(c)
            if v:
                lines.append(f"   - {c}: {v}")
        lines.append("")  # 各候補の間に空行
    return "\n".join(lines).rstrip()

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

    # 遅延インポート
    try:
        from nlp_extract import extract_query
        from disambiguator import detect, apply_choice_to_query
        from search_core import run_query_system
        from postprocess import reorder_and_pair
        from formatters import to_plain_text
    except Exception as e:
        logger.error("delayed import failed: %r\n%s", e, traceback.format_exc())
        return PlainTextResponse("OK", status_code=200)

    for event in events:
        try:
            if not (isinstance(event, MessageEvent) and isinstance(event.message, TextMessage)):
                continue

            user_text_raw = (event.message.text or "")
            user_text = user_text_raw.strip()
            user_id = _sender_id_from_event(event)
            u = _norm(user_text)

            # --- Clarify待ちを最優先で処理（ここでは 0/1 をグローバル扱いしない） ---
            sess0 = _SESS.get(user_id) or {}
            if sess0.get("mode") == "await_clarify":
                clar = sess0.get("clarify") or {}
                base_q = sess0.get("base_query") or {}
                chs = clar.get("choices") or []

                # 明示ワードでのみ終了/リセットを許可（数値 1/0 は Clarify用として扱う）
                if u in {"終了", "終わり"}:
                    _reset_session(user_id)
                    _reply_text(event.reply_token, "終了しました。またどうぞ！")
                    continue
                if u in {"リセット", "ﾘｾｯﾄ"}:
                    _reset_session(user_id)
                    _reply_text(
                        event.reply_token,
                        "新しい検索を始めます。条件を入力してください。",
                        quick_items=_qr_exit_only_items()
                    )
                    continue

                parsed_labels = _parse_clarify_answer(user_text, chs)
                if not parsed_labels:
                    msg = "選択肢を解釈できませんでした。候補から選ぶか、番号/ラベル/『all』で入力してください。"
                    _reply_text(event.reply_token, msg, quick_items=_qr_for_clarify(clar))
                    continue

                # Clarify反映 → 検索
                try:
                    query_after = apply_choice_to_query(base_q, parsed_labels, clar)
                except Exception as e:
                    rid = _rid()
                    logger.error("[%s] apply_choice_to_query failed: %r\nclar=%r\nbase=%r\n", rid, e, clar, base_q)
                    _reply_text(event.reply_token, f"選択の処理でエラーが発生しました。最初から入力し直してください。（Error ID: {rid}）")
                    _reset_session(user_id)
                    continue

                results = run_query_system(query_after)
                results = reorder_and_pair(results, user_text, query_after)

                if not results:
                    _reply_text(
                        event.reply_token,
                        "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。",
                        quick_items=_qr_reset_and_exit_items()
                    )
                    _reset_session(user_id)
                    continue

                facets = _build_facets(results)
                _S(user_id)
                _SESS[user_id]["refine_stack"] = []
                _push_snapshot(user_id, results, query_after, facets)

                allow_show_all = _has_any_condition(query_after)
                if len(results) >= 10:
                    _SESS[user_id]["mode"] = "await_refine"
                    _SESS[user_id]["base_query"] = query_after
                    _SESS[user_id]["active_filters"] = {}
                    _SESS[user_id]["last_facets"] = facets
                    _SESS[user_id]["clarify"] = None

                    qr_items = _make_qr_for_refine(user_id, facets, allow_show_all=allow_show_all)
                    msg = (
                        f"検索結果が多いです（{len(results)}件）。\n"
                        "『列:値』（例：機械:UC-500）で追加指定するか、下の候補から選んでください。"
                    )
                    _reply_text(event.reply_token, msg, quick_items=qr_items)
                    continue

                text_msg = _render_refined_simple(results, header="【検索結果】")
                tail = "\n\n新しい検索を行う場合はゼロ、０、またはリセット指示をお願いします。"
                _reply_text(event.reply_token, (text_msg + tail)[:4900], quick_items=_qr_reset_and_exit_items())

                _SESS[user_id]["mode"] = "await_refine"
                _SESS[user_id]["base_query"] = query_after
                _SESS[user_id]["active_filters"] = {}
                _SESS[user_id]["last_facets"] = facets
                _SESS[user_id]["clarify"] = None
                continue

            # --- グローバルコマンド（Clarify待ち以外で有効） ---
            if u in {"0", "０", "ゼロ", "ﾘｾｯﾄ", "リセット"}:
                _reset_session(user_id)
                _reply_text(
                    event.reply_token,
                    "新しい検索を始めます。条件を入力してください。",
                    quick_items=_qr_exit_only_items()
                )
                continue
            if u in {"1", "１", "終わり", "終了"}:
                _reset_session(user_id)
                _reply_text(event.reply_token, "終了しました。またどうぞ！")
                continue

            # --- Undo（戻る） ---
            if u in {"戻る", "前に戻る", "前の結果", "undo", "back", "戻す", "ひとつ戻る"}:
                snap = _undo_snapshot(user_id)
                if not snap:
                    _reply_text(
                        event.reply_token,
                        "これ以上戻れません。初回の結果です。",
                        quick_items=_qr_reset_and_exit_items()
                    )
                    continue
                msg = _render_refined_simple(snap["rows"], header="【前の結果に戻りました】")
                s = _S(user_id)
                s["mode"] = "await_refine"
                s["base_query"] = snap["query"]
                s["active_filters"] = {}
                s["last_facets"] = snap.get("facets") or _build_facets(snap["rows"])
                _SESS[user_id] = s
                qr_items = _make_qr_for_refine(user_id, s["last_facets"], allow_show_all=_has_any_condition(s["base_query"]))
                _reply_text(event.reply_token, (msg + "\n\n条件を追加して絞り込みできます。")[:4900], quick_items=qr_items)
                continue

            # --- 既に絞り込み待ち（ファセット提示済み）の場合 ---
            sess = _SESS.get(user_id) or {}
            if sess.get("mode") == "await_refine":
                if u in {"全件表示", "全件", "全表示"}:
                    snap = _current_snapshot(user_id)
                    if not snap:
                        _reply_text(event.reply_token, "全件表示できる状態ではありません。", quick_items=_qr_reset_and_exit_items())
                        continue
                    msg = _render_refined_simple(snap["rows"], header="【全件表示（現在の条件）】")
                    qr_items = _make_qr_for_refine(user_id, snap.get("facets") or _build_facets(snap["rows"]),
                                                   allow_show_all=_has_any_condition(sess.get("base_query") or {}))
                    _reply_text(event.reply_token, (msg + "\n（長文は途中で切れる場合があります）")[:4900], quick_items=qr_items)
                    continue

                parsed = _parse_colon_filter(user_text)
                base_query = sess.get("base_query") or {}
                active_filters = sess.get("active_filters") or {}

                if parsed:
                    col, val = parsed
                    col_norm = col
                    for full, ali in ALIAS.items():
                        if col == ali:
                            col_norm = full
                            break

                    vals = active_filters.get(col_norm, [])
                    if val not in vals:
                        vals = vals + [val]
                    active_filters[col_norm] = vals

                    q2 = _apply_refine(base_query, active_filters)
                    results = run_query_system(q2)
                    results = reorder_and_pair(results, user_text, q2)

                    if not results:
                        _reply_text(
                            event.reply_token,
                            "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。",
                            quick_items=_qr_reset_and_exit_items()
                        )
                        _reset_session(user_id)
                        continue

                    facets2 = _build_facets(results)
                    _push_snapshot(user_id, results, q2, facets2)

                    if len(results) >= 10:
                        sess["mode"] = "await_refine"
                        sess["base_query"] = base_query
                        sess["active_filters"] = active_filters
                        sess["last_facets"] = facets2
                        _SESS[user_id] = sess

                        qr_items = _make_qr_for_refine(user_id, facets2, allow_show_all=_has_any_condition(q2))
                        msg = (
                            f"検索結果が多いです（{len(results)}件）。\n"
                            "『列:値』（例：機械:UC-500）で追加指定するか、下の候補から選んでください。"
                        )
                        _reply_text(event.reply_token, msg, quick_items=qr_items)
                        continue

                    text_msg = _render_refined_simple(results, header="【絞り込み結果】")
                    tail = "\n\n新しい検索を行う場合はゼロ、０、またはリセット指示をお願いします。"
                    _reply_text(
                        event.reply_token,
                        (text_msg + tail)[:4900],
                        quick_items=_make_qr_for_refine(user_id, facets2, allow_show_all=_has_any_condition(q2))
                    )
                    sess["mode"] = "await_refine"
                    sess["base_query"] = base_query
                    sess["active_filters"] = active_filters
                    sess["last_facets"] = facets2
                    _SESS[user_id] = sess
                    continue

                facets = sess.get("last_facets") or {}
                qr_items = _make_qr_for_refine(user_id, facets, allow_show_all=_has_any_condition(sess.get("base_query") or {}))
                _reply_text(
                    event.reply_token,
                    "追加条件は『列:値』（例：機械:UC-500）の形式で入力してください。",
                    quick_items=qr_items
                )
                continue

            # --- 新規検索フロー ---
            # ❶ 抽出
            try:
                query, explain = extract_query(user_text)
            except Exception as e:
                rid = _rid()
                logger.error("[extract_query %s] failed: %r\ntext=%r\ntrace=\n%s",
                             rid, e, user_text, traceback.format_exc())
                _reply_text(
                    event.reply_token,
                    f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）"
                )
                continue

            # ❷ Clarify 判定
            c_from_needs = _clarify_from_needs_choice(query)
            clarifies = []
            if c_from_needs:
                clarifies = [c_from_needs]
            else:
                try:
                    clarifies = detect(user_text) or []
                except Exception:
                    clarifies = []

            if clarifies:
                c = clarifies[0]
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
                s = _S(user_id)
                s["mode"] = "await_clarify"
                s["clarify"] = c
                s["base_query"] = query
                s["active_filters"] = {}
                s["last_facets"] = {}
                _SESS[user_id] = s

                _reply_text(event.reply_token, "\n".join(lines), quick_items=_qr_for_clarify(c))
                continue

            # ❸ 検索 → 並べ替え
            rid = _rid()
            try:
                results = run_query_system(query)
                results = reorder_and_pair(results, user_text, query)
            except Exception as e:
                logger.error("[%s] search failed: %r\ntext=%r\nquery=%r\ntrace=\n%s",
                             rid, e, user_text, query, traceback.format_exc())
                _reply_text(
                    event.reply_token,
                    f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）"
                )
                continue

            # ❹ ヒット0
            if not results:
                _reply_text(
                    event.reply_token,
                    "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。",
                    quick_items=_qr_reset_and_exit_items()
                )
                continue

            # ❺ ヒット多い → ファセット提示（初回結果をpush）
            if len(results) >= 10:
                facets = _build_facets(results)
                s = _S(user_id)
                s["mode"] = "await_refine"
                s["base_query"] = query
                s["active_filters"] = {}
                s["last_facets"] = facets
                s["clarify"] = None
                _SESS[user_id] = s

                _push_snapshot(user_id, results, query, facets)

                allow_show_all = _has_any_condition(query)
                qr_items = _make_qr_for_refine(user_id, facets, allow_show_all=allow_show_all)
                msg = (
                    f"検索結果が多いです（{len(results)}件）。\n"
                    "『列:値』（例：機械:UC-500）で追加指定するか、下の候補から選んでください。"
                )
                _reply_text(event.reply_token, msg, quick_items=qr_items)
                continue

            # ❻ 適量ヒット → 表示（空行入り）
            text_msg = _render_refined_simple(results, header="【検索結果】")
            tail = "\n\n新しい検索を行う場合はゼロ、０、またはリセット指示をお願いします。"
            _reply_text(
                event.reply_token,
                (text_msg + tail)[:4900],
                quick_items=_qr_reset_and_exit_items()
            )

        except Exception as e:
            logger.error("event handling failed: %r\n%s", e, traceback.format_exc())
            try:
                _reply_text(event.reply_token, "内部エラーが発生しました。最初から入力し直してください。", quick_items=_qr_reset_and_exit_items())
            except Exception:
                pass
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
                        "text": text.encode("utf-8", "replace").decode("utf-8", "replace"),
                        "explain": explain,
                        "mods": {"nlp_extract_file": getattr(_ne, "__file__", None),
                                 "disambiguator_file": getattr(_da, "__file__", None)},
                        "query": query,
                        "needs_choice": (query.get("_needs_choice") or {}).get("下地の状況"),
                        "clarify_final": clarify,
                    }
                return res

            results = run_query_system(query)
            results = reorder_and_pair(results, text, query)
            rendered = to_plain_text(results, query, "(dev)")
            if not results:
                rendered = f"該当なしでした。\n条件: {query}"

            res = {"status": "ok", "result_text": rendered, "query": query}
            if debug:
                res["debug"] = {
                    "text": text.encode("utf-8", "replace").decode("utf-8", "replace"),
                    "explain": explain,
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
            id2label = {str(x.get("id", "")).strip(): str(x.get("label", "")).strip() for x in chs}
            labels_set = {str(x.get("label", "")).strip() for x in chs}

            parsed_chosen_labels: List[str] = []

            if chosen_text:
                t = _norm(chosen_text).lower()
                if t in {"all", "全部", "全て", "すべて"}:
                    parsed_chosen_labels = [v for v in labels_set]
                elif t in {"unknown", "わからない", "任せる"}:
                    parsed_chosen_labels = [chs[0]["label"]] if chs else []
                else:
                    for lab in labels_set:
                        if t in _norm(lab).lower() or t == _norm(lab).lower():
                            parsed_chosen_labels = [lab]
                            break

            if not parsed_chosen_labels and chosen:
                tmp: List[str] = []
                for it in chosen:
                    s = str(it).strip()
                    if s in id2label and id2label[s]:
                        tmp.append(id2label[s])
                    elif s in labels_set:
                        tmp.append(s)
                parsed_chosen_labels = tmp

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
                rendered = f"該当なしでした。\n条件: {query_after}"

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
