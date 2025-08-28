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
#   "mode": "idle" | "await_refine",
#   "base_query": dict,               # 最初の抽出クエリを保持（上書きせずに積み上げ）
#   "active_filters": Dict[col,List], # 追加絞り込み（複数回分）
#   "last_facets": Dict[col,List],    # 直近ヒットのファセット
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
    return [
        ("0 リセット", "0"),
        ("1 終了", "1"),
    ]

def _qr_from_facets(facets: Dict[str, List[str]], per_col: int = 3) -> List[Tuple[str, str]]:
    """検索結果から得たファセットをQRに（各列最大 per_col 件）"""
    qr: List[Tuple[str, str]] = []
    for col, vals in facets.items():
        alias = ALIAS.get(col, col)
        for v in vals[:per_col]:
            qr.append((_ellipsize(f"{alias}:{v}", 20), f"{col}:{v}"))
    return qr

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
        # 最低限テキストだけ再送（QRが原因の400を回避）
        try:
            line_bot_api.reply_message(
                token, TextSendMessage(text=text[:4900])
            )
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
    }

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
            # カンマ/読点/空白で分割（セル内多値）
            parts = [p.strip() for p in re.split(r"[,\s、]+", v) if p.strip()]
            if parts:
                facets[c].update(parts)
    # 空列は削除し、ソート
    out: Dict[str, List[str]] = {}
    for c, s in facets.items():
        if s:
            out[c] = sorted(s)
    return out

def _apply_refine(base_query: Dict[str, Any], active_filters: Dict[str, List[str]]) -> Dict[str, Any]:
    """ベースクエリに active_filters（列: 値の配列で AND）をマージ"""
    q = dict(base_query)
    for col, vals in active_filters.items():
        # 既にベースに同名キーがあれば交差（AND）に寄せたいが、まずは上書き優先でシンプルに
        q[col] = list(vals)
    return q

def _parse_colon_filter(text: str) -> Optional[Tuple[str, str]]:
    """
    '列:値' / '列＝値' / '列=値' 形式を抽出。先頭/末尾空白は無視。
    """
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
        from formatters import to_plain_text
        from postprocess import reorder_and_pair
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

            # --- グローバルコマンド ---
            u = _norm(user_text)
            if u in {"0", "０", "ゼロ", "ﾘｾｯﾄ", "リセット"}:
                _reset_session(user_id)
                _reply_text(
                    event.reply_token,
                    "新しい検索を始めます。条件を入力してください。",
                    quick_items=_qr_reset_and_exit_items()
                )
                continue
            if u in {"1", "１", "終わり", "終了"}:
                _reset_session(user_id)
                _reply_text(event.reply_token, "終了しました。またどうぞ！")
                continue

            # --- 既に絞り込み待ち（ファセット提示済み）の場合 ---
            sess = _SESS.get(user_id) or {}
            if sess.get("mode") == "await_refine":
                parsed = _parse_colon_filter(user_text)
                base_query = sess.get("base_query") or {}
                active_filters = sess.get("active_filters") or {}

                if parsed:
                    col, val = parsed
                    # 既知列名のゆれを吸収（エイリアス逆引き）
                    # まず完全一致
                    col_norm = col
                    # エイリアス一致（機種 → ライナックス機種名 等）
                    for full, ali in ALIAS.items():
                        if col == ali:
                            col_norm = full
                            break

                    # 追加
                    vals = active_filters.get(col_norm, [])
                    if val not in vals:
                        vals = vals + [val]
                    active_filters[col_norm] = vals

                    # 再検索
                    q2 = _apply_refine(base_query, active_filters)
                    results = run_query_system(q2)
                    results = reorder_and_pair(results, user_text, q2)

                    if not results:
                        # 該当なし → 案内＆リセット/終了
                        _reply_text(
                            event.reply_token,
                            "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。",
                            quick_items=_qr_reset_and_exit_items()
                        )
                        _reset_session(user_id)
                        continue

                    # ヒットが多いときはさらに絞り込めるように（ラベル短いQR）
                    if len(results) >= 10:
                        facets = _build_facets(results)
                        sess["mode"] = "await_refine"
                        sess["base_query"] = base_query
                        sess["active_filters"] = active_filters
                        sess["last_facets"] = facets
                        _SESS[user_id] = sess

                        qr_items = _qr_from_facets(facets, per_col=3) + _qr_reset_and_exit_items()
                        msg = (
                            f"検索結果が多いです（{len(results)}件）。\n"
                            "『列:値』（例：機械:UC-500）で追加指定するか、下の候補から選んでください。"
                        )
                        _reply_text(event.reply_token, msg, quick_items=qr_items)
                        continue

                    # ちょうど良い件数 → 結果表示 + 継続絞り込みのヒント＆リセット/終了の案内
                    text_msg = to_plain_text(results, q2, "(refined)")
                    tail = "\n\n新しい検索を行う場合はゼロ、０、またはリセット指示をお願いします。"
                    _reply_text(
                        event.reply_token,
                        (text_msg + tail)[:4900],
                        quick_items=_qr_reset_and_exit_items()
                    )
                    # さらに絞り込みを続けたい場合のため、状態は維持
                    sess["mode"] = "await_refine"
                    sess["base_query"] = base_query
                    sess["active_filters"] = active_filters
                    sess["last_facets"] = _build_facets(results)
                    _SESS[user_id] = sess
                    continue

                # 形式不明 → ガイド再提示（候補QRを出し続ける）
                facets = sess.get("last_facets") or {}
                qr_items = _qr_from_facets(facets, per_col=3) + _qr_reset_and_exit_items()
                _reply_text(
                    event.reply_token,
                    "追加条件は『列:値』（例：機械:UC-500）の形式で入力してください。",
                    quick_items=qr_items
                )
                continue

            # --- ここから新規検索フロー ---
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

            # ❷ Clarify 判定（まず _needs_choice）
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
                # 選択肢の提示（テキスト本文に列挙）
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
                # Clarify は既存の /dev/choose 相当をアプリ内で扱っていないため、
                # ここでは一旦ガイドのみ（必要ならここに Clarify 応答ロジックを拡張）
                _reply_text(
                    event.reply_token,
                    "\n".join(lines),
                    quick_items=_qr_reset_and_exit_items()
                )
                # Clarifyの簡易状態（必要なら実装）
                _SESS[user_id] = {
                    "mode": "idle",
                    "base_query": query,
                    "active_filters": {},
                    "last_facets": {},
                }
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

            # ❺ ヒット多い → ファセット提示して追加入力へ
            if len(results) >= 10:
                facets = _build_facets(results)

                # 全件（=初期結果）に近い場合は「該当なし扱いのガイド」へ逃がす条件
                # → ここでは results が全件かどうかを判別できないので、
                #    ファセットが極端に広すぎるときは候補数を抑えつつ案内
                qr_items = _qr_from_facets(facets, per_col=3) + _qr_reset_and_exit_items()
                msg = (
                    f"検索結果が多いです（{len(results)}件）。\n"
                    "『列:値』（例：機械:UC-500）で追加指定するか、下の候補から選んでください。"
                )
                _reply_text(event.reply_token, msg, quick_items=qr_items)

                # セッション保存：以降の追加入力はこの条件に積み上げる
                _SESS[user_id] = {
                    "mode": "await_refine",
                    "base_query": query,         # 最初の検索条件を保持
                    "active_filters": {},        # 以降の追加分
                    "last_facets": facets,
                }
                continue

            # ❻ 適量ヒット → 表示
            text_msg = to_plain_text(results, query, explain)
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
        """
        本文(text)を投げると、抽出→Clarify 判定（まず _needs_choice）→
        - Clarify あり: clarify を返す
        - なし: そのまま検索→整形テキストを返す

        debug=true で内部状態を追加
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
                        "mods": {"nlp_extract_file": getattr(_ne, "__file__", None),
                                 "disambiguator_file": getattr(_da, "__file__", None)},
                        "query": query,
                        "needs_choice": (query.get("_needs_choice") or {}).get("下地の状況"),
                        "clarify_final": clarify,
                    }
                return res

            # Clarify なし → 検索
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
            id2label = {str(x.get("id", "")).strip(): str(x.get("label", "")).strip() for x in chs}
            labels_set = {str(x.get("label", "")).strip() for x in chs}

            parsed_chosen_labels: List[str] = []

            # 自然文優先
            if chosen_text:
                t = _norm(chosen_text).lower()
                if t in {"all", "全部", "全て", "すべて"}:
                    parsed_chosen_labels = [v for v in labels_set]
                elif t in {"unknown", "わからない", "任せる"}:
                    parsed_chosen_labels = [chs[0]["label"]] if chs else []
                else:
                    # 単純部分一致/完全一致どちらも拾う（スコア付けは省略）
                    for lab in labels_set:
                        if t in _norm(lab).lower() or t == _norm(lab).lower():
                            parsed_chosen_labels = [lab]
                            break

            # 明示配列（["1","3"] or ["厚膜塗料（〜）"]）
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

            # 反映 → 検索
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
