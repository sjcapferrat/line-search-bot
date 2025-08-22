# app.py
from __future__ import annotations

import os
import uuid
import logging
import traceback
import re
from typing import Dict, Any, List

from fastapi import FastAPI, Request, Body
from fastapi.responses import PlainTextResponse, Response
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

# ------------------------------
# 基本セットアップ
# ------------------------------
app = FastAPI()
logger = logging.getLogger("app")

def _rid() -> str:
    return uuid.uuid4().hex[:8]

# ユーザーごとの確認待ち状態（揮発）
_PENDING: Dict[str, Dict[str, Any]] = {}  # {"clarify": dict, "query": dict, "raw": str}

ALLOW_DEV = os.environ.get("ALLOW_DEV", "1") == "1"  # 本番は 0 推奨

# JSON に charset を明示（PowerShell 等の文字化け対策）
@app.middleware("http")
async def add_json_charset(request: Request, call_next):
    resp = await call_next(request)
    ctype = resp.headers.get("content-type", "")
    if ctype.startswith("application/json") and "charset=" not in ctype:
        resp.headers["content-type"] = "application/json; charset=utf-8"
    return resp

# Render のヘルスチェック対策（/ を 200 に、favicon も 204）
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)

# LINE 資格情報
CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN) if CHANNEL_ACCESS_TOKEN else None
parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

# ------------------------------
# Webhook
# ------------------------------
@app.post("/callback")
async def callback(request: Request):
    if not parser:
        logger.error("LINE credentials not set")
        return PlainTextResponse("OK", status_code=200)

    # 署名 + 本文
    signature = request.headers.get("X-Line-Signature") or request.headers.get("x-line-signature", "")
    try:
        body_text = (await request.body()).decode("utf-8")
    except Exception as e:
        logger.error("read body failed: %r", e)
        return PlainTextResponse("OK", status_code=200)

    logger.info("==> /callback hit, bytes=%s", len(body_text))

    # 解析
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

    # 遅延インポート（起動を軽く）
    try:
        from nlp_extract import extract_query
        from search_core import run_query_system
        from formatters import to_plain_text
        from disambiguator import detect, apply_choice_to_query
        from postprocess import reorder_and_pair
    except Exception as e:
        logger.error("delayed import failed: %r\n%s", e, traceback.format_exc())
        return PlainTextResponse("OK", status_code=200)

    # イベント処理
    for event in events:
        try:
            if not (isinstance(event, MessageEvent) and isinstance(event.message, TextMessage)):
                continue

            user_text = (event.message.text or "").strip()

            # 送信者ID（user / group / room）
            src = getattr(event, "source", None)
            sender_id = None
            if src:
                sender_id = getattr(src, "user_id", None) or getattr(src, "group_id", None) or getattr(src, "room_id", None)
            sender_id = sender_id or "unknown"

            # ❶ 「前の質問の回答」待ち
            if sender_id in _PENDING:
                rid = _rid()
                try:
                    pending = _PENDING.pop(sender_id)
                    clarify = pending["clarify"]
                    raw_lower = user_text.lower().strip()

                    # 回答の解釈（"1,3" / "all" / "unknown" / スペース区切り）
                    if raw_lower in {"all", "すべて", "全部", "全て"}:
                        chosen = ["all"]
                    elif raw_lower in {"unknown", "わからない", "任せる"}:
                        chosen = ["unknown"]
                    else:
                        raw_norm = raw_lower.replace("，", ",")
                        chosen = [x.strip() for x in re.split(r"[,\s]+", raw_norm) if x.strip()]

                    # 反映 → 検索 → 並べ替え
                    query_after = apply_choice_to_query(pending["query"], chosen, clarify)
                    results = run_query_system(query_after)
                    results = reorder_and_pair(results, pending["raw"], query_after)

                    # 表示
                    text_msg = to_plain_text(results, query_after, "(clarified)")
                    if not results:
                        text_msg = f"該当なしでした。\n条件: {query_after}"

                    if line_bot_api:
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=text_msg[:4900]))
                    continue

                except Exception as e:
                    # text 未定義警告を避けつつログ（repr で安全）
                    text_for_log = pending["raw"] if "pending" in locals() and isinstance(pending, dict) else None
                    logger.error(
                        "[%s] clarify handling failed: %r\ntext=%r\nclarify=%r\ntrace=\n%s",
                        rid, e, text_for_log, locals().get("clarify"), traceback.format_exc()
                    )
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text=f"選択の処理でエラーが発生しました。最初から入力し直してください。（Error ID: {rid}）")
                        )
                    continue

            # ❷ 抽出（同期関数なので await なし）
            try:
                query, explain = extract_query(user_text)
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

            # ❸ 曖昧語検出 → 必要なら確認
            try:
                clarifies = detect(user_text)
            except Exception as e:
                logger.error("detect failed: %r\n%s", e, traceback.format_exc())
                clarifies = []

            if clarifies:
                # まず最初の曖昧項目のみ質問（複数時は順送り）
                c = clarifies[0]
                _PENDING[sender_id] = {"clarify": c, "query": query, "raw": user_text}

                # “自動確定”があるなら質問せずそのまま検索
                auto_labels = c.get("auto", [])
                if auto_labels:
                    try:
                        query_after = apply_choice_to_query(query, auto_labels, c)
                        results = run_query_system(query_after)
                        results = reorder_and_pair(results, user_text, query_after)
                        text_msg = to_plain_text(results, query_after, "(auto-clarified)")
                        if not results:
                            text_msg = f"該当なしでした。\n条件: {query_after}"
                        if line_bot_api:
                            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=text_msg[:4900]))
                        continue
                    except Exception as e:
                        rid = _rid()
                        logger.error(
                            "[%s] auto-clarify failed: %r\ntext=%r\nclarify=%r\nquery=%r\ntrace=\n%s",
                            rid, e, user_text, c, query, traceback.format_exc()
                        )
                        if line_bot_api:
                            line_bot_api.reply_message(
                                event.reply_token,
                                TextSendMessage(text=f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）")
                            )
                        continue

                # 自動確定なし → 質問メッセージ（番号 + all/unknown）
                lines: List[str] = [str(c.get("question"))]
                for ch in c.get("choices", []):
                    # choices は {"id": "1", "label": "..."} の dict 前提
                    cid = str(ch.get("id", "")).strip()
                    label = str(ch.get("label", "")).strip()
                    if cid:
                        lines.append(f"{cid}) {label}")
                lines.append("all) すべて")
                lines.append("unknown) わからない（おすすめ）")
                lines.append("※ 番号をカンマ or スペース区切りで送ってください。例: 1,2")
                if line_bot_api:
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n".join(lines)))
                continue

            # ❹ そのまま検索 → 並べ替え
            rid = _rid()
            try:
                results = run_query_system(query)
                results = reorder_and_pair(results, user_text, query)
            except Exception as e:
                text_for_log = user_text  # 常にある
                logger.error(
                    "[%s] search failed: %r\ntext=%r\nquery=%r\ntrace=\n%s",
                    rid, e, text_for_log, query, traceback.format_exc()
                )
                if line_bot_api:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=f"検索中にエラーが発生しました。時間をおいてお試しください。（Error ID: {rid}）")
                    )
                continue

            if not results:
                if line_bot_api:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=f"該当なしでした。\n条件: {query}")
                    )
                continue

            # ❺ 表示（検索結果は改変しない）
            text_msg = to_plain_text(results, query, explain)
            if line_bot_api:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=text_msg[:4900]))

        except Exception as e:
            logger.error("event handling failed: %r\n%s", e, traceback.format_exc())
            continue

    return PlainTextResponse("OK", status_code=200)

# ------------------------------
# 開発用 API（ALLOW_DEV=1 のときだけ）
# ------------------------------
if ALLOW_DEV:
    @app.post("/dev/run")
    async def dev_run(payload: dict = Body(...)):
        text = (payload.get("text") or "").strip()
        if not text:
            return {"status": "error", "message": "text を入れてください"}

        from nlp_extract import extract_query
        from disambiguator import detect
        from search_core import run_query_system
        from postprocess import reorder_and_pair
        from formatters import to_plain_text

        query, explain = extract_query(text)
        clarifies = detect(text)
        if clarifies:
            c = clarifies[0]
            return {
                "status": "clarify",
                "question": c.get("question"),
                "column": c.get("column"),
                "choices": c.get("choices", []),
                "hint": "番号やラベルを chosen に入れて /dev/choose へPOSTしてください。",
                "text": text,
            }

        results = run_query_system(query)
        results = reorder_and_pair(results, text, query)
        rendered = to_plain_text(results, query, "(dev)")
        if not results:
            rendered = f"該当なしでした。\n条件: {query}"
        return {"status": "ok", "result_text": rendered, "query": query}

    @app.post("/dev/choose")
    async def dev_choose(payload: dict = Body(...)):
        text = (payload.get("text") or "").strip()
        chosen = payload.get("chosen") or []
        if not text:
            return {"status": "error", "message": "text を入れてください"}

        from nlp_extract import extract_query
        from disambiguator import detect, apply_choice_to_query
        from search_core import run_query_system
        from postprocess import reorder_and_pair
        from formatters import to_plain_text

        query, _ = extract_query(text)
        clarifies = detect(text)
        if not clarifies:
            return {"status": "error", "message": "clarify は不要でした（/dev/run を先に）"}
        c = clarifies[0]

        chosen = [str(x) for x in chosen]
        query_after = apply_choice_to_query(query, chosen, c)
        results = run_query_system(query_after)
        results = reorder_and_pair(results, text, query_after)
        rendered = to_plain_text(results, query_after, "(dev clarified)")
        if not results:
            rendered = f"該当なしでした。\n条件: {query_after}"
        return {"status": "ok", "result_text": rendered, "query": query_after}
