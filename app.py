from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from linebot import LineBotApi, WebhookParser
from linebot.models import MessageEvent, TextMessage, TextSendMessage
from linebot.exceptions import InvalidSignatureError
import os, traceback, logging

app = FastAPI()
logger = logging.getLogger("uvicorn")

CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN) if CHANNEL_ACCESS_TOKEN else None
parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.post("/callback")
async def callback(request: Request):
    if not parser:
        logger.error("LINE credentials not set")
        return PlainTextResponse("OK", status_code=200)

    signature = request.headers.get("X-Line-Signature") or request.headers.get("x-line-signature", "")
    try:
        body_text = (await request.body()).decode("utf-8")
    except Exception as e:
        logger.error("read body failed: %s", e)
        return PlainTextResponse("OK", status_code=200)

    logger.info("==> /callback hit, bytes=%s", len(body_text))

    try:
        events = parser.parse(body_text, signature)
    except InvalidSignatureError:
        return PlainTextResponse("Invalid signature", status_code=400)
    except Exception as e:
        logger.error("parser.parse failed: %s\n%s", e, traceback.format_exc())
        return PlainTextResponse("OK", status_code=200)

    if not events:
        logger.info("no events (verify?) -> 200")
        return PlainTextResponse("OK", status_code=200)

    # 🔽 NLP & 検索・整形を遅延インポート
    try:
        from nlp_extract import extract_query
        from search_core import run_query_system
        from formatters import to_plain_text
    except Exception as e:
        logger.error("delayed import failed: %s\n%s", e, traceback.format_exc())
        return PlainTextResponse("OK", status_code=200)

    for event in events:
        try:
            if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
                user_text = (event.message.text or "").strip()

                try:
                    query, explain = await extract_query(user_text)
                    results = run_query_system(query)
                except Exception as e:
                    logger.error("search pipeline failed: %s\n%s", e, traceback.format_exc())
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text="検索中にエラーが発生しました。時間をおいてお試しください。")
                        )
                    continue

                if not results:
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text=f"該当なしでした。\n条件: {query}")
                        )
                    continue

                # ⚠️ 検索結果は改ざんせず見せ方だけ整形
                text_msg = to_plain_text(results, query, explain)
                if line_bot_api:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=text_msg[:4900])
                    )

        except Exception as e:
            logger.error("event handling failed: %s\n%s", e, traceback.format_exc())
            continue

    return PlainTextResponse("OK", status_code=200)
