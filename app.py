from fastapi import FastAPI, Request, HTTPException
from linebot import LineBotApi, WebhookParser
from linebot.models import MessageEvent, TextMessage, TextSendMessage
from linebot.exceptions import InvalidSignatureError
import os

CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")

app = FastAPI()
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN) if CHANNEL_ACCESS_TOKEN else None
parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.post("/callback")
async def callback(request: Request):
    if not parser:
        raise HTTPException(status_code=500, detail="LINE credentials not set")

    # 🔽遅延インポート
    from nlp_extract import extract_query
    from search_core import run_query_system
    from formatters import to_plain_text   # これを使う

    signature = request.headers.get("x-line-signature", "")
    body_text = (await request.body()).decode("utf-8")

    try:
        events = parser.parse(body_text, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
            user_text = event.message.text.strip()

            try:
                query, explain = await extract_query(user_text)
                results = run_query_system(query)
            except Exception as e:
                print(f"[ERROR] search failed: {e}")
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

            text_msg = to_plain_text(results, query, explain)  # ← データは改ざんせず整形のみ
            if line_bot_api:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=text_msg[:4900])
                )

    return "OK"
