from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
import traceback, logging
logger = logging.getLogger("uvicorn")
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import os

# FastAPI インスタンス
app = FastAPI()

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

# 環境変数から LINE のチャネル情報を取得
CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(CHANNEL_SECRET)


@app.get("/")
async def root():
    """動作確認用エンドポイント"""
    return {"message": "Hello! FastAPI + LINE Bot is running."}


@app.post("/callback")
async def callback(request: Request):
    if not parser:
        return PlainTextResponse("OK", status_code=200)
    
     # 🔽 遅延インポートをガード
    try:
        from nlp_extract import extract_query
        from search_core import run_query_system
        from formatters import to_plain_text
    except Exception as e:
        print("[ERR] delayed import failed:", e, traceback.format_exc())
        # Verify失敗させないため200で返す
        return PlainTextResponse("OK", status_code=200)

    logger.info("==> /callback hit")  # ★これだけでも到達確認できる

    """LINE Messaging API からのコールバックを処理"""
    signature = request.headers.get("X-Line-Signature", "")

    # リクエストボディを文字列として取得
    body = await request.body()
    body_text = body.decode("utf-8")

    try:
        events = parser.parse(body_text, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    # イベントを処理
    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
            reply_text = f"あなたが送ったメッセージ: {event.message.text}"
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=reply_text)
            )

    return PlainTextResponse("OK")
