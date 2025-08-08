import os
from fastapi import FastAPI, Request, HTTPException
from linebot import LineBotApi, WebhookParser
from linebot.models import MessageEvent, TextMessage, TextSendMessage, FlexSendMessage, QuickReply, QuickReplyButton, MessageAction
from linebot.exceptions import InvalidSignatureError
from nlp_extract import extract_query
from search_core import run_query_system
from formatters import to_plain_text, to_flex_message

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
    signature = request.headers.get("x-line-signature", "")
    body = await request.body()
    body_text = body.decode("utf-8")

    try:
        events = parser.parse(body_text, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
            user_text = event.message.text.strip()

            # 1) 自然文 → 構造化クエリ（GPT+ルール）
            query, explain = await extract_query(user_text)

            # 2) 検索：既存クエリシステムをそのまま呼ぶ（※内容は絶対に加工しない）
            try:
                results = run_query_system(query)  # ← ここをあなたの既存実装に接続
            except Exception as e:
                err_msg = f"検索システム呼び出しでエラーが発生しました: {e}"
                if line_bot_api:
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=err_msg))
                continue

            # 3) 出口：内容は改ざんせず見せ方だけ整える
            if not results:
                reply_text = (
                    "該当する結果は見つかりませんでした。\n"
                    f"解釈した条件: {json_safe(query)}\n\n"
                    "条件を広げますか？深さ・厚さや下地の種類を変更できます。"
                )
                quick = QuickReply(items=[
                    QuickReplyButton(action=MessageAction(label="深さ・厚さを変更", text="深さ・厚さを2mmにして再検索")),
                    QuickReplyButton(action=MessageAction(label="下地を変更", text="下地をコンクリートで")),
                    QuickReplyButton(action=MessageAction(label="条件クリア", text="条件をリセット"))
                ])
                if line_bot_api:
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text, quick_reply=quick))
                continue

            text_msg = to_plain_text(results, query, explain)
            flex_msg = to_flex_message(results)

            if line_bot_api:
                line_bot_api.reply_message(
                    event.reply_token,
                    messages=[
                        TextSendMessage(text=text_msg),
                        FlexSendMessage(alt_text="検索結果", contents=flex_msg),
                    ]
                )

    return "OK"

def json_safe(o):
    try:
        import json
        return json.dumps(o, ensure_ascii=False)
    except Exception:
        return str(o)
