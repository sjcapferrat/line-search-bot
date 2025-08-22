from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from linebot import LineBotApi, WebhookParser
from linebot.models import MessageEvent, TextMessage, TextSendMessage
from linebot.exceptions import InvalidSignatureError
from disambiguator import detect, apply_choice_to_query
from postprocess import reorder_and_pair
# ユーザーごとの確認待ち状態（揮発）
_PENDING: dict[str, dict] = {}  # key: sender_id, value: {"clarify": Clarify, "query": dict, "raw": str}
import os, traceback, logging

ALLOW_DEV = os.environ.get("ALLOW_DEV", "1") == "1"  # 開発中は 1 のまま。これらは LINEの署名チェックを通さず、内部の処理だけを使います。LINEをつないだら、不要なら ALLOW_DEV=0 にして止めてOK

app = FastAPI()
@app.middleware("http")
async def add_json_charset(request: Request, call_next):
    resp = await call_next(request)
    ctype = resp.headers.get("content-type", "")
    if ctype.startswith("application/json") and "charset=" not in ctype:
        resp.headers["content-type"] = "application/json; charset=utf-8"
    return resp

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
    import re  # 選択IDのパースに使用

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

    # 🔽 NLP & 検索・整形・曖昧語判定・結果後処理を遅延インポート
    try:
        from nlp_extract import extract_query
        from search_core import run_query_system
        from formatters import to_plain_text
        from disambiguator import detect, apply_choice_to_query
        from postprocess import reorder_and_pair
    except Exception as e:
        logger.error("delayed import failed: %s\n%s", e, traceback.format_exc())
        return PlainTextResponse("OK", status_code=200)

    for event in events:
        try:
            if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
                user_text = (event.message.text or "").strip()

                # 送信者IDを特定（ユーザー / グループ / ルーム）
                src = getattr(event, "source", None)
                sender_id = None
                if src:
                    sender_id = getattr(src, "user_id", None) or getattr(src, "group_id", None) or getattr(src, "room_id", None)
                sender_id = sender_id or "unknown"

                # ❶ もし「前の質問の回答」待ちなら、その選択を反映して検索へ
                if sender_id in _PENDING:
                    try:
                        pending = _PENDING.pop(sender_id)
                        clarify = pending["clarify"]
                        raw_lower = user_text.lower().strip()

                        # 回答の解釈（"1,3" / "all" / "unknown" / スペース区切りにも対応）
                        if raw_lower in {"all", "すべて", "全部", "全て"}:
                            chosen = ["all"]
                        elif raw_lower in {"unknown", "わからない", "任せる"}:
                            chosen = ["unknown"]
                        else:
                            raw_norm = raw_lower.replace("，", ",")
                            chosen = [x.strip() for x in re.split(r"[,\s]+", raw_norm) if x.strip()]

                        # 選択内容を抽出フィルタに追記
                        query_after = apply_choice_to_query(pending["query"], chosen, clarify)

                        # 検索 → 並べ替え＆工程ペア挿入
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
                        logger.error("clarify handling failed: %s\n%s", e, traceback.format_exc())
                        if line_bot_api:
                            line_bot_api.reply_message(
                                event.reply_token,
                                TextSendMessage(text="選択の処理でエラーが発生しました。最初から入力し直してください。")
                            )
                        continue

                # ❷ 通常フロー：まず抽出
                try:
                    query, explain = await extract_query(user_text)
                except Exception as e:
                    logger.error("extract_query failed: %s\n%s", e, traceback.format_exc())
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text="検索中にエラーが発生しました。時間をおいてお試しください。")
                        )
                    continue

                # ❸ 検索前に「曖昧語」を検出し、該当すれば確認を促す
                try:
                    clarifies = detect(user_text)
                except Exception as e:
                    logger.error("detect failed: %s\n%s", e, traceback.format_exc())
                    clarifies = []

                if clarifies:
                    # 今は最初の曖昧項目だけを尋ねる（複数ヒット時は順番に）
                    c = clarifies[0]
                    _PENDING[sender_id] = {"clarify": c, "query": query, "raw": user_text}

                    auto_labels = c.auto if hasattr(c, "auto") else c.get("auto", [])
                    if auto_labels:
                        query_after = apply_choice_to_query(query, auto_labels, c)
                        results = run_query_system(query_after)
                        results = reorder_and_pair(results, user_text, query_after)
                        text_msg = to_plain_text(results, query_after, "(auto-clarified)")
                    if line_bot_api:
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=text_msg[:4900]))
                    continue

                    # 質問メッセージ（番号選択 + all/unknown）
                    lines = [c.question]
                    for ch in c.choices:
                        if ch.id.isdigit():
                            lines.append(f"{ch.id}) {ch.label}")
                    lines.append("all) すべて")
                    lines.append("unknown) わからない（おすすめ）")
                    lines.append("※ 番号をカンマ or スペース区切りで送ってください。例: 1,2")
                    if line_bot_api:
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n".join(lines)))
                    continue

                # ❹ 曖昧でなければそのまま検索 → 並べ替え＆工程ペア
                try:
                    results = run_query_system(query)
                except Exception as e:
                    logger.error("search pipeline failed: %s\n%s", e, traceback.format_exc())
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text="検索中にエラーが発生しました。時間をおいてお試しください。")
                        )
                    continue

                results = reorder_and_pair(results, user_text, query)

                if not results:
                    if line_bot_api:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text=f"該当なしでした。\n条件: {query}")
                        )
                    continue

                # ❺ 見せ方だけ整形（検索結果は改ざんしない方針）
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

from fastapi import Body

if ALLOW_DEV:
    @app.post("/dev/run")
    async def dev_run(payload: dict = Body(...)):
        """
        本文(text)を投げると、抽出→曖昧語チェック→
        - 曖昧語あり: clarify を返す
        - なし: そのまま検索→整形テキストを返す
        """
        text = (payload.get("text") or "").strip()
        if not text:
            return {"status": "error", "message": "text を入れてください"}

        # 遅延インポート（本番と同じ）
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
                "text": text,   # 次の呼び出しで使う
            }

        # 曖昧語なし→検索→整形
        results = run_query_system(query)
        results = reorder_and_pair(results, text, query)
        rendered = to_plain_text(results, query, "(dev)")
        if not results:
            rendered = f"該当なしでした。\n条件: {query}"
        return {"status": "ok", "result_text": rendered, "query": query}

    @app.post("/dev/choose")
    async def dev_choose(payload: dict = Body(...)):
        """
        /dev/run で clarify が出たときの 2段目。
        { "text": "...", "chosen": ["2"] } などを渡す。
        """
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

        # 選択反映 → 再検索
        chosen = [str(x) for x in chosen]
        query_after = apply_choice_to_query(query, chosen, c)
        results = run_query_system(query_after)
        results = reorder_and_pair(results, text, query_after)
        rendered = to_plain_text(results, query_after, "(dev clarified)")
        if not results:
            rendered = f"該当なしでした。\n条件: {query_after}"
        return {"status": "ok", "result_text": rendered, "query": query_after}

