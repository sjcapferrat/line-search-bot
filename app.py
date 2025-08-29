# -*- coding: utf-8 -*-
"""
app.py（シンプル版 v2.3）

変更点（v2.2 → v2.3）
- START_TEXT 定数で初期表示テキストを一元管理（実改行混入の構文エラーを防止）
- /callback の Verify（空 events）を常に即 200 で高速応答
- それ以外は署名検証の上で handler.handle()
- 既存の: UTF-8 JSON, QuickReply 個数制限, label20文字制限, 数字送信, 出現順候補など維持

＋ この版で追加（招待＋id対応）
- ALLOWED_USER_IDS（環境変数, カンマ区切り）でホワイトリスト制
- 1:1以外（グループ/ルーム）は案内だけ返して終了
- 「id / uid / ユーザーid」を受けたら userId を返信
- /health は GET/HEAD/POST すべて200
- dev_run でも "id" を送ると user_id を返す（ローカル検証用）

起動例:
  uvicorn app:app --host 0.0.0.0 --port 8000

依存:
  fastapi, uvicorn, pandas, (line-bot-sdk: 本番LINE利用時のみ)
環境変数:
  RAG_CSV_PATH=.../restructured_file.csv
  LINE_CHANNEL_ACCESS_TOKEN, LINE_CHANNEL_SECRET（LINE連携時のみ）
  ALLOWED_USER_IDS（任意・カンマ区切り。設定時はホワイトリスト制）
"""
from __future__ import annotations
import os
import re
import json
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
import pandas as pd

# ====== LINE SDK（未設定ならダミーで起動可能） ======
try:
    from linebot import LineBotApi, WebhookHandler
    from linebot.exceptions import InvalidSignatureError
    from linebot.models import (
        MessageEvent, TextMessage, TextSendMessage,
        QuickReply, QuickReplyButton, MessageAction,
    )
    LINE_AVAILABLE = True
except Exception:
    LINE_AVAILABLE = False

# =============================
# 設定
# =============================
CSV_PATH = os.environ.get("RAG_CSV_PATH", "restructured_file.csv")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
# 招待制ホワイトリスト（カンマ区切り: Uxxxx,Uyyyy,... ／未設定なら誰でも利用可）
ALLOWED_USER_IDS = set(u.strip() for u in os.environ.get("ALLOWED_USER_IDS", "").split(",") if u.strip())

MAX_QUICKREPLIES = 12   # 端末差を考慮して 12 に制限（LINEは13前後が上限）
RESULTS_REFINE_THRESHOLD = 5
APP_VERSION = "app.py (simple v2.3)"

# =============================
# UTF-8 JSON（PowerShell/LINEでの文字化け対策）
# =============================
class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"
    def render(self, content: object) -> bytes:
        return json.dumps(content, ensure_ascii=False).encode("utf-8")

app = FastAPI(default_response_class=UTF8JSONResponse)

# =============================
# データ読み込み
# =============================
REQUIRED_COLUMNS = [
    "作業名",
    "下地の状況",
    "処理する深さ・厚さ",
    "工程数",
    "機械カテゴリー",
    "ライナックス機種名",
    "使用カッター名",
    "作業効率評価",
]

def load_dataframe(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSVが見つかりません: {path}")
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"CSV列が不足: {missing}")
    for c in REQUIRED_COLUMNS:
        df[c] = df[c].fillna("").astype(str)
    return df

DF = load_dataframe(CSV_PATH)

# ユニーク値（出現順）

def _unique_in_order(series: pd.Series) -> List[str]:
    return [x for x in pd.unique(series.tolist()) if str(x) != ""]

ALL_UNIQUE = {
    "作業名": _unique_in_order(DF["作業名"]),
    "下地の状況": _unique_in_order(DF["下地の状況"]),
    "機械カテゴリー": _unique_in_order(DF["機械カテゴリー"]),
    "ライナックス機種名": _unique_in_order(DF["ライナックス機種名"]),
}

# =============================
# ユーティリティ
# =============================
ZEN2HAN_TABLE = str.maketrans({
    "０":"0","１":"1","２":"2","３":"3","４":"4",
    "５":"5","６":"6","７":"7","８":"8","９":"9"
})

def to_int_or_none(text: str) -> Optional[int]:
    t = (text or "").strip().translate(ZEN2HAN_TABLE)
    if re.fullmatch(r"\d+", t):
        try:
            return int(t)
        except Exception:
            return None
    return None

# QuickReply 生成ヘルパ

def assemble_quick(options: List[str], controls: List[str] | Tuple[str, ...] = ()) -> List[str]:
    """候補 + コントロールを常に MAX_QUICKREPLIES 以内に丸める"""
    controls = [c for c in controls if c]
    limit_for_options = max(0, MAX_QUICKREPLIES - len(controls))
    return options[:limit_for_options] + controls

def clip_label(label: str, maxlen: int = 20) -> str:
    """QuickReply の label は 20 文字以内。超過は『…』で丸める。"""
    return label if len(label) <= maxlen else (label[: maxlen - 1] + "…")

# =============================
# セッション管理
# =============================
class Stage:
    IDLE = "IDLE"
    CHOOSE_TASK = "CHOOSE_TASK"
    CHOOSE_BASE = "CHOOSE_BASE"
    ASK_OPTIONAL = "ASK_OPTIONAL"
    CHOOSE_MACHINE_CAT = "CHOOSE_MACHINE_CAT"
    CHOOSE_MODEL = "CHOOSE_MODEL"
    SHOW_RESULTS = "SHOW_RESULTS"
    REFINE_MORE = "REFINE_MORE"

class SearchSession:
    def __init__(self):
        self.stage: str = Stage.IDLE
        self.filters: Dict[str, Optional[str]] = {
            "作業名": None,
            "下地の状況": None,
            "機械カテゴリー": None,
            "ライナックス機種名": None,
        }
        self.last_results: Optional[pd.DataFrame] = None

    def reset(self):
        self.__init__()

SESSIONS: Dict[str, SearchSession] = {}

def get_session(user_key: str) -> SearchSession:
    sess = SESSIONS.get(user_key)
    if not sess:
        sess = SearchSession()
        SESSIONS[user_key] = sess
    return sess

# =============================
# 検索ロジック
# =============================

def apply_filters(df: pd.DataFrame, filters: Dict[str, Optional[str]]) -> pd.DataFrame:
    q = df.copy()
    for key, val in filters.items():
        if key in q.columns and val:
            q = q[q[key] == val]
    return q


def result_to_text(rows: pd.DataFrame, limit: int = 20) -> str:
    if rows.empty:
        return "該当がありませんでした。選び直してください。"
    view = rows[[
        "作業名",
        "下地の状況",
        "ライナックス機種名",
        "使用カッター名",
        "工程数",
        "作業効率評価",
        "処理する深さ・厚さ",
    ]].head(limit)
    lines = ["=== 検索結果 ==="]
    for _, r in view.iterrows():
        lines.append(
            f"・{r['作業名']}｜{r['下地の状況']}｜{r['ライナックス機種名']}｜{r['使用カッター名']}｜{r['工程数']}｜{r['作業効率評価']}｜{r['処理する深さ・厚さ']}"
        )
    count = len(rows)
    if count > limit:
        lines.append(f"(他 {count - limit} 件)")
    return "\n".join(lines)


def next_refine_suggestions(rows: pd.DataFrame, used_optional: Optional[str]) -> Tuple[str, List[str]]:
    # 使っていない軸を優先
    if used_optional == "機械カテゴリー":
        order = ["ライナックス機種名", "作業効率評価", "工程数"]
    elif used_optional == "ライナックス機種名":
        order = ["機械カテゴリー", "作業効率評価", "工程数"]
    else:
        order = ["機械カテゴリー", "ライナックス機種名", "作業効率評価", "工程数"]
    for col in order:
        vals = [x for x in rows[col].unique().tolist() if x]
        if len(vals) >= 2:
            return col, vals
    return "", []

# =============================
# ダイアログ制御
# =============================
WELCOME = (
    "新しい検索を始めます。\n"
    "・まず『作業名』を選択してください。\n"
    "（ヒント: 途中で『やり直す』『終了』と入力できます）"
)

ASK_TASK = "作業名を選んでください"
ASK_BASE = "下地の状況を選んでください"
ASK_OPTIONAL = (
    "任意で更に絞り込みますか？\n"
    "1: 機械カテゴリーから選ぶ\n"
    "2: ライナックス機種名から選ぶ\n"
    "3: このまま検索する"
)
ASK_MACHINE_CAT = "機械カテゴリーを選んでください"
ASK_MODEL = "ライナックス機種名を選んでください"

START_TEXT = WELCOME + "\n\n" + ASK_TASK


def _used_optional(sess: SearchSession) -> Optional[str]:
    if sess.filters.get("機械カテゴリー"):
        return "機械カテゴリー"
    if sess.filters.get("ライナックス機種名"):
        return "ライナックス機種名"
    return None


def _unique_filtered(column: str, sess: SearchSession) -> List[str]:
    # 必須条件（作業名/下地）だけを仮適用して候補を出す
    tmp_filters = {k: v for k, v in sess.filters.items() if v and k in ("作業名", "下地の状況")}
    sub = apply_filters(DF, tmp_filters)
    vals = [x for x in sub[column].unique().tolist() if x]
    if not vals:
        vals = ALL_UNIQUE[column]
    return vals


def resolve_choice(text: str, options: List[str]) -> Optional[str]:
    t = (text or "").strip()
    n = to_int_or_none(t)
    if n is not None:
        idx = n - 1
        if 0 <= idx < len(options):
            return options[idx]
        return None
    for opt in options:
        if t == opt:
            return opt
    return None


def _do_search_and_maybe_refine(sess: SearchSession, used_optional: Optional[str]) -> Dict[str, object]:
    results = apply_filters(DF, sess.filters)
    sess.last_results = results

    if results.empty:
        sess.stage = Stage.CHOOSE_TASK
        return {
            "text": "該当がありませんでした。条件を見直してください。\n\nまず『作業名』から選び直しましょう。",
            "quick": assemble_quick(ALL_UNIQUE["作業名"], ["終了"]),
        }

    if len(results) >= RESULTS_REFINE_THRESHOLD:
        sess.stage = Stage.REFINE_MORE
        col, vals = next_refine_suggestions(results, used_optional)
        if not col or not vals:
            sess.stage = Stage.SHOW_RESULTS
            return {"text": result_to_text(results), "quick": ["やり直す", "終了"]}
        return {
            "text": f"該当 {len(results)} 件。『{col}』で更に絞り込めます。",
            "quick": assemble_quick(vals, ["やり直す", "終了"]),
        }

    sess.stage = Stage.SHOW_RESULTS
    return {"text": result_to_text(results), "quick": ["やり直す", "終了"]}


def handle_text(user_key: str, text: str) -> Dict[str, object]:
    sess = get_session(user_key)
    t = (text or "").strip()

    if t.lower() in ("id", "uid") or t in ("ユーザーid", "ユーザid"):
        # user_key が 'user:Uxxxxxxxx' 形式のときも素で渡されたときも拾えるように
        uid = user_key
        if uid.startswith("user:"):
            uid = uid[len("user:"):]
        return {"text": f"あなたの userId は:\n{uid}", "quick": ["検索"]}

    # 共通コマンド
    if t in ("終了", "exit", "quit"):
        sess.reset()
        return {"text": "終了しました。いつでも『検索』で再開できます。", "quick": ["検索"]}
    if t in ("やり直す", "reset", "リセット"):
        sess.reset()
        sess.stage = Stage.CHOOSE_TASK
        return {"text": START_TEXT, "quick": assemble_quick(ALL_UNIQUE["作業名"], ["終了"])}

    # 初回起点
    if sess.stage == Stage.IDLE:
        sess.stage = Stage.CHOOSE_TASK
        return {"text": START_TEXT, "quick": assemble_quick(ALL_UNIQUE["作業名"], ["終了"])}

    # 作業名選択
    if sess.stage == Stage.CHOOSE_TASK:
        choice = resolve_choice(t, ALL_UNIQUE["作業名"])
        if not choice:
            return {
                "text": "すみません、番号または候補から選んでください。\n" + ASK_TASK,
                "quick": assemble_quick(ALL_UNIQUE["作業名"], ["終了"]),
            }
        sess.filters["作業名"] = choice
        sess.stage = Stage.CHOOSE_BASE
        return {
            "text": f"『{choice}』を選択。\n\n" + ASK_BASE,
            "quick": assemble_quick(_unique_filtered("下地の状況", sess), ["やり直す", "終了"]),
        }

    # 下地の状況選択
    if sess.stage == Stage.CHOOSE_BASE:
        base_options = _unique_filtered("下地の状況", sess)
        choice = resolve_choice(t, base_options)
        if not choice:
            return {
                "text": "すみません、番号または候補から選んでください。\n" + ASK_BASE,
                "quick": assemble_quick(base_options, ["やり直す", "終了"]),
            }
        sess.filters["下地の状況"] = choice
        sess.stage = Stage.ASK_OPTIONAL
        return {"text": f"『{choice}』を選択。\n\n" + ASK_OPTIONAL, "quick": ["1", "2", "3", "やり直す", "終了"]}

    # 任意絞込の種類
    if sess.stage == Stage.ASK_OPTIONAL:
        n = to_int_or_none(t)
        if n == 1:
            sess.stage = Stage.CHOOSE_MACHINE_CAT
            return {
                "text": ASK_MACHINE_CAT,
                "quick": assemble_quick(_unique_filtered("機械カテゴリー", sess), ["やり直す", "終了"]),
            }
        elif n == 2:
            sess.stage = Stage.CHOOSE_MODEL
            return {
                "text": ASK_MODEL,
                "quick": assemble_quick(_unique_filtered("ライナックス機種名", sess), ["やり直す", "終了"]),
            }
        elif n == 3:
            return _do_search_and_maybe_refine(sess, used_optional=None)
        else:
            return {"text": "1/2/3 のいずれかを選んでください。\n" + ASK_OPTIONAL, "quick": ["1", "2", "3", "やり直す", "終了"]}

    # 機械カテゴリー
    if sess.stage == Stage.CHOOSE_MACHINE_CAT:
        options = _unique_filtered("機械カテゴリー", sess)
        choice = resolve_choice(t, options)
        if not choice:
            return {
                "text": "候補から選んでください。\n" + ASK_MACHINE_CAT,
                "quick": assemble_quick(options, ["やり直す", "終了"]),
            }
        sess.filters["機械カテゴリー"] = choice
        return _do_search_and_maybe_refine(sess, used_optional="機械カテゴリー")

    # 機種
    if sess.stage == Stage.CHOOSE_MODEL:
        options = _unique_filtered("ライナックス機種名", sess)
        choice = resolve_choice(t, options)
        if not choice:
            return {
                "text": "候補から選んでください。\n" + ASK_MODEL,
                "quick": assemble_quick(options, ["やり直す", "終了"]),
            }
        sess.filters["ライナックス機種名"] = choice
        return _do_search_and_maybe_refine(sess, used_optional="ライナックス機種名")

    # 多件時の追加絞込
    if sess.stage == Stage.REFINE_MORE:
        col, vals = next_refine_suggestions(sess.last_results, _used_optional(sess))
        if not col or not vals:
            sess.stage = Stage.SHOW_RESULTS
            return {"text": result_to_text(sess.last_results), "quick": ["やり直す", "終了"]}
        choice = resolve_choice(t, vals)
        if not choice:
            return {"text": f"『{col}』から選んでください。", "quick": assemble_quick(vals, ["やり直す", "終了"])}
        filtered = sess.last_results[sess.last_results[col] == choice]
        sess.last_results = filtered
        if len(filtered) >= RESULTS_REFINE_THRESHOLD:
            return {
                "text": f"『{col} = {choice}』で絞り込みました。(件数: {len(filtered)})\nさらに絞り込み可能です。",
                "quick": assemble_quick(next_refine_suggestions(filtered, _used_optional(sess))[1], ["やり直す", "終了"]),
            }
        else:
            sess.stage = Stage.SHOW_RESULTS
            return {"text": result_to_text(filtered), "quick": ["やり直す", "終了"]}

    # 結果表示中
    if sess.stage == Stage.SHOW_RESULTS:
        return {"text": "新しい検索を始めるには『やり直す』を送ってください。", "quick": ["やり直す", "終了"]}

    # フォールバック
    sess.stage = Stage.CHOOSE_TASK
    return {"text": START_TEXT, "quick": assemble_quick(ALL_UNIQUE["作業名"], ["終了"])}

# =============================
# FastAPI ルーティング
# =============================
@app.get("/")
def root():
    return {"status": "ok", "msg": APP_VERSION}

@app.api_route("/health", methods=["GET", "HEAD", "POST"])  # 監視ツール対策
def health():
    return {"ok": True}

# デバッグ用 API
# リクエスト: {"user_id": "u1", "text": "検索"}
# レスポンス: {"text": "...", "quick": ["..."]}
@app.post("/dev/run")
async def dev_run(req: Request):
    body = await req.json()
    user_id = str(body.get("user_id", "dev"))
    text = str(body.get("text", ""))

    # dev用ショートカット："id" で user_id を返す
    t = (text or "").strip()
    if t.lower() in ("id", "uid") or t in ("ユーザーid", "ユーザid"):
        return UTF8JSONResponse({"text": f"(dev) your user_id: {user_id}", "quick": []})

    out = handle_text(user_id, text)
    return UTF8JSONResponse(out)

# ====== LINE Webhook ======
if LINE_AVAILABLE and CHANNEL_ACCESS_TOKEN and CHANNEL_SECRET:
    line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
    handler = WebhookHandler(CHANNEL_SECRET)

    @app.post("/callback")
    async def callback(request: Request):
        # 1) ボディ取得
        body_bytes = await request.body()
        body_text = body_bytes.decode("utf-8")
        # 2) Verify（空 events）は即200
        try:
            data = json.loads(body_text)
            if isinstance(data, dict) and data.get("events") == []:
                return PlainTextResponse("OK", status_code=200)
        except Exception:
            # 読めない場合でも Verify などの疎通用途では 200 即返し
            return PlainTextResponse("OK", status_code=200)

        # 3) 通常イベントは署名検証
        signature = request.headers.get("X-Line-Signature", "")
        try:
            handler.handle(body_text, signature)
        except InvalidSignatureError:
            return PlainTextResponse("Invalid signature", status_code=400)
        return PlainTextResponse("OK")

    @handler.add(MessageEvent, message=TextMessage)
    def on_message(event: MessageEvent):
        # セッションキー：1:1/グループ/ルームの別を吸収
        def _source_key(src) -> str:
            uid = getattr(src, "user_id", None)
            gid = getattr(src, "group_id", None)
            rid = getattr(src, "room_id", None)
            if uid:
                return f"user:{uid}"
            if gid:
                return f"group:{gid}"
            if rid:
                return f"room:{rid}"
            return "anon"

        # === 送信元取得＆ログ
        src = event.source
        uid = getattr(src, "user_id", None)
        gid = getattr(src, "group_id", None)
        rid = getattr(src, "room_id", None)
        print(f"[EVENT] uid={uid} gid={gid} rid={rid}")

        # グループ/ルームは案内だけ返して終了（1:1のみ運用）
        if gid or rid:
            try:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage("このボットは1:1トークのみ対応です。友だちチャットでお試しください。")
                )
            finally:
                return

        raw_text = event.message.text or ""
        t = raw_text.strip()

        # (A) テスター向けショートカット：id/uid/ユーザーid
        if t.lower() in ("id", "uid") or t in ("ユーザーid", "ユーザid"):
            try:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(f"あなたの userId は:\n{uid}")
                )
            finally:
                return

        # (B) ホワイトリスト（設定時のみ有効）
        if ALLOWED_USER_IDS and uid and uid not in ALLOWED_USER_IDS:
            try:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage("このボットは招待制です（権限がありません）。")
                )
            finally:
                print(f"[DENY] uid={uid}")
                return

        # (C) 通常フロー
        user_key = _source_key(src)
        out = handle_text(user_key, raw_text)

        # QuickReply を数値送信に統一（長文は20字へ丸め）
        quick = out.get("quick", [])
        actions: List[MessageAction] = []
        pure_numeric = all(((q.strip().isdigit() and len(q.strip()) <= 2) or q in ("やり直す", "終了")) for q in quick)
        if pure_numeric:
            for lbl in quick[:MAX_QUICKREPLIES]:
                actions.append(MessageAction(label=clip_label(lbl, 20), text=lbl))
        else:
            numbered = []
            idx = 1
            for q in quick:
                if q in ("やり直す", "終了"):
                    actions.append(MessageAction(label=q, text=q))
                else:
                    if len(numbered) < MAX_QUICKREPLIES:
                        label = clip_label(f"{idx}. {q}", 20)
                        actions.append(MessageAction(label=label, text=str(idx)))
                        numbered.append(q)
                        idx += 1
            if not numbered and not actions:
                for lbl in quick[:MAX_QUICKREPLIES]:
                    actions.append(MessageAction(label=clip_label(lbl, 20), text=lbl))

        quickreply = QuickReply(items=[QuickReplyButton(action=a) for a in actions]) if actions else None
        try:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=out.get("text", ""), quick_reply=quickreply)
            )
        except Exception as e:
            print(f"[reply_error] {e}")
else:
    @app.post("/callback")
    async def callback_dummy(request: Request):
        return PlainTextResponse("LINE not configured", status_code=200)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
