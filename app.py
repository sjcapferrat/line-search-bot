# -*- coding: utf-8 -*-
"""
app.py（シンプル版 v2.0）

目的:
- 入力フローを「選択式」に限定し、自由入力の複雑さを排除
- 絞り込み条件は必須2項目 + 任意1項目まで
  必須: 「作業名」「下地の状況」
  任意: 「機械カテゴリー」 または 「ライナックス機種名」(どちらか一方)
- 検索結果が0件 → これまで同様にやり直し誘導
- 検索結果が5件以上 → その結果集合から更なる絞り込み候補を提示して誘導

補足:
- LINEのWebhook(/callback)と、ローカル検証用の簡易デバッグAPI(/dev/run)の両対応。
- セッションは簡易にインメモリ。実運用ではRedis等を推奨。
- データはCSV(UTF-8)を想定。環境変数RAG_CSV_PATHで差し替え可能。

依存:
- fastapi
- uvicorn
- pandas
- (任意) line-bot-sdk  # 実運用時のみ必要

起動例:
  uvicorn app:app --reload --port 8000

"""
from __future__ import annotations
import os
import re
import json
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

import pandas as pd

try:
    # 実運用でLINEボットを使う場合のみ必要
    from linebot import LineBotApi, WebhookHandler
    from linebot.exceptions import InvalidSignatureError
    from linebot.models import MessageEvent, TextMessage, TextSendMessage, QuickReply, QuickReplyButton, MessageAction
    LINE_AVAILABLE = True
except Exception:
    LINE_AVAILABLE = False

# =============================
# 設定
# =============================
CSV_PATH = os.environ.get("RAG_CSV_PATH", "restructured_file.csv")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")

MAX_QUICKREPLIES = 12  # LINEのQuickReplyは最大13(端末差あり)なので余裕を見て12
RESULTS_REFINE_THRESHOLD = 5

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
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"CSV列が不足: {missing}")
    # 文字列化 + 欠損処理
    for c in REQUIRED_COLUMNS:
        df[c] = df[c].fillna("").astype(str)
    return df

DF = load_dataframe(CSV_PATH)

# ユニーク値（全体）: CSV出現順
def _unique_in_order(series: pd.Series) -> List[str]:
    # pandas.unique は最初の出現順を保持する
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
    """全角数字対応のint変換。失敗したらNone"""
    text = (text or "").strip().translate(ZEN2HAN_TABLE)
    if re.fullmatch(r"\d+", text):
        try:
            return int(text)
        except Exception:
            return None
    return None

# =============================
# セッション管理
# =============================
class Stage:
    IDLE = "IDLE"                     # 初期状態
    CHOOSE_TASK = "CHOOSE_TASK"       # 作業名 選択
    CHOOSE_BASE = "CHOOSE_BASE"       # 下地の状況 選択
    ASK_OPTIONAL = "ASK_OPTIONAL"     # 任意絞込の種類選択
    CHOOSE_MACHINE_CAT = "CHOOSE_MACHINE_CAT"  # 機械カテゴリー 選択
    CHOOSE_MODEL = "CHOOSE_MODEL"              # ライナックス機種名 選択
    SHOW_RESULTS = "SHOW_RESULTS"     # 結果表示
    REFINE_MORE = "REFINE_MORE"       # 結果が多い → 追加絞込

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

def get_session(user_id: str) -> SearchSession:
    sess = SESSIONS.get(user_id)
    if not sess:
        sess = SearchSession()
        SESSIONS[user_id] = sess
    return sess

# =============================
# メッセージ生成（LINE QuickReply 互換）
# =============================

def make_quick_choices(labels: List[str]) -> List[str]:
    """12個に丸める"""
    return labels[:MAX_QUICKREPLIES]

# =============================
# 検索ロジック
# =============================

def apply_filters(df: pd.DataFrame, filters: Dict[str, Optional[str]]) -> pd.DataFrame:
    q = df.copy()
    for key, val in filters.items():
        if key not in q.columns:
            continue
        if val:
            q = q[q[key] == val]
    return q


def result_to_text(rows: pd.DataFrame, limit: int = 20) -> str:
    if rows.empty:
        return "該当がありませんでした。選び直してください。"
    # 見やすい主要列だけ表示
    view = rows[[
        "作業名",
        "下地の状況",
        "ライナックス機種名",
        "使用カッター名",
        "工程数",
        "作業効率評価",
        "処理する深さ・厚さ",
    ]].head(limit)
    lines = [
        "=== 検索結果 ===",
    ]
    for _, r in view.iterrows():
        lines.append(
            f"・{r['作業名']}｜{r['下地の状況']}｜{r['ライナックス機種名']}｜{r['使用カッター名']}｜{r['工程数']}｜{r['作業効率評価']}｜{r['処理する深さ・厚さ']}"
        )
    count = len(rows)
    if count > limit:
        lines.append(f"(他 {count - limit} 件)")
    return "\n".join(lines)


def next_refine_suggestions(rows: pd.DataFrame, used_optional: Optional[str]) -> Tuple[str, List[str]]:
    """追加絞込候補を生成。used_optionalは "機械カテゴリー" or "ライナックス機種名" or None"""
    # まだ使ってない方を優先
    candidates_order = []
    if used_optional == "機械カテゴリー":
        candidates_order = ["ライナックス機種名", "作業効率評価", "工程数"]
    elif used_optional == "ライナックス機種名":
        candidates_order = ["機械カテゴリー", "作業効率評価", "工程数"]
    else:
        candidates_order = ["機械カテゴリー", "ライナックス機種名", "作業効率評価", "工程数"]

    for col in candidates_order:
        vals = [x for x in rows[col].unique().tolist() if x]
        if len(vals) >= 2:  # 候補が1つだけなら意味が薄い
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

DONE_AND_OPTIONS = "この条件で検索しました。必要なら更に絞り込めます。"


def handle_text(user_id: str, text: str) -> Dict[str, object]:
    """会話のメインハンドラ。戻り値は {"text": str, "quick": [str, ...]} の形。"""
    sess = get_session(user_id)
    t = (text or "").strip()

    # 共通コマンド
    if t in ("終了", "exit", "quit"):
        sess.reset()
        return {"text": "終了しました。いつでも『検索』で再開できます。", "quick": ["検索"]}
    if t in ("やり直す", "reset", "リセット"):
        sess.reset()
        sess.stage = Stage.CHOOSE_TASK
        return {"text": WELCOME + "\n\n" + ASK_TASK, "quick": make_quick_choices(ALL_UNIQUE["作業名"]) + ["終了"]}

    # 初回起点
    if sess.stage == Stage.IDLE:
        sess.stage = Stage.CHOOSE_TASK
        return {"text": WELCOME + "\n\n" + ASK_TASK, "quick": make_quick_choices(ALL_UNIQUE["作業名"]) + ["終了"]}

    # 作業名選択
    if sess.stage == Stage.CHOOSE_TASK:
        choice = resolve_choice(t, ALL_UNIQUE["作業名"])
        if not choice:
            return {"text": "すみません、番号または候補から選んでください。\n" + ASK_TASK,
                    "quick": make_quick_choices(ALL_UNIQUE["作業名"]) + ["終了"]}
        sess.filters["作業名"] = choice
        sess.stage = Stage.CHOOSE_BASE
        return {"text": f"『{choice}』を選択。\n\n" + ASK_BASE,
                "quick": make_quick_choices(_unique_filtered("下地の状況", sess)) + ["やり直す", "終了"]}

    # 下地の状況選択
    if sess.stage == Stage.CHOOSE_BASE:
        base_options = _unique_filtered("下地の状況", sess)
        choice = resolve_choice(t, base_options)
        if not choice:
            return {"text": "すみません、番号または候補から選んでください。\n" + ASK_BASE,
                    "quick": make_quick_choices(base_options) + ["やり直す", "終了"]}
        sess.filters["下地の状況"] = choice
        sess.stage = Stage.ASK_OPTIONAL
        return {"text": f"『{choice}』を選択。\n\n" + ASK_OPTIONAL,
                "quick": ["1", "2", "3", "やり直す", "終了"]}

    # 任意絞込の種類選択
    if sess.stage == Stage.ASK_OPTIONAL:
        n = to_int_or_none(t)
        if n == 1:
            sess.stage = Stage.CHOOSE_MACHINE_CAT
            return {"text": ASK_MACHINE_CAT,
                    "quick": make_quick_choices(_unique_filtered("機械カテゴリー", sess)) + ["やり直す", "終了"]}
        elif n == 2:
            sess.stage = Stage.CHOOSE_MODEL
            return {"text": ASK_MODEL,
                    "quick": make_quick_choices(_unique_filtered("ライナックス機種名", sess)) + ["やり直す", "終了"]}
        elif n == 3:
            # 任意絞込なしで検索
            return _do_search_and_maybe_refine(sess, used_optional=None)
        else:
            return {"text": "1/2/3 のいずれかを選んでください。\n" + ASK_OPTIONAL,
                    "quick": ["1", "2", "3", "やり直す", "終了"]}

    # 機械カテゴリー選択
    if sess.stage == Stage.CHOOSE_MACHINE_CAT:
        options = _unique_filtered("機械カテゴリー", sess)
        choice = resolve_choice(t, options)
        if not choice:
            return {"text": "候補から選んでください。\n" + ASK_MACHINE_CAT,
                    "quick": make_quick_choices(options) + ["やり直す", "終了"]}
        sess.filters["機械カテゴリー"] = choice
        return _do_search_and_maybe_refine(sess, used_optional="機械カテゴリー")

    # 機種選択
    if sess.stage == Stage.CHOOSE_MODEL:
        options = _unique_filtered("ライナックス機種名", sess)
        choice = resolve_choice(t, options)
        if not choice:
            return {"text": "候補から選んでください。\n" + ASK_MODEL,
                    "quick": make_quick_choices(options) + ["やり直す", "終了"]}
        sess.filters["ライナックス機種名"] = choice
        return _do_search_and_maybe_refine(sess, used_optional="ライナックス機種名")

    # 多件時の追加絞込（REFINE_MORE）
    if sess.stage == Stage.REFINE_MORE:
        # 前回提案した列名と候補はlast_resultsに基づき都度再計算
        col, vals = next_refine_suggestions(sess.last_results, _used_optional(sess))
        if not col or not vals:
            # もう絞れない → そのまま結果表示
            sess.stage = Stage.SHOW_RESULTS
            return {"text": result_to_text(sess.last_results), "quick": ["やり直す", "終了"]}
        choice = resolve_choice(t, vals)
        if not choice:
            return {"text": f"『{col}』から選んでください。",
                    "quick": make_quick_choices(vals) + ["やり直す", "終了"]}
        # 一時的な絞込（オプション列に限らず一段追加）
        filtered = sess.last_results[sess.last_results[col] == choice]
        sess.last_results = filtered
        if len(filtered) >= RESULTS_REFINE_THRESHOLD:
            # さらに絞る
            return {"text": f"『{col} = {choice}』で絞り込みました。(件数: {len(filtered)})\nさらに絞り込み可能です。",
                    "quick": make_quick_choices(next_refine_suggestions(filtered, _used_optional(sess))[1]) + ["やり直す", "終了"]}
        else:
            sess.stage = Stage.SHOW_RESULTS
            return {"text": result_to_text(filtered), "quick": ["やり直す", "終了"]}

    # 結果表示中に他入力が来た場合はやり直し提案
    if sess.stage == Stage.SHOW_RESULTS:
        return {"text": "新しい検索を始めるには『やり直す』を送ってください。", "quick": ["やり直す", "終了"]}

    # フォールバック
    sess.stage = Stage.CHOOSE_TASK
    return {"text": WELCOME + "\n\n" + ASK_TASK, "quick": make_quick_choices(ALL_UNIQUE["作業名"]) + ["終了"]}


def _used_optional(sess: SearchSession) -> Optional[str]:
    if sess.filters.get("機械カテゴリー"):
        return "機械カテゴリー"
    if sess.filters.get("ライナックス機種名"):
        return "ライナックス機種名"
    return None


def _do_search_and_maybe_refine(sess: SearchSession, used_optional: Optional[str]) -> Dict[str, object]:
    results = apply_filters(DF, sess.filters)
    sess.last_results = results

    if results.empty:
        sess.stage = Stage.CHOOSE_TASK
        # 代表的な候補を提示（全体ユニークから上位を表示）
        suggestion = make_quick_choices(ALL_UNIQUE["作業名"])  # シンプルに作業名を先頭に
        return {"text": "該当がありませんでした。条件を見直してください。\n\nまず『作業名』から選び直しましょう。",
                "quick": suggestion + ["終了"]}

    if len(results) >= RESULTS_REFINE_THRESHOLD:
        sess.stage = Stage.REFINE_MORE
        col, vals = next_refine_suggestions(results, used_optional)
        if not col or not vals:
            # 絞込候補がない→そのまま表示
            sess.stage = Stage.SHOW_RESULTS
            return {"text": result_to_text(results), "quick": ["やり直す", "終了"]}
        return {"text": f"該当 {len(results)} 件。『{col}』で更に絞り込めます。",
                "quick": make_quick_choices(vals) + ["やり直す", "終了"]}

    # 件数が少ないのでそのまま表示
    sess.stage = Stage.SHOW_RESULTS
    return {"text": result_to_text(results), "quick": ["やり直す", "終了"]}


def _unique_filtered(column: str, sess: SearchSession) -> List[str]:
    """既に選択済みの必須条件を考慮して候補を絞ったユニーク値を返す。"""
    tmp_filters = {k: v for k, v in sess.filters.items() if v and k in ("作業名", "下地の状況")}
    sub = apply_filters(DF, tmp_filters)
    vals = [x for x in sub[column].unique().tolist() if x]
    if not vals:
        # もし空なら全体ユニーク
        vals = ALL_UNIQUE[column]
    return vals


def resolve_choice(text: str, options: List[str]) -> Optional[str]:
    """ユーザー入力から選択肢を決定。数字もラベルも受け付ける。"""
    t = (text or "").strip()
    n = to_int_or_none(t)
    if n is not None:
        idx = n - 1
        if 0 <= idx < len(options):
            return options[idx]
        return None
    # ラベル一致
    for opt in options:
        if t == opt:
            return opt
    return None

# =============================
# FastAPI ルーティング
# =============================
# UTF-8 を明示して JSON を返す（PowerShell 5.1 の既知の文字化け対策）
class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"
    def render(self, content: object) -> bytes:
        return json.dumps(content, ensure_ascii=False).encode("utf-8")

app = FastAPI(default_response_class=UTF8JSONResponse)

@app.get("/")
def root():
    return {"status": "ok", "msg": "app.py (simple v2.0)"}

# ---- デバッグ用: /dev/run ----
# リクエスト: {"user_id": "u1", "text": "検索"}
# レスポンス: {"text": "...", "quick": ["..."]}
@app.post("/dev/run")
async def dev_run(req: Request):
    body = await req.json()
    user_id = str(body.get("user_id", "dev"))
    text = str(body.get("text", ""))
    out = handle_text(user_id, text)
    return UTF8JSONResponse(out)

# ---- LINE Webhook ----
if LINE_AVAILABLE and CHANNEL_ACCESS_TOKEN and CHANNEL_SECRET:
    line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
    handler = WebhookHandler(CHANNEL_SECRET)

    @app.post("/callback")
    async def callback(request: Request):
        signature = request.headers.get('X-Line-Signature', '')
        body = await request.body()
        body_text = body.decode('utf-8')
        try:
            handler.handle(body_text, signature)
        except InvalidSignatureError:
            return PlainTextResponse("Invalid signature", status_code=400)
        return PlainTextResponse('OK')

    @handler.add(MessageEvent, message=TextMessage)
    def on_message(event: MessageEvent):
        user_id = event.source.user_id or "anon"
        text = event.message.text or ""
        out = handle_text(user_id, text)
        quick = out.get("quick", [])
        actions = [MessageAction(label=lbl, text=lbl) for lbl in quick[:MAX_QUICKREPLIES]]
        quickreply = QuickReply(items=[QuickReplyButton(action=a) for a in actions]) if actions else None
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=out.get("text", ""), quick_reply=quickreply)
        )
else:
    # ダミーの/callback（LINE未設定でも起動できるように）
    @app.post("/callback")
    async def callback_dummy(request: Request):
        return PlainTextResponse('LINE not configured', status_code=200)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
