# -*- coding: utf-8 -*-
"""
app.py（シンプル版 v2.9）

変更点:
- v2.6 → v2.8
  1) 深さ正規化の  を辞書版に変更（長さ不一致エラー修正）
  2) DataFrame の真偽判定エラー回避（`df or DF` をやめ、None 明示判定に統一）
  3) 深さ候補抽出を堅牢化（非文字/NaN混入でもOK）
  4) /version で BOOT ログのコミット表示はそのまま
- v2.8 → v2.9
  1) 深さ検索を堅牢化（範囲のかぶりによる検索絞りができない事態を回避）
  2) 一次／二次工程に係るペア表示を厳密化
"""
from __future__ import annotations
import os
import re
import json
import math
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
import pandas as pd

# === 表示とペア補完ユーティリティ ===
from formatters import to_plain_text  # 件数ヘッダ・空行・並び順・（ペア候補）表示＋工程ラベル
from search_core import prepare_with_pairs  # ペア候補を未絞り込み結果から補完
from postprocess import reorder_and_pair    # あれば利用（一次/二次の並び整形）

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
ALLOWED_USER_IDS = set(u.strip() for u in os.environ.get("ALLOWED_USER_IDS", "").split(",") if u.strip())

MAX_QUICKREPLIES = 12
RESULTS_REFINE_THRESHOLD = 5

# =============================
# UTF-8 JSON
# =============================
class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"
    def render(self, content: object) -> bytes:
        return json.dumps(content, ensure_ascii=False).encode("utf-8")

APP_VERSION = "app.py (simple v2.9)"
app = FastAPI(default_response_class=UTF8JSONResponse)

# === Version meta ===
import pathlib, os as _os

def _read_git_sha() -> str:
    env_sha = _os.environ.get("RENDER_GIT_COMMIT")
    if env_sha:
        return env_sha
    p = pathlib.Path(__file__).with_name("git_sha.txt")
    if p.exists():
        try:
            return p.read_text(encoding="utf-8").strip()
        except Exception:
            pass
    return "unknown"

GIT_SHA = _read_git_sha()

import logging
logger = logging.getLogger("uvicorn.error")  # Renderのログに確実に出る

@app.get("/version")
def version():
    return {"app_version": APP_VERSION, "git_sha": GIT_SHA}

@app.on_event("startup")
async def _boot_log():
    logger.info(f"[BOOT] {APP_VERSION} commit={GIT_SHA}")
    
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

def _unique_in_order(series: pd.Series) -> List[str]:
    # FutureWarning 回避: series を直接 pd.unique に
    return [x for x in pd.unique(series) if str(x) != ""]

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

def assemble_quick(options: List[str], controls: List[str] | Tuple[str, ...] = ()) -> List[str]:
    controls = [c for c in controls if c]
    limit_for_options = max(0, MAX_QUICKREPLIES - len(controls))
    return options[:limit_for_options] + controls

def clip_label(label: str, maxlen: int = 20) -> str:
    return label if len(label) <= maxlen else (label[: maxlen - 1] + "…")

# ---- 工程正規化とフラグ付与 --------------------------------
def _normalize_stage(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip().upper()
    if "単一" in s or "SINGLE" in s:
        return "SINGLE"
    if s.startswith("A") or "一次" in s or "一次工程" in s:
        return "A"
    if s.startswith("B") or "二次" in s or "二次工程" in s:
        return "B"
    return None

def _stage_hit_flag(row_stage_norm: Optional[str], stage_filter_val: Optional[str]) -> bool:
    if not stage_filter_val:
        return False
    want = _normalize_stage(stage_filter_val)
    return (want is not None) and (row_stage_norm == want)

def _annotate_stage_flags(rows: List[Dict]) -> List[Dict]:
    out = []
    for r in rows or []:
        rr = dict(r)
        rr["_stage"] = _normalize_stage(rr.get("工程数"))
        out.append(rr)
    return out
# -------------------------------------------------------------

# ---- 深さ/厚さユーティリティ -------------------------------
_NUM_RE = re.compile(r"(\d+(?:\.\d+)?)")

def _normalize_depth_str(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # 全角→半角（辞書版：1:1 マッピング）
    z2h = str.maketrans({
        "－": "-", "ー": "-",  # 長音もハイフン扱い（保険）
        "０": "0","１": "1","２": "2","３": "3","４": "4",
        "５": "5","６": "6","７": "7","８": "8","９": "9",
        "．": ".", "。": ".",  # 句点混入の保険
        "〜": "~","～": "~",   # 波ダッシュを ~ に
        "㎜": "mm",
    })
    s = s.translate(z2h).replace(" ", "")
    # ハイフン/ダッシュ類を揃える → 最後に ~ は - 扱いへ
    s = s.replace("–", "-").replace("—", "-").replace("―", "-").replace("‐", "-")
    s = s.replace("~", "-")
    # 単値なら mm を付与
    if re.fullmatch(r"\d+(?:\.\d+)?", s):
        s = s + "mm"
    # "mm" を小文字に揃える
    s = re.sub(r"(?<=\d)\s*mm$", "mm", s, flags=re.I)
    return s

def _parse_depth_range_cell(cell: str) -> Optional[Tuple[float, float]]:
    if not cell:
        return None
    t = (cell.replace("㎜", "mm")
              .replace("〜", "~").replace("～", "~")
              .replace("–", "-").replace("—", "-").replace("―", "-").replace("‐", "-").replace("−", "-"))
    # a-b
    m = re.search(r"(\d+(?:\.\d+)?)\s*[-~]\s*(\d+(?:\.\d+)?)", t)
    if m:
        lo = float(m.group(1)); hi = float(m.group(2))
        if lo > hi:
            lo, hi = hi, lo
        return (lo, hi)
    # ~b （0-b と解釈）
    m = re.search(r"^\s*~\s*(\d+(?:\.\d+)?)", t)
    if m:
        hi = float(m.group(1))
        return (0.0, hi)
    # 単値
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:mm|ミリ|ﾐﾘ)?\b", t, re.IGNORECASE)
    if m:
        v = float(m.group(1))
        return (v, v)
    return None

def _range_overlap(a: Tuple[float, float], b: Tuple[float, float]) -> bool:
    return not (a[1] <= b[0] or b[1] <= a[0])

def _depth_candidates_from_df(df: pd.DataFrame, limit: int = 8) -> List[str]:
    vals = set()
    if "処理する深さ・厚さ" in df.columns:
        for v in df["処理する深さ・厚さ"].tolist():
            n = _normalize_depth_str(v if isinstance(v, str) else str(v) if v is not None else None)
            if n:
                vals.add(n)
    # 数値下限でソート（レンジは下限→幅）
    def keyfun(x: str):
        m = re.match(r"(\d+(?:\.\d+)?)(?:-(\d+(?:\.\d+)?))?", x)
        if m:
            lo = float(m.group(1))
            hi = float(m.group(2)) if m.group(2) else lo
            return (lo, hi - lo)
        return (math.inf, 0.0)
    return sorted(vals, key=keyfun)[:limit]

def _filter_df_by_depth(df: pd.DataFrame, choice: str) -> pd.DataFrame:
    """
    深さ/厚さは『候補で選んだ表記（正規化後）と完全一致』で抽出する。
    例: choice='6.0-12.0mm' を選んだら CSV の '処理する深さ・厚さ'
        が正規化後に '6.0-12.0mm' になる行だけを返す。
    """
    if "処理する深さ・厚さ" not in df.columns:
        return df

    want = _normalize_depth_str(choice) or ""
    if not want:
        return df

    def norm_cell(x: str) -> str:
        return _normalize_depth_str(x) or ""

    mask = df["処理する深さ・厚さ"].apply(lambda x: norm_cell(x) == want)
    return df[mask]

# -------------------------------------------------------------

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
            "工程数": None,  # NEW: 工程数フィルタ保持（A/B/単一）
        }
        self.last_results: Optional[pd.DataFrame] = None
        # ペア補完用に「未絞り込み（=作業名+下地）」の直近ヒットを保持
        self.last_unfiltered_hits: List[Dict] = []
        # 多数時に提示する深さ/厚さ候補
        self.depth_options: List[str] = []
        self.depth_selected: Optional[str] = None

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
# 検索ロジック（pandasベースは維持）
# =============================
def apply_filters(df: pd.DataFrame, filters: Dict[str, Optional[str]]) -> pd.DataFrame:
    q = df.copy()
    for key, val in filters.items():
        if key in q.columns and val:
            q = q[q[key] == val]
    return q

# --- formatters に渡すための query dict を生成（リスト形式で） ---
def _make_query_for_formatters(sess: SearchSession) -> Dict[str, object]:
    def _as_list(v: Optional[str]) -> List[str]:
        return [v] if v else []
    return {
        "作業名": _as_list(sess.filters.get("作業名")),
        "下地の状況": _as_list(sess.filters.get("下地の状況")),
        "機械カテゴリー": _as_list(sess.filters.get("機械カテゴリー")),
        "ライナックス機種名": _as_list(sess.filters.get("ライナックス機種名")),
        "処理する深さ・厚さ": [],
        "作業効率評価": [],
        "工程数": _as_list(sess.filters.get("工程数")),
    }

# --- 結果のテキストを生成（ペア補完＋並び順＋件数ヘッダは formatters に任せる） ---
def _row_key(r: dict) -> tuple:
    # ヒット集合を安定識別するキー（工程や深さも含む）
    return (
        r.get("作業名",""), r.get("下地の状況",""),
        r.get("ライナックス機種名",""), r.get("使用カッター名",""),
        r.get("工程数",""), r.get("処理する深さ・厚さ","")
    )

def build_results_text(sess: SearchSession, filtered_df: pd.DataFrame) -> str:
    # いま表示対象（= 本当にヒットした行）
    cur_rows = filtered_df.to_dict(orient="records")
    cur_rows = _annotate_stage_flags(cur_rows)
    hit_keys = { _row_key(r) for r in cur_rows }

    # 「作業名＋下地」だけで抽出（ペア補完のベース）
    base_filters = {k: v for k, v in sess.filters.items() if k in ("作業名", "下地の状況")}
    prev_df = apply_filters(DF, base_filters)
    prev_rows = _annotate_stage_flags(prev_df.to_dict(orient="records"))
    sess.last_unfiltered_hits = prev_rows

    # ペア補完
    augmented = prepare_with_pairs(cur_rows, prev_rows)

    # 並べ替え（ここでカスタムキーが落ちても後で上書きする）
    try:
        augmented = reorder_and_pair(augmented)
    except Exception:
        pass

    # === 最終ラベリング ===
    # ルール：
    #  - _row_key が hit_keys に含まれる → 「検索ヒットした工程」
    #  - それ以外で A/B 工程 or _pair_candidate=True → 「検索結果とペアになる工程」
    #  - 単一工程はラベル空
    for r in augmented:
        st = (r.get("_stage") or "").upper()
        is_single = (st == "SINGLE")
        in_hits = (_row_key(r) in hit_keys)
        is_pair_flag = bool(r.get("_pair_candidate", False))
        is_pair_stage = (st in ("A","B")) or ("一次" in (r.get("工程数",""))) or ("二次" in (r.get("工程数","")))

        if is_single:
            r["_is_hit"] = in_hits
            r["_hit_stage"] = in_hits
            r["_hit_label"] = ""   # 単一はラベル空
        else:
            if in_hits:
                r["_is_hit"] = True
                r["_hit_stage"] = True
                r["_hit_label"] = "検索ヒットした工程"
            elif is_pair_flag or is_pair_stage:
                r["_is_hit"] = False
                r["_hit_stage"] = False
                r["_hit_label"] = "検索結果とペアになる工程"
            else:
                # 念のためのフォールバック（通常ここには来ない）
                r["_is_hit"] = in_hits
                r["_hit_stage"] = in_hits
                r["_hit_label"] = "検索ヒットした工程" if in_hits else ""

    qdict = _make_query_for_formatters(sess)
    return to_plain_text(augmented, qdict, explain="")

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

def next_refine_suggestions(rows: pd.DataFrame, used_optional: Optional[str]) -> Tuple[str, List[str]]:
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
# 検索→表示
# =============================
def _do_search_and_maybe_refine(sess: SearchSession, used_optional: Optional[str]) -> Dict[str, object]:
    results = apply_filters(DF, sess.filters)
    if sess.depth_selected:
        results = _filter_df_by_depth(results, sess.depth_selected)
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

        # 深さ/厚さ候補を抽出して Quick に混ぜる
        depth_opts = _depth_candidates_from_df(results)
        sess.depth_options = depth_opts[:]  # セッション保持
        quick_opts = depth_opts + (vals or [])

        if not quick_opts:
            # 何も出せない場合は即表示
            sess.stage = Stage.SHOW_RESULTS
            return {"text": build_results_text(sess, results), "quick": ["やり直す", "終了"]}

        msg = f"該当 {len(results)} 件。"
        if col and vals:
            msg += f"『{col}』で更に絞り込めます。"
        if depth_opts:
            msg += "\nまたは『深さ/厚さ』で絞り込めます。"
        return {
            "text": msg,
            "quick": assemble_quick(quick_opts, ["やり直す", "終了"]),
        }

    # 少件数はすぐ表示（ペア候補付き）
    sess.stage = Stage.SHOW_RESULTS
    return {"text": build_results_text(sess, results), "quick": ["やり直す", "終了"]}

# =============================
# テキストハンドラ
# =============================
def handle_text(user_key: str, text: str) -> Dict[str, object]:
    sess = get_session(user_key)
    t = (text or "").strip()

    if t.lower() in ("id", "uid") or t in ("ユーザーid", "ユーザid"):
        uid = user_key
        if uid.startswith("user:"):
            uid = uid[len("user:"):]
        return {"text": f"あなたの userId は:\n{uid}", "quick": ["検索"]}

    if t in ("終了", "exit", "quit"):
        sess.reset()
        return {"text": "終了しました。いつでも『検索』で再開できます。", "quick": ["検索"]}
    if t in ("やり直す", "reset", "リセット"):
        sess.reset()
        sess.stage = Stage.CHOOSE_TASK
        return {"text": START_TEXT, "quick": assemble_quick(ALL_UNIQUE["作業名"], ["終了"])}

    if sess.stage == Stage.IDLE:
        sess.stage = Stage.CHOOSE_TASK
        return {"text": START_TEXT, "quick": assemble_quick(ALL_UNIQUE["作業名"], ["終了"])}

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

    if sess.stage == Stage.CHOOSE_BASE:
        base_options = _unique_filtered("下地の状況", sess)
        choice = resolve_choice(t, base_options)
        if not choice:
            return {
                "text": "すみません、番号または候補から選んでください。\n" + ASK_BASE,
                "quick": assemble_quick(base_options, ["やり直す", "終了"]),
            }
        sess.filters["下地の状況"] = choice

        # ★ 深さ候補の抽出（作業名＋下地で限定）
        base_df = apply_filters(DF, {k:v for k,v in sess.filters.items() if k in ("作業名","下地の状況")})
        depth_opts = _depth_candidates_from_df(base_df, limit=99999)
        sess.depth_options = depth_opts[:]
        sess.depth_selected = None

        if depth_opts:
            # 深さ優先：まず深さを選ばせる
            sess.stage = "CHOOSE_DEPTH"
            msg = "深さ/厚さを選んでください\n" + "\n".join(f"{i}. {v}" for i,v in enumerate(depth_opts,1))
            return {"text": f"『{choice}』を選択。\n\n{msg}", "quick": assemble_quick(depth_opts, ["やり直す", "終了"])}

        # 深さ候補が無いときだけ任意絞り込みへ
        sess.stage = Stage.ASK_OPTIONAL
        return {"text": f"『{choice}』を選択。\n\n" + ASK_OPTIONAL, "quick": ["1", "2", "3", "やり直す", "終了"]}

    if sess.stage == "CHOOSE_DEPTH":
        opts = sess.depth_options or []
        choice = resolve_choice(t, opts)
        if not choice:
            msg = "すみません、番号または候補から選んでください。\n深さ/厚さを選んでください\n" + "\n".join(f"{i}. {v}" for i,v in enumerate(opts,1))
            return {"text": msg, "quick": assemble_quick(opts, ["やり直す", "終了"])}

        # 深さを確定
        sess.depth_selected = choice

        # 以降の検索に深さを必ず適用
        base_df = apply_filters(DF, sess.filters)
        filtered = _filter_df_by_depth(base_df, choice)
        sess.last_results = filtered

        if filtered.empty:
            sess.stage = Stage.CHOOSE_TASK
            return {
                "text": "該当がありませんでした。条件を見直してください。\n\nまず『作業名』から選び直しましょう。",
                "quick": assemble_quick(ALL_UNIQUE["作業名"], ["終了"]),
            }

        if len(filtered) >= RESULTS_REFINE_THRESHOLD:
            sess.stage = Stage.REFINE_MORE
            col, vals = next_refine_suggestions(filtered, _used_optional(sess))
            depth_now = _depth_candidates_from_df(filtered)  # 追加で別深さに切替えたい時のために表示
            quick_opts = (depth_now or []) + (vals or [])
            msg = f"該当 {len(filtered)} 件。"
            if col and vals:
                msg += f"『{col}』で更に絞り込めます。"
            if depth_now:
                msg += "\nまたは『深さ/厚さ』で絞り込めます。"
            return {"text": msg, "quick": assemble_quick(quick_opts, ["やり直す", "終了"])}

        sess.stage = Stage.SHOW_RESULTS
        return {"text": build_results_text(sess, filtered), "quick": ["やり直す", "終了"]}

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

    if sess.stage == Stage.REFINE_MORE:
        col, vals = next_refine_suggestions(sess.last_results, _used_optional(sess))

        # 直前の結果を必ず基準にする（None対策）
        base_df: pd.DataFrame = sess.last_results if (sess.last_results is not None) else DF

        # 表示候補（深さ/厚さ + 軸候補）
        depth_now = _depth_candidates_from_df(base_df)
        combined_opts = (depth_now or []) + (vals or [])

        if not combined_opts:
            sess.stage = Stage.SHOW_RESULTS
            return {"text": build_results_text(sess, base_df), "quick": ["やり直す", "終了"]}

        choice = resolve_choice(t, combined_opts)
        if not choice:
            hint = "候補から選んでください。"
            if col:
                hint = f"『{col}』または『深さ/厚さ』の候補から選んでください。"
            return {"text": hint, "quick": assemble_quick(combined_opts, ["やり直す", "終了"])}

        # === 実際の絞り込み ===
        filtered = base_df
        msg_col_part = ""

        if depth_now and (choice in depth_now):
            filtered = _filter_df_by_depth(base_df, choice)
            msg_col_part = f"深さ/厚さ ≈ {choice}"
        else:
            if col:
                filtered = base_df[base_df[col] == choice]
                msg_col_part = f"{col} = {choice}"
                if col == "工程数":
                    sess.filters["工程数"] = choice
            else:
                msg_col_part = f"{choice}"

        # ここが超重要：必ず更新して次ステップの候補計算の基準にする
        sess.last_results = filtered

        if len(filtered) >= RESULTS_REFINE_THRESHOLD:
            next_col, next_vals = next_refine_suggestions(filtered, _used_optional(sess))
            next_depth = _depth_candidates_from_df(filtered)
            return {
                "text": f"『{msg_col_part}』で絞り込みました。(件数: {len(filtered)})\nさらに絞り込み可能です。",
                "quick": assemble_quick((next_depth or []) + (next_vals or []), ["やり直す", "終了"]),
            }
        else:
            sess.stage = Stage.SHOW_RESULTS
            return {"text": build_results_text(sess, filtered), "quick": ["やり直す", "終了"]}

    if sess.stage == Stage.SHOW_RESULTS:
        return {"text": "新しい検索を始めるには『やり直す』を送ってください。", "quick": ["やり直す", "終了"]}

    sess.stage = Stage.CHOOSE_TASK
    return {"text": START_TEXT, "quick": assemble_quick(ALL_UNIQUE["作業名"], ["終了"])}

# =============================
# FastAPI ルーティング
# =============================
@app.get("/")
def root():
    return {"status": "ok", "msg": APP_VERSION}

@app.api_route("/health", methods=["GET", "HEAD", "POST"])
def health():
    return {"ok": True}

@app.post("/dev/run")
async def dev_run(req: Request):
    body = await req.json()
    user_id = str(body.get("user_id", "dev"))
    text = str(body.get("text", ""))

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
        body_bytes = await request.body()
        body_text = body_bytes.decode("utf-8")
        try:
            data = json.loads(body_text)
            if isinstance(data, dict) and data.get("events") == []:
                return PlainTextResponse("OK", status_code=200)
        except Exception:
            return PlainTextResponse("OK", status_code=200)

        signature = request.headers.get("X-Line-Signature", "")
        try:
            handler.handle(body_text, signature)
        except InvalidSignatureError:
            return PlainTextResponse("Invalid signature", status_code=400)
        return PlainTextResponse("OK")

    @handler.add(MessageEvent, message=TextMessage)
    def on_message(event: MessageEvent):
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

        src = event.source
        uid = getattr(src, "user_id", None)
        gid = getattr(src, "group_id", None)
        rid = getattr(src, "room_id", None)
        print(f"[EVENT] uid={uid} gid={gid} rid={rid}")

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

        if t.lower() in ("id", "uid") or t in ("ユーザーid", "ユーザid"):
            try:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(f"あなたの userId は:\n{uid}")
                )
            finally:
                return

        if ALLOWED_USER_IDS and uid and uid not in ALLOWED_USER_IDS:
            try:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage("このボットは招待制です（権限がありません）。")
                )
            finally:
                print(f"[DENY] uid={uid}")
                return

        user_key = _source_key(src)
        out = handle_text(user_key, raw_text)

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
