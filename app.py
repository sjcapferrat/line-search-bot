# -*- coding: utf-8 -*-
"""
app.py（v2.9）
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
# あれば利用（一次/二次の並び整形）※無くても起動可能に
try:
    from postprocess import reorder_and_pair
except Exception:
    def reorder_and_pair(rows):
        return rows

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

MAX_QUICKREPLIES = int(os.environ.get("MAX_QUICKREPLIES", "7"))  # 制御ボタンのみで使用
RESULTS_REFINE_THRESHOLD = int(os.environ.get("RESULTS_REFINE_THRESHOLD", "3"))

# =============================
# UTF-8 JSON
# =============================
class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"
    def render(self, content: object) -> bytes:
        return json.dumps(content, ensure_ascii=False).encode("utf-8")

APP_VERSION = "app.py (v2.9 depth-first & pair-label)"
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
        df[c] = df[c].fillna("").astype(str).str.strip()  # ← strip を追加（空白起因の不一致防止）
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
    """
    旧：候補も含めて上限をかけていた。
    新：候補は本文で全件列挙するため、ここでは controls（やり直す/終了等）だけ詰める想定。
    """
    controls = [c for c in controls if c]
    limit_for_controls = MAX_QUICKREPLIES
    return controls[:limit_for_controls]

def clip_label(label: str, maxlen: int = 20) -> str:
    return label if len(label) <= maxlen else (label[: maxlen - 1] + "…")

def _numbered_list(options: List[str]) -> str:
    return "\n".join(f"{i}. {opt}" for i, opt in enumerate(options, 1))

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
    # 全角→半角
    z2h = str.maketrans({
        "－": "-", "ー": "-",
        "０": "0","１": "1","２": "2","３": "3","４": "4",
        "５": "5","６": "6","７": "7","８": "8","９": "9",
        "．": ".", "。": ".",
        "〜": "~","～": "~",
        "㎜": "mm",
    })
    s = s.translate(z2h).replace(" ", "")
    s = s.replace("–", "-").replace("—", "-").replace("―", "-").replace("‐", "-")
    s = s.replace("~", "-")
    if re.fullmatch(r"\d+(?:\.\d+)?", s):
        s = s + "mm"
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
    # ~b （0-b）
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
    return not (a[1] < b[0] or b[1] < a[0])

def _depth_candidates_from_df(df: pd.DataFrame, limit: int = 99999) -> List[str]:
    """候補数制限なしに変更"""
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
    want = _normalize_depth_str(choice) or ""
    m = re.match(r"(\d+(?:\.\d+)?)(?:-(\d+(?:\.\d+)?))?", want)
    if not m:
        return df
    lo = float(m.group(1))
    hi = float(m.group(2)) if m.group(2) else lo
    def ok(cell: str) -> bool:
        rng = _parse_depth_range_cell(cell or "")
        return bool(rng and _range_overlap(rng, (lo, hi)))
    if "処理する深さ・厚さ" not in df.columns:
        return df
    mask = df["処理する深さ・厚さ"].apply(ok)
    out = df[mask]
    return out
# -------------------------------------------------------------

# =============================
# セッション管理
# =============================
class Stage:
    IDLE = "IDLE"
    CHOOSE_TASK = "CHOOSE_TASK"
    CHOOSE_BASE = "CHOOSE_BASE"
    ASK_OPTIONAL = "ASK_OPTIONAL"
    CHOOSE_DEPTH = "CHOOSE_DEPTH"   # 深さ/厚さを最優先で選ぶ
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
            "工程数": None,  # A/B/単一
        }
        self.last_results: Optional[pd.DataFrame] = None
        self.last_unfiltered_hits: List[Dict] = []  # ペア補完用（作業名+下地のみ適用の直近ヒット）
        self.depth_options: List[str] = []
        self.depth_first_mode: bool = False
        self.depth_selected: Optional[str] = None
        # ★ 追加：候補のフルセットを本文に列挙し、番号で選ばせる
        self.pending_kind: Optional[str] = None
        self.pending_options: List[str] = []

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

def _depth_opts_for_base(sess: SearchSession, limit: int = 99999) -> List[str]:
    base_filters = {k: v for k, v in sess.filters.items() if k in ("作業名", "下地の状況")}
    base_df = apply_filters(DF, base_filters)
    return _depth_candidates_from_df(base_df, limit=limit)

def _make_query_for_formatters(sess: SearchSession) -> Dict[str, object]:
    def _as_list(v: Optional[str]) -> List[str]:
        return [v] if v else []
    return {
        "作業名": _as_list(sess.filters.get("作業名")),
        "下地の状況": _as_list(sess.filters.get("下地の状況")),
        "機械カテゴリー": _as_list(sess.filters.get("機械カテゴリー")),
        "ライナックス機種名": _as_list(sess.filters.get("ライナックス機種名")),
        "処理する深さ・厚さ": _as_list(sess.depth_selected),
        "作業効率評価": [],
        "工程数": _as_list(sess.filters.get("工程数")),
    }

# --- キー化して“ヒット集合”を追跡（ペア工程のラベル分離に使用） ---
def _row_key(r: dict) -> tuple:
    return (
        r.get("作業名",""), r.get("下地の状況",""),
        r.get("ライナックス機種名",""), r.get("使用カッター名",""),
        r.get("工程数",""), r.get("処理する深さ・厚さ","")
    )

# --- 結果のテキストを生成（ペア補完＋並び順＋件数ヘッダは formatters に任せる） ---
def build_results_text(sess: SearchSession, filtered_df: pd.DataFrame) -> str:
    # 前段（未絞り込み）は「作業名＋下地」のみ適用
    base_filters = {k: v for k, v in sess.filters.items() if k in ("作業名", "下地の状況")}
    prev_df = apply_filters(DF, base_filters)
    prev_rows = prev_df.to_dict(orient="records")
    prev_rows = _annotate_stage_flags(prev_rows)
    sess.last_unfiltered_hits = prev_rows

    # 現在の表示対象（= ヒット）
    cur_rows = filtered_df.to_dict(orient="records")
    cur_rows = _annotate_stage_flags(cur_rows)
    cur_keys = { _row_key(r) for r in cur_rows }

    # ペア候補を補完
    augmented = prepare_with_pairs(cur_rows, prev_rows)

    # ★ ラベル付け：ヒット/ペアを明確化（単一はラベル無し）
    for r in augmented:
        key_in_hit = (_row_key(r) in cur_keys)
        is_single = ((_normalize_stage(r.get("工程数")) or "").upper() == "SINGLE")

        if is_single:
            r["_is_hit"] = key_in_hit
            r["_hit_stage"] = key_in_hit
            r["_hit_label"] = ""  # 単一はラベル無し
            continue

        if key_in_hit:
            r["_is_hit"] = True
            r["_hit_stage"] = True
            r["_hit_label"] = "検索ヒットした工程"
        else:
            r["_is_hit"] = False
            r["_hit_stage"] = False
            r["_hit_label"] = "検索結果とペアになる工程"

    # 一次/二次の並べ方を整える（あれば）
    try:
        augmented = reorder_and_pair(augmented)
    except Exception:
        pass

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
ASK_BASE = "下地の状況を番号で選んでください"
ASK_OPTIONAL = (
    "任意で更に絞り込みますか？\n"
    "1: 機械カテゴリーから選ぶ\n"
    "2: ライナックス機種名から選ぶ\n"
    "3: このまま検索する"
)
ASK_DEPTH = "深さ/厚さを番号で選んでください（※先に選ぶ必要があります）"
ASK_MACHINE_CAT = "機械カテゴリーを番号で選んでください"
ASK_MODEL = "ライナックス機種名を番号で選んでください"
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

def _resolve_from_pending(sess: SearchSession, text: str) -> Optional[str]:
    t = (text or "").strip()
    n = to_int_or_none(t)
    if n is not None and 1 <= n <= len(sess.pending_options):
        return sess.pending_options[n - 1]
    for opt in sess.pending_options:
        if t == opt:
            return opt
    return None

# =============================
# 検索→表示
# =============================
def _do_search_and_maybe_refine(sess: SearchSession, used_optional: Optional[str]) -> Dict[str, object]:
    results = apply_filters(DF, sess.filters)
    # 深さが選択済みなら常に適用
    if sess.depth_selected:
        results = _filter_df_by_depth(results, sess.depth_selected)
    sess.last_results = results

    # ペンディング候補はクリア
    sess.pending_kind = None
    sess.pending_options = []

    if results.empty:
        sess.stage = Stage.CHOOSE_TASK
        return {
            "text": "該当がありませんでした。条件を見直してください。\n\nまず『作業名』から選び直しましょう。",
            "quick": assemble_quick([], ["終了"]),
        }

    if len(results) >= RESULTS_REFINE_THRESHOLD:
        sess.stage = Stage.REFINE_MORE
        col, vals = next_refine_suggestions(results, used_optional=None)
        depth_opts = [] if sess.depth_selected else _depth_candidates_from_df(results)
        combined_opts = (depth_opts or []) + (vals or [])

        if not combined_opts:
            # 何も出せない場合は即表示
            sess.stage = Stage.SHOW_RESULTS
            return {"text": build_results_text(sess, results), "quick": assemble_quick([], ["やり直す", "終了"])}

        # ★ 本文に全件列挙し、番号で選ばせる
        sess.pending_kind = "REFINE"
        sess.pending_options = combined_opts[:]
        msg = [f"該当 {len(results)} 件。"]
        if depth_opts:
            msg.append("まず『深さ/厚さ』で絞り込むこともできます。")
        if col and vals:
            msg.append(f"または『{col}』で更に絞り込めます。")
        body = "\n\n" + _numbered_list(combined_opts)
        return {"text": " ".join(msg) + body, "quick": assemble_quick([], ["やり直す", "終了"])}

    # 少件数はすぐ表示（ペア候補付き）
    sess.stage = Stage.SHOW_RESULTS
    return {"text": build_results_text(sess, results), "quick": assemble_quick([], ["やり直す", "終了"])}

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
        # 作業名は従来の簡易提示のまま（必要なら番号列挙に拡張可）
        opts = ALL_UNIQUE["作業名"]
        text = START_TEXT + "\n\n" + _numbered_list(opts)
        sess.pending_kind = "作業名"
        sess.pending_options = opts[:]
        return {"text": text, "quick": assemble_quick([], ["終了"])}

    # セッション未開始 → 初期表示
    if sess.stage == Stage.IDLE:
        sess.stage = Stage.CHOOSE_TASK
        opts = ALL_UNIQUE["作業名"]
        text = START_TEXT + "\n\n" + _numbered_list(opts)
        sess.pending_kind = "作業名"
        sess.pending_options = opts[:]
        return {"text": text, "quick": assemble_quick([], ["終了"])}

    # 作業名選択（番号/テキスト両対応）
    if sess.stage == Stage.CHOOSE_TASK:
        if sess.pending_kind != "作業名" or not sess.pending_options:
            sess.pending_kind = "作業名"
            sess.pending_options = ALL_UNIQUE["作業名"][:]
        choice = _resolve_from_pending(sess, t)
        if not choice:
            text = ASK_TASK + "\n\n" + _numbered_list(sess.pending_options)
            return {"text": text, "quick": assemble_quick([], ["終了"])}
        sess.filters["作業名"] = choice

        # 次：下地の状況（全件列挙、1件ならバイパス）
        sess.stage = Stage.CHOOSE_BASE
        base_options = _unique_filtered("下地の状況", sess)
        if len(base_options) == 1:
            only = base_options[0]
            sess.filters["下地の状況"] = only
            # 深さ候補があれば深さ優先へ
            depth_opts = _depth_opts_for_base(sess)
            if depth_opts:
                sess.depth_first_mode = True
                sess.depth_selected = None
                sess.depth_options = depth_opts[:]
                sess.stage = Stage.CHOOSE_DEPTH
                if len(depth_opts) == 1:
                    # 深さも1件なら即適用して検索
                    sess.depth_selected = depth_opts[0]
                    return _do_search_and_maybe_refine(sess, used_optional=None)
                sess.pending_kind = "処理する深さ・厚さ"
                sess.pending_options = depth_opts[:]
                text = f"『{choice}』を選択。\n\n{ASK_DEPTH}\n\n" + _numbered_list(depth_opts)
                return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}
            # 深さ候補が無ければ任意絞り込み
            sess.depth_first_mode = False
            sess.depth_selected = None
            return {"text": f"『{choice} / {only}』を選択。\n\n" + ASK_OPTIONAL, "quick": ["1", "2", "3", "やり直す", "終了"]}
        else:
            sess.pending_kind = "下地の状況"
            sess.pending_options = base_options[:]
            text = f"『{choice}』を選択。\n\n{ASK_BASE}\n\n" + _numbered_list(base_options)
            return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}

    # 下地の状況
    if sess.stage == Stage.CHOOSE_BASE:
        if sess.pending_kind != "下地の状況" or not sess.pending_options:
            base_options = _unique_filtered("下地の状況", sess)
            sess.pending_kind = "下地の状況"
            sess.pending_options = base_options[:]
        choice = _resolve_from_pending(sess, t)
        if not choice:
            text = ASK_BASE + "\n\n" + _numbered_list(sess.pending_options)
            return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}
        sess.filters["下地の状況"] = choice

        # 深さ候補があれば深さ優先へ（1件なら自動決定）
        depth_opts = _depth_opts_for_base(sess)
        if depth_opts:
            sess.depth_first_mode = True
            sess.depth_selected = None
            sess.depth_options = depth_opts[:]
            sess.stage = Stage.CHOOSE_DEPTH
            if len(depth_opts) == 1:
                sess.depth_selected = depth_opts[0]
                # 深さ適用後に即検索 or 追加絞り込み
                return _do_search_and_maybe_refine(sess, used_optional=None)
            sess.pending_kind = "処理する深さ・厚さ"
            sess.pending_options = depth_opts[:]
            text = f"『{choice}』を選択。\n\n{ASK_DEPTH}\n\n" + _numbered_list(depth_opts)
            return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}
        # 深さ候補がない場合のみ通常の任意絞り込みへ
        sess.depth_first_mode = False
        sess.depth_selected = None
        sess.stage = Stage.ASK_OPTIONAL
        return {"text": f"『{choice}』を選択。\n\n" + ASK_OPTIONAL, "quick": ["1", "2", "3", "やり直す", "終了"]}

    # 深さ/厚さ（番号選択）
    if sess.stage == Stage.CHOOSE_DEPTH:
        if sess.pending_kind != "処理する深さ・厚さ" or not sess.pending_options:
            opts = sess.depth_options or _depth_opts_for_base(sess)
            sess.pending_kind = "処理する深さ・厚さ"
            sess.pending_options = opts[:]
        choice = _resolve_from_pending(sess, t)
        if not choice:
            text = ASK_DEPTH + "\n\n" + _numbered_list(sess.pending_options)
            return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}
        sess.depth_selected = choice
        # 深さを適用した状態で次へ
        return _do_search_and_maybe_refine(sess, used_optional=None)

    if sess.stage == Stage.ASK_OPTIONAL:
        # 深さ優先モードで未選択なら、まず深さへ誘導
        if sess.depth_first_mode and not sess.depth_selected:
            sess.stage = Stage.CHOOSE_DEPTH
            opts = sess.depth_options or _depth_opts_for_base(sess)
            sess.pending_kind = "処理する深さ・厚さ"
            sess.pending_options = opts[:]
            text = "まず『深さ/厚さ』を選んでください。\n\n" + _numbered_list(opts)
            return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}
        n = to_int_or_none(t)
        if n == 1:
            sess.stage = Stage.CHOOSE_MACHINE_CAT
            options = _unique_filtered("機械カテゴリー", sess)
            sess.pending_kind = "機械カテゴリー"
            sess.pending_options = options[:]
            text = ASK_MACHINE_CAT + "\n\n" + _numbered_list(options)
            return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}
        elif n == 2:
            sess.stage = Stage.CHOOSE_MODEL
            options = _unique_filtered("ライナックス機種名", sess)
            sess.pending_kind = "ライナックス機種名"
            sess.pending_options = options[:]
            text = ASK_MODEL + "\n\n" + _numbered_list(options)
            return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}
        elif n == 3:
            return _do_search_and_maybe_refine(sess, used_optional=None)
        else:
            return {"text": "1/2/3 のいずれかを選んでください。\n" + ASK_OPTIONAL, "quick": ["1", "2", "3", "やり直す", "終了"]}

    if sess.stage == Stage.CHOOSE_MACHINE_CAT:
        if sess.pending_kind != "機械カテゴリー" or not sess.pending_options:
            options = _unique_filtered("機械カテゴリー", sess)
            sess.pending_kind = "機械カテゴリー"
            sess.pending_options = options[:]
        choice = _resolve_from_pending(sess, t)
        if not choice:
            text = ASK_MACHINE_CAT + "\n\n" + _numbered_list(sess.pending_options)
            return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}
        sess.filters["機械カテゴリー"] = choice
        return _do_search_and_maybe_refine(sess, used_optional="機械カテゴリー")

    if sess.stage == Stage.CHOOSE_MODEL:
        if sess.pending_kind != "ライナックス機種名" or not sess.pending_options:
            options = _unique_filtered("ライナックス機種名", sess)
            sess.pending_kind = "ライナックス機種名"
            sess.pending_options = options[:]
        choice = _resolve_from_pending(sess, t)
        if not choice:
            text = ASK_MODEL + "\n\n" + _numbered_list(sess.pending_options)
            return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}
        sess.filters["ライナックス機種名"] = choice
        return _do_search_and_maybe_refine(sess, used_optional="ライナックス機種名")

    if sess.stage == Stage.REFINE_MORE:
        # 直前に提示した combined_opts の中から選んでもらう
        if sess.pending_kind != "REFINE" or not sess.pending_options:
            base_df: pd.DataFrame = sess.last_results if (sess.last_results is not None) else DF
            col, vals = next_refine_suggestions(base_df, _used_optional(sess))
            depth_now = [] if sess.depth_selected else _depth_candidates_from_df(base_df)
            combined_opts = (depth_now or []) + (vals or [])
            sess.pending_kind = "REFINE"
            sess.pending_options = combined_opts[:]

        choice = _resolve_from_pending(sess, t)
        if not choice:
            hint = "候補から番号で選んでください。"
            text = hint + "\n\n" + _numbered_list(sess.pending_options)
            return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}

        base_df = sess.last_results if (sess.last_results is not None) else DF
        col, vals = next_refine_suggestions(base_df, _used_optional(sess))
        depth_now = [] if sess.depth_selected else _depth_candidates_from_df(base_df)

        # === 実際の絞り込み ===
        filtered = base_df
        msg_col_part = ""

        if depth_now and (choice in depth_now):
            filtered = _filter_df_by_depth(base_df, choice)
            sess.depth_selected = choice  # 深さを固定
            msg_col_part = f"深さ/厚さ ≈ {choice}"
        else:
            if col and (choice in (vals or [])):
                filtered = base_df[base_df[col] == choice]
                msg_col_part = f"{col} = {choice}"
                if col == "工程数":
                    sess.filters["工程数"] = choice
            else:
                # 何らかの直接一致（安全側で同値フィルタを試みる）
                for c in ["機械カテゴリー", "ライナックス機種名", "作業効率評価", "工程数"]:
                    if c in base_df.columns and choice in base_df[c].unique().tolist():
                        filtered = base_df[base_df[c] == choice]
                        msg_col_part = f"{c} = {choice}"
                        if c == "工程数":
                            sess.filters["工程数"] = choice
                        break
                if not msg_col_part:
                    msg_col_part = f"{choice}"

        sess.last_results = filtered  # 次の候補計算の基準

        if len(filtered) >= RESULTS_REFINE_THRESHOLD:
            next_col, next_vals = next_refine_suggestions(filtered, _used_optional(sess))
            next_depth = [] if sess.depth_selected else _depth_candidates_from_df(filtered)
            combined_opts = (next_depth or []) + (next_vals or [])
            if combined_opts:
                sess.pending_kind = "REFINE"
                sess.pending_options = combined_opts[:]
                text = f"『{msg_col_part}』で絞り込みました。(件数: {len(filtered)})\nさらに絞り込み可能です。\n\n" + _numbered_list(combined_opts)
                return {"text": text, "quick": assemble_quick([], ["やり直す", "終了"])}
            else:
                sess.stage = Stage.SHOW_RESULTS
                return {"text": build_results_text(sess, filtered), "quick": assemble_quick([], ["やり直す", "終了"])}
        else:
            sess.stage = Stage.SHOW_RESULTS
            return {"text": build_results_text(sess, filtered), "quick": assemble_quick([], ["やり直す", "終了"])}

    if sess.stage == Stage.SHOW_RESULTS:
        return {"text": "新しい検索を始めるには『やり直す』を送ってください。", "quick": ["やり直す", "終了"]}

    # フォールバック：作業名に戻す
    sess.stage = Stage.CHOOSE_TASK
    opts = ALL_UNIQUE["作業名"]
    text = START_TEXT + "\n\n" + _numbered_list(opts)
    sess.pending_kind = "作業名"
    sess.pending_options = opts[:]
    return {"text": text, "quick": assemble_quick([], ["終了"])}

# =============================
# FastAPI ルーティング
# =============================
# "/" に HEAD も許可（監視ツール対策）
@app.api_route("/", methods=["GET", "HEAD"])
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

        # Quick Reply は制御ボタンのみ（やり直す/終了など）
        quick = out.get("quick", [])
        actions: List[MessageAction] = []
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
