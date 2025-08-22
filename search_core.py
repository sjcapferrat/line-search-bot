# search_core.py — フィルタを堅牢化（全角/半角・カッコ差・mm表記を吸収）＋深さの数値判定を拡張
from __future__ import annotations
import os, csv, re
from typing import Dict, List, Any, Tuple, Optional

# 既存のアダプタは温存（文字列クエリのときに使用）
try:
    from search_adapter import run_query_system as _adapter_run  # type: ignore
except Exception:
    _adapter_run = None  # CSV直フィルタ専用モードでも動くように

# ------- 正規化ユーティリティ -------
def _canon_text(s: str) -> str:
    """比較用に正規化：全角→半角, 全角カッコ→半角, 全角空白→半角, 空白除去"""
    if s is None:
        return ""
    t = str(s)
    # 全角→半角のざっくり
    tbl = str.maketrans({
        "（": "(", "）": ")",
        "　": " ",
    })
    t = t.translate(tbl)
    # 空白全除去（列値比較は強めに）
    t = re.sub(r"\s+", "", t)
    return t

_NUM_RE = re.compile(r"(\d+(?:\.\d+)?)")

def _to_mm_value(s: str) -> Optional[float]:
    """
    '1', '1.0', '1mm', '１㎜', '1 ミリ' などを float(mm) に。
    失敗時 None。
    """
    if s is None:
        return None
    t = str(s)
    # 単位/記号ゆれの吸収
    t = (t.replace("㎜", "mm")
           .replace("ＭＭ", "mm")
           .replace("ｍｍ", "mm")
           .replace("ｍ", "m")  # 念のため
           # ダッシュ・チルダ類は念のため統一
           .replace("〜", "-").replace("～", "-")
           .replace("–", "-").replace("—", "-").replace("―", "-").replace("‐", "-").replace("−", "-")
    )
    m = _NUM_RE.search(t)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None

def _parse_depth_range(cell: str) -> Optional[Tuple[float, float]]:
    """
    行側の '0.4-1.0mm' / '0.5～1.0㎜' / '~7mm' / '3mm' などを (lo, hi) に。
    優先順:
      1) 範囲 a-b / a～b
      2) 左開区間 ~b / ～b （0〜b と解釈）
      3) 単発 3mm / 3 → (3, 3)
    失敗時 None。
    """
    if not cell:
        return None
    raw = str(cell)

    # 正規化（合字とダッシュ類）
    t = (raw.replace("㎜", "mm")
             .replace("〜", "~").replace("～", "~"))  # チルダ系は一旦 '~' に統一
    t = (t.replace("–", "-").replace("—", "-").replace("―", "-").replace("‐", "-").replace("−", "-"))

    # 1) 範囲 a-b / a~b
    m = re.search(r"(\d+(?:\.\d+)?)\s*[-~]\s*(\d+(?:\.\d+)?)", t)
    if m:
        lo = float(m.group(1))
        hi = float(m.group(2))
        if lo > hi:
            lo, hi = hi, lo
        return (lo, hi)

    # 2) 左開区間 ~b
    m = re.search(r"^\s*~\s*(\d+(?:\.\d+)?)", t)
    if m:
        hi = float(m.group(1))
        return (0.0, hi)

    # 3) 単発（mm 有無は問わず）→ (v, v)
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:mm|ミリ|ﾐﾘ)?\b", t, re.IGNORECASE)
    if m:
        v = float(m.group(1))
        return (v, v)

    return None

# ------- CSV ロード -------
def _csv_path() -> str:
    # 環境変数があれば最優先
    p = os.environ.get("RAG_CSV_PATH")
    if p and os.path.exists(p):
        return p
    # リポジトリ直下のデフォルト名（本ファイルの親の親）
    cand = os.path.join(os.path.dirname(os.path.dirname(__file__)), "restructured_file.csv")
    if os.path.exists(cand):
        return cand
    # 実行カレントにも置いてある場合に対応
    if os.path.exists("restructured_file.csv"):
        return "restructured_file.csv"
    raise FileNotFoundError("restructured_file.csv が見つかりません。環境変数 RAG_CSV_PATH を設定してください。")

def _load_rows() -> List[Dict[str, str]]:
    path = _csv_path()
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows

# ------- マッチ判定（文字列） -------
def _cell_contains_any(row_val: str, wants: List[str]) -> bool:
    """
    カンマ区切り・全角/半角・カッコ差を吸収して 'OR' マッチ
    """
    if not wants:
        return True
    rv = _canon_text(row_val)
    # 行側が 'A,B,C' のようなケースに対応（各要素で比較）
    parts = [_canon_text(p) for p in re.split(r"[、,]", row_val or "")]
    if not parts:
        parts = [rv]

    for w in wants:
        cw = _canon_text(w)
        # 完全一致 or 要素一致（※部分一致はここでは行わない：既存方針を踏襲）
        if cw == rv or cw in parts:
            return True
    return False

# ------- 深さ判定（数値） -------
def _range_overlap(a: Tuple[float, float], b: Tuple[float, float]) -> bool:
    return not (a[1] < b[0] or b[1] < a[0])

def _depth_match_row(row_depth_cell: str,
                     depth_range: Optional[Tuple[float, float]],
                     depth_value: Optional[float],
                     wants_depth_strings: List[str]) -> bool:
    """
    優先順:
      1) depth_range: 行レンジと重なりがあるか
      2) depth_value: 行レンジに包含されるか
      3) wants_depth_strings（後方互換）: いずれかが包含されるか
      4) いずれも未指定なら True
    """
    rng = _parse_depth_range(row_depth_cell)

    if depth_range:
        if rng is None:
            return False
        return _range_overlap(rng, depth_range)

    if depth_value is not None:
        if rng is None:
            return False
        return (rng[0] <= depth_value <= rng[1])

    if wants_depth_strings:
        if rng is None:
            return False
        lo, hi = rng
        for s in wants_depth_strings:
            v = _to_mm_value(s)
            if v is None:
                continue
            if lo <= v <= hi:
                return True
        return False

    return True

# ------- 1行マッチ -------
def _row_match(row: Dict[str, str], q: Dict[str, Any]) -> bool:
    # 各キーは AND、値配列は OR
    if not _cell_contains_any(row.get("下地の状況", ""),         q.get("下地の状況", [])):         return False
    if not _cell_contains_any(row.get("作業名", ""),             q.get("作業名", [])):             return False
    if not _cell_contains_any(row.get("機械カテゴリー", ""),     q.get("機械カテゴリー", [])):     return False
    if not _cell_contains_any(row.get("ライナックス機種名", ""), q.get("ライナックス機種名", [])): return False
    if not _cell_contains_any(row.get("使用カッター名", ""),     q.get("使用カッター名", [])):     return False
    if not _cell_contains_any(row.get("工程数", ""),           q.get("工程数", [])):           return False

    # --- 深さ条件（新キー優先／旧キーは後方互換） ---
    depth_range = q.get("depth_range")                    # 例: (lo, hi)
    depth_value = q.get("depth_value")                    # 例: 3.0
    wants_depth_strings = q.get("処理する深さ・厚さ", [])  # 旧互換（['1', '1mm', ...]）

    if not _depth_match_row(row.get("処理する深さ・厚さ", ""),
                            depth_range, depth_value, wants_depth_strings):
        return False

    # 作業効率評価（◎/○/〇/△）は OR
    if not _cell_contains_any(row.get("作業効率評価", ""),       q.get("作業効率評価", [])):       return False

    return True

# ------- ソート（単一工程を優先、評価は ◎>○/〇>△） -------
def _sort_key(row: Dict[str, str]) -> Tuple[int, int]:
    eng = row.get("工程数", "")
    # 単一工程を優先（値が小さいほど先）
    k_eng = 0 if "単一" in eng else 1
    eff = row.get("作業効率評価", "")
    eff_norm = eff.replace("〇", "○")
    rank_map = {"◎": 0, "○": 1, "△": 2}
    k_eff = rank_map.get(eff_norm, 9)
    return (k_eng, k_eff)

# ------- 公開エントリ -------
def run_query_system(query: Any) -> List[Dict[str, Any]]:
    """
    - query が dict のとき: CSV直フィルタ
        * 新キー:
            depth_range=(lo, hi)   … レンジ重なり判定
            depth_value=v          … 値の包含判定
          （従来の "処理する深さ・厚さ": ["1", "1mm", ...] も後方互換で解釈）
    - query が str のとき: 既存アダプタ（原ロジック）へ委譲
    """
    # 文字列（自然文）のまま来たら、従来アダプタへ
    if not isinstance(query, dict):
        if _adapter_run is None:
            raise RuntimeError("search_adapter が見つかりません。dict クエリで呼び出してください。")
        return _adapter_run(query)  # type: ignore

    rows = _load_rows()
    hits = [r for r in rows if _row_match(r, query)]

    # 既定の並び: 単一工程 → 評価◎→○→△
    hits.sort(key=_sort_key)

    return hits
