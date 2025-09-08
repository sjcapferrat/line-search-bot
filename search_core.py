# search_core.py
# CSV直フィルタの堅牢化 + 高レベルAPI(run_query)でUX分岐を提供
from __future__ import annotations

import os
import csv
import re
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict

# ==============================
# 定数・データクラス
# ==============================
EVAL_ORDER = {"◎": 0, "○": 1, "〇": 1, "△": 2, "": 9}

@dataclass
class SearchOutcome:
    status: str  # ok / invalid_conditions / no_results / range_out / need_refine
    singles: Optional[List[Dict[str, Any]]] = None
    pairs: Optional[List[Dict[str, Any]]] = None
    message: Optional[str] = None
    suggest_depth: Optional[float] = None  # mm（範囲外時の提案）
    total_hits: Optional[int] = None
    raw_hits: Optional[List[Dict[str, Any]]] = field(default=None)  # need_refine で利用
    depth_candidates: Optional[List[str]] = None  # 追加: 絞り込み候補（深さ/厚さ）

# ==============================
# パス・CSVロード
# ==============================
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

# ==============================
# 正規化・深さユーティリティ
# ==============================
# 数値抽出（小数対応）
_NUM_RE = re.compile(r"(\d+(?:\.\d+)?)")

def _canon_text(s: str) -> str:
    """
    比較用に正規化：全角→半角, 全角カッコ→半角, 全角空白→半角, ダッシュ/マイナス/ハイフン類・空白を除去,
    大文字小文字を同一視。→ Pg600 と Pg-600 の揺れを吸収。
    """
    if s is None:
        return ""
    t = str(s)
    # 全角→半角のざっくり
    t = t.translate(str.maketrans({"（": "(", "）": ")", "　": " "}))
    # ダッシュ/ハイフン類は除去（記号差を吸収）
    t = t.translate(str.maketrans({c: "" for c in "‐-‒–—―−-"}))
    # 空白除去
    t = re.sub(r"\s+", "", t)
    # 統一のため小文字化
    t = t.lower()
    return t

def _to_mm_value(s: str) -> Optional[float]:
    """
    '1', '1.0', '1mm', '１㎜', '1 ミリ' などを float(mm) に。
    失敗時 None。
    """
    if s is None:
        return None
    t = str(s)
    t = (
        t.replace("㎜", "mm")
         .replace("ＭＭ", "mm")
         .replace("ｍｍ", "mm")
         .replace("ｍ", "m")
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
    """
    if not cell:
        return None
    raw = str(cell)
    t = (
        raw.replace("㎜", "mm")
           .replace("〜", "~").replace("～", "~")
           .replace("–", "-").replace("—", "-").replace("―", "-").replace("‐", "-").replace("−", "-")
    )
    # 1) 範囲 a-b / a~b
    m = re.search(r"(\d+(?:\.\d+)?)\s*[-~]\s*(\d+(?:\.\d+)?)", t)
    if m:
        lo = float(m.group(1)); hi = float(m.group(2))
        if lo > hi:
            lo, hi = hi, lo
        return (lo, hi)
    # 2) 左開区間 ~b
    m = re.search(r"^\s*~\s*(\d+(?:\.\d+)?)", t)
    if m:
        hi = float(m.group(1))
        return (0.0, hi)
    # 3) 単発
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:mm|ミリ|ﾐﾘ)?\b", t, re.IGNORECASE)
    if m:
        v = float(m.group(1))
        return (v, v)
    return None

def _range_overlap(a: Tuple[float, float], b: Tuple[float, float]) -> bool:
    return not (a[1] < b[0] or b[1] < a[0])

# --- 追加：工程正規化と深さ候補抽出 ---
def _normalize_stage(raw: Optional[str]) -> Optional[str]:
    """
    行/条件の工程表現を 'SINGLE' / 'A' / 'B' に正規化。
    """
    if not raw:
        return None
    s = str(raw).strip().upper()
    # よくある表記ゆれ対応
    if "単一" in s or "SINGLE" in s:
        return "SINGLE"
    if s.startswith("A") or "一次" in s:
        return "A"
    if s.startswith("B") or "二次" in s:
        return "B"
    if "一次工程" in s:
        return "A"
    if "二次工程" in s:
        return "B"
    return None

def _normalize_depth_str(v: Optional[str]) -> Optional[str]:
    """
    深さ/厚さの表示候補を正規化（全角→半角・~/-統一・末尾mmなどを補正）
    """
    if not v:
        return None
    s = str(v).strip()
    z2h = str.maketrans("－０１２３４５６７８９．〜～", "-0123456789.~")
    s = s.translate(z2h)
    s = s.replace(" ", "")
    # 先頭のプレフィクスを軽く除去
    s = re.sub(r"^(処理する深さ・厚さ|処理深さ|厚さ)\s*[:：]?\s*", "", s)
    # レンジの ~ を - に
    s = s.replace("~", "-").replace("–", "-")
    # 単値なら mm 付与
    if re.fullmatch(r"\d+(?:\.\d+)?", s):
        s = s + "mm"
    # "mm" 統一
    s = re.sub(r"(?<=\d)\s*mm$", "mm", s, flags=re.I)
    return s

def _sort_depth_strings(vals: List[str]) -> List[str]:
    def keyfun(x: str):
        m = re.match(r"(\d+(?:\.\d+)?)(?:-(\d+(?:\.\d+)?))?", x)
        if m:
            lo = float(m.group(1))
            hi = float(m.group(2)) if m.group(2) else lo
            return (lo, hi - lo)
        return (999999.0, 0.0)
    return sorted(vals, key=keyfun)

def _collect_depth_candidates_from_rows(rows: List[Dict[str, Any]], limit: int = 8) -> List[str]:
    vals = set()
    for r in rows or []:
        v = _normalize_depth_str(r.get("処理する深さ・厚さ"))
        if v:
            vals.add(v)
    return _sort_depth_strings(list(vals))[:limit]

# ==============================
# 行マッチ（AND/OR）
# ==============================
def _cell_contains_any(row_val: str, wants: List[str]) -> bool:
    """
    カンマ区切り・全角/半角・カッコ差・ハイフン揺れを吸収して 'OR' マッチ
    """
    if not wants:
        return True
    rv_norm = _canon_text(row_val)
    parts = [_canon_text(p) for p in re.split(r"[、,]", row_val or "")]
    if not parts:
        parts = [rv_norm]

    for w in wants:
        cw = _canon_text(w)
        # 完全一致 or 要素一致
        if cw == rv_norm or cw in parts:
            return True
    return False

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

def _row_match(row: Dict[str, str], q: Dict[str, Any]) -> bool:
    # 各キーは AND、値配列は OR
    if not _cell_contains_any(row.get("下地の状況", ""),         q.get("下地の状況", [])):         return False
    if not _cell_contains_any(row.get("作業名", ""),             q.get("作業名", [])):             return False
    if not _cell_contains_any(row.get("機械カテゴリー", ""),     q.get("機械カテゴリー", [])):     return False
    if not _cell_contains_any(row.get("ライナックス機種名", ""), q.get("ライナックス機種名", [])): return False
    if not _cell_contains_any(row.get("使用カッター名", ""),     q.get("使用カッター名", [])):     return False
    if not _cell_contains_any(row.get("工程数", ""),             q.get("工程数", [])):             return False

    # 深さ条件
    depth_range = q.get("depth_range")                    # 例: (lo, hi)
    depth_value = q.get("depth_value")                    # 例: 3.0
    wants_depth_strings = q.get("処理する深さ・厚さ", [])  # 旧互換

    if not _depth_match_row(row.get("処理する深さ・厚さ", ""),
                            depth_range, depth_value, wants_depth_strings):
        return False

    # 作業効率評価（◎/○/〇/△）は OR
    if not _cell_contains_any(row.get("作業効率評価", ""),       q.get("作業効率評価", [])):       return False

    return True

# ==============================
# 並び順
# ==============================
def _sort_key(row: Dict[str, str]) -> Tuple[int, int]:
    # 単一工程を優先、次に評価（◎>○/〇>△）
    eng = row.get("工程数", "")
    k_eng = 0 if "単一" in eng else 1
    eff = (row.get("作業効率評価", "") or "").replace("〇", "○")
    k_eff = {"◎": 0, "○": 1, "△": 2}.get(eff, 9)
    return (k_eng, k_eff)

def sort_by_eval(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda r: EVAL_ORDER.get((r.get("作業効率評価", "") or ""), 9))

# ==============================
# 公開: 低レベル CSV フィルタ
# ==============================
# （自然文を直接投げたい旧経路に対応）
try:
    from search_adapter import run_query_system as _adapter_run  # type: ignore
except Exception:
    _adapter_run = None

def _stage_hit_flag_for_row(row_stage_norm: Optional[str], q: Dict[str, Any]) -> bool:
    """
    工程フィルタが指定されているときのみ、row が工程的にヒットしたかを判定。
    """
    wants: List[str] = q.get("工程数", []) or []
    if not wants:
        return False  # 工程フィルタ未指定時は False（情報として付与しない）
    # 正規化セット
    want_norms = set()
    for w in wants:
        n = _normalize_stage(w)
        if n:
            want_norms.add(n)
        # 直接「一次工程」「二次工程」「単一」等の文字列でもOK
        if "一次" in str(w):
            want_norms.add("A")
        if "二次" in str(w):
            want_norms.add("B")
        if "単一" in str(w):
            want_norms.add("SINGLE")
    if not want_norms:
        return False
    return (row_stage_norm in want_norms)

def run_query_system(query: Any) -> List[Dict[str, Any]]:
    """
    - query が dict のとき: CSV直フィルタ
        * 新キー:
            depth_range=(lo, hi)   … レンジ重なり判定
            depth_value=v          … 値の包含判定
          （従来の "処理する深さ・厚さ": ["1", "1mm", ...] も後方互換で解釈）
        * 追加の付帯情報:
            _stage: 'SINGLE'/'A'/'B'
            _hit_stage: True/False（工程フィルタにヒットしたか）
    - query が str のとき: 既存アダプタへ委譲
    """
    # 文字列（自然文）のまま来たら、従来アダプタへ
    if not isinstance(query, dict):
        if _adapter_run is None:
            raise RuntimeError("search_adapter が見つかりません。dict クエリで呼び出してください。")
        return _adapter_run(query)  # type: ignore

    rows = _load_rows()
    hits: List[Dict[str, Any]] = []
    for r in rows:
        if _row_match(r, query):
            rr = dict(r)
            stage_norm = _normalize_stage(rr.get("工程数"))
            rr["_stage"] = stage_norm
            rr["_hit_stage"] = _stage_hit_flag_for_row(stage_norm, query)
            hits.append(rr)

    # 既定の並び: 単一工程 → 評価◎→○/〇→△
    hits.sort(key=_sort_key)
    return hits

# ==============================
# 高レベル API（UX分岐）
# ==============================
def _is_query_empty(q: Dict[str, Any]) -> bool:
    """
    有効な条件が何も無ければ True。
    """
    keys_multi = ["下地の状況", "作業名", "機械カテゴリー", "ライナックス機種名",
                  "使用カッター名", "工程数", "作業効率評価", "処理する深さ・厚さ"]
    if any(q.get(k) for k in keys_multi):
        return False
    if q.get("depth_value") is not None or q.get("depth_range") is not None:
        return False
    return True

def _remove_depth(q: Dict[str, Any]) -> Dict[str, Any]:
    nq = dict(q)
    nq.pop("depth_value", None)
    nq.pop("depth_range", None)
    # 旧互換キーも除去
    if isinstance(nq.get("処理する深さ・厚さ"), list):
        nq["処理する深さ・厚さ"] = []
    return nq

def _estimate_allowed_range_without_depth(q: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    """
    深さ条件を外した状態で該当行のレンジ最小/最大を推定。
    なければ全体から推定。
    """
    # まずその他条件で絞る
    base_rows = run_query_system(_remove_depth(q))
    if not base_rows:
        # 全体から推定
        base_rows = _load_rows()

    lows: List[float] = []
    highs: List[float] = []
    for r in base_rows:
        rng = _parse_depth_range(r.get("処理する深さ・厚さ", ""))
        if rng:
            lows.append(rng[0]); highs.append(rng[1])

    if not lows or not highs:
        return None
    return (min(lows), max(highs))

def _format_rows(hits: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    いまはペアリング無しで singles にそのまま入れる。
    必要があればここでグルーピング/ペア生成を実装。
    """
    return (hits, [])

def clamp_depth(desired: float, min_mm: float, max_mm: float) -> float:
    return max(min(desired, max_mm), min_mm)

def run_query(query: Dict[str, Any]) -> SearchOutcome:
    """
    extract_query() からの dict を受け取り、UXに沿った分岐を行う高レベルAPI。
    """
    # 1) 無効条件
    if not isinstance(query, dict) or _is_query_empty(query):
        return SearchOutcome(
            status="invalid_conditions",
            message="検索条件が認識されませんでした。他の入力をお願いします。",
            total_hits=0
        )

    # 2) 検索実行
    hits = run_query_system(query)

    # 3) 深さ指定があるときに推奨レンジ外を検出（depth_value のみを対象）
    depth_value = query.get("depth_value")
    if depth_value is not None:
        est = _estimate_allowed_range_without_depth(query)
        if est is not None:
            min_mm, max_mm = est
            if not (min_mm <= depth_value <= max_mm):
                # 深さ以外の条件で再フィルタ（深さを外す）
                hits_wo_depth = run_query_system(_remove_depth(query))
                sdepth = clamp_depth(depth_value, min_mm, max_mm)
                msg = "処理する深さ・厚さが推奨する幅を超えているようです。"
                if len(hits_wo_depth) > 0:
                    msg += f" 推奨範囲内の例として {sdepth:.1f}mm があります。再検索してみますか？"
                return SearchOutcome(
                    status="range_out",
                    message=msg,
                    suggest_depth=sdepth,
                    total_hits=len(hits_wo_depth),
                    raw_hits=hits_wo_depth,
                    depth_candidates=_collect_depth_candidates_from_rows(hits_wo_depth)
                )

    # 4) 該当なし
    if not hits:
        return SearchOutcome(
            status="no_results",
            message="該当なしでした。もう一度検索条件を入れなおしてください。終了なら１または「終わり」「終了」などと入力してください。",
            total_hits=0
        )

    # 5) 多すぎる → 絞り込み or 上位5
    if len(hits) >= 10:
        return SearchOutcome(
            status="need_refine",
            message=f"検索結果数が多いです（{len(hits)}件）。他条件で絞りますか？それとも評価順の上位5件を表示しますか？",
            total_hits=len(hits),
            raw_hits=hits,
            depth_candidates=_collect_depth_candidates_from_rows(hits)
        )

    # 6) OK（表示用に整形）
    singles, pairs = _format_rows(hits)
    return SearchOutcome(
        status="ok",
        singles=sort_by_eval(singles),
        pairs=pairs,
        total_hits=len(hits),
        depth_candidates=_collect_depth_candidates_from_rows(hits)
    )

# ==============================
# 追加：ペア候補ユーティリティ（最小パッチ）
# ==============================
def _pair_key(r: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    対工程の一致判定キー。
    - 作業ID があればそれを最優先で使う（存在しなければ空文字）
    - なければ「作業名 / 下地の状況 / 処理する深さ・厚さ」で判定
    """
    work_id = str(r.get("作業ID", "") or "").strip()
    if work_id:
        return (work_id, "", "", "")
    return (
        str(r.get("作業名", "")).strip(),
        str(r.get("下地の状況", "")).strip(),
        str(r.get("処理する深さ・厚さ", "")).strip(),
        ""  # 長さ合わせ用
    )

def build_stage_index(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str, str], Dict[str, List[Dict[str, Any]]]]:
    """
    未フィルタ候補を、キーごとに 一次工程 / 二次工程 / 単一 に分類して保持。
    """
    idx: Dict[Tuple[str, str, str, str], Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: {"一次工程": [], "二次工程": [], "単一": []})
    for r in rows or []:
        key = _pair_key(r)
        stage = str(r.get("工程数", "")).strip()
        if stage == "一次工程":
            idx[key]["一次工程"].append(r)
        elif stage == "二次工程":
            idx[key]["二次工程"].append(r)
        else:
            idx[key]["単一"].append(r)
    return idx

def augment_with_pair_candidates(current_rows: List[Dict[str, Any]],
                                 previous_unfiltered_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    機械カテゴリー/機種などで絞り込んだあとの current_rows に対し、
    直前の未絞り込み候補 previous_unfiltered_rows から「反対側工程」を（ペア候補）として追記する。
    - 片側が一次工程なら二次工程を、二次工程なら一次工程を全件追加
    - 重複は抑止（完全一致シグネチャで管理）
    """
    if not current_rows or not previous_unfiltered_rows:
        return current_rows or []

    prev_idx = build_stage_index(previous_unfiltered_rows)
    augmented = list(current_rows)
    seen = set()

    for r in current_rows:
        stage = str(r.get("工程数", "")).strip()
        if stage not in ("一次工程", "二次工程"):
            continue
        key = _pair_key(r)
        want = "二次工程" if stage == "一次工程" else "一次工程"
        candidates = prev_idx.get(key, {}).get(want, [])
        for c in candidates:
            sig = (
                c.get("作業ID",""), c.get("作業名",""), c.get("下地の状況",""),
                c.get("処理する深さ・厚さ",""), c.get("工程数",""),
                c.get("ライナックス機種名",""), c.get("使用カッター名","")
            )
            if sig in seen:
                continue
            seen.add(sig)
            cc = dict(c)
            cc["_pair_candidate"] = True
            augmented.append(cc)

    return augmented

def prepare_with_pairs(filtered_hits: List[Dict[str, Any]],
                       previous_unfiltered_hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    外部（app.py等）から使う想定のラッパ。
    - ペア候補を付与
    - 既存の画面向けソート（単一優先→評価）を適用して返す
    """
    augmented = augment_with_pair_candidates(filtered_hits, previous_unfiltered_hits)
    augmented.sort(key=_sort_key)
    return augmented
