# postprocess.py — 結果の並べ替え＆工程ペア展開（不足ペアはCSVから補完）
# 仕様:
# - 全体: 単一工程を先に、次にペア（一次→二次）
# - グループ: (作業名, 下地の状況, 処理する深さ・厚さ) 単位でまとめる
# - 同一グループ内: 一次は「全部」効率順、二次も「全部」効率順で並べる
# - 片方しかヒットしていない場合は CSV 全体からもう片方を「補完」して必ずペア表示
# - 効率: ◎ > ○/〇 > △ （'〇' と '○' は同値）
# - 重複は作業ID優先で除去
from typing import List, Dict, Any, Tuple
import os, csv

EFF_RANK_MAP = {
    "◎": 3,
    "○": 2,
    "〇": 2,  # 全角の丸
    "△": 1,
}

def eff_rank(val: str) -> int:
    v = (val or "").strip()
    return EFF_RANK_MAP.get(v, 0)

def canon_stage(s: str) -> str:
    """工程数の正規化: 単一/一次/二次 の3分類へ"""
    if not s:
        return ""
    t = str(s)
    if "単一" in t:
        return "単一"
    if "一次" in t:
        return "一次"
    if "二次" in t:
        return "二次"
    return t

def _row_key_for_group(r: Dict[str, Any]) -> Tuple[str, str, str]:
    return (
        str(r.get("作業名", "") or "").strip(),
        str(r.get("下地の状況", "") or "").strip(),
        str(r.get("処理する深さ・厚さ", "") or "").strip(),
    )

def _row_id(r: Dict[str, Any]) -> Tuple:
    """重複除去用キー。作業IDがあれば優先、無ければ主要列で近似キー。"""
    sid = r.get("作業ID")
    if sid is not None:
        return ("ID", str(sid))
    return (
        "K",
        str(r.get("作業名", "")),
        str(r.get("下地の状況", "")),
        str(r.get("処理する深さ・厚さ", "")),
        str(r.get("工程数", "")),
        str(r.get("機械カテゴリー", "")),
        str(r.get("ライナックス機種名", "")),
        str(r.get("使用カッター名", "")),
        str(r.get("作業効率評価", "")),
    )

def _sort_key_eff(r: Dict[str, Any]) -> Tuple[int, str, str]:
    """効率降順 → その後は機種名・カッター名で安定化"""
    return (-eff_rank(r.get("作業効率評価", "")),
            str(r.get("ライナックス機種名", "")),
            str(r.get("使用カッター名", "")))

# ------------ CSV全体のグループ索引（一次/二次）を作る ------------
_ALL_GROUPS: Dict[Tuple[str, str, str], Dict[str, List[Dict[str, Any]]]] | None = None

def _read_csv_rows(path: str) -> List[Dict[str, Any]]:
    # エンコーディングはUTF-8優先、失敗時にcp932を試す
    for enc in ("utf-8", "utf-8-sig", "cp932"):
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except Exception:
            continue
    return []

def _build_all_groups() -> Dict[Tuple[str, str, str], Dict[str, List[Dict[str, Any]]]]:
    path = os.environ.get("RAG_CSV_PATH", "./restructured_file.csv")
    rows = _read_csv_rows(path)
    groups: Dict[Tuple[str, str, str], Dict[str, List[Dict[str, Any]]]] = {}
    for r in rows:
        stage = canon_stage(r.get("工程数", ""))
        if stage not in ("一次", "二次"):
            continue  # 単一はペア補完対象外
        key = _row_key_for_group(r)
        bucket = groups.setdefault(key, {"一次": [], "二次": []})
        bucket[stage].append(r)
    return groups

def _get_all_groups():
    global _ALL_GROUPS
    if _ALL_GROUPS is None:
        _ALL_GROUPS = _build_all_groups()
    return _ALL_GROUPS

# ------------ 並べ替え & ペア補完の本体 ------------
def reorder_and_pair(rows: List[Dict[str, Any]],
                     query_text: str = "",
                     filters_effective: Dict[str, List[str]] | None = None
                     ) -> List[Dict[str, Any]]:
    if not rows:
        return []

    all_groups = _get_all_groups()

    # 1) ヒット結果をグルーピング
    groups_hit: Dict[Tuple[str, str, str], Dict[str, List[Dict[str, Any]]]] = {}
    singles_all: List[Dict[str, Any]] = []
    for r in rows:
        stage = canon_stage(r.get("工程数", ""))
        if stage == "単一":
            singles_all.append(r)
            continue
        key = _row_key_for_group(r)
        bucket = groups_hit.setdefault(key, {"一次": [], "二次": []})
        if stage == "一次":
            bucket["一次"].append(r)
        elif stage == "二次":
            bucket["二次"].append(r)
        else:
            singles_all.append(r)

    # 2) 欠けている工程を CSV 全体から補完（同一グループのみ）
    for key, parts in list(groups_hit.items()):
        full = all_groups.get(key)
        if not full:
            continue
        if not parts["一次"] and full["一次"]:
            parts["一次"].extend(full["一次"])
        if not parts["二次"] and full["二次"]:
            parts["二次"].extend(full["二次"])

    # 3) 単一工程は効率順で並べ
    singles_all.sort(key=_sort_key_eff)

    # 4) ペア展開: 各グループで一次（全部,効率順）→二次（全部,効率順）
    paired_all: List[Dict[str, Any]] = []
    for key, parts in groups_hit.items():
        primary = sorted(parts.get("一次", []), key=_sort_key_eff)
        second  = sorted(parts.get("二次", []), key=_sort_key_eff)
        paired_all.extend(primary)
        paired_all.extend(second)

    # 5) 連結 & 重複除去
    out: List[Dict[str, Any]] = []
    seen = set()
    for r in singles_all + paired_all:
        k = _row_id(r)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)

    return out
