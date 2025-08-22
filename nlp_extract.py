# -*- coding: utf-8 -*-
"""
nlp_extract.py — フル装備 + 最小パッチ + 下地の状況は最初からユーザー選択必須
- synonyms.yaml は「エイリアス→正規」/「正規→[エイリアス]」両対応、YAML優先でコード内最小辞書とマージ
- 正規化（㎜→mm、全角→半角、～/〜/各種ダッシュ→-、全角m/M→m）
- 深さ抽出は範囲優先→単発（unitless range も許容）
- “1語句→最も確からしい1カラムだけ”に割当て（列優先度で決定）
- 同一語句が同一カラム内の複数正規ラベルにマップされた場合は全部採用
- CSV補完モード: "off" | "literal"(既定) | "partial"
- “下地の状況”の曖昧語（アクリル/エポキシ/ウレタン）は常にユーザー選択必須
  -> filters["_needs_choice"]["下地の状況"] = {"term": <語>, "candidates": [...]}
"""

from __future__ import annotations
import os
import re
import sys
from pathlib import Path
from functools import lru_cache
from typing import Dict, List, Tuple, Any, Optional, Set

import yaml
import pandas as pd

# ------------------------------
# 列名
# ------------------------------
COLUMNS = [
    "作業ID",
    "作業名",
    "下地の状況",
    "処理する深さ・厚さ",
    "工程数",
    "機械カテゴリー",
    "ライナックス機種名",
    "使用カッター名",
    "作業効率評価",
]

# ------------------------------
# カラム優先度（小さいほど強い）
# ------------------------------
COLUMN_PRIORITY = [
    "作業名",
    "下地の状況",
    "ライナックス機種名",
    "機械カテゴリー",
    "使用カッター名",
    "工程数",
    "作業効率評価",
]
_PRIO = {c: i for i, c in enumerate(COLUMN_PRIORITY)}

# ------------------------------
# CSV補完モード（最小パッチ）
#   "off"     … 補完しない
#   "literal" … 入力トークンが正規ラベルと完全一致のみ採用（推奨・既定）
#   "partial" … 部分一致補完（広がり大）
# ------------------------------
CSV_COMPLETION_MODE = "literal"

# ------------------------------
# 「下地の状況」曖昧語は最初から選択必須に
# ------------------------------
SUBSTRATE_FORCE_CHOICE = True

# ------------------------------
# 正規化
# ------------------------------
_Z2H_MAP = {
    ord('０'): '0', ord('１'): '1', ord('２'): '2', ord('３'): '3', ord('４'): '4',
    ord('５'): '5', ord('６'): '6', ord('７'): '7', ord('８'): '8', ord('９'): '9',
    ord('．'): '.', ord('，'): ',', ord('、'): ',',
    ord('－'): '-', ord('―'): '-', ord('‐'): '-', ord('–'): '-', ord('—'): '-',
    ord('〜'): '-', ord('～'): '-',
    ord('（'): '(', ord('）'): ')',
    ord('　'): ' ',
    ord('㎜'): 'mm',
    ord('ｍ'): 'm', ord('Ｍ'): 'm',
}
def z2h(s: str) -> str:
    return s.translate(_Z2H_MAP)
def normalize(s: str) -> str:
    t = z2h(s).strip()
    t = t.replace("ｍｍ", "mm").replace("ＭＭ", "mm")
    t = re.sub(r"\s+", " ", t)
    return t

_JA_TRAILING_PARTICLES = set("をにはがへとでもや")
def _strip_trailing_particle(s: str) -> str:
    s = normalize(s)
    return s[:-1] if s and s[-1] in _JA_TRAILING_PARTICLES else s

# ------------------------------
# CSV（任意）
# ------------------------------
@lru_cache(maxsize=1)
def _df() -> pd.DataFrame:
    here = Path(__file__).resolve()
    cands = [
        os.environ.get("RAG_CSV_PATH") or "",
        str(here.parent / "restructured_file.csv"),
        str(here.parent.parent / "restructured_file.csv"),
        str(Path.cwd() / "restructured_file.csv"),
    ]
    for p in filter(None, cands):
        if os.path.exists(p):
            df = pd.read_csv(p, dtype=str, encoding="utf-8-sig", keep_default_na=False)
            for c in df.columns:
                df[c] = df[c].apply(lambda x: normalize(x) if isinstance(x, str) else x)
            return df
    return pd.DataFrame(columns=COLUMNS)

@lru_cache(maxsize=1)
def _labels_by_col() -> Dict[str, List[str]]:
    df = _df()
    labels: Dict[str, List[str]] = {}
    for col in COLUMNS:
        if col not in df.columns:
            labels[col] = []
            continue
        uniq = set()
        for raw in df[col].astype(str).tolist():
            for part in [p.strip() for p in re.split(r"[,\s、]+", raw) if p.strip()]:
                uniq.add(part)
        labels[col] = sorted(uniq)
    return labels

# ------------------------------
# synonyms.yaml 探索
# ------------------------------
def _synonyms_path() -> str:
    env = os.environ.get("RAG_SYNONYMS_PATH")
    if env and os.path.exists(env):
        return env
    here = Path(__file__).resolve()
    for p in [
        str(here.parent / "synonyms.yaml"),
        str(here.parent.parent / "synonyms.yaml"),
        str(Path.cwd() / "synonyms.yaml"),
    ]:
        if os.path.exists(p):
            return p
    return ""

# ------------------------------
# synonyms.yaml 読込（canonical→[aliases]）
# YAML: エイリアス→正規 / 正規→[エイリアス] の両対応、YAML優先でマージ
# ------------------------------
@lru_cache(maxsize=1)
def _load_synonyms() -> Dict[str, Dict[str, List[str]]]:
    code_side: Dict[str, Dict[str, List[str]]] = {
        "作業名": {
            "雨打たれ処理": ["雨打たれ", "雨うたれ"],
            "表面目荒らし": ["目荒らし", "表面荒らし", "メ荒らし", "メアラシ"],
            "表面ハツリ": ["ハツリ", "はつり", "斫り"],
            "表面研ぎ出し": ["研ぎ出し", "研出し", "とぎ出し", "磨き"],
        },
    }
    path = _synonyms_path()
    if not path:
        return code_side
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception:
        return code_side

    def _split_words(val) -> List[str]:
        if isinstance(val, list):
            return [normalize(str(w)) for w in val if str(w).strip()]
        s = normalize(str(val))
        return [w for w in re.split(r"[,\s、]+", s) if w]

    labels_by_col = _labels_by_col()
    from_yaml: Dict[str, Dict[str, List[str]]] = {}

    for col, mapping in (data or {}).items():
        if not isinstance(mapping, dict):
            continue
        from_yaml.setdefault(col, {})
        labels = set(labels_by_col.get(col, []))

        for left, right in mapping.items():
            left_n = normalize(str(left))
            right_list = _split_words(right)

            if left_n in labels:
                from_yaml[col].setdefault(left_n, [])
                for alias in right_list:
                    if alias and alias not in from_yaml[col][left_n]:
                        from_yaml[col][left_n].append(alias)
            else:
                for canonical in right_list:
                    if not canonical:
                        continue
                    from_yaml[col].setdefault(canonical, [])
                    if left_n and left_n not in from_yaml[col][canonical]:
                        from_yaml[col][canonical].append(left_n)

    merged: Dict[str, Dict[str, List[str]]] = {}
    for col in set(code_side.keys()) | set(from_yaml.keys()):
        merged[col] = {}
        for label, alist in code_side.get(col, {}).items():
            merged[col][label] = [normalize(a) for a in alist]
        for label, alist in from_yaml.get(col, {}).items():
            merged[col].setdefault(label, [])
            for a in alist:
                a_n = normalize(a)
                if a_n not in merged[col][label]:
                    merged[col][label].append(a_n)
    return merged

# ------------------------------
# 逆引き（1エイリアス→複数正規ラベル可）
# alias_index[col][alias_or_canonical] = [canonical_label, ...]
# ------------------------------
@lru_cache(maxsize=1)
def _compile_alias_index() -> Dict[str, Dict[str, List[str]]]:
    alias_index: Dict[str, Dict[str, List[str]]] = {}
    tables = _load_synonyms()
    for col, mapping in tables.items():
        idx: Dict[str, List[str]] = {}
        for canonical, aliases in mapping.items():
            key = normalize(canonical)
            idx.setdefault(key, [])
            if canonical not in idx[key]:
                idx[key].append(canonical)
            for a in aliases:
                ak = normalize(a)
                if not ak:
                    continue
                idx.setdefault(ak, [])
                if canonical not in idx[ak]:
                    idx[ak].append(canonical)
        alias_index[col] = idx
    return alias_index

# ------------------------------
# 深さ(mm) 抽出（範囲優先→単発）
# ------------------------------
_NUM = r"(?:\d+(?:\.\d+)?)"
_RE_DEPTH_RANGE = re.compile(
    rf"({_NUM})\s*-\s*({_NUM})(?:\s*(?:mm|ミリ|ﾐﾘ))?",
    re.IGNORECASE,
)
_RE_DEPTH_SINGLE_MM = re.compile(
    rf"({_NUM})\s*(?:mm|ミリ|ﾐﾘ)(?![A-Za-z0-9])",
    re.IGNORECASE,
)
def extract_depth(text: str) -> Optional[Tuple[str, float, float] | Tuple[str, float]]:
    t = normalize(text)
    m = _RE_DEPTH_RANGE.search(t)
    if m:
        lo = float(m.group(1)); hi = float(m.group(2))
        if lo > hi: lo, hi = hi, lo
        return ('range', lo, hi)
    m = _RE_DEPTH_SINGLE_MM.search(t)
    if m:
        v = float(m.group(1))
        return ('single', v)
    return None

def _extract_depth_numbers(text: str) -> List[float]:
    t = normalize(text)
    nums: List[float] = []
    for m in re.finditer(rf"\b({_NUM})\b", t):
        try: nums.append(float(m.group(1)))
        except: pass
    out: List[float] = []
    for v in nums:
        if v not in out: out.append(v)
    return out

# ------------------------------
# トークナイズ
# ------------------------------
def _tokenize_ja(text: str) -> List[str]:
    t = normalize(text)
    toks = re.findall(r"[A-Za-z0-9\.\-\+%]+|[\u3040-\u30FF\u4E00-\u9FFF]+|[^\s]", t)
    return [tok for tok in toks if tok.strip()]

# ------------------------------
# CSV 既存ラベルへの寄せ
# ------------------------------
def _canonicalize_label(col: str, label: str) -> str:
    labels = _labels_by_col().get(col, [])
    if label in labels:
        return label
    cand = [lb for lb in labels if (label in lb) or (lb in label)]
    if cand:
        cand.sort(key=len, reverse=True)
        return cand[0]
    return label

# ------------------------------
# alias を全文から収集（全カラム）
# 戻り: { alias_key: [(col, canonical_label), ...] }
# ------------------------------
def _gather_alias_hits_all_cols(text: str) -> Dict[str, List[Tuple[str, str]]]:
    idx_all = _compile_alias_index()
    t = normalize(text)
    t2 = normalize(_strip_trailing_particle(text))
    hits: Dict[str, List[Tuple[str, str]]] = {}
    for col, idx in idx_all.items():
        for alias_key, canon_list in idx.items():
            ak = normalize(alias_key)
            if not ak:
                continue
            if (ak in t) or (ak in t2):
                for canonical in canon_list:
                    hits.setdefault(ak, []).append((col, canonical))
    return hits

# ------------------------------
# CSV 補完（モード切替）
# ------------------------------
def _csv_only_match_labels(col: str, tokens: List[str], consumed_alias: Set[str]) -> List[str]:
    mode = CSV_COMPLETION_MODE
    if mode == "off":
        return []
    labels = _labels_by_col().get(col, [])
    if not labels:
        return []
    if mode == "literal":
        tset = {normalize(_strip_trailing_particle(tok)) for tok in tokens}
        return [lb for lb in labels if normalize(lb) in tset]
    # partial（従来の補完）
    hits: List[str] = []
    t_join = " ".join(tokens)
    for lb in sorted(labels, key=len, reverse=True):
        if lb and re.search(re.escape(lb), t_join):
            if lb not in hits:
                hits.append(lb)
    if not hits:
        for tok in tokens:
            k = normalize(_strip_trailing_particle(tok))
            if k in consumed_alias:
                continue
            for lb in labels:
                lb_n = normalize(lb)
                if lb_n.startswith(k) or (k and k in lb_n):
                    if lb not in hits:
                        hits.append(lb)
    return hits

# ------------------------------
# 下地の状況：曖昧語（アクリル/エポキシ/ウレタン）を検知し、必ずユーザー選択に回す
# ------------------------------
_AMBIG_SUBSTRATE = {
    "アクリル": [
        "防塵塗料（アクリル）",
        "防塵塗料（アクリル塗り重ね）",
    ],
    "エポキシ": [
        "防塵塗料（エポキシ）",
        "防塵塗料（エポキシ塗り重ね）",
        "厚膜塗料（エポキシ）",
    ],
    "ウレタン": [
        "厚膜塗料（ウレタン）",
        "厚膜塗料（水性硬質ウレタン）",
    ],
}

def _resolve_ambiguous_substrate(text: str) -> Tuple[List[str], Optional[Dict[str, Any]]]:
    """
    戻り:
      (resolved_labels, pending_choice)
      - SUBSTRATE_FORCE_CHOICE=True のときは、該当語を見つけたら常に pending_choice を返す
    """
    t = normalize(text)
    for term, cands in _AMBIG_SUBSTRATE.items():
        if term in t:
            if SUBSTRATE_FORCE_CHOICE:
                return [], {"term": term, "candidates": cands}
            # 将来、強制しない場合のヒューリスティックをここに書ける
            return [], {"term": term, "candidates": cands}
    return [], None

# ------------------------------
# 便利：作業名のみ
# ------------------------------
def find_canon_labels(text: str) -> Set[str]:
    alias_hits = _gather_alias_hits_all_cols(text)
    consumed_alias: Set[str] = set()
    out: Set[str] = set()
    for ak, cand_list in alias_hits.items():
        cand_list.sort(key=lambda x: _PRIO.get(x[0], 9999))
        win_col = cand_list[0][0]
        consumed_alias.add(ak)
        for col, label in cand_list:
            if col == win_col and col == "作業名":
                out.add(label)
    return out

# ------------------------------
# メイン：フィルタ抽出
# ------------------------------
def extract_query(text: str) -> Tuple[Dict[str, Any], str]:
    tokens = _tokenize_ja(text)

    # 深さ
    depth_info = extract_depth(text)
    depth_range: Optional[Tuple[float, float]] = None
    depth_value: Optional[float] = None
    depth_strs: List[str] = []
    if depth_info:
        if depth_info[0] == 'range':
            _, lo, hi = depth_info
            depth_range = (lo, hi); depth_strs = [f"{lo:g}", f"{hi:g}"]
        else:
            _, v = depth_info
            depth_value = v; depth_strs = [f"{v:g}"]
    else:
        nums = _extract_depth_numbers(text)
        depth_strs = [f"{d:g}" for d in nums]

    filters: Dict[str, Any] = {
        "作業名": [],
        "下地の状況": [],
        "処理する深さ・厚さ": depth_strs[:],  # 後方互換（検索側は depth_* を優先）
        "工程数": [],
        "機械カテゴリー": [],
        "ライナックス機種名": [],
        "使用カッター名": [],
        "作業効率評価": [],
    }
    if depth_range:
        filters["depth_range"] = depth_range
    if depth_value is not None:
        filters["depth_value"] = depth_value

    # --- まず「下地の状況」の曖昧語を検知：常にユーザー選択へ回す ---
    resolved_sub, pending = _resolve_ambiguous_substrate(text)
    if resolved_sub:
        for lab in resolved_sub:
            if lab not in filters["下地の状況"]:
                filters["下地の状況"].append(lab)
    if pending:
        filters.setdefault("_needs_choice", {})["下地の状況"] = pending

    # --- alias 一括収集 → 優先度で勝者カラムにだけ割当て（同一カラム内は全部） ---
    alias_hits = _gather_alias_hits_all_cols(text)
    consumed_alias: Set[str] = set()

    for ak, cand_list in alias_hits.items():
        cand_list.sort(key=lambda x: _PRIO.get(x[0], 9999))
        win_col = cand_list[0][0]

        # 下地の状況は、選択保留がある場合はここでの自動割当てを抑制
        if (win_col == "下地の状況") and (("_needs_choice" in filters) or resolved_sub):
            consumed_alias.add(ak)
            continue

        added: Set[str] = set()
        for col, label in cand_list:
            if col == win_col and label not in added:
                label2 = _canonicalize_label(col, label)
                if label2 not in filters[col]:
                    filters[col].append(label2)
                added.add(label)
        consumed_alias.add(ak)

    # --- 必要なら CSV 補完（モードに従う） ---
    for col in ["作業名", "下地の状況", "工程数", "機械カテゴリー", "ライナックス機種名", "使用カッター名", "作業効率評価"]:
        if filters[col]:
            continue
        for h in _csv_only_match_labels(col, tokens, consumed_alias):
            h2 = _canonicalize_label(col, h)
            if h2 not in filters[col]:
                filters[col].append(h2)

    # 説明文（空の列は出さない）
    parts = []
    for k in ["作業名", "下地の状況", "工程数", "機械カテゴリー", "ライナックス機種名", "使用カッター名", "作業効率評価"]:
        v = filters.get(k) or []
        if v:
            parts.append(f"{k}={v}")
    if depth_range:
        parts.append(f"深さ=range({depth_range[0]:g}-{depth_range[1]:g}mm)")
    elif depth_value is not None:
        parts.append(f"深さ=single({depth_value:g}mm)")
    elif depth_strs:
        parts.append(f"深さ≈{depth_strs}")
    if "_needs_choice" in filters and "下地の状況" in filters["_needs_choice"]:
        ch = filters["_needs_choice"]["下地の状況"]
        parts.append(f"要選択: 下地の状況={ch['candidates']} (term={ch['term']})")

    explain = "抽出条件: " + (", ".join(parts) if parts else "（該当なし）")
    return filters, explain

# 直接デバッグ
if __name__ == "__main__":
    text = " ".join(sys.argv[1:]) or "エポキシで削りたい"
    f, ex = extract_query(text)
    print(ex)
    if f.get("_needs_choice"):
        print("NEEDS_CHOICE:", f["_needs_choice"])

__all__ = [
    "z2h", "normalize",
    "extract_depth", "extract_query",
    "find_canon_labels",
]
