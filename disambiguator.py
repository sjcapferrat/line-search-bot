# -*- coding: utf-8 -*-
"""
曖昧語の事前確認＆自動確定ロジック
- detect(text) : ユーザー入力から曖昧語の確認項目リストを返す（choices, auto 付き）
- remove_triggers(text, triggers) : 曖昧語（トリガー語）を入力文から取り除く
"""

from __future__ import annotations
import re
from typing import List, Dict, Any

# --------------------------------
# 正規化（全角→半角など）: maketrans ではなく dict 版 translate を使用
# --------------------------------
_Z2H_MAP = {
    ord('０'): '0', ord('１'): '1', ord('２'): '2', ord('３'): '3', ord('４'): '4',
    ord('５'): '5', ord('６'): '6', ord('７'): '7', ord('８'): '8', ord('９'): '9',
    ord('．'): '.', ord('，'): ',', ord('、'): ',',
    ord('－'): '-', ord('―'): '-', ord('‐'): '-',
    ord('～'): '~',
    ord('（'): '(', ord('）'): ')',
    ord('　'): ' ',          # 全角スペース
    ord('㎜'): 'mm',
    ord('ｍ'): 'm', ord('Ｍ'): 'M',
}

def z2h(s: str) -> str:
    return s.translate(_Z2H_MAP)

def normalize(s: str) -> str:
    t = z2h(s).strip()
    t = t.replace("ｍｍ", "mm").replace("ＭＭ", "mm")
    t = re.sub(r"\s+", " ", t)
    return t


# --------------------------------
# 選択肢定義（曖昧語 → 質問文・対象列・候補ラベル）
# --------------------------------
def _choices(trigger: str) -> Dict[str, Any]:
    """
    trigger に応じた {question, column, choices(list[{'id','label'}])} を返す
    """
    if trigger == "アクリル":
        return {
            "question": "「アクリル」はどれですか？（複数可）",
            "column": "下地の状況",
            "choices": [
                {"id": "1", "label": "防塵塗料（アクリル）"},
                {"id": "2", "label": "防塵塗料（アクリル塗り重ね）"},
            ],
        }
    if trigger == "エポキシ":
        return {
            "question": "「エポキシ」はどれですか？（複数可）",
            "column": "下地の状況",
            "choices": [
                {"id": "1", "label": "防塵塗料（エポキシ）"},
                {"id": "2", "label": "厚膜塗料（エポキシ）"},
                {"id": "3", "label": "防塵塗料（エポキシ塗り重ね）"},
            ],
        }
    if trigger == "ウレタン":
        return {
            "question": "「ウレタン」はどれですか？（複数可）",
            "column": "下地の状況",
            "choices": [
                {"id": "1", "label": "厚膜塗料（ウレタン）"},
                {"id": "2", "label": "厚膜塗料（水性硬質ウレタン）"},
            ],
        }
    if trigger == "ハツリ":
        return {
            "question": "「ハツリ」はどれですか？（複数可）",
            "column": None,  # 複数列に跨る
            "choices": [
                {"id": "1", "label": "作業名=表面ハツリ"},
                {"id": "2", "label": "機械カテゴリー=床ハツリ機"},
                {"id": "3", "label": "機械カテゴリー=ハンディハツリ機"},
            ],
        }
    # 未定義
    return {"question": "", "column": None, "choices": []}


# --------------------------------
# 自動確定ルール
# --------------------------------
def _auto_labels_for(trigger: str, text_norm: str) -> List[str]:
    """
    より具体的な語が含まれているときは自動確定（clarify を聞かずに自動選択）
    """
    auto: List[str] = []
    if trigger == "ウレタン":
        # 「水性硬質ウレタン」 or 「水硬ウレタン」が入っていれば 2 を自動選択
        if ("水性硬質ウレタン" in text_norm) or ("水硬ウレタン" in text_norm):
            auto.append("厚膜塗料（水性硬質ウレタン）")
    return auto


# --------------------------------
# 外部 API：曖昧語の検出
# --------------------------------
def detect(text_raw: str) -> List[Dict[str, Any]]:
    """
    入力から、曖昧語（アクリル/エポキシ/ウレタン/ハツリ）が含まれていれば
    各曖昧語ごとに {trigger, question, choices, auto} を返す。
    """
    t = normalize(text_raw)
    clarifies: List[Dict[str, Any]] = []

    for trig in ("アクリル", "エポキシ", "ウレタン", "ハツリ"):
        if trig in t:
            meta = _choices(trig)
            auto = _auto_labels_for(trig, t)
            clarifies.append({
                "trigger": trig,
                "question": meta["question"],
                "choices": meta["choices"],
                "auto": auto,             # 自動選択するラベル（あれば）
                "column": meta["column"], # 参考（CLI では使わなくてもOK）
            })

    return clarifies


# --------------------------------
# 外部 API：トリガー語の除去
# --------------------------------
def remove_triggers(text_raw: str, triggers: List[str]) -> str:
    """
    ユーザー入力から、曖昧トリガー語を取り除いて返す。
    例外：『水性硬質ウレタン』『水硬ウレタン』の中の「ウレタン」は残す。
    """
    t = normalize(text_raw)

    for trig in triggers:
        if trig == "ウレタン":
            # 「水性硬質ウレタン」「水硬ウレタン」の一部でない『ウレタン』だけ除去
            t = re.sub(r"(?<!水性硬質)(?<!水硬)ウレタン", "", t)
        elif trig in ("アクリル", "エポキシ", "ハツリ"):
            t = t.replace(trig, "")
        # それ以外はそのまま

    # 余った連続スペースを整形
    t = re.sub(r"\s+", " ", t).strip()
    return t

def apply_choice_to_query(query: Dict[str, Any],
                          chosen: List[str],
                          clarify: Dict[str, Any]) -> Dict[str, Any]:
    """
    query: nlp_extract.extract_query が返す dict（検索フィルタ）
    chosen: ["1","3"] / ["all"] / ["unknown"] / ラベル文字列の混在も許容
    clarify: detect() が返す {trigger, question, choices:[{id,label}], column} 形式の dict
    """
    # deepcopyは使わずに浅いコピー＋必要箇所だけ新規オブジェクトを作成
    q: Dict[str, Any] = dict(query)

    trig = clarify.get("trigger")
    col  = clarify.get("column")     # 例: "下地の状況" / None（=複数列にまたがるケース）
    chs  = clarify.get("choices", []) or []

    # id->label マップと全ラベル
    id2label = {}
    all_labels: List[str] = []
    for c in chs:
        if not isinstance(c, dict):
            continue
        cid = c.get("id")
        lab = c.get("label")
        if lab is None:
            continue
        lab = str(lab)
        all_labels.append(lab)
        if cid is not None:
            id2label[str(cid)] = lab

    # chosen の正規化
    normalized = {str(x).strip().lower() for x in chosen if str(x).strip()}
    if "all" in normalized:
        labels = all_labels
    elif "unknown" in normalized:
        labels = []  # 何も追加しない（＝曖昧語を実質スキップ）
    else:
        labels: List[str] = []
        for x in chosen:
            sx = str(x).strip()
            if not sx:
                continue
            # 番号指定なら対応ラベルに展開、ラベル指定ならそのまま
            labels.append(id2label.get(sx, sx))
        labels = [s for s in labels if s]

    # `_needs_choice` を “新しい dict” にして掃除（原本の参照は触らない）
    needs_orig = q.get("_needs_choice")
    if isinstance(needs_orig, dict):
        needs_new = dict(needs_orig)
        if col:
            needs_new.pop(col, None)
        else:
            # カラム未指定の曖昧解消（例: ハツリ）では既知のキーを念のため掃除
            needs_new.pop("下地の状況", None)
        if needs_new:
            q["_needs_choice"] = needs_new
        else:
            q.pop("_needs_choice", None)

    # リストを必ず新規作成してから代入（＝原本のリストを共有しない）
    def _append(qkey: str, vals: List[str]) -> None:
        if not vals:
            return
        existed = list(q.get(qkey) or [])
        for v in vals:
            if v not in existed:
                existed.append(v)
        q[qkey] = existed

    # 反映：単一カラム or 複数カラム
    if col:
        # 例: col="下地の状況" にラベル群を追加
        _append(col, labels)
        return q

    # 複数カラム（例: "作業名=表面ハツリ", "機械カテゴリー=床ハツリ機" 形式）
    for lab in labels:
        if "=" in lab:
            k, v = lab.split("=", 1)
            _append(k.strip(), [v.strip()])

    return q
