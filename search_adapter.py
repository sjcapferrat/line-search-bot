# search_adapter.py
# ユーザーの ver4.2_python_based_RAG_wo_GPT.py を「関数」として呼び出す薄いアダプタ
# ファイル名にドットが含まれてインポートできない場合があるため、importlib で動的ロードします。

import os
import importlib.util
from typing import Dict, List

# === 設定 ===
# 1) 環境変数 SEARCH_SCRIPT_PATH にフルパスを設定（推奨）
# 2) なければ同梱の ./ver4_2_python_based_RAG_wo_GPT.py を探します（手動で置いてください）
SEARCH_SCRIPT_PATH = os.environ.get("SEARCH_SCRIPT_PATH", os.path.abspath("./ver4_2_python_based_RAG_wo_GPT.py"))
CSV_PATH_ENV = os.environ.get("RAG_CSV_PATH")  # 任意：CSVパスを上書きしたいときに使用

def _load_user_module():
    if not os.path.exists(SEARCH_SCRIPT_PATH):
        raise FileNotFoundError(f"ユーザー検索スクリプトが見つかりません: {SEARCH_SCRIPT_PATH}\n"
                                f"環境変数 SEARCH_SCRIPT_PATH を設定するか、同梱先を確認してください。")
    spec = importlib.util.spec_from_file_location("user_rag_module", SEARCH_SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod

# グローバル初期化（1回だけ）
_mod = _load_user_module()

# CSVパスの上書き（任意）
if CSV_PATH_ENV:
    # ユーザースクリプト側に CSV_PATH がある前提
    if hasattr(_mod, "CSV_PATH"):
        _mod.CSV_PATH = CSV_PATH_ENV

# データの初回ロード
_raw_df, _norm_df = _mod.load_data(getattr(_mod, "CSV_PATH"))
_uniq = _mod.build_unique_dict(_norm_df)
_known_keywords = _mod.build_known_keywords(_raw_df)
_synonym_dict = _mod.build_auto_synonyms(_norm_df, [
    "作業名", "下地の状況", "処理する深さ・厚さ",
    "ライナックス機種名", "使用カッター名", "工程数"
])

def natural_text_to_filters(user_text: str) -> Dict[str, List[str]]:
    """自然文からフィルタ辞書を作成（ユーザー関数群をそのまま利用）"""
    filters = {cat: [] for cat in _mod.FILTER_COLUMNS}
    suggestions = _mod.suggest_filters(_uniq, user_text, _known_keywords, _synonym_dict)
    for cat in _mod.FILTER_COLUMNS:
        filters[cat].extend(suggestions.get(cat, []))
    return filters

def run_query_with_filters(filters: Dict[str, List[str]]):
    """フィルタ辞書を入力として DataFrame を返す（内容は一切改変しない）"""
    hits_norm = _mod.filter_data(_norm_df, filters)
    hits_raw = _raw_df.loc[hits_norm.index]
    return hits_raw

def run_query_system(input_query):
    """
    LINE側から呼ばれるエントリ。
    - input_query が str のとき：自然文として解釈 → フィルタ生成
    - input_query が dict のとき：そのままフィルタとみなす（キーはユーザーの列名）
    戻り値は List[dict]
    """
    if isinstance(input_query, str):
        filters = natural_text_to_filters(input_query)
    elif isinstance(input_query, dict):
        filters = {cat: [] for cat in _mod.FILTER_COLUMNS}
        for k, v in input_query.items():
            if k in filters:
                filters[k] = v if isinstance(v, list) else [v]
    else:
        raise TypeError("input_query には str（自然文）または dict（フィルタ辞書）を指定してください。")

    df = run_query_with_filters(filters)
    return df.to_dict("records")
