# search_adapter.py（遅延ロード版）
# 起動時は一切ファイルを読まず、最初の検索時にだけ
# ユーザーのスクリプト＆CSVをロードします。

import os, importlib.util
from typing import Dict, List, Tuple

SEARCH_SCRIPT_PATH = os.environ.get(
    "SEARCH_SCRIPT_PATH",
    os.path.abspath("./ver4_2_python_based_RAG_wo_GPT.py")
)
CSV_PATH_ENV = os.environ.get("RAG_CSV_PATH")

_mod = None
_ctx = None  # (_raw_df, _norm_df, _uniq, _known_keywords, _synonym_dict)

def _load_user_module():
    """ユーザースクリプトを動的ロード（未ロードならロード）"""
    global _mod
    if _mod is not None:
        return _mod
    if not os.path.exists(SEARCH_SCRIPT_PATH):
        raise FileNotFoundError(
            f"ユーザー検索スクリプトが見つかりません: {SEARCH_SCRIPT_PATH}"
        )
    spec = importlib.util.spec_from_file_location("user_rag_module", SEARCH_SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)

    # 必要なら CSV_PATH を上書き
    if CSV_PATH_ENV and hasattr(mod, "CSV_PATH"):
        mod.CSV_PATH = CSV_PATH_ENV

    _mod = mod
    return mod

def _ensure_context():
    """
    検索に必要なデータを初回だけ構築してキャッシュ。
    以後はキャッシュを使う。
    """
    global _ctx
    if _ctx is not None:
        return _ctx
    mod = _load_user_module()
    raw_df, norm_df = mod.load_data(getattr(mod, "CSV_PATH"))
    uniq = mod.build_unique_dict(norm_df)
    known_keywords = mod.build_known_keywords(raw_df)
    synonym_dict = mod.build_auto_synonyms(norm_df, [
        "作業名", "下地の状況", "処理する深さ・厚さ",
        "ライナックス機種名", "使用カッター名", "工程数",
    ])
    _ctx = (raw_df, norm_df, uniq, known_keywords, synonym_dict)
    return _ctx

def natural_text_to_filters(user_text: str) -> Dict[str, List[str]]:
    mod = _load_user_module()
    raw_df, norm_df, uniq, known_keywords, synonym_dict = _ensure_context()
    filters = {cat: [] for cat in mod.FILTER_COLUMNS}
    suggestions = mod.suggest_filters(uniq, user_text, known_keywords, synonym_dict)
    for cat in mod.FILTER_COLUMNS:
        filters[cat].extend(suggestions.get(cat, []))
    return filters

def run_query_with_filters(filters: Dict[str, List[str]]):
    mod = _load_user_module()
    raw_df, norm_df, *_ = _ensure_context()
    hits_norm = mod.filter_data(norm_df, filters)
    hits_raw = raw_df.loc[hits_norm.index]
    return hits_raw

def run_query_system(input_query):
    """
    LINE側から呼ばれるエントリ。
    - input_query が str: 自然文として解釈 → フィルタ生成
    - input_query が dict: そのままフィルタ辞書（キーはユーザーの列名）
    戻り値: List[dict]（改変なし）
    """
    mod = _load_user_module()

    if isinstance(input_query, str):
        filters = natural_text_to_filters(input_query)
    elif isinstance(input_query, dict):
        filters = {cat: [] for cat in mod.FILTER_COLUMNS}
        for k, v in input_query.items():
            if k in filters:
                filters[k] = v if isinstance(v, list) else [v]
    else:
        raise TypeError("input_query は str（自然文）または dict（フィルタ辞書）で指定してください。")

    df = run_query_with_filters(filters)
    return df.to_dict("records")
