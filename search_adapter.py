# search_adapter.py — ver: resilient dynamic loader
# - ver4_2_python_based_RAG_wo_GPT.py を動的ロード
# - 初回だけCSV等を読み込みキャッシュ
# - input が str（自然文）/ dict（フィルタ）どちらでも検索実行
# - postprocess が使うための _ensure_context を公開

from __future__ import annotations
import os
import importlib.util
from typing import Dict, List, Tuple, Any, Optional

# === 設定 ===
SEARCH_SCRIPT_PATH = os.environ.get(
    "SEARCH_SCRIPT_PATH",
    os.path.abspath("./ver4_2_python_based_RAG_wo_GPT.py"),
)
CSV_PATH_ENV = os.environ.get("RAG_CSV_PATH")

_mod = None  # type: Optional[Any]
_ctx = None  # type: Optional[Tuple[Any, Any, Any, Any, Any]]

__all__ = [
    "natural_text_to_filters",
    "run_query_with_filters",
    "run_query_system",
    "_ensure_context",
    "reset_cache",
]

def reset_cache() -> None:
    """テスト/リロード用：動的ロード済みモジュールとデータキャッシュを破棄"""
    global _mod, _ctx
    _mod = None
    _ctx = None

def _load_user_module():
    """
    ユーザースクリプト（ver4_2_python_based_RAG_wo_GPT.py）を動的ロード。
    - 存在チェック
    - 必要関数の存在チェック
    - 環境変数で CSV パス上書き
    """
    global _mod
    if _mod is not None:
        return _mod

    if not os.path.exists(SEARCH_SCRIPT_PATH):
        raise FileNotFoundError(f"ユーザー検索スクリプトが見つかりません: {SEARCH_SCRIPT_PATH}")

    spec = importlib.util.spec_from_file_location("user_rag_module", SEARCH_SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"spec 作成に失敗: {SEARCH_SCRIPT_PATH}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    # 期待する属性/関数の存在を軽く確認
    required = [
        "load_data",
        "build_unique_dict",
        "build_known_keywords",
        "build_auto_synonyms",
        "filter_data",
        "suggest_filters",
        "FILTER_COLUMNS",
    ]
    missing = [name for name in required if not hasattr(mod, name)]
    if missing:
        raise AttributeError(f"{os.path.basename(SEARCH_SCRIPT_PATH)} に必要関数が不足: {missing}")

    # CSV パスの上書き（環境依存を吸収）
    if CSV_PATH_ENV:
        if hasattr(mod, "CSV_PATH"):
            mod.CSV_PATH = CSV_PATH_ENV
        else:
            mod.CSV_PATH = CSV_PATH_ENV  # 無ければ作る

    _mod = mod
    return mod

def _ensure_context():
    """
    検索に必要なデータを初回だけ構築してキャッシュ。
    以後はキャッシュを使う。
    戻り値: (raw_df, norm_df, uniq, known_keywords, synonym_dict)
    """
    global _ctx
    if _ctx is not None:
        return _ctx

    mod = _load_user_module()
    raw_df, norm_df = mod.load_data(getattr(mod, "CSV_PATH", None))
    uniq = mod.build_unique_dict(norm_df)
    known_keywords = mod.build_known_keywords(raw_df)
    synonym_dict = mod.build_auto_synonyms(
        norm_df,
        ["作業名", "下地の状況", "処理する深さ・厚さ", "ライナックス機種名", "使用カッター名", "工程数"],
    )
    _ctx = (raw_df, norm_df, uniq, known_keywords, synonym_dict)
    return _ctx

def natural_text_to_filters(user_text: str) -> Dict[str, List[str]]:
    """
    自然文 → フィルタ辞書
    """
    mod = _load_user_module()
    raw_df, norm_df, uniq, known_keywords, synonym_dict = _ensure_context()
    filters = {cat: [] for cat in mod.FILTER_COLUMNS}
    suggestions = mod.suggest_filters(uniq, user_text, known_keywords, synonym_dict)
    for cat in mod.FILTER_COLUMNS:
        filters[cat].extend(suggestions.get(cat, []))
    return filters

def run_query_with_filters(filters: Dict[str, List[str]]):
    """
    フィルタ辞書で検索し、生DF（raw_df側）のヒット行を返す（pandas.DataFrame）
    """
    mod = _load_user_module()
    raw_df, norm_df, *_ = _ensure_context()
    hits_norm = mod.filter_data(norm_df, filters)
    hits_raw = raw_df.loc[hits_norm.index]
    return hits_raw

def run_query_system(input_query: Any):
    """
    共通エントリ：
    - input_query が str: 自然文として解釈 → フィルタ辞書に変換
    - input_query が dict: そのままフィルタ辞書（キーは FILTER_COLUMNS）
    戻り値: List[dict]（改変なし・表示整形は呼び出し側で）
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
