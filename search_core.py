from typing import Dict, Any, List
from search_adapter import run_query_system as _adapter_run

def run_query_system(query: Dict[str, Any]) -> List[dict]:
    """LINE側から呼ばれる共通エントリ。内容は一切改変せず返す。"""
    return _adapter_run(query)
