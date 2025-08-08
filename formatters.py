from typing import List, Dict, Any

def to_plain_text(results: List[dict], query: Dict[str,Any], explain: str) -> str:
    header = "以下は、解釈した条件に基づく検索結果です（内容はデータそのまま表示）。\n"
    cond = f"条件: {query}\n"
    rows = []
    for r in results:
        rows.append(
            f"{r.get('作業効率評価','')} "
            f"{r.get('ライナックス機種名','')} + {r.get('使用カッター名','')} | "
            f"{r.get('作業名','')} / {r.get('下地の状況','')} / {r.get('処理する深さ・厚さ','')}"
        )
    note = "\n※ 評価の意味: ◎=非常に適, ○=適, △=一部条件で可\n"
    return header + cond + "\n".join(rows) + note

def to_flex_message(results: List[dict]) -> dict:
    bubbles = []
    for r in results[:10]:
        bubbles.append({
            "type": "bubble",
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {"type":"text","text": f"{r.get('作業効率評価','')} {r.get('ライナックス機種名','')} + {r.get('使用カッター名','')}", "weight":"bold", "wrap": True},
                    {"type":"text","text": f"{r.get('作業名','')} / {r.get('下地の状況','')}", "size":"sm", "wrap":True},
                    {"type":"text","text": f"{r.get('処理する深さ・厚さ','')}", "size":"sm", "wrap":True}
                ]
            }
        })
    return {"type": "carousel", "contents": bubbles if bubbles else [{
        "type":"bubble","body":{"type":"box","layout":"vertical","contents":[{"type":"text","text":"結果なし"}]}}]}
