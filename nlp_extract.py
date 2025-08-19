# nlp_extract.py（安全版）
import os, re, json
from typing import Tuple, Dict, Any

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
client = None
if OPENAI_API_KEY:
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_API_KEY)

FUNCTION_SCHEMA = {
    "name": "build_search_query",
    "description": "ユーザーの日本語入力から検索条件を抽出し、構造化クエリを返す。",
    "parameters": {
        "type": "object",
        "properties": {
            "作業名": {"type": "string"},
            "下地の状況": {"type": "string"},
            "深さまたは厚さ": {"type": "string"},
            "機械カテゴリー": {"type": "string"},
            "ライナックス機種名": {"type": "string"},
            "使用カッター名": {"type": "string"},
            "作業効率評価": {"type": "string", "enum": ["◎","○","△",""]},
        },
        "required": []
    }
}

SYSTEM = (
    "あなたは検索条件抽出器です。ユーザーの自然文を、"
    "可能な範囲で上記スキーマのキーに正規化してください。"
    "不明な項目は出力しないで構いません。"
    "単位や表記揺れ（mm/ミリ、MMA/メタクリルなど）を正規化してください。"
)

async def extract_query(user_text: str) -> Tuple[Dict[str, Any], str]:
    rule_query = rule_based_guess(user_text)

    # 🔐 鍵が無ければルールのみ
    if client is None:
        return rule_query, f"(GPT未使用) 抽出条件: {rule_query}"

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": user_text},
            ],
            tools=[{"type": "function", "function": FUNCTION_SCHEMA}],
            tool_choice={"type": "function", "function": {"name": "build_search_query"}},
        )
        choice = resp.choices[0]
        tool_calls = getattr(choice.message, "tool_calls", None) or []
        if not tool_calls:
            return rule_query, f"(GPT応答にツール呼び出しなし) 抽出条件: {rule_query}"

        gpt_args = json.loads(tool_calls[0].function.arguments)
        merged = {**rule_query, **{k: v for k, v in gpt_args.items() if v}}
        explain = f"抽出条件: {merged}"
        return merged, explain

    except Exception as e:
        return rule_query, f"(GPT抽出失敗のためルール適用) 抽出条件: {rule_query} / error={e}"

def rule_based_guess(text: str):
    q: Dict[str, str] = {}
    m = re.search(r'(\d+(?:\.\d+)?)\s*(?:mm|ミリ|ｍｍ)', text)
    if m:
        q["深さまたは厚さ"] = f"{m.group(1)}mm"
    for kw in ["表面目荒らし","表面ハツリ","表面研ぎ出し","雨打たれ処理","塗膜や堆積物の除去","張り物除去","溝切り"]:
        if kw in text:
            q["作業名"] = "表面ハツリ" if kw in ("ハツリ","表面ハツリ") else kw
    return q
