import os, re, json
from typing import Tuple, Dict, Any
from openai import OpenAI

# OpenAIのキーは環境変数 OPENAI_API_KEY を利用
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

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

    # OpenAI が未設定ならルールのみ
    if client.api_key is None or client.api_key == "":
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
        tool_call = resp.choices[0].message.tool_calls[0]
        gpt_args = json.loads(tool_call.function.arguments)

        merged = {**rule_query, **{k: v for k, v in gpt_args.items() if v}}
        explain = f"抽出条件: {merged}"
        return merged, explain

    except Exception as e:
        return rule_query, f"(GPT抽出失敗のためルール適用) 抽出条件: {rule_query} / error={e}"

def rule_based_guess(text: str):
    q: Dict[str, str] = {}
    # 深さ/厚さ
    m = re.search(r'(\d+(?:\.\d+)?)\s*(mm|ミリ|ｍｍ)', text)
    if m:
        q["深さまたは厚さ"] = f"{m.group(1)}mm"
    # 代表的な作業名キーワード（必要に応じて拡張）
    keywords = [
        "表面目荒らし","表面ハツリ","表面研ぎ出し","雨打たれ処理",
        "塗膜や堆積物の除去","張り物除去","溝切り"
    ]
    for kw in keywords:
        if kw in text:
            if kw == "ハツリ":
                q["作業名"] = "表面ハツリ"
            else:
                q["作業名"] = kw
    return q
