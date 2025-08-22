# quick_extract.py
# 使い方:
#   python -X utf8 quick_extract.py "雨うたれを3mm削りたい"

import sys
from nlp_extract import extract_query

if len(sys.argv) < 2:
    print("Usage: python quick_extract.py \"文章\"")
    sys.exit(1)

text = sys.argv[1]
filters, explain = extract_query(text)

print("入力文:", text)
print(explain)

COLUMNS = [
    "作業名",
    "下地の状況",
    "処理する深さ・厚さ",
    "工程数",
    "機械カテゴリー",
    "ライナックス機種名",
    "使用カッター名",
    "作業効率評価",
]

# 文字列系（非空のみ）
for col in ["作業名", "下地の状況", "工程数", "機械カテゴリー", "ライナックス機種名", "使用カッター名", "作業効率評価"]:
    vals = filters.get(col) or []
    if vals:
        print(f"{col}:", vals)

# 深さは数値キー優先
dr = filters.get("depth_range")
dv = filters.get("depth_value")
if dr:
    print(f"処理する深さ・厚さ: {dr[0]:g}-{dr[1]:g}mm (range)")
elif dv is not None:
    print(f"処理する深さ・厚さ: {dv:g}mm (single)")
else:
    raw = filters.get("処理する深さ・厚さ") or []
    if raw:
        print(f"処理する深さ・厚さ(候補): {raw}")
