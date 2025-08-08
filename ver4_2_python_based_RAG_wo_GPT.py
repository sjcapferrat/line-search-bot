import pandas as pd
import re
import unicodedata
from typing import Dict, List, Tuple

#―――――――――― 設定 ――――――――――#
CSV_PATH = r"C:\Users\takeda\Documents\projectRAG\restructured_file.csv"
MAX_DISPLAY_ROWS = 30

cols = ["作業名", "下地の状況", "処理する深さ・厚さ",
        "ライナックス機種名", "使用カッター名", "工程数", "作業効率評価"]

FILTER_COLUMNS = {
    "作業名": ["作業名"],
    "下地の状況": ["下地の状況"],
    "処理する深さ・厚さ": ["処理する深さ・厚さ"],
    "機械カテゴリー": ["機械カテゴリー"],
    "ライナックス機種名": ["ライナックス機種名"],
    "使用カッター名": ["使用カッター名"],
    "作業効率評価": ["作業効率評価"],
}

CATEGORY_NUM_MAP = {
    "1": "作業名",
    "2": "下地の状況",
    "3": "処理する深さ・厚さ",
    "4": "機械カテゴリー",
    "5": "ライナックス機種名",
    "6": "使用カッター名",
    "7": "作業効率評価",
}

#―――――――――― ユーティリティ関数 ――――――――――#
def normalize_text(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[、。・，,]", " ", s)
    return s.strip().lower()

def load_data(path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    raw_df = pd.read_csv(path, dtype=str).fillna("")
    norm_df = raw_df.copy()
    for c in raw_df.columns:
        norm_df[c] = raw_df[c].apply(normalize_text)
    return raw_df, norm_df

def build_unique_dict(norm_df: pd.DataFrame) -> Dict[str, List[str]]:
    uniq: Dict[str, List[str]] = {}
    for cat, columns in FILTER_COLUMNS.items():
        values = pd.concat([norm_df[c] for c in columns])
        uniq[cat] = sorted(set(values.values) - {""})
    return uniq

def split_keywords(text: str) -> List[str]:
    text = normalize_text(text)
    return re.split(r"\s+", text)

def expand_synonyms(keywords: List[str], synonym_dict: Dict[str, List[str]]) -> List[str]:
    expanded = []
    for word in keywords:
        expanded.append(word)
        expanded.extend(synonym_dict.get(word, []))
    return list(set(expanded))

def build_auto_synonyms(norm_df: pd.DataFrame, target_columns: List[str], min_len: int = 2) -> Dict[str, List[str]]:
    keyword_map = {}
    values = set()
    for col in target_columns:
        values.update(norm_df[col].unique())
    for val in values:
        for other in values:
            if val == other:
                continue
            if len(val) >= min_len and val in other:
                keyword_map.setdefault(val, []).append(other)
    return keyword_map

def build_known_keywords(df: pd.DataFrame, min_length: int = 2) -> List[str]:
    unique_words = set()
    for col in df.columns:
        for val in df[col]:
            val = str(val)
            val = re.sub(r"[（）()「」【】\[\]〈〉]", " ", val)
            tokens = re.split(r"[,\u3001\u3002\u3000\s]+", val)
            for t in tokens:
                t_norm = normalize_text(t)
                if len(t_norm) >= min_length and re.search(r'\w', t_norm):
                    unique_words.add(t_norm)
    return sorted(unique_words)

def select_category_by_number() -> str:
    print("\nカテゴリを番号で選んでください（Enterでスキップ）:")
    for num, name in CATEGORY_NUM_MAP.items():
        print(f"[{num}] {name}")
    choice = normalize_text(input("> ").strip())
    if not choice:
        return ""
    return CATEGORY_NUM_MAP.get(choice, "")

def parse_depth_range(text: str) -> Tuple[float, float]:
    match = re.search(r"([0-9.]+)\s*-\s*([0-9.]+)", text)
    if match:
        return float(match.group(1)), float(match.group(2))
    return (None, None)

def is_value_in_range(value: float, text_range: str) -> bool:
    low, high = parse_depth_range(text_range)
    if low is None or high is None:
        return False
    return low <= value <= high

def suggest_filters(
    uniq: Dict[str, List[str]],
    query: str,
    known_keywords: List[str],
    synonym_dict: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    suggestions: Dict[str, List[str]] = {cat: [] for cat in FILTER_COLUMNS}
    query_keywords = split_keywords(query)
    expanded_keywords = expand_synonyms(query_keywords, synonym_dict)

    matched_terms = [word for word in known_keywords if any(eq in word for eq in expanded_keywords)]
    print("\n[DEBUG] 抽出されたキーワード候補:", matched_terms)

    depth_match = re.findall(r"[\d\\.]+", query)
    if depth_match:
        try:
            val = float(depth_match[0])
            print("[DEBUG] 数値による処理深さの抽出:", val)
            suggestions["処理する深さ・厚さ"].append(str(val))
        except:
            pass

    for word in matched_terms:
        for cat, values in uniq.items():
            if cat == "処理する深さ・厚さ":
                continue
            matches = [v for v in values if word in v]
            suggestions[cat].extend(matches)

    return suggestions

def filter_data(norm_df: pd.DataFrame, filters: Dict[str, List[str]]) -> pd.DataFrame:
    mask = pd.Series(True, index=norm_df.index)
    for cat, terms in filters.items():
        if not terms:
            continue
        if cat == "処理する深さ・厚さ":
            try:
                target_value = float(terms[0])
                match_mask = norm_df["処理する深さ・厚さ"].apply(
                    lambda x: is_value_in_range(target_value, x)
                )
                mask &= match_mask
            except Exception as e:
                print(f"[WARN] 深さの判定でエラー: {e.__class__.__name__} - {e}")
                mask &= False
        else:
            pat = "|".join(re.escape(t) for t in terms)
            sub = pd.Series(False, index=norm_df.index)
            for col in FILTER_COLUMNS.get(cat, []):
                sub |= norm_df[col].str.contains(pat, na=False)
            mask &= sub
    return norm_df[mask]

def extract_engineering_pairs(filtered_hits: pd.DataFrame, full_df: pd.DataFrame) -> List[pd.DataFrame]:
    seen = set()
    pairs = []
    candidate_keys = [
        (row["作業名"], row["下地の状況"], row["処理する深さ・厚さ"])
        for _, row in filtered_hits.iterrows()
        if row["工程数"] in ["一次工程", "二次工程"]
    ]
    for key in candidate_keys:
        if key in seen:
            continue
        seen.add(key)
        step1 = full_df[
            (full_df["作業名"] == key[0]) &
            (full_df["下地の状況"] == key[1]) &
            (full_df["処理する深さ・厚さ"] == key[2]) &
            (full_df["工程数"] == "一次工程")
        ]
        step2 = full_df[
            (full_df["作業名"] == key[0]) &
            (full_df["下地の状況"] == key[1]) &
            (full_df["処理する深さ・厚さ"] == key[2]) &
            (full_df["工程数"] == "二次工程")
        ]
        if not step1.empty and not step2.empty:
            pairs.append(pd.concat([step1, step2]))
    return pairs

def print_pair_results(pairs: List[pd.DataFrame]):
    def sort(df):
        df = df.copy()
        order = {"◎": 0, "〇": 1, "○": 1, "△": 2}
        df["_工程順"] = df["工程数"].map(lambda x: 0 if "一次" in x else 1 if "二次" in x else 2)
        df["_効率順"] = df["作業効率評価"].map(lambda x: order.get(x.strip(), 99))
        return df.sort_values(["_工程順", "_効率順"]).drop(columns=["_工程順", "_効率順"])
    if pairs:
        print("\n===（一次工程＋二次工程ペア）===")
        for i, pair in enumerate(pairs, 1):
            print(f"\n--- ペア {i} ---")
            print(sort(pair)[cols].to_string(index=False))

def print_solo_results(df: pd.DataFrame, exclude_keys: List[Tuple[str, str, str]]):
    def sort(df):
        df = df.copy()
        order = {"◎": 0, "〇": 1, "○": 1, "△": 2}
        df["_工程順"] = df["工程数"].map(lambda x: 0 if "一次" in x else 1 if "二次" in x else 2)
        df["_効率順"] = df["作業効率評価"].map(lambda x: order.get(x.strip(), 99))
        return df.sort_values(["_工程順", "_効率順"]).drop(columns=["_工程順", "_効率順"])
    df = df[~df.apply(lambda r: (r["作業名"], r["下地の状況"], r["処理する深さ・厚さ"]) in exclude_keys, axis=1)]
    if not df.empty:
        print("\n=== 単一工程・その他の工法結果 ===\n")
        print(sort(df)[cols].to_string(index=False))

def print_row(row):
    print(f"{row['作業名']: <12} {row['下地の状況']: <20} {row['処理する深さ・厚さ']: <15} "
          f"{row['ライナックス機種名']: <10} {row['使用カッター名']: <15} {row['工程数']: <8} {row['作業効率評価']}")

def summarize_and_print(results, raw_df):
    if results.empty:
        print("\n該当する工法は見つかりませんでした。")
        return

    efficiency_order = {"◎": 0, "○": 1, "〇": 1, "△": 2}

    # 単一工程
    singles = results[results["工程数"] == "単一"]

    # 一次 or 二次工程がヒットした行
    multi_hits = results[results["工程数"] != "単一"]

    # === ペア補完 ===
    pairs = []
    seen_keys = set()
    for _, row in multi_hits.iterrows():
        key = (row["作業名"], row["下地の状況"], row["処理する深さ・厚さ"])
        if key in seen_keys:
            continue
        seen_keys.add(key)

        step1 = raw_df[
            (raw_df["作業名"] == key[0]) &
            (raw_df["下地の状況"] == key[1]) &
            (raw_df["処理する深さ・厚さ"] == key[2]) &
            (raw_df["工程数"] == "一次工程")
        ]
        step2 = raw_df[
            (raw_df["作業名"] == key[0]) &
            (raw_df["下地の状況"] == key[1]) &
            (raw_df["処理する深さ・厚さ"] == key[2]) &
            (raw_df["工程数"] == "二次工程")
        ]

        if not step1.empty or not step2.empty:
            # 工程ごとに◎→○→△ソート
            step1_sorted = step1.sort_values(
                by="作業効率評価", key=lambda col: col.map(efficiency_order)
            )
            step2_sorted = step2.sort_values(
                by="作業効率評価", key=lambda col: col.map(efficiency_order)
            )
            pairs.append(pd.concat([step1_sorted, step2_sorted]))

    # === 単一工程表示（◎→○→△） ===
    if not singles.empty:
        singles_sorted = singles.sort_values(
            by="作業効率評価", key=lambda col: col.map(efficiency_order)
        )
        print("\n=== 単一工程・その他の工法結果 ===\n")
        print(singles_sorted[
            ["作業名", "下地の状況", "処理する深さ・厚さ",
             "ライナックス機種名", "使用カッター名", "工程数", "作業効率評価"]
        ].to_string(index=False))

    # === ペア表示（一次工程→二次工程、工程内で◎→○→△） ===
    if pairs:
        print("\n===（一次工程＋二次工程ペア）===\n")
        for i, pair in enumerate(pairs, 1):
            print(f"--- ペア {i} ---")
            print(pair[
                ["作業名", "下地の状況", "処理する深さ・厚さ",
                 "ライナックス機種名", "使用カッター名", "工程数", "作業効率評価"]
            ].to_string(index=False))
            print()

    print(f"=== 総該当件数: {len(results)} 件 ===\n")

#―――――――――― メインフロー ――――――――――#

def main():
    raw_df, norm_df = load_data(CSV_PATH)
    uniq = build_unique_dict(norm_df)
    known_keywords = build_known_keywords(raw_df)

    # ✅ 類義語展開の対象列を拡張（6列すべて）
    synonym_dict = build_auto_synonyms(norm_df, [
        "作業名", "下地の状況", "処理する深さ・厚さ",
        "ライナックス機種名", "使用カッター名", "工程数"
    ])

    filters: Dict[str, List[str]] = {cat: [] for cat in FILTER_COLUMNS}

    while True:
        query = input("\n抽出したい工法またはキーワードを入力してください> ").strip()
        if not query:
            print("入力なし。終了します。")
            break

        suggestions = suggest_filters(uniq, query, known_keywords, synonym_dict)
        for cat in FILTER_COLUMNS:
            filters[cat].extend(suggestions.get(cat, []))

        while True:
            print("\n--- 現在のフィルタ条件 ---")
            for cat, terms in filters.items():
                display = ["未指定"]
                if terms:
                    if cat == "処理する深さ・厚さ":
                        try:
                            target_value = float(terms[0])

                            # ✅ 作業名 or 下地の状況が指定されていなければ処理しない
                            if not filters["作業名"] and not filters["下地の状況"]:
                                display = ["未指定（作業名または下地の状況の指定が必要）"]
                                filters[cat] = []
                            else:
                                sub_df = norm_df

                                if filters["作業名"]:
                                    work_pat = "|".join(re.escape(t) for t in filters["作業名"])
                                    sub_df = sub_df[sub_df["作業名"].str.contains(work_pat, na=False)]

                                if filters["下地の状況"]:
                                    base_pat = "|".join(re.escape(t) for t in filters["下地の状況"])
                                    sub_df = sub_df[sub_df["下地の状況"].str.contains(base_pat, na=False)]

                                matched_ranges = sorted({
                                    raw_df.at[i, "処理する深さ・厚さ"]
                                    for i in sub_df.index
                                    if is_value_in_range(target_value, norm_df.at[i, "処理する深さ・厚さ"])
                                })
                                display = matched_ranges if matched_ranges else ["未指定（該当なし）"]
                                if display == ["未指定（該当なし）"]:
                                    filters[cat] = []
                        except Exception as e:
                            display = [f"解析エラー: {e.__class__.__name__} - {e}"]
                    else:
                        matched_values = set()
                        for col in FILTER_COLUMNS.get(cat, []):
                            for t in terms:
                                matched_values |= set(
                                    raw_df[col][norm_df[col].str.contains(re.escape(t), na=False)].unique()
                                )
                        display = sorted(matched_values) if matched_values else ["未指定（該当なし）"]
                        if display == ["未指定（該当なし）"]:
                            filters[cat] = []

                print(f"{cat}: {', '.join(display)}")

            ans = normalize_text(input(
                "\nこの条件で検索を実行しますか？ "
                "(1=実行 / 2=キャンセル / 3=キーワード追加 / 4=条件クリア / 5=条件一部修正)> "
            ))

            if ans in ['y', '1']:
                hits_norm = filter_data(norm_df, filters)
                hits_raw = raw_df.loc[hits_norm.index]

                if hits_raw.empty:
                    print("該当データはありませんでした。")
                else:
                    if "工程数" not in hits_raw.columns:
                        hits_raw = hits_raw.merge(
                            raw_df[["作業ID", "工程数"]],
                            on="作業ID",
                            how="left"
                        )
                    summarize_and_print(hits_raw, raw_df)

                again = normalize_text(input("\n条件を変更して再検索しますか？ (y/n)> "))
                if again != 'y':
                    print("終了します。")
                    return
                else:
                    print("\n🔁 前回のフィルタ条件を維持して再検索を開始します。")
                    continue

            elif ans in ['n', '2']:
                print("検索をキャンセルしました。")
                break

            elif ans in ['a', '3']:
                cat_add = select_category_by_number()
                if cat_add:
                    extra = input(f"{cat_add} に追加するキーワードをカンマ区切りで入力（Enterでキャンセル）> ").strip()
                    if not extra:
                        print(f"{cat_add} のキーワード追加をスキップしました。")
                        continue
                    new_terms = []
                    for t in extra.split(','):
                        t_norm = normalize_text(t)
                        if not t_norm:
                            continue
                        if cat_add == "処理する深さ・厚さ":
                            match = re.search(r"[\d.]+", t_norm)
                            if match:
                                new_terms.append(match.group(0))
                            else:
                                print(f"無効な深さ指定: {t}")
                        else:
                            new_terms.append(t_norm)
                    filters[cat_add].extend(new_terms)
                else:
                    print("カテゴリ入力をスキップしました。")

            elif ans in ['c', '4']:
                filters = {cat: [] for cat in FILTER_COLUMNS}
                print("フィルタ条件をすべてクリアしました。新たにキーワードを入力してください。")
                break

            elif ans in ['m', '5']:
                cat_mod = select_category_by_number()
                if cat_mod:
                    current_terms = filters.get(cat_mod, [])
                    if not current_terms:
                        print(f"{cat_mod} は現在 未指定 です。")
                    else:
                        print(f"\n{cat_mod} の現在の条件: {', '.join(current_terms)}")

                    print("1 = 条件を上書き / 2 = 条件を削除（未指定に戻す） / 3 = キャンセル")
                    action = normalize_text(input("> ").strip())
                    if action == '1':
                        new_input = input(f"{cat_mod} の新しい値をカンマ区切りで入力> ").strip()
                        new_terms = []
                        for t in new_input.split(','):
                            t_norm = normalize_text(t)
                            if not t_norm:
                                continue
                            if cat_mod == "処理する深さ・厚さ":
                                match = re.search(r"[\d.]+", t_norm)
                                if match:
                                    new_terms.append(match.group(0))
                                else:
                                    print(f"無効な深さ指定: {t}")
                            else:
                                new_terms.append(t_norm)
                        filters[cat_mod] = new_terms
                        print(f"{cat_mod} の条件を上書きしました。")
                    elif action == '2':
                        filters[cat_mod] = []
                        print(f"{cat_mod} の条件を削除しました。")
                    elif action == '3':
                        print("変更をキャンセルしました。")
                    else:
                        print("無効な選択です。")
                else:
                    print("カテゴリ入力をスキップしました。")

            else:
                print("1〜5 または y, n, a, c, m のいずれかを入力してください。")



if __name__ == '__main__':
    main()