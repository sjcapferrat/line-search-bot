from typing import List, Dict, Any, Tuple

# ---- 内部ヘルパ -------------------------------------------------

def _join(vals: List[str], sep: str = "、") -> str:
    vals = [v for v in (vals or []) if str(v).strip()]
    return sep.join(vals)

def _eff_norm(mark: str) -> str:
    # 〇 と ○ を統一
    return (mark or "").replace("〇", "○")

def _count_by_eff(rows: List[dict]) -> Tuple[int, int, int]:
    g = s = w = 0  # ◎, ○, △
    for r in rows:
        m = _eff_norm(r.get("作業効率評価", ""))
        if "◎" in m:
            g += 1
        elif "○" in m:
            s += 1
        elif "△" in m:
            w += 1
    return g, s, w

def _humanize_query(query: Dict[str, Any]) -> List[str]:
    """
    人間向けの説明行を作る（空は出さない）
    """
    out = []
    if query.get("作業名"):
        out.append(f"作業: {_join(query['作業名'])}")
    if query.get("下地の状況"):
        out.append(f"下地: {_join(query['下地の状況'])}")
    # 深さ
    if "depth_range" in query and query["depth_range"]:
        lo, hi = query["depth_range"]
        out.append(f"深さ: {lo:g}–{hi:g}mm")
    elif "depth_value" in query and query["depth_value"] is not None:
        out.append(f"深さ: {query['depth_value']:g}mm")
    elif query.get("処理する深さ・厚さ"):
        out.append(f"深さ（目安）: {_join([str(x) for x in query['処理する深さ・厚さ']], ' / ')}")
    if query.get("工程数"):
        out.append(f"工程数: {_join(query['工程数'])}")
    if query.get("機械カテゴリー"):
        out.append(f"機械カテゴリ: {_join(query['機械カテゴリー'])}")
    if query.get("ライナックス機種名"):
        out.append(f"機種: {_join(query['ライナックス機種名'])}")
    if query.get("使用カッター名"):
        out.append(f"カッター: {_join(query['使用カッター名'])}")
    if query.get("作業効率評価"):
        out.append(f"評価: {_join(query['作業効率評価'])}")
    return out

# 並び順制御
_EFF_RANK = {"◎": 0, "○": 1, "△": 2, "": 3, None: 3}  # ◎→○→△→空

def _is_single(r: dict) -> bool:
    # search_core/app から _stage='SINGLE' が来る想定／無ければ工程数文字列で判定
    st = (r.get("_stage") or "").upper()
    if st == "SINGLE":
        return True
    s = str(r.get("工程数", "")).strip()
    return s in ("単一", "単一工程")

def _is_pair_stage(r: dict) -> bool:
    st = (r.get("_stage") or "").upper()
    if st in ("A", "B"):
        return True
    s = str(r.get("工程数", "")).strip()
    return ("一次" in s) or ("二次" in s)

def _sort_for_view(rows: List[dict]) -> List[dict]:
    def key(r: dict):
        single_rank = 0 if _is_single(r) else 1
        eff = _eff_norm(r.get("作業効率評価", "") or "")
        eff_rank = _EFF_RANK.get(eff, 3)
        return (single_rank, eff_rank)
    return sorted(rows, key=key)

# ---- ラベル決定（最重要の修正） -------------------------------

def _stage_hit_label(r: dict) -> str:
    """
    行の末尾に付ける工程ラベルを決定。
    優先順:
      1) _hit_label があればそのまま使用（"検索ヒットした工程" / "検索結果とペアになる工程"）
      2) _is_hit があれば True→ヒット / False→ペア
      3) _stage + _hit_stage があればそれに従う
      4) 最後に _pair_candidate の有無で判定
      5) 単一工程は空文字（ラベル無し）
    """
    if _is_single(r):
        return ""

    # 1) 明示ラベル
    lbl = r.get("_hit_label")
    if isinstance(lbl, str) and lbl.strip():
        return f"（{lbl.strip()}）"

    # 2) 明示フラグ
    if "_is_hit" in r:
        return "（検索ヒットした工程）" if r.get("_is_hit") else "（検索結果とペアになる工程）"

    # 3) _stage/_hit_stage
    st = (r.get("_stage") or "").upper()
    if st in ("A", "B"):
        return "（検索ヒットした工程）" if r.get("_hit_stage") else "（検索結果とペアになる工程）"

    # 4) フォールバック: 工程表記 + _pair_candidate
    steps = str(r.get("工程数", "")).strip()
    is_pair_stage = ("一次" in steps) or ("二次" in steps)
    if not is_pair_stage:
        return ""
    return "（検索結果とペアになる工程）" if r.get("_pair_candidate") else "（検索ヒットした工程）"

# ---- 行レンダリング --------------------------------------------

def _render_line(r: dict) -> str:
    eff    = _eff_norm(r.get("作業効率評価", ""))
    mech   = r.get("ライナックス機種名", "") or "-"
    cutter = r.get("使用カッター名", "") or "-"
    job    = r.get("作業名", "") or "-"
    sub    = r.get("下地の状況", "") or "-"
    depth  = r.get("処理する深さ・厚さ", "") or "-"
    steps  = r.get("工程数", "")

    steps_sfx = f" / 工程: {steps}" if steps else ""
    stage_sfx = _stage_hit_label(r)

    # 例）◎ K-200ENV + ブロックチップⅡ | 作業 / 下地 / 0.5–1.0mm / 工程: 一次工程 （検索ヒットした工程）
    return (
        f"{eff or '・'} {mech} + {cutter} | "
        f"{job} / {sub} / {depth}{steps_sfx}"
        f"{(' ' + stage_sfx) if stage_sfx else ''}"
    )

def _summary_line(rows: List[dict]) -> str:
    """
    2行目用サマリー文：
      - 「全て単一工程」
      - 「単一＋一次／二次が含まれる」
      - 「すべて一次／二次で単一なし」
    """
    if not rows:
        return "検索結果はありません"
    has_single = any(_is_single(r) for r in rows)
    has_pair   = any(_is_pair_stage(r) for r in rows)
    if has_single and not has_pair:
        return "検索された工法は全て単一工程です"
    elif has_single and has_pair:
        return "検索された工法の中には単一工程のほか、一次／二次工程が含まれます"
    else:
        return "検索された工法はすべて一次／二次工程で、単一工程はありません"

# ---- 公開関数 ---------------------------------------------------

def to_plain_text(results: List[dict], query: Dict[str, Any], explain: str) -> str:
    """
    人に読みやすいテキスト整形。
      - 先頭: 「＝＝＝検索結果＝＝＝{件数}件」
      - 2行目: サマリー（単一/一次/二次）
      - 空行
      - 本文（並びは 単一→◎→○→△→空、ペア補完は _pair_candidate ではなく工程ラベルで表示）
      - 末尾に凡例・抽出条件サマリ・評価内訳
    """
    ordered = _sort_for_view(results or [])
    total = len(ordered)

    header_results = f"＝＝＝検索結果＝＝＝{total}件"
    summary_line = _summary_line(ordered) if total > 0 else "該当するレコードは見つかりませんでした。"

    if total == 0:
        qlines = _humanize_query(query)
        header_query = "🔎 抽出条件\n" + ("\n".join(f"・{ln}" for ln in qlines) if qlines else "・（特になし）")
        legend = "※ 評価の意味: ◎=非常に適, ○=適, △=一部条件で可\n"
        ex = f"（{explain}）" if explain else ""
        return f"{header_results}\n{summary_line}\n\n{legend}{ex}\n\n{header_query}".strip()

    SHOW_MAX = 30
    shown = ordered[:SHOW_MAX]
    lines = [_render_line(r) for r in shown]
    more = f"\n…ほか {total - SHOW_MAX} 件" if total > SHOW_MAX else ""

    qlines = _humanize_query(query)
    header_query = "🔎 抽出条件\n" + ("\n".join(f"・{ln}" for ln in qlines) if qlines else "・（特になし）")
    g, s, w = _count_by_eff(ordered)
    summary_tail = f"\n\n📊 内訳: ◎{g} / ○{s} / △{w}"
    legend = "\n\n※ 評価の意味: ◎=非常に適, ○=適, △=一部条件で可"
    ex = f"\n（{explain}）" if explain else ""

    return (
        f"{header_results}\n{summary_line}\n\n" + "\n\n".join(lines) + more +
        legend + ex + "\n\n" + header_query + summary_tail
    )

def to_flex_message(results: List[dict]) -> dict:
    """
    LINEのFlex Message用（上位10件）。
      - タイトル頭の「（ペア候補）」プレフィックスを廃止
      - 一次/二次工程なら本文の末尾にラベル（検索ヒット/ペア）を付与
    """
    bubbles = []
    for r in (results or [])[:10]:
        eff    = _eff_norm(r.get("作業効率評価",""))
        mech   = r.get("ライナックス機種名","") or "-"
        cutter = r.get("使用カッター名","") or "-"
        job    = r.get("作業名","") or "-"
        sub    = r.get("下地の状況","") or "-"
        depth  = r.get("処理する深さ・厚さ","") or "-"
        steps  = r.get("工程数","")

        subtitle = f"{job} / {sub}"
        depth_line = f"{depth}"
        if steps:
            depth_line += f" / 工程: {steps}"

        stage_mark = _stage_hit_label(r)
        if stage_mark:
            depth_line += f" {stage_mark}"

        bubbles.append({
            "type": "bubble",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "contents": [
                    {
                        "type":"text",
                        "text": f"{eff or '・'} {mech} + {cutter}",
                        "weight":"bold",
                        "wrap": True
                    },
                    {"type":"text","text": subtitle, "size":"sm", "wrap":True},
                    {"type":"text","text": depth_line, "size":"sm", "wrap":True}
                ]
            }
        })

    if not bubbles:
        bubbles = [{
            "type":"bubble",
            "body":{
                "type":"box",
                "layout":"vertical",
                "contents":[{"type":"text","text":"結果なし"}]
            }
        }]

    return {"type": "carousel", "contents": bubbles}

# ---- 既存インターフェース（必要なら使用） ---------------------

def qr_reset_and_exit():
    return [
        {"type":"action","action":{"type":"message","label":"新しい検索(0)","text":"0"}},
        {"type":"action","action":{"type":"message","label":"終了(1)","text":"1"}},
    ]

def tail_reset_hint(text: str) -> str:
    return text + "新しい検索を行う場合はゼロ、０、またはリセット指示をお願いします。"

def msg_no_results():
    return "該当なしでした。もう一度検索条件を入れなおしてください。終了なら1または『終わり』『終了』と入力してください。"

def msg_invalid_conditions():
    return "検索条件が認識されませんでした。他の入力をお願いします。"

def qr_refine_or_rank():
    return [
        {"type":"action","action":{"type":"message","label":"他条件で絞る","text":"絞り込む"}},
        {"type":"action","action":{"type":"message","label":"評価順(上位5)","text":"上位5"}},
        {"type":"action","action":{"type":"message","label":"全件","text":"全件"}},
    ]
