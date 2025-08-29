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

# 並び順制御（ここを追加）
_EFF_RANK = {"◎": 0, "○": 1, "": 3, None: 3, "△": 2}  # ◎→○→△→空（△は2とする）
def _is_single(r: dict) -> bool:
    s = str(r.get("工程数", "")).strip()
    return s in ("単一", "単一工程")

def _sort_for_view(rows: List[dict]) -> List[dict]:
    def key(r: dict):
        single_rank = 0 if _is_single(r) else 1
        eff = _eff_norm(r.get("作業効率評価", "") or "")
        eff_rank = _EFF_RANK.get(eff, 3)
        return (single_rank, eff_rank)
    return sorted(rows, key=key)

def _render_line(r: dict) -> str:
    eff   = _eff_norm(r.get("作業効率評価", ""))
    mech  = r.get("ライナックス機種名", "") or "-"
    cutter= r.get("使用カッター名", "") or "-"
    job   = r.get("作業名", "") or "-"
    sub   = r.get("下地の状況", "") or "-"
    depth = r.get("処理する深さ・厚さ", "") or "-"
    steps = r.get("工程数", "")
    steps_sfx = f" / 工程: {steps}" if steps else ""
    prefix = "（ペア候補）" if r.get("_pair_candidate") else ""
    # 例）◎ K-200ENV + ブロックチップⅡ | 塗膜や堆積物の除去 / 厚膜塗料（エポキシ） / 厚さ 0.5–1.0mm / 工程: 単一
    return (
        f"{prefix}{eff or '・'} {mech} + {cutter} | "
        f"{job} / {sub} / {depth}{steps_sfx}"
    )

# ---- 公開関数 ---------------------------------------------------

def to_plain_text(results: List[dict], query: Dict[str, Any], explain: str) -> str:
    """
    人に読みやすいテキスト整形。
    変更点：
      - 先頭の件数見出しを「＝＝＝検索結果＝＝＝{件数}件」に統一
      - 見出しのあとに1行の空行
      - 並びは「単一工程を最優先 → ◎→○→△→空」に再ソート（formatters側で安全に実施）
      - ペア候補（_pair_candidate=True）は先頭に「（ペア候補）」を表示
    既存仕様：
      - 多すぎる場合は先頭30件のみ表示（LINE 文字数対策）
      - 抽出条件の要約や評価内訳は維持（末尾に）
    """
    # 並び順をここで安全に確定
    ordered = _sort_for_view(results or [])
    total = len(ordered)

    # 件数見出し（ユーザー要望の書式）
    header_results = f"＝＝＝検索結果＝＝＝{total}件"

    if total == 0:
        # 条件サマリは既存のまま残す
        qlines = _humanize_query(query)
        header_query = "🔎 抽出条件\n" + ("\n".join(f"・{ln}" for ln in qlines) if qlines else "・（特になし）")
        legend = "※ 評価の意味: ◎=非常に適, ○=適, △=一部条件で可\n"
        ex = f"（{explain}）" if explain else ""
        return f"{header_results}\n\n該当するレコードは見つかりませんでした。\n{legend}{ex}\n\n{header_query}".strip()

    # 上限30件表示（既存踏襲）
    SHOW_MAX = 30
    shown = ordered[:SHOW_MAX]
    lines = [_render_line(r) for r in shown]
    more = f"\n…ほか {total - SHOW_MAX} 件" if total > SHOW_MAX else ""

    # 参考情報（従来の要約と内訳も最後に残す）
    qlines = _humanize_query(query)
    header_query = "🔎 抽出条件\n" + ("\n".join(f"・{ln}" for ln in qlines) if qlines else "・（特になし）")
    g, s, w = _count_by_eff(ordered)
    summary_tail = f"\n\n📊 内訳: ◎{g} / ○{s} / △{w}"
    legend = "\n\n※ 評価の意味: ◎=非常に適, ○=適, △=一部条件で可"
    ex = f"\n（{explain}）" if explain else ""

    # 見出し → 空行 → 本文
    return (
        f"{header_results}\n\n" + "\n".join(lines) + more +
        legend + ex + "\n\n" + header_query + summary_tail
    )

def to_flex_message(results: List[dict]) -> dict:
    """
    LINEのFlex Message用（上位10件）。
    （最小変更）ペア候補の印だけタイトルの頭に付与
    """
    bubbles = []
    for r in (results or [])[:10]:
        eff   = _eff_norm(r.get("作業効率評価",""))
        mech  = r.get("ライナックス機種名","") or "-"
        cutter= r.get("使用カッター名","") or "-"
        job   = r.get("作業名","") or "-"
        sub   = r.get("下地の状況","") or "-"
        depth = r.get("処理する深さ・厚さ","") or "-"
        steps = r.get("工程数","")
        prefix = "（ペア候補）" if r.get("_pair_candidate") else ""

        subtitle = f"{job} / {sub}"
        depth_line = f"{depth}"
        if steps:
            depth_line += f" / 工程: {steps}"

        bubbles.append({
            "type": "bubble",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "contents": [
                    {
                        "type":"text",
                        "text": f"{prefix}{eff or '・'} {mech} + {cutter}",
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
