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

def _render_line(r: dict) -> str:
    eff   = _eff_norm(r.get("作業効率評価", ""))
    mech  = r.get("ライナックス機種名", "") or "-"
    cutter= r.get("使用カッター名", "") or "-"
    job   = r.get("作業名", "") or "-"
    sub   = r.get("下地の状況", "") or "-"
    depth = r.get("処理する深さ・厚さ", "") or "-"
    steps = r.get("工程数", "")
    steps_sfx = f" / 工程: {steps}" if steps else ""
    # 例）◎ K-200ENV + ブロックチップⅡ | 塗膜や堆積物の除去 / 厚膜塗料（エポキシ） / 厚さ 0.5–1.0mm / 工程: 単一
    return (
        f"{eff or '・'} {mech} + {cutter} | "
        f"{job} / {sub} / {depth}{steps_sfx}"
    )

# ---- 公開関数 ---------------------------------------------------

def to_plain_text(results: List[dict], query: Dict[str, Any], explain: str) -> str:
    """
    人に読みやすいテキスト整形。
    - 頭に“抽出条件”の要約（箇条書き）
    - ヒット件数と評価の内訳
    - 並びは search_core.py のソート結果をそのまま
    - 多すぎる場合は先頭30件のみ表示（LINE 文字数対策）
    """
    # 条件の人間向け要約
    qlines = _humanize_query(query)
    header = "🔎 抽出条件\n" + ("\n".join(f"・{ln}" for ln in qlines) if qlines else "・（特になし）")

    # 件数サマリ
    total = len(results)
    g, s, w = _count_by_eff(results)
    summary = f"\n\n📊 該当 {total} 件（内訳: ◎{g} / ○{s} / △{w}）"

    # リスト本文
    if total == 0:
        body = "\n\n該当するレコードは見つかりませんでした。条件を少し緩めて再検索してみてください。"
        note = "\n"
        legend = "※ 評価の意味: ◎=非常に適, ○=適, △=一部条件で可\n"
        # explain があれば最後に小さく添える
        ex = f"（{explain}）" if explain else ""
        return f"{header}{summary}{body}\n{legend}{ex}".strip()

    SHOW_MAX = 30  # LINE の文字数安全圏
    shown = results[:SHOW_MAX]
    lines = [_render_line(r) for r in shown]
    more = ""
    if total > SHOW_MAX:
        more = f"\n…ほか {total - SHOW_MAX} 件"

    legend = "\n\n※ 評価の意味: ◎=非常に適, ○=適, △=一部条件で可"
    # explain を補助情報として末尾に（冗長にならないよう括弧で）
    ex = f"\n（{explain}）" if explain else ""

    return f"{header}{summary}\n\n" + "\n".join(lines) + more + legend + ex

def to_flex_message(results: List[dict]) -> dict:
    """
    LINEのFlex Message用（上位10件）。
    """
    bubbles = []
    for r in results[:10]:
        eff   = _eff_norm(r.get("作業効率評価",""))
        mech  = r.get("ライナックス機種名","") or "-"
        cutter= r.get("使用カッター名","") or "-"
        job   = r.get("作業名","") or "-"
        sub   = r.get("下地の状況","") or "-"
        depth = r.get("処理する深さ・厚さ","") or "-"
        steps = r.get("工程数","")

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
