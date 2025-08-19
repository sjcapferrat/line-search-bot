# nlp_extract.py  — CSVの8項目だけを使うデータ駆動抽出器（GPTなしでも動作）
import os, re, json
from typing import Tuple, Dict, Any, List, Set
import pandas as pd

# === 対象カラム（この8項目“のみ”を抽出） ===
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

# === 設定 ===
CSV_PATH = os.environ.get("RAG_CSV_PATH", "./restructured_file.csv")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")  # 任意

# === （任意）OpenAI：鍵がある時だけ初期化。無ければ完全ルールベースで動作 ===
client = None
if OPENAI_API_KEY:
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
    except Exception:
        client = None  # SDK相性で失敗してもフォールバック

# ---------- 正規化ユーティリティ ----------
MM_PAT = re.compile(r'(\d+(?:\.\d+)?)\s*(?:mm|ｍｍ|ミリ|ﾐﾘ)', re.IGNORECASE)
ENG_COUNT_PAT = re.compile(r'(\d+)\s*工程')
PLAIN_NUM_PAT = re.compile(r'(?<!\d)(\d{1,2})(?!\d)')  # 文中の孤立した1~2桁

def z2h(s: str) -> str:
    # 全角→半角（最低限：英数字・記号の一部）
    tbl = str.maketrans(
        "０１２３４５６７８９．－　（）",
        "0123456789.- ()"
    )
    return s.translate(tbl)

def normalize(s: str) -> str:
    s = z2h(s).strip()
    # 代表的な揺れ
    s = s.replace("　", " ").replace("〜", "~").replace("―", "-").replace("‐", "-")
    return s

def norm_token(s: str) -> str:
    return normalize(s).lower()

# ---------- 語彙のロード（CSVから一度だけ） ----------
_VOCAB: Dict[str, Set[str]] = {col: set() for col in COLUMNS}
_SYMBOLS_EVAL = {"◎", "○", "△"}  # 作業効率評価

def _load_vocab():
    global _VOCAB
    df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8", keep_default_na=False)
    # 列が欠けていないか軽くチェック
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"CSVに必要列が見つかりません: {missing}")

    for col in COLUMNS:
        # 空白セルや同義の重複を除去
        uniq = set()
        for v in df[col].astype(str).tolist():
            v = v.strip()
            if not v or v.lower() in {"nan", "none"}:
                continue
            uniq.add(v)
        _VOCAB[col] = uniq

# 起動時に語彙を読み込む
_load_vocab()

# ---------- メイン抽出 ----------
async def extract_query(user_text: str) -> Tuple[Dict[str, Any], str]:
    """
    入力の自然文から、8項目だけをキーに持つ検索条件(dict)を返す。
    値は原則List[str]（複数ヒット想定）。数値は正規化する。
    """
    text = normalize(user_text)
    text_lower = text.lower()

    result: Dict[str, Any] = {}

    # 1) 数値系（“処理する深さ・厚さ”、工程数）
    depth = _extract_depth_mm(text_lower)
    if depth:
        result["処理する深さ・厚さ"] = [depth]  # 例: "5mm"

    steps = _extract_steps(text_lower)
    if steps:
        result["工程数"] = [steps]            # 例: "2"

    # 評価記号（◎/○/△）
    eff = _extract_efficiency(text)
    if eff:
        result["作業効率評価"] = [eff]

    # 2) カテゴリ語彙の部分一致（CSV由来の正解語彙のみ）
    # 文字数の短い語（1文字）での誤爆を避けるため、2文字以上に限定
    for col in COLUMNS:
        if col in ("処理する深さ・厚さ", "工程数", "作業効率評価"):
            continue  # 数値/記号系は上で処理
        hits = _match_column_values(text, _VOCAB[col])
        if hits:
            result[col] = sorted(hits, key=lambda s: (-len(s), s))  # 長い一致を優先

    # （任意）OpenAI 併用：CSV語彙で拾えなかった“曖昧語”だけ補完したい場合に使用
    # ただし「8項目以外」は絶対に返さないポリシーを維持
    explanation_bits = []
    if client:
        # 今回は“安全第一”で無効化するか、コメントアウトで必要時に使ってください。
        pass

    # 説明文の生成（ログ・デバッグ用）
    for col in COLUMNS:
        if col in result:
            explanation_bits.append(f"{col}={result[col]}")
    explain = "抽出条件: " + " / ".join(explanation_bits) if explanation_bits else "抽出条件: （該当なし）"

    return result, explain

# ---------- 抽出サブルーチン ----------
def _extract_depth_mm(text_lower: str) -> str | None:
    """
    '5mm', '５ｍｍ', 'ミリ', 'mm' などを '5mm' に正規化。
    """
    m = MM_PAT.search(text_lower)
    if m:
        val = m.group(1)
        return f"{val}mm"
    return None

def _extract_steps(text_lower: str) -> str | None:
    """
    '2工程' → '2' に正規化。単に '2' とだけ書かれている場合も近傍の語に '工程' があれば対応。
    """
    m = ENG_COUNT_PAT.search(text_lower)
    if m:
        return m.group(1)
    # “工程”がすぐ近くに無いケースは誤爆が増えるので、ここでは採用しない。
    return None

def _extract_efficiency(text: str) -> str | None:
    for sym in _SYMBOLS_EVAL:
        if sym in text:
            return sym
    # “良い/普通/悪い”などを勝手に写像しない（方針：CSVのラベル以外は使わない）
    return None

def _match_column_values(text: str, vocab: Set[str]) -> List[str]:
    """
    CSVの語彙集合と自然文を突き合わせて部分一致で拾う。
    2文字未満はスキップ。全角・半角の揺れは normalize 済みの text に対して素朴検索。
    """
    text_n = normalize(text)
    hits: Set[str] = set()
    for cand in vocab:
        c = cand.strip()
        if len(c) < 2:
            continue
        # ひらがな/カタカナ/英数混在でも normalize で緩和（完全変換はしない）
        if normalize(c) in text_n:
            hits.add(cand)
    return list(hits)
