# LINE 検索ボット（Renderデプロイ最小構成）

## 1. 環境変数
Render の **Environment** に以下を設定してください。

- `LINE_CHANNEL_SECRET`
- `LINE_CHANNEL_ACCESS_TOKEN`
- `OPENAI_API_KEY`（任意。未設定ならルールベースのみで抽出します）

## 2. デプロイ手順（Render）
1. 本リポジトリを GitHub にプッシュ
2. Render > New > Web Service で本リポジトリを選択
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
5. デプロイ後、`https://xxxx.onrender.com/callback` を LINE Developers の Webhook に設定 & 有効化
6. `GET /healthz` でヘルスチェック可

## 3. 既存クエリシステムとの接続
`search_core.py` をあなたの実装に置き換えてください。

- **A) 直接関数呼び出し**（推奨）: pandas の DataFrame を `to_dict('records')` で返すだけ
- **B) HTTP API 呼び出し**: 既存APIがある場合はそのままPOST/GET
- **C) サブプロセス**: 既存CLIを呼び出して JSON を受け取る

> 重要: **検索結果の内容は絶対に改変しない**（順序やフィルタも変更しない）。

## 4. テスト方法（ローカル）
```
pip install -r requirements.txt
uvicorn app:app --reload
```
- 別ターミナルで `curl localhost:8000/healthz`
- LINE連携テストは `ngrok http 8000` で一時公開して Webhook に設定

## 5. UIについて
- `formatters.py` は見せ方だけを調整（結果データはそのまま）
- Quick Reply・Flex Message の追加は安全

## 6. NLP 抽出
- `nlp_extract.py` は GPT Function Calling + ルールベースのハイブリッド
- OPENAI_API_KEY 未設定でも動作（ルールのみ）

## 7. あなたの既存スクリプトの接続方法（重要）
- `search_adapter.py` が importlib で **ver4.2_python_based_RAG_wo_GPT.py** を動的ロードします。
- ファイル名にドットがあり通常importできないため、**環境変数 `SEARCH_SCRIPT_PATH`** にフルパスを設定してください。
  - 例: `C:\\Users\\takeda\\Documents\\projectRAG\\ver4.2_python_based_RAG_wo_GPT.py`
- もしくは、ファイル名を `ver4_2_python_based_RAG_wo_GPT.py` にリネームして本プロジェクト直下に置けば、環境変数なしでも読み込みます。

### 任意のCSVパスを使いたい場合
- 環境変数 `RAG_CSV_PATH` を設定すると、ユーザースクリプト内の `CSV_PATH` を上書きします。

