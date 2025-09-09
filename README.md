# LINE 検索ボット（Renderデプロイ最小構成）

## 1. 環境変数
Render の **Environment** に以下を設定してください。

- `LINE_CHANNEL_SECRET`
- `LINE_CHANNEL_ACCESS_TOKEN`
- `OPENAI_API_KEY`（任意。未設定ならルールベースのみで抽出します）
- `RAG_CSV_PATH`（任意。CSVパスを変更したい場合）
- `SEARCH_SCRIPT_PATH`（任意。独自スクリプトを接続したい場合）

## 2. デプロイ手順（Render）
1. 本リポジトリを GitHub にプッシュ
2. Render > New > Web Service で本リポジトリを選択
3. Build Command:  
   ```bash
   pip install -r requirements.txt && git rev-parse HEAD > git_sha.txt
Start Command:

bash
Copy code
uvicorn app:app --host 0.0.0.0 --port $PORT
デプロイ後、https://xxxx.onrender.com/callback を LINE Developers の Webhook に設定 & 有効化

ヘルスチェック: GET /health

3. バージョン確認（運用メモ）
GET /version で現在のアプリバージョンと git commit SHA を確認可能です。

json
Copy code
{"app_version": "app.py (simple v2.6)", "git_sha": "abcdef123456..."}
4. 既存クエリシステムとの接続
search_core.py をあなたの実装に置き換えてください。

A) 直接関数呼び出し（推奨）: pandas の DataFrame を to_dict('records') で返すだけ

B) HTTP API 呼び出し: 既存APIがある場合はそのままPOST/GET

C) サブプロセス: 既存CLIを呼び出して JSON を受け取る

重要: 検索結果の内容は改変しない（順序やフィルタも変更しない）。

5. テスト方法（ローカル）
bash
Copy code
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
別ターミナルで:

bash
Copy code
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/version
LINE連携テストは ngrok http 8000 で一時公開して Webhook に設定

6. UIについて
formatters.py は見せ方だけを調整（結果データはそのまま）

Quick Reply・Flex Message の追加は安全

7. NLP 抽出
nlp_extract.py は GPT Function Calling + ルールベースのハイブリッド

OPENAI_API_KEY 未設定でも動作（ルールのみ）

8. 任意のCSV/スクリプト指定
RAG_CSV_PATH = ./restructured_file.csv

SEARCH_SCRIPT_PATH = ./ver4_2_python_based_RAG_wo_GPT.py

9. 起動確認とトラブルシュート
ヘルスチェック:
GET https://<your-app>.onrender.com/health

バージョン確認:
GET https://<your-app>.onrender.com/version

Verify 失敗/502 のとき:

FileNotFoundError → CSV/py の置き忘れ or パス違い

OpenAIError: api_key → OPENAI_API_KEY 未設定

Invalid signature（手動POST時）→ 正常。Webhookエンドポイントは生きています

10. 実装メモ
app.py は 遅延インポート 版

search_adapter.py は 遅延ロード 版（最初の検索時に CSV を読み込む）