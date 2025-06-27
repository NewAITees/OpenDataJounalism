# OpenDataJounalism

## e-stat AI Query Translator MCP

自然言語で e-stat の統計データを検索できる MCP（Model Context Protocol）機能です。

### 基本的な使用方法

```python
from opendatajounalism.mcp import EstatQueryTranslator

# トランスレータの初期化
translator = EstatQueryTranslator()

# 自然言語クエリを変換
results = translator.translate_query("東京都の年齢別人口が知りたい")

# 結果の取得
if results:
    result = results[0]
    print(f"統計表ID: {result.stats_data_id}")
    print(f"パラメータ: {result.parameters}")
```

### デモの実行

```bash
# デモスクリプトの実行
uv run python demo_mcp.py

# テストの実行
uv run pytest tests/mcp/
```

### 対応クエリ例

- "東京都の年齢別人口が知りたい"
- "最新の完全失業率を見たい"
- "都道府県別の人口を比較したい"
- "2020年の男女別人口データが欲しい"