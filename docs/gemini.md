# 重要情報まとめ - OpenDataJournalism プロジェクト

## プロジェクト概要
- **目的**: e-stat（日本政府統計ポータル）データを使用した日本のオープンデータジャーナリズム
- **核心機能**: Ollama AIを使用した自然言語クエリによるe-stat統計表の自動推薦システム
- **アーキテクチャ**: MCP (Model Context Protocol) ベースの統合システム

## 技術スタック

### 主要依存関係
- **pandas-estat**: 日本のe-stat API専用ライブラリ
- **geopandas, folium**: 地理空間データ処理・可視化
- **jageocoder**: 日本住所ジオコーディング
- **japanize-matplotlib**: 日本語フォント対応
- **ollama**: AI統合（自然言語クエリ処理）
- **sqlite3**: メタデータキャッシュ

### 開発ツール
- **uv**: パッケージマネージャー（必須使用）
- **ruff**: 高速linter/formatter
- **pytest**: テストフレームワーク
- **mypy**: 型チェック

## システム構成

### コア MCP モジュール
1. **EstatQueryTranslator** (`src/opendatajounalism/mcp/estat_query_translator.py`)
   - 自然言語クエリの統計表パラメータ変換
   - Ollama統合による高精度AI推薦

2. **OllamaStatsMCP** (`src/opendatajounalism/mcp/ollama_integration.py`)
   - 全e-stat統計表情報をOllamaに提供
   - 統計表ID・軸情報の自動推薦

3. **EstatMetadataLoader** (`src/opendatajounalism/mcp/estat_metadata_loader.py`)
   - 実際のe-statメタデータ取得・キャッシュ
   - SQLiteデータベース管理

### データフロー
```
自然言語クエリ → Ollama AI → 統計表ID+軸情報 → e-stat API → データ取得・分析
```

## 重要な設定情報

### 環境変数（.env ファイルで管理）
- **ESTAT_APPID**: e-stat API キー（必須）
  - 取得先: https://www.e-stat.go.jp/api/
  - 未設定時はフォールバックデータ使用
  - **重要**: .envファイルを作成し、実際のAPIキーを設定する必要があります

### データベース
- **catalog_index.db**: メタデータキャッシュ（SQLite）
- 場所: `data/mcp/catalog_index.db`

## 開発コマンド

### 基本操作
```bash
# 依存関係インストール
uv sync

# メインアプリケーション実行
uv run python main.py

# パッケージ追加（必須：uv使用）
uv add <package-name>
uv add --dev <dev-package-name>
```

### 品質チェック
```bash
# コード品質チェック
uv run ruff format --check .
uv run ruff check .
uv run pytest
uv run mypy src/

# 全品質チェック
uv run pre-commit run --all-files
```

### テスト実行
```bash
# 統合テスト
python test_real_ollama_integration.py

# 人口分析レポート生成
python population_analysis_report.py
```

## 現在の分析成果

### 完成した分析
1. **人口減少分析**: 2008年ピーク後の継続的減少
2. **世帯構造変化**: 単独世帯25.6%→35.1%増加
3. **高齢化進行**: 高齢化率17.4%→29.1%

### 分析品質の課題（ユーザーフィードバック）
- **表面的分析**: データの記述レベルに留まる
- **因果関係不足**: なぜそうなったかの深い分析が欠如
- **初心者レベル**: 専門的洞察が不十分

## 改善すべき分析観点

### 深掘りが必要な領域
1. **一人世帯増加の詳細分析**
   - 年齢層別（20-30代、30-40代、50-60代、70代以上）
   - 都市部vs地方の差異
   - 性別・所得階層別の違い
   - 未婚率・離婚率との相関

2. **社会経済的影響の定量化**
   - 消費パターンへの影響
   - 住宅需要変化
   - 社会保障負担の具体的数値

3. **地域格差の因果分析**
   - 経済活動との関係
   - 交通インフラの影響
   - 政策効果の検証

4. **国際比較・将来予測**
   - 他先進国との比較
   - 2040年予測モデル

## Git管理

### 除外ファイル
- `uv.lock` (gitignoreに追加済み)
- `analysis_output/` (生成ファイル)
- `data/mcp/` (キャッシュファイル)

### 最新コミット
- 6f09be9: e-stat MCP自然言語クエリシステム実装完了

## 注意事項
- **必須**: uvコマンドでのパッケージ管理
- **禁止**: pyproject.toml直接編集
- **推奨**: 仮想環境での開発
- **重要**: 分析品質の向上が急務

## 次のアクション
1. 一人世帯増加の多角的詳細分析実施
2. 因果関係の定量的解明
3. 政策提言レベルの深い洞察提供
4. 国際的視点での比較分析