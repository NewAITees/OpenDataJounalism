## 運用ルール
1. タスクを追加するときはチェックボックス形式で書く
2. 完了したら [x] にする
3. セクションが全て完了したら、セクションごと削除してよい

## フォルダ再構成 + config.yaml 外出し

- [x] tasks/ ディレクトリ・ファイル作成
- [x] config.yaml 作成
- [x] docs/, pipeline_archive/, scripts/ ディレクトリ作成
- [x] ファイルをgit mvで移動
- [x] runner.py を config.yaml 読み込み対応に更新
- [x] pipeline.py のパス参照を更新
- [x] uv add pyyaml

## 次フェーズ（再構成完了後）

- [x] pipeline.py を REST API + SQLite キャッシュ方式で全面書き直し
- [x] 動作確認（ESTAT_APPID が設定済みであること前提）

## ランダム複数統計レポート化（今回）

- [x] 統計ID選定をランダム化
- [x] 1サイクルで複数統計IDを取得
- [x] 複数統計の分析結果を統合して記事生成
- [x] 実行確認（`uv run python pipeline.py`）

## 全体レビューとコミット（今回）

- [x] 変更全体の差分確認（staged/unstaged/untracked）
- [x] 主要コードのレビュー（pipeline.py, runner.py, src/, tests/）
- [x] 必要修正の実装
- [x] テスト実行（`uv run pytest` と対象統合テスト）
- [ ] lint・型チェックの全体解消（既存違反が大量に残存）
- [x] tasks/todo.md と tasks/lessons.md を更新
- [ ] 変更をコミット
