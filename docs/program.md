# データジャーナリズム研究プログラム

## 目標
日本のオープンデータ（e-Stat）を活用して、一般市民が関心を持てる
データジャーナリズム記事を自動的に生成・改善すること。

## 評価基準（高スコアの条件）
1. **データの実在性**: 実際のe-Statデータを使用（フォールバックは-20点）
2. **洞察の深さ**: 数値の羅列でなく、パターン・変化・異常値を読み取っている
3. **物語性**: 読者が引き込まれる切り口・問いがある
4. **比較軸の明確さ**: 時系列・地域・属性のいずれかで対比がある
5. **日本語の自然さ**: 読みやすく簡潔な文章
6. **新鮮さ**: 過去のサイクルと異なるテーマ・データソース（同じテーマは-30点）

## pipeline.py の構造（必須）

pipeline.py は以下の4段階で構成すること:

```python
def select_topic() -> dict:
    """
    Ollamaがe-Stat統計カタログを参照して、
    過去に使っていないテーマ・統計IDを選ぶ。
    返り値例: {"stat_id": "0003036516", "theme": "賃金の地域格差", "reason": "..."}
    """

def fetch_data(topic: dict | None = None) -> pd.DataFrame:
    """select_topic()の結果を使ってe-Stat APIからデータを取得"""

def analyze(df: pd.DataFrame) -> dict[str, Any]:
    """データ分析"""

def generate_story(analysis: dict[str, Any]) -> str:
    """記事生成 → output/story.md に書き込む"""
```

fetch_data() の中で select_topic() を呼び出すこと:
```python
def fetch_data() -> pd.DataFrame:
    topic = select_topic()
    stat_id = topic.get("stat_id", "0000020101")
    # ... そのIDでAPIコール
```

## 利用可能なe-Stat統計カタログ（必ず異なるIDを選ぶこと）

| 分野 | 統計ID | 内容 |
|---|---|---|
| 人口 | 0000020101 | 人口推計（月報）※過去に使用済み、再使用-30点 |
| 労働 | 0003036516 | 毎月勤労統計（賃金・労働時間、産業別） |
| 労働 | 0000040101 | 労働力調査（就業・失業）※過去に使用済み、再使用-30点 |
| 家計 | 0000060033 | 家計調査（二人以上世帯、支出・収入） |
| 物価 | 0000060100 | 消費者物価指数（全国・地域別） |
| 教育 | 0000030001 | 学校基本調査（進学率・学校数） |
| 住宅 | 0000020602 | 住民基本台帳（人口移動報告） |
| 農業 | 0000020301 | 農業センサス（農家数・農地面積） |
| 商業 | 0000060073 | 商業統計（売場面積・年間販売額） |
| 工業 | 0000060046 | 工業統計（製造品出荷額）|
| 医療 | 0000030047 | 医療施設調査（病院・診療所数）|
| 交通 | 0003215843 | 人口動態統計（出生・死亡・婚姻） |

## 絶対ルール（エージェントは必ず守ること）

1. `fetch_data() -> pd.DataFrame` のシグネチャ変更禁止
2. `analyze(df: pd.DataFrame) -> dict[str, Any]` のシグネチャ変更禁止
3. `generate_story(analysis: dict[str, Any]) -> str` のシグネチャ変更禁止
4. `generate_story()` は必ず `output/story.md` に書き込むこと
5. Ollama呼び出し（必ずこの形式）:
   ```python
   requests.post("http://localhost:11434/api/generate",
       json={"model": "qwen3.5:9b", "prompt": ...,
             "stream": False, "think": False,
             "options": {"temperature": 0.7}},
       timeout=480)
   ```
   ※ `"think": False` は `options` の**外**、トップレベルに置くこと
6. e-Stat API: `https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData`
