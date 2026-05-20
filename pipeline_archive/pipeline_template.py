"""
自律型データジャーナリズム パイプライン（テンプレート）
WARNING: runner.py が毎サイクル書き換えます。手動編集は次サイクルで失われます。

4段構成:
  select_topic()     ← Ollamaがカタログから未使用テーマを選ぶ
  fetch_data()       ← 選ばれた統計IDでe-Stat APIを叩く
  analyze(df)        ← 分析
  generate_story()   ← Ollamaで記事生成 → output/story.md
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3.1:8b"
ESTAT_BASE = "https://api.e-stat.go.jp/rest/3.0/app/json"
ESTAT_APPID = os.getenv("ESTAT_APPID", "")

# e-Stat 統計カタログ（IDと概要）
STAT_CATALOG = {
    "0003036516": "毎月勤労統計：産業別・都道府県別の賃金・労働時間",
    "0000060033": "家計調査：二人以上世帯の消費支出・収入",
    "0000060100": "消費者物価指数：全国・地域別の物価変動",
    "0000030001": "学校基本調査：進学率・学校数・生徒数",
    "0000020602": "住民基本台帳人口移動報告：都道府県間転入・転出",
    "0000020301": "農業センサス：農家数・農地面積・農業産出額",
    "0000060073": "商業統計：売場面積・年間販売額・事業所数",
    "0000060046": "工業統計：製造品出荷額・従業者数・付加価値",
    "0000030047": "医療施設調査：病院・診療所・病床数の地域分布",
    "0003215843": "人口動態統計：出生率・死亡率・婚姻率・離婚率",
    "0000060392": "サービス産業動向調査：売上高・従業者数",
    "0000030005": "社会生活基本調査：生活時間・余暇活動",
}


def select_topic() -> dict[str, str]:
    """
    lessons.mdを読んで過去に使ったテーマを確認し、
    Ollamaに未使用の統計IDとテーマを選ばせる。
    返り値: {"stat_id": "...", "theme": "...", "angle": "..."}
    """
    lessons = Path("lessons.md").read_text(encoding="utf-8") if Path("lessons.md").exists() else ""
    used_ids = set()
    for line in lessons.splitlines():
        if "使用統計ID" in line:
            for sid in STAT_CATALOG:
                if sid in line:
                    used_ids.add(sid)

    available = {k: v for k, v in STAT_CATALOG.items() if k not in used_ids}
    if not available:
        available = STAT_CATALOG  # 全部使い切ったらリセット

    catalog_text = "\n".join(f"- {sid}: {desc}" for sid, desc in available.items())
    used_text = "\n".join(f"- {sid}" for sid in used_ids) if used_ids else "なし"

    prompt = f"""あなたはデータジャーナリストです。
以下のe-Stat統計カタログから、最も面白い記事が書けそうな統計を1つ選んでください。

【利用可能な統計】
{catalog_text}

【過去に使用済み（選択禁止）】
{used_text}

選んだ統計について、以下のJSON形式のみで回答（他のテキスト不要）:
{{"stat_id": "統計ID", "theme": "記事のテーマ（20字以内）", "angle": "切り口・問い（30字以内）"}}"""

    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "think": False,
                "options": {"temperature": 0.9, "num_predict": 200},
            },
            timeout=120,
        )
        raw = resp.json().get("response", "")
        import re

        m = re.search(r"\{.*?\}", raw, re.DOTALL)
        if m:
            topic = json.loads(m.group())
            if topic.get("stat_id") in STAT_CATALOG:
                print(f"選択テーマ: {topic.get('theme')} (ID: {topic.get('stat_id')})")
                return topic
    except Exception as e:
        print(f"select_topic エラー: {e}")

    # フォールバック：未使用IDから先頭を選ぶ
    fallback_id = next(iter(available))
    return {
        "stat_id": fallback_id,
        "theme": STAT_CATALOG[fallback_id][:20],
        "angle": "地域格差を探る",
    }


def fetch_data() -> pd.DataFrame:
    """select_topic()でテーマを選んでからe-Statデータを取得する。"""
    topic = select_topic()
    stat_id = topic.get("stat_id", "0003036516")

    if not ESTAT_APPID:
        print(f"ESTAT_APPID未設定 → サンプルデータ（テーマ: {topic.get('theme')}）")
        return _sample_data(topic)

    params = {
        "appId": ESTAT_APPID,
        "statsDataId": stat_id,
        "limit": "200",
        "metaGetFlg": "N",
    }
    try:
        resp = requests.get(f"{ESTAT_BASE}/getStatsData", params=params, timeout=30)
        resp.raise_for_status()
        values = (
            resp.json()
            .get("GET_STATS_DATA", {})
            .get("STATISTICAL_DATA", {})
            .get("DATA_INF", {})
            .get("VALUE", [])
        )
        if values:
            df = pd.DataFrame(values)
            if "$" in df.columns:
                df["value"] = pd.to_numeric(df["$"], errors="coerce")
            df.attrs["topic"] = topic
            df.attrs["stat_id"] = stat_id
            print(f"e-Stat取得: {stat_id} ({len(df)}行)")
            return df
    except Exception as e:
        print(f"e-Stat APIエラー: {e}")

    return _sample_data(topic)


def _sample_data(topic: dict) -> pd.DataFrame:
    """統計IDに応じたサンプルデータを返す。"""
    stat_id = topic.get("stat_id", "")
    theme = topic.get("theme", "統計データ")

    # 汎用サンプル（都道府県×数値2列）
    base = pd.DataFrame(
        {
            "prefecture": [
                "東京都",
                "大阪府",
                "愛知県",
                "神奈川県",
                "福岡県",
                "北海道",
                "宮城県",
                "広島県",
                "新潟県",
                "沖縄県",
            ],
            "value_a": [100, 85, 90, 92, 78, 65, 70, 75, 60, 55],
            "value_b": [120, 95, 88, 105, 82, 70, 72, 80, 58, 62],
            "year": [2023] * 10,
        }
    )
    base.attrs["topic"] = topic
    base.attrs["stat_id"] = stat_id
    print(f"サンプルデータ使用 (stat_id={stat_id}, theme={theme})")
    return base


def analyze(df: pd.DataFrame) -> dict[str, Any]:
    """データを分析して記事用の知見を抽出する。"""
    topic = df.attrs.get("topic", {})
    stat_id = df.attrs.get("stat_id", "不明")
    theme = topic.get("theme", "統計分析")
    angle = topic.get("angle", "地域差の考察")

    result: dict[str, Any] = {
        "theme": theme,
        "angle": angle,
        "stat_id": stat_id,
        "row_count": len(df),
        "columns": list(df.columns),
    }

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if numeric_cols:
        col = numeric_cols[0]
        if "prefecture" in df.columns:
            top3 = df.nlargest(3, col)[["prefecture", col]].to_dict(orient="records")
            bot3 = df.nsmallest(3, col)[["prefecture", col]].to_dict(orient="records")
            result["top3"] = top3
            result["bottom3"] = bot3
            result["max_val"] = float(df[col].max())
            result["min_val"] = float(df[col].min())
            result["mean_val"] = float(df[col].mean())
            result["spread_pct"] = round((df[col].max() - df[col].min()) / df[col].mean() * 100, 1)
        result["summary"] = df[numeric_cols].describe().to_dict()

    return result


def generate_story(analysis: dict[str, Any]) -> str:
    """分析結果をもとにOllamaで記事を生成し、output/story.md に保存する。"""
    theme = analysis.get("theme", "統計データ分析")
    angle = analysis.get("angle", "")
    stat_id = analysis.get("stat_id", "")

    # 分析サマリーをテキスト化
    lines = []
    for k, v in analysis.items():
        if k in ("summary", "columns"):
            continue
        if isinstance(v, list):
            lines.append(f"{k}:")
            for item in v:
                lines.append(f"  {item}")
        else:
            lines.append(f"{k}: {v}")
    analysis_text = "\n".join(lines)

    prompt = f"""あなたは日本のデータジャーナリストです。
以下の分析データをもとに、読者が「なるほど」と感じる記事を書いてください。

【テーマ】{theme}
【切り口】{angle}
【統計ID】{stat_id}

【分析データ】
{analysis_text}

【執筆ルール】
- 冒頭に読者を引き込む一文（問い・驚き・逆説）
- 具体的な数値を引用して主張を支える
- 「なぜそうなのか」の考察を必ず入れる
- 全体600字程度、マークダウン形式（##見出しと箇条書き使用）

記事:"""

    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "think": False,
                "options": {"temperature": 0.8},
            },
            timeout=480,
        )
        resp.raise_for_status()
        story = resp.json().get("response", "").strip()
    except Exception as e:
        story = f"# 記事生成エラー\n\n{e}\n\n## データ概要\n\nテーマ: {theme}\n統計ID: {stat_id}"

    Path("output").mkdir(exist_ok=True)
    Path("output/story.md").write_text(story, encoding="utf-8")
    return story


if __name__ == "__main__":
    print("=== pipeline.py 単体テスト ===")
    df = fetch_data()
    print(f"データ: {len(df)}行, 列: {list(df.columns)}")
    analysis = analyze(df)
    print(f"テーマ: {analysis.get('theme')}")
    story = generate_story(analysis)
    print("\n--- 生成記事（先頭300字）---")
    print(story[:300])
