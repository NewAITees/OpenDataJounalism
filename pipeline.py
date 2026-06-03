"""
データジャーナリズムシステム パイプライン (サイクル11 用)
ターゲット: e-Stat 公開カタログの実在するデータのみを使用
テーマ選定: select_topic() で動的に決定 (労働・家計・住宅・医療など)
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# 設定変数
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen3.5:9b"
ESTAT_API_URL = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
ESTAT_LIST_URL = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsList"
OUTPUT_DIR = Path("output")
OUTPUT_FILE = OUTPUT_DIR / "story.md"
CHART_DIR = OUTPUT_DIR / "charts"
CHART_DIR.mkdir(parents=True, exist_ok=True)

# e-Stat 統計分野コード（02〜16）と分野名
# getStatsList の statsField パラメータに使う
STAT_FIELDS = {
    "02": "人口・世帯",
    "03": "労働・賃金",
    "04": "農林水産業",
    "05": "鉱工業",
    "06": "商業・サービス業",
    "07": "企業・家計・経済",
    "08": "住宅・土地・建設",
    "09": "エネルギー・水",
    "10": "運輸・観光",
    "11": "情報通信・科学技術",
    "12": "教育・文化・スポーツ・生活",
    "13": "行財政",
    "14": "司法・安全・環境",
    "15": "社会保障・衛生",
    "16": "国際",
}

ESTAT_APPID = os.getenv("ESTAT_APPID", "")


def select_topic() -> dict:
    """
    ランダムに統計分野を選び、getStatsList で表一覧を取得し、
    その中からランダムに1表を選んで statsDataId とテーマを返す。
    """
    import random

    if not ESTAT_APPID:
        raise RuntimeError("ESTAT_APPID が設定されていません。")

    field_code = random.choice(list(STAT_FIELDS.keys()))
    field_name = STAT_FIELDS[field_code]

    params = {
        "appId": ESTAT_APPID,
        "statsField": field_code,
        "limit": 100,
    }
    resp = requests.get(ESTAT_LIST_URL, params=params, timeout=30)
    resp.raise_for_status()

    tables = resp.json().get("GET_STATS_LIST", {}).get("DATALIST_INF", {}).get("TABLE_INF", [])
    if isinstance(tables, dict):
        tables = [tables]

    if not tables:
        raise RuntimeError(f"statsField={field_code} に表が見つかりませんでした。")

    table = random.choice(tables)
    stats_data_id = table["@id"]
    title = table.get("TITLE", {})
    title_str = title.get("$", title) if isinstance(title, dict) else str(title)

    return {
        "stats_data_id": stats_data_id,
        "field_code": field_code,
        "field_name": field_name,
        "title": title_str,
    }


def fetch_data(topic: dict | None = None) -> pd.DataFrame:
    """
    select_topic() の結果（stats_data_id）を使って e-Stat API からデータを取得する。
    """
    if topic is None:
        topic = select_topic()

    stats_data_id = topic.get("stats_data_id", "")
    if not stats_data_id:
        print("[ERROR] stats_data_id が空です。")
        return pd.DataFrame()

    print(f"[INFO] データ取得開始: {stats_data_id} ({topic.get('title', '')})")

    params: dict[str, Any] = {
        "statsDataId": stats_data_id,
        "appId": ESTAT_APPID,
        "limit": 10000,
    }

    try:
        response = requests.get(ESTAT_API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        values = (
            data.get("GET_STATS_DATA", {})
            .get("STATISTICAL_DATA", {})
            .get("DATA_INF", {})
            .get("VALUE", [])
        )

        if not values:
            print("[WARNING] DATA_INF.VALUE が空です。")
            return pd.DataFrame()

        df = pd.DataFrame(values)
        if "$" in df.columns:
            df["value"] = pd.to_numeric(df["$"], errors="coerce")

        print(f"[INFO] データ取得に成功しました。行数: {len(df)}, 列: {df.columns.tolist()}")
        return df

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] e-Stat API コールに失敗しました: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"[ERROR] 予期しないエラーが発生しました: {e}")
        return pd.DataFrame()


def analyze(df: pd.DataFrame) -> dict[str, Any]:
    """
    データ分析を行う。
    可視化用の数値（平均値、標準偏差、時系列変化など）を抽出し、
    地域間や産業間の格差を浮き彫りにする。
    """
    if df.empty:
        return {"error": "データが空です。"}

    print("[INFO] データ分析を開始します。")

    columns = df.columns.tolist()
    print(f"[INFO] 利用可能な列: {columns}")

    # e-Stat v3 の VALUE 配列は {"@area": ..., "@time": ..., "@cat01": ..., "$": "数値"} 形式
    value_col = "value" if "value" in columns else "$"
    area_col = "@area" if "@area" in columns else None
    time_col = "@time" if "@time" in columns else None

    analysis: dict[str, Any] = {"data_type": "一般分析", "insight": ""}

    if value_col in columns:
        numeric = df[value_col].dropna()
        analysis["total_rows"] = len(numeric)
        analysis["mean"] = float(numeric.mean())
        analysis["std"] = float(numeric.std())

    if area_col and value_col in columns:
        region_avg = df.groupby(area_col)[value_col].mean().dropna()
        if not region_avg.empty:
            max_region = region_avg.idxmax()
            min_region = region_avg.idxmin()
            diff = float(region_avg.max() - region_avg.min())
            analysis.update(
                {
                    "data_type": "地域比較",
                    "max_region": max_region,
                    "min_region": min_region,
                    "diff": diff,
                    "insight": f"地域間の差は {diff:.2f}（最大: {max_region}, 最小: {min_region}）",
                }
            )

    if time_col and value_col in columns:
        try:
            time_data = df.sort_values(time_col)
            numeric_time = pd.to_numeric(time_data[value_col], errors="coerce").dropna()
            if len(numeric_time) >= 2:
                first_val = float(numeric_time.iloc[0])
                last_val = float(numeric_time.iloc[-1])
                change_rate = ((last_val - first_val) / first_val * 100) if first_val != 0 else 0
                analysis["trend"] = {
                    "start": first_val,
                    "end": last_val,
                    "change_rate": change_rate,
                }
                analysis["insight"] += f" 時系列変化率: {change_rate:.2f}%"
        except Exception as e:
            print(f"[WARNING] 時系列分析に失敗しました: {e}")

    print(f"[INFO] 分析完了。タイプ: {analysis.get('data_type', '不明')}")
    return analysis


def generate_story(analysis: dict[str, Any], topic: dict | None = None) -> str:
    """
    分析結果に基づいて記事生成を行う。
    生成された記事は output/story.md に書き込む。
    """
    if "error" in analysis:
        return analysis["error"]

    # 分析結果の抽出
    insight = analysis.get("insight", "")
    data_type = analysis.get("data_type", "")
    max_region = analysis.get("max_region", "")
    min_region = analysis.get("min_region", "")
    diff = analysis.get("diff", 0)
    trend = analysis.get("trend", {})

    # 記事の構成
    # タイトル: データの切り口に基づいて生成
    # 本文: 洞察・数値・物語性を組み合わせて作成

    prompt = f"""
    以下の分析結果に基づいて、データジャーナリズム記事を作成してください。

    分析結果:
    - 洞察: {insight}
    - データタイプ: {data_type}
    - 最大値の地域/産業: {max_region}
    - 最小値の地域/産業: {min_region}
    - 差: {diff}
    - 時系列変化率: {trend.get("change_rate", "N/A")}%

    要件:
    1. タイトルは魅力的で、読者の関心を引きつけるものにする。
    2. 導入: 社会的な問題や疑問を提起する。
    3. 本論: データの数値・傾向を具体的に提示し、物語性を加える。
    4. 結論: 今後の展望や対策を提案する。
    5. 出典: e-Stat 統計 ID を明記する。
    6. 日本語の自然な文章で、専門用語は必要最小限にする。
    7. マークダウン形式で出力する。

    出力形式:
    # タイトル

    導入

    本論

    結論

    出典
    """

    # Ollama を呼び出して記事生成
    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "think": False,
                "options": {"temperature": 0.7},
            },
            timeout=480,
        )
        response.raise_for_status()
        content = response.json().get("response", "")

        if not content:
            print("[WARNING] Ollama からの応答が空です。")
            content = "# データ分析記事\n\n分析結果に基づいての記事を生成できませんでした。\n\n出典：e-Stat\n"

        # ファイルに書き込む
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(content)

        print(f"[INFO] 記事が {OUTPUT_FILE} に書き込まれました。")
        return content

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Ollama API コールに失敗しました: {e}")
        # エラー時のフォールバック記事
        stat_id_str = topic.get("stat_id", "不明") if topic else "不明"
        fallback = f"""# データ分析記事

## 導入
e-Stat 統計データに基づき、地域間・産業間の格差や時系列の変化について分析を行いました。

## 本論
- **洞察**: {insight}
- **データタイプ**: {data_type}
- **最大値の地域/産業**: {max_region}
- **最小値の地域/産業**: {min_region}
- **差**: {diff}

## 結論
データに基づく分析が、社会問題の解決に貢献することを願っています。

## 出典
e-Stat 統計 ID: {stat_id_str}
"""
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(fallback)
        print(f"[INFO] フォールバック記事が {OUTPUT_FILE} に書き込まれました。")
        return fallback
    except Exception as e:
        print(f"[ERROR] 予期しないエラーが発生しました: {e}")
        return "# エラーが発生しました。\n\n詳細を確認してください。"


# メイン実行
if __name__ == "__main__":
    # select_topic() でテーマを選択
    topic = select_topic()

    # データ取得
    df = fetch_data(topic)

    # データ分析
    analysis = analyze(df)

    # 記事生成
    generate_story(analysis, topic)
