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

    prompt = f"""あなたはデ
"""
    return {"stat_id": list(available.keys())[0], "theme": prompt.split(":")[1].strip(), "reason": ""}


def fetch_data() -> pd.DataFrame:
    topic = select_topic()
    stat_id = topic.get("stat_id", "")
    if not stat_id:
        raise ValueError("統計IDが未選択です")

    params = {
        "appId": ESTAT_APPID,
        "statId": stat_id,
        "version": "1",
        "format": "json",
    }
    response = requests.get(f"{ESTAT_BASE}/getStatsData", params=params)
    if not response.ok:
        raise ValueError("e-Stat API が正常に動作しませんでした")

    data = json.loads(response.content.decode("utf-8"))
    return pd.DataFrame(data["data"]["result"])


def analyze(df: pd.DataFrame) -> dict[str, Any]:
    """
    データ分析
    返り値: {key: value}
    """
    # ここにデータの分析を書く
    analysis = {}
    return analysis


def generate_story(analysis: dict[str, Any]) -> str:
    prompt = f"""
テーマ：{select_topic()["theme"]}
統計ID：{select_topic()["stat_id"]}
"""
    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "llama3.1:8b",
            "prompt": prompt,
            "stream": False,
            "think": False,
            "options": {"temperature": 0.7},
        },
        timeout=480,
    )
    content = response.json().get("response", "")
    return content


if __name__ == "__main__":
    df = fetch_data()
    analysis = analyze(df)
    generate_story(analysis)
