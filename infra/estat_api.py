"""
e-Stat API v3 ラッパー。変更しない安定インフラ。
"""

from __future__ import annotations

import os
import random
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

APPID: str = os.getenv("ESTAT_APPID", "")
BASE = "https://api.e-stat.go.jp/rest/3.0/app/json"

STAT_FIELDS: dict[str, str] = {
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


def pick_random_table() -> dict[str, str]:
    """ランダムに統計分野を選び、その中からランダムに1表を返す。"""
    if not APPID:
        raise RuntimeError("ESTAT_APPID が未設定です。")

    field_code = random.choice(list(STAT_FIELDS.keys()))
    field_name = STAT_FIELDS[field_code]

    resp = requests.get(
        f"{BASE}/getStatsList",
        params={"appId": APPID, "statsField": field_code, "limit": 100},
        timeout=30,
    )
    resp.raise_for_status()

    tables = resp.json().get("GET_STATS_LIST", {}).get("DATALIST_INF", {}).get("TABLE_INF", [])
    if isinstance(tables, dict):
        tables = [tables]
    if not tables:
        raise RuntimeError(f"statsField={field_code} に表が見つかりません。")

    table = random.choice(tables)
    title = table.get("TITLE", {})
    title_str = title.get("$", str(title)) if isinstance(title, dict) else str(title)

    return {
        "stats_data_id": table["@id"],
        "field_code": field_code,
        "field_name": field_name,
        "title": title_str,
    }


def fetch_values(stats_data_id: str, limit: int = 10000) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    statsDataId でデータを取得し (DataFrame, CLASS情報dict) を返す。
    CLASS情報はコードと名称のマッピング。
    """
    if not APPID:
        raise RuntimeError("ESTAT_APPID が未設定です。")

    resp = requests.get(
        f"{BASE}/getStatsData",
        params={"appId": APPID, "statsDataId": stats_data_id, "limit": limit},
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json().get("GET_STATS_DATA", {})

    result_status = body.get("RESULT", {}).get("STATUS", -1)
    if result_status != 0:
        msg = body.get("RESULT", {}).get("ERROR_MSG", "不明なエラー")
        raise RuntimeError(f"e-Stat STATUS={result_status}: {msg}")

    stat = body.get("STATISTICAL_DATA", {})

    # CLASS情報: {dim_id: {code: name}}
    class_map: dict[str, dict[str, str]] = {}
    class_objs = stat.get("CLASS_INF", {}).get("CLASS_OBJ", [])
    if isinstance(class_objs, dict):
        class_objs = [class_objs]
    for obj in class_objs:
        dim_id = obj.get("@id", "")
        items = obj.get("CLASS", [])
        if isinstance(items, dict):
            items = [items]
        class_map[dim_id] = {item["@code"]: item["@name"] for item in items}

    values = stat.get("DATA_INF", {}).get("VALUE", [])
    if not values:
        raise RuntimeError("DATA_INF.VALUE が空です。")

    df = pd.DataFrame(values)
    df["value"] = pd.to_numeric(df.get("$", pd.Series(dtype=float)), errors="coerce")

    return df, class_map
