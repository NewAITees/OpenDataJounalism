"""
自律型データジャーナリズム パイプライン
WARNING: runner.py が毎サイクル書き換えます。手動編集は次サイクルで失われます。

4段構成:
  select_topic()           ← Ollamaがカタログから未使用テーマを選ぶ
  fetch_data()             ← e-Stat全件をSQLiteに格納（キャッシュ有）→ 軽量infoを返す
  analyze(info)            ← SQL集計のみ（全データはDBのまま・メモリに乗せない）
  generate_story(analysis) ← Ollamaで記事生成 → output/story.md
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

ESTAT_APPID: str = os.getenv("ESTAT_APPID", "")
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen3.5:9b"
ESTAT_BASE = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
DB_PATH = Path("data/estat_cache.db")
PAGE_SIZE = 10000

STAT_CATALOG: dict[str, str] = {
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

CATALOG_DB = Path("data/mcp/catalog_index.db")
METADATA_DB = Path("data/mcp/estat_metadata.db")


# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------


def _call_ollama(prompt: str) -> str:
    try:
        resp = requests.post(
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
        resp.raise_for_status()
        return resp.json().get("response", "")
    except Exception as e:
        print(f"  [Ollama ERROR] {e}")
        return ""


# ---------------------------------------------------------------------------
# catalog_index.db / estat_metadata.db ユーティリティ
# ---------------------------------------------------------------------------


def _load_catalog_from_db() -> dict[str, dict[str, str]]:
    """
    catalog_index.db の stats_tables から統計表情報を取得する。
    Returns: {stats_data_id: {table_name, description, field_name, keywords}}
    """
    if not CATALOG_DB.exists():
        return {}
    try:
        conn = sqlite3.connect(CATALOG_DB)
        rows = conn.execute(
            "SELECT stats_data_id, table_name, description, field_name, keywords FROM stats_tables"
        ).fetchall()
        conn.close()
        return {
            r[0]: {
                "table_name": r[1] or "",
                "description": r[2] or "",
                "field_name": r[3] or "",
                "keywords": r[4] or "[]",
            }
            for r in rows
            if r[0]
        }
    except Exception as e:
        print(f"  [catalog_db] 読み込みエラー: {e}")
        return {}


def _save_cls_info_to_metadata_db(
    table_id: str,
    cls_info: dict[str, tuple[str, dict[str, str]]],
) -> None:
    """
    フェッチ時に得たCLASS_INF（軸情報）を estat_metadata.db に保存する。
    table_metadata: 軸オブジェクトの定義
    class_values:  各コードと名前のマッピング
    """
    if not METADATA_DB.exists():
        return
    try:
        conn = sqlite3.connect(METADATA_DB)
        for cls_obj_id, (col_name, code_map) in cls_info.items():
            conn.execute(
                "INSERT OR REPLACE INTO table_metadata VALUES (?, ?, ?, ?, ?, ?)",
                (table_id, cls_obj_id, col_name, col_name, "", ""),
            )
            for code, name in code_map.items():
                conn.execute(
                    "INSERT OR REPLACE INTO class_values VALUES (?, ?, ?, ?, ?, ?)",
                    (table_id, cls_obj_id, code, name, "", ""),
                )
        conn.commit()
        conn.close()
        print(f"  [metadata_db] {table_id} の軸情報を保存しました（{len(cls_info)}軸）")
    except Exception as e:
        print(f"  [metadata_db] 保存エラー: {e}")


def _load_col_descriptions_from_metadata_db(table_id: str) -> dict[str, str]:
    """
    estat_metadata.db から軸名→説明のマッピングを取得する。
    analyze() でカラムの意味を補足するために使う。
    """
    if not METADATA_DB.exists():
        return {}
    try:
        conn = sqlite3.connect(METADATA_DB)
        rows = conn.execute(
            "SELECT class_obj_id, class_obj_name FROM table_metadata WHERE table_id = ?",
            (table_id,),
        ).fetchall()
        conn.close()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# e-Stat API → SQLite（ページネーション全件取得）
# ---------------------------------------------------------------------------


def _extract_cls_info(stat_data: dict[str, Any]) -> dict[str, tuple[str, dict[str, str]]]:
    """CLASS_INF から {cls_id: (col_name, {code: name})} を構築する。"""
    class_objs = stat_data.get("CLASS_INF", {}).get("CLASS_OBJ", [])
    if isinstance(class_objs, dict):
        class_objs = [class_objs]
    cls_info: dict[str, tuple[str, dict[str, str]]] = {}
    for obj in class_objs:
        cls_id = obj["@id"]
        col_name = obj["@name"]
        classes = obj.get("CLASS", [])
        if isinstance(classes, dict):
            classes = [classes]
        code_map = {c["@code"]: c["@name"] for c in classes}
        cls_info[cls_id] = (col_name, code_map)
    return cls_info


def _parse_values(
    stat_data: dict[str, Any],
    cls_info: dict[str, tuple[str, dict[str, str]]],
) -> list[dict[str, Any]]:
    """DATA_INF.VALUE をコード→名前変換してrowsリストで返す。"""
    values = stat_data.get("DATA_INF", {}).get("VALUE", [])
    if isinstance(values, dict):
        values = [values]
    rows: list[dict[str, Any]] = []
    for v in values:
        row: dict[str, Any] = {}
        for attr, code in v.items():
            if attr == "$":
                row["value"] = code
            elif attr.startswith("@") and attr != "@unit":
                key = attr[1:]
                if key in cls_info:
                    col_name, code_map = cls_info[key]
                    row[col_name] = code_map.get(code, code)
        rows.append(row)
    return rows


def _fetch_all_pages(stat_id: str, conn: sqlite3.Connection) -> tuple[list[str], int]:
    """
    e-Stat APIを全ページ取得してSQLiteテーブルに格納する。
    1ページ目でCLASS_INFを取得してコードマップを構築。
    2ページ目以降はmetaGetFlg=Nで高速化。
    Returns: (col_names, total_rows)
    """
    table_name = f"stat_{stat_id}"
    start = 1
    total_rows = 0
    col_names: list[str] = []
    cls_info: dict[str, tuple[str, dict[str, str]]] = {}
    table_created = False

    print(f"  [API] 統計ID={stat_id} 全件取得開始...")

    while True:
        params: dict[str, Any] = {
            "appId": ESTAT_APPID,
            "statsDataId": stat_id,
            "startPosition": start,
            "limit": PAGE_SIZE,
            "metaGetFlg": "Y" if start == 1 else "N",
            "cntGetFlg": "N",
            "explanationGetFlg": "N",
            "annotationGetFlg": "N",
            "replaceSpChars": "0",
        }

        resp = requests.get(ESTAT_BASE, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        stat_data = data["GET_STATS_DATA"]["STATISTICAL_DATA"]

        # 1ページ目のみCLASS_INFを解析 → metadata_dbに保存
        if start == 1:
            cls_info = _extract_cls_info(stat_data)
            _save_cls_info_to_metadata_db(stat_id, cls_info)

        rows = _parse_values(stat_data, cls_info)
        if not rows:
            break

        # テーブル作成（初回のみ）
        if not table_created:
            first_row = rows[0]
            col_names = ["value"] + [k for k in first_row if k != "value"]
            col_defs = ", ".join(f'"{c}" TEXT' for c in col_names)
            conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.execute(f'CREATE TABLE "{table_name}" ({col_defs})')
            conn.commit()
            table_created = True

        # バッチ挿入
        placeholders = ", ".join(["?"] * len(col_names))
        batch = [[row.get(c) for c in col_names] for row in rows]
        conn.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', batch)
        conn.commit()

        total_rows += len(rows)
        result_inf = stat_data.get("RESULT_INF", {})
        next_key = result_inf.get("NEXT_KEY")
        print(f"  [API] {total_rows:,}件取得済み")

        if not next_key:
            break
        start = int(next_key)
        time.sleep(0.3)

    print(f"  [API] 取得完了: 計{total_rows:,}行")
    return col_names, total_rows


def _ensure_cached(stat_id: str, theme: str, angle: str) -> dict[str, Any]:
    """
    DBにキャッシュがあれば即返す。なければ全件取得してDBに格納する。
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fetch_meta (
            stat_id    TEXT PRIMARY KEY,
            fetched_at TEXT,
            total_rows INTEGER,
            col_names  TEXT
        )
    """)
    conn.commit()

    row = conn.execute(
        "SELECT fetched_at, total_rows, col_names FROM fetch_meta WHERE stat_id = ?",
        (stat_id,),
    ).fetchone()

    if row:
        print(f"  [DB] キャッシュHIT: {stat_id} ({row[1]:,}行, 取得日: {row[0][:10]})")
        col_names = json.loads(row[2])
        total_rows = row[1]
        cached = True
    else:
        print("  [DB] キャッシュなし → APIから全件取得します")
        col_names, total_rows = _fetch_all_pages(stat_id, conn)
        conn.execute(
            "INSERT OR REPLACE INTO fetch_meta VALUES (?, datetime('now'), ?, ?)",
            (stat_id, total_rows, json.dumps(col_names, ensure_ascii=False)),
        )
        conn.commit()
        cached = False

    conn.close()

    return {
        "stat_id": stat_id,
        "table": f"stat_{stat_id}",
        "db_path": str(DB_PATH),
        "cols": col_names,
        "rows": total_rows,
        "theme": theme,
        "angle": angle,
        "cached": cached,
    }


# ---------------------------------------------------------------------------
# パイプライン 4関数
# ---------------------------------------------------------------------------


def select_topic() -> dict[str, str]:
    """
    catalog_index.db + STAT_CATALOG + lessons.md を組み合わせて
    Ollamaに未使用の統計IDを選ばせる。
    """
    lessons_path = Path("docs/lessons.md")
    lessons = lessons_path.read_text(encoding="utf-8") if lessons_path.exists() else ""

    # 過去使用済みIDを抽出
    used_ids: set[str] = set()
    for line in lessons.splitlines():
        if "使用統計ID" in line:
            for sid in STAT_CATALOG:
                if sid in line:
                    used_ids.add(sid)

    available = {k: v for k, v in STAT_CATALOG.items() if k not in used_ids}
    if not available:
        available = STAT_CATALOG  # 全使用済みならリセット

    # catalog_index.db から追加情報を取得してプロンプトを強化
    catalog_db_entries = _load_catalog_from_db()

    catalog_lines: list[str] = []
    for sid, desc in available.items():
        db_info = catalog_db_entries.get(sid, {})
        if db_info:
            keywords = db_info.get("keywords", "[]")
            try:
                kw_list = json.loads(keywords)
                kw_str = "、".join(kw_list[:5])
            except Exception:
                kw_str = ""
            line = f"- {sid}: {desc}"
            if kw_str:
                line += f"（キーワード: {kw_str}）"
            catalog_lines.append(line)
        else:
            catalog_lines.append(f"- {sid}: {desc}")

    catalog_text = "\n".join(catalog_lines)
    used_text = "\n".join(f"- {sid}" for sid in used_ids) or "なし"

    prompt = f"""あなたはデータジャーナリストです。
以下のe-Stat統計カタログから、最も面白い記事が書けそうな統計を1つ選んでください。

【利用可能な統計】
{catalog_text}

【過去に使用済み（選択禁止）】
{used_text}

選んだ統計について、以下のJSON形式のみで回答（他のテキスト不要）:
{{"stat_id": "統計ID（10桁の数字）", "theme": "記事テーマ（20字以内）", "angle": "切り口・問い（30字以内）"}}"""

    raw = _call_ollama(prompt)

    try:
        m = re.search(r"\{.*?\}", raw, re.DOTALL)
        if m:
            result = json.loads(m.group())
            if result.get("stat_id") in STAT_CATALOG:
                print(
                    f"  [topic] {result['stat_id']}: {result.get('theme')} / {result.get('angle')}"
                )
                return result
    except Exception:
        pass

    # フォールバック：未使用IDの先頭
    fallback_id = next(iter(available))
    print(f"  [topic] フォールバック: {fallback_id}")
    return {
        "stat_id": fallback_id,
        "theme": STAT_CATALOG[fallback_id][:20],
        "angle": "地域格差と時系列変化を探る",
    }


def fetch_data() -> dict[str, Any]:
    """トピック選択 → DBキャッシュ確認/全件取得 → 軽量な情報dictを返す。"""
    topic = select_topic()
    return _ensure_cached(topic["stat_id"], topic["theme"], topic["angle"])


def analyze(info: dict[str, Any]) -> dict[str, Any]:
    """
    SQLite上でGROUP BY集計のみ実施。
    全データはDBのままでメモリに乗せない。
    """
    table = info["table"]
    db_path = info["db_path"]
    cols = info["cols"]
    stat_id = info["stat_id"]

    conn = sqlite3.connect(db_path)

    # metadata_db から軸の説明を取得（カラム名の補足情報）
    col_descriptions = _load_col_descriptions_from_metadata_db(stat_id)

    analysis: dict[str, Any] = {
        "stat_id": stat_id,
        "theme": info["theme"],
        "angle": info["angle"],
        "total_rows": info["rows"],
        "cols": cols,
        "col_descriptions": col_descriptions,
        "value_stats": {},
        "time_trend": [],
        "regional_diff": [],
        "top_findings": [],
    }

    # 基本統計（SQLで集計）
    if "value" in cols:
        row = conn.execute(
            f"SELECT MIN(CAST(value AS REAL)), MAX(CAST(value AS REAL)), "
            f"AVG(CAST(value AS REAL)), COUNT(*) "
            f'FROM "{table}" WHERE value IS NOT NULL AND value != ""'
        ).fetchone()
        if row and row[0] is not None:
            analysis["value_stats"] = {
                "min": round(row[0], 2),
                "max": round(row[1], 2),
                "mean": round(row[2], 2),
                "count": row[3],
            }

    # 時系列トレンド：「年」「時間」「期間」を含む列でGROUP BY
    time_col = next(
        (c for c in cols if any(k in c for k in ["年", "時間", "期間", "time", "Time"])),
        None,
    )
    if time_col and "value" in cols:
        rows = conn.execute(
            f'SELECT "{time_col}", AVG(CAST(value AS REAL)) '
            f'FROM "{table}" WHERE value IS NOT NULL AND value != "" '
            f'GROUP BY "{time_col}" ORDER BY "{time_col}" LIMIT 20'
        ).fetchall()
        analysis["time_trend"] = [
            {"period": r[0], "avg": round(r[1], 2)} for r in rows if r[1] is not None
        ]

    # 地域差：「都道府県」「地域」「地区」「市区」を含む列でGROUP BY
    region_col = next(
        (c for c in cols if any(k in c for k in ["都道府県", "地域", "地区", "市区"])),
        None,
    )
    if region_col and "value" in cols:
        rows = conn.execute(
            f'SELECT "{region_col}", AVG(CAST(value AS REAL)) as avg_val '
            f'FROM "{table}" WHERE value IS NOT NULL AND value != "" '
            f'GROUP BY "{region_col}" ORDER BY avg_val DESC LIMIT 15'
        ).fetchall()
        analysis["regional_diff"] = [
            {"region": r[0], "avg": round(r[1], 2)} for r in rows if r[1] is not None
        ]

    conn.close()

    # 主要発見をテキストで構築
    findings: list[str] = []
    if len(analysis["time_trend"]) >= 2:
        first = analysis["time_trend"][0]
        last = analysis["time_trend"][-1]
        if first["avg"] != 0:
            change = (last["avg"] - first["avg"]) / abs(first["avg"]) * 100
            direction = "増加" if change > 0 else "減少"
            findings.append(
                f"{first['period']}→{last['period']}で{abs(change):.1f}%{direction}"
                f"（{first['avg']} → {last['avg']}）"
            )
    if len(analysis["regional_diff"]) >= 2:
        top = analysis["regional_diff"][0]
        bottom = analysis["regional_diff"][-1]
        if bottom["avg"] != 0:
            gap = top["avg"] / bottom["avg"]
            findings.append(
                f"最高: {top['region']}({top['avg']}) / "
                f"最低: {bottom['region']}({bottom['avg']}) → {gap:.1f}倍の地域格差"
            )
    if analysis["value_stats"]:
        s = analysis["value_stats"]
        findings.append(
            f"平均{s['mean']:.2f}（最小{s['min']:.2f}〜最大{s['max']:.2f}、"
            f"有効件数{s['count']:,}件）"
        )

    analysis["top_findings"] = findings
    return analysis


def generate_story(analysis: dict[str, Any]) -> str:
    """分析結果をもとにOllamaで記事を生成し output/story.md に保存する。"""
    trend_text = "\n".join(f"  {t['period']}: {t['avg']}" for t in analysis["time_trend"][:12])
    region_text = "\n".join(f"  {r['region']}: {r['avg']}" for r in analysis["regional_diff"][:10])

    prompt = f"""あなたは日本のデータジャーナリストです。
以下の統計分析結果をもとに、一般市民が関心を持てる日本語の記事を書いてください。

【統計ID】{analysis["stat_id"]}（e-Stat実データ、{analysis["total_rows"]:,}行）
【テーマ】{analysis["theme"]}
【切り口】{analysis["angle"]}

【基本統計】
平均: {analysis["value_stats"].get("mean", "N/A")} / 最大: {analysis["value_stats"].get("max", "N/A")} / 最小: {analysis["value_stats"].get("min", "N/A")}

【時系列変化】
{trend_text or "（データなし）"}

【地域差（高い順）】
{region_text or "（データなし）"}

【主要発見】
{chr(10).join(f"・{f}" for f in analysis["top_findings"])}

【記事の要件】
1. 読者を引き込む問いかけで書き始める
2. 上記の実数値を具体的に引用する（統計ID {analysis["stat_id"]} のデータより）
3. 時系列・地域比較のどちらかを必ず含める
4. 数値の羅列でなく、パターン・変化・異常値の洞察を書く
5. 800〜1200字程度、Markdown形式
6. 末尾に「データ出典: e-Stat 統計ID {analysis["stat_id"]}」を明記

記事のみ出力（前置き・説明不要）:"""

    content = _call_ollama(prompt)

    story_path = Path("output/story.md")
    story_path.parent.mkdir(exist_ok=True)
    story_path.write_text(content, encoding="utf-8")
    print(f"  [story] output/story.md 保存完了（{len(content):,}文字）")

    return content


if __name__ == "__main__":
    df = fetch_data()
    analysis = analyze(df)
    generate_story(analysis)
