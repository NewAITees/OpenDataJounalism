"""
自律型データジャーナリズム パイプライン (内部ループ型 v2)
WARNING: runner.py が毎サイクル書き換えます。手動編集は次サイクルで失われます。

内部ループ構造:
  for each dataset:
    [1] fetch_data     → SQLite キャッシュ/API全件取得
    [2] read_catalog   → 軸メタデータ取得
    [3] decide_viz     → Ollama: 何をグラフにするか決定
    [4] gen_viz_code   → Ollama: matplotlib コード生成
    [5] exec_viz       → サブプロセスで実行 → PNG保存
    [6] analyze_chart  → Ollama: 数値データから洞察生成
  completion_flag = True
  [7] generate_story  → output/story.md
"""

from __future__ import annotations

import json
import os
import random
import re
import sqlite3
import subprocess
import sys
import tempfile
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
CHART_DIR = Path("output/charts")
PAGE_SIZE = 10000
MAX_DATASETS = 3

# e-Stat API で実在を確認済みの統計表ID（STATUS=0 のもののみ）
STAT_CATALOG: dict[str, str] = {
    # --- 労働・賃金 ---
    "0003036516": "毎月勤労統計：産業別・都道府県別の賃金・労働時間",
    "0000040101": "労働力調査：就業・失業の動向",
    "0003356101": "毎月勤労統計：産業・事業所規模別の賃金と雇用",
    "0003296362": "賃金構造基本統計：産業別所定内賃金の構成",
    "0003296512": "賃金構造基本統計：賃金分布",
    "0003004521": "就業構造基本調査2007：雇用形態・従業上の地位",
    "0003086553": "就業構造基本調査2012：雇用形態・従業上の地位",
    # --- 人口・世帯 ---
    "0000020101": "人口推計：都道府県別・月次人口",
    "0000150041": "国勢調査1993：人口・世帯・住居の状況",
    "0000150271": "国勢調査2006：人口・世帯・住居の状況",
    # --- 家計・消費 ---
    "0000010112": "家計調査：世帯の収入・支出・貯蓄",
    "0000030005": "社会生活基本調査：生活時間・余暇活動",
    "0003085504": "社会生活基本調査：行動者率と行動時間",
    # --- 農林水産業 ---
    "0000020301": "農業センサス：農家数・農地面積・農業産出額",
    "0002065075": "農業経営統計2021：農産物生産費・経営収支",
    "0002112323": "農業経営統計2022：農産物生産費・経営収支",
    "0001993642": "農業経営統計2020：農地・市町村・経営収支",
    # --- 医療・福祉 ---
    "0000030047": "医療施設調査：病院・診療所・病床数の地域分布",
    "0004002240": "医療統計：施設・在宅サービス・次回受診施設",
    "0003276720": "医療施設調査：施設数・病床数（最新）",
    "0003079737": "社会福祉施設等調査：施設数・定員・在所者数",
    # --- 教育・文化 ---
    "0000030001": "学校基本調査：進学率・学校数・生徒数",
    "0003021794": "社会教育調査：公民館・図書館・博物館数",
    # --- 住宅・土地 ---
    "0000080407": "住宅土地統計1993：住宅の種類・所有関係・建て方",
    "0003355288": "住宅土地統計2018：住宅の種類・所有関係・建て方",
    # --- 産業・経済 ---
    "0003179201": "産業利用統計：農業用施設・機械の利用状況",
    "0003463798": "国民経済計算：GDP構成要素の推移",
}

CATALOG_DB = Path("data/mcp/catalog_index.db")
METADATA_DB = Path("data/mcp/estat_metadata.db")


# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------


def _call_ollama(prompt: str, max_tokens: int = 2048) -> str:
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "think": False,
                "options": {"temperature": 0.7, "num_predict": max_tokens},
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
    except Exception as e:
        print(f"  [metadata_db] 保存エラー: {e}")


def _load_col_descriptions_from_metadata_db(table_id: str) -> dict[str, str]:
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


def _extract_cls_info(
    stat_data: dict[str, Any],
) -> dict[str, tuple[str, dict[str, str]]]:
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

        if start == 1:
            cls_info = _extract_cls_info(stat_data)
            _save_cls_info_to_metadata_db(stat_id, cls_info)

        rows = _parse_values(stat_data, cls_info)
        if not rows:
            break

        if not table_created:
            first_row = rows[0]
            col_names = ["value"] + [k for k in first_row if k != "value"]
            col_defs = ", ".join(f'"{c}" TEXT' for c in col_names)
            conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.execute(f'CREATE TABLE "{table_name}" ({col_defs})')
            conn.commit()
            table_created = True

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
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS fetch_meta (
            stat_id TEXT PRIMARY KEY, fetched_at TEXT,
            total_rows INTEGER, col_names TEXT)"""
    )
    conn.commit()

    row = conn.execute(
        "SELECT fetched_at, total_rows, col_names FROM fetch_meta WHERE stat_id = ?",
        (stat_id,),
    ).fetchone()

    if row:
        print(f"  [DB] キャッシュHIT: {stat_id} ({row[1]:,}行, {row[0][:10]})")
        col_names = json.loads(row[2])
        total_rows = row[1]
        cached = True
    else:
        print("  [DB] キャッシュなし → APIから全件取得")
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
# 内部ループ: 可視化フェーズ
# ---------------------------------------------------------------------------


def _decide_viz_direction(dataset: dict[str, Any]) -> dict[str, Any]:
    """Ollamaがデータ構造から可視化方針を決定する。"""
    cols = dataset["cols"]
    stat_id = dataset["stat_id"]
    catalog_desc = STAT_CATALOG.get(stat_id, "")

    # サンプル行を取得して構造把握
    conn = sqlite3.connect(dataset["db_path"])
    try:
        sample_rows = conn.execute(f'SELECT * FROM "{dataset["table"]}" LIMIT 5').fetchall()
    except Exception:
        sample_rows = []
    conn.close()

    sample_text = "\n".join(str(r) for r in sample_rows[:3])

    default: dict[str, Any] = {
        "chart_type": "bar",
        "x_col": cols[1] if len(cols) > 1 else cols[0],
        "y_col": "value",
        "title": catalog_desc[:30],
        "limit": 20,
        "description": "基本集計の棒グラフ",
    }

    # x_col が cols に存在するか確認するためのバリデーション用
    non_value_cols = [c for c in cols if c != "value"]
    if not non_value_cols:
        return default

    prompt = f"""統計データの可視化方針を1つ決めてください。

統計ID: {stat_id}
説明: {catalog_desc}
カラム一覧: {cols}
サンプル（最初3行）:
{sample_text}

JSONのみで回答（コードブロック不要）:
{{"chart_type": "bar", "x_col": "カラム名（上記一覧から選ぶこと）", "y_col": "value", "title": "グラフタイトル（日本語30字以内）", "limit": 20, "description": "この可視化で何がわかるか（1文）"}}"""

    raw = _call_ollama(prompt, max_tokens=512)
    try:
        m = re.search(r"\{.*?\}", raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            # x_col がカラム一覧に存在するか検証
            if parsed.get("x_col") in cols:
                default.update(parsed)
    except Exception:
        pass

    return default


def _generate_viz_code(dataset: dict[str, Any], viz_plan: dict[str, Any]) -> str:
    """Ollamaにmatplotlib可視化コードを生成させる。失敗時はフォールバック。"""
    stat_id = dataset["stat_id"]
    table = dataset["table"]
    db_path = dataset["db_path"]
    chart_path = str(CHART_DIR / f"{stat_id}.png")
    x_col = viz_plan["x_col"]
    y_col = viz_plan.get("y_col", "value")
    title = viz_plan.get("title", stat_id)
    limit = int(viz_plan.get("limit", 20))

    prompt = f"""Pythonコードを書いてください。SQLiteからデータを読み棒グラフを保存する。

DB: {db_path}
テーブル: {table}
X軸: {x_col}
Y軸: {y_col}（CAST REAL、NULL除外）
タイトル: {title}
上位{limit}件
保存先: {chart_path}

必須:
- import matplotlib; matplotlib.use('Agg') を最初に書く
- import japanize_matplotlib
- Path("{chart_path}").parent.mkdir(parents=True, exist_ok=True)
- plt.tight_layout(); plt.savefig(..., dpi=100, bbox_inches='tight'); plt.close()

```python
コードのみ
```"""

    raw = _call_ollama(prompt, max_tokens=1024)
    m = re.search(r"```python\n(.*?)```", raw, re.DOTALL)
    if m:
        return m.group(1)
    m = re.search(r"```\n(.*?)```", raw, re.DOTALL)
    if m:
        return m.group(1)

    # フォールバック: シンプルな棒グラフ
    return f"""import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import japanize_matplotlib
import sqlite3
from pathlib import Path

Path(r"{chart_path}").parent.mkdir(parents=True, exist_ok=True)
conn = sqlite3.connect(r"{db_path}")
rows = conn.execute(
    'SELECT "{x_col}", AVG(CAST("{y_col}" AS REAL)) FROM "{table}" '
    'WHERE "{y_col}" IS NOT NULL AND "{y_col}" != "" '
    'GROUP BY "{x_col}" ORDER BY AVG(CAST("{y_col}" AS REAL)) DESC LIMIT {limit}'
).fetchall()
conn.close()
if rows:
    labels = [str(r[0])[:15] for r in rows]
    values = [r[1] if r[1] is not None else 0 for r in rows]
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(range(len(labels)), values)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.set_title("{title}")
    plt.tight_layout()
    plt.savefig(r"{chart_path}", dpi=100, bbox_inches='tight')
    plt.close()
"""


def _execute_viz(dataset: dict[str, Any], code: str, iteration: int = 1) -> str | None:
    """可視化コードをサブプロセスで実行。成功時PNGパス、失敗時None。"""
    stat_id = dataset["stat_id"]
    chart_path = CHART_DIR / f"{stat_id}_iter{iteration}.png"
    CHART_DIR.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8") as f:
        f.write(code)
        tmp_path = f.name

    try:
        result = subprocess.run(
            [sys.executable, tmp_path],
            capture_output=True,
            text=True,
            timeout=60,
            cwd=str(Path.cwd()),
        )
        Path(tmp_path).unlink(missing_ok=True)

        if result.returncode == 0 and chart_path.exists():
            print(f"  [viz] {stat_id}: グラフ生成 → {chart_path.name}")
            return str(chart_path)
        else:
            print(f"  [viz] {stat_id}: 失敗 (rc={result.returncode})")
            if result.stderr:
                print(f"       {result.stderr[:150]}")
            return None
    except Exception as e:
        Path(tmp_path).unlink(missing_ok=True)
        print(f"  [viz] {stat_id}: 実行エラー: {e}")
        return None


def _analyze_chart(
    dataset: dict[str, Any],
    viz_plan: dict[str, Any],
    chart_path: str | None,
) -> dict[str, Any]:
    """SQL集計データをもとにOllamaが洞察を生成する。"""
    stat_id = dataset["stat_id"]
    table = dataset["table"]
    db_path = dataset["db_path"]
    x_col = viz_plan.get("x_col", "")
    y_col = viz_plan.get("y_col", "value")
    limit = int(viz_plan.get("limit", 20))

    conn = sqlite3.connect(db_path)
    try:
        data_rows = conn.execute(
            f'SELECT "{x_col}", AVG(CAST("{y_col}" AS REAL)) FROM "{table}" '
            f'WHERE "{y_col}" IS NOT NULL AND "{y_col}" != "" '
            f'GROUP BY "{x_col}" ORDER BY AVG(CAST("{y_col}" AS REAL)) DESC LIMIT {limit}'
        ).fetchall()
    except Exception:
        data_rows = []

    try:
        s = conn.execute(
            f'SELECT MIN(CAST("{y_col}" AS REAL)), MAX(CAST("{y_col}" AS REAL)), '
            f'AVG(CAST("{y_col}" AS REAL)), COUNT(*) FROM "{table}" '
            f'WHERE "{y_col}" IS NOT NULL AND "{y_col}" != ""'
        ).fetchone()
        stats_text = (
            f"最小={s[0]:.2f}, 最大={s[1]:.2f}, 平均={s[2]:.2f}, 件数={s[3]:,}"
            if s and s[0] is not None
            else ""
        )
    except Exception:
        stats_text = ""
    conn.close()

    data_text = "\n".join(f"{r[0]}: {r[1]:.2f}" for r in data_rows[:15] if r[1] is not None)

    result: dict[str, Any] = {
        "stat_id": stat_id,
        "catalog_desc": STAT_CATALOG.get(stat_id, ""),
        "chart_path": chart_path,
        "headline": "",
        "findings": [],
        "implication": "",
        "data_rows": data_rows[:10],
        "stats": stats_text,
        "x_col": x_col,
    }

    if not data_text and not stats_text:
        result["headline"] = "データ集計不可"
        return result

    prompt = f"""データジャーナリストとして、以下のデータから記事に使える洞察を挙げてください。

統計ID: {stat_id}
説明: {STAT_CATALOG.get(stat_id, "")}
グラフタイトル: {viz_plan.get("title", "")}
分析観点: {viz_plan.get("description", "")}

【集計データ（{x_col}別 上位{limit}件）】
{data_text or "（データなし）"}

【基本統計】
{stats_text or "（集計不可）"}

JSONのみで回答:
{{"headline": "最重要発見（1文、数値必須）", "findings": ["発見1（数値入り）", "発見2（数値入り）", "発見3"], "implication": "社会的含意・示唆（1文）"}}"""

    raw = _call_ollama(prompt, max_tokens=512)
    try:
        m = re.search(r"\{.*?\}", raw, re.DOTALL)
        if m:
            result.update(json.loads(m.group()))
    except Exception:
        pass

    print(f"  [analyze] {stat_id}: {result.get('headline', '')[:60]}")
    return result


def _decide_next_axis(
    dataset: dict[str, Any],
    past_findings: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, bool]:
    """
    これまでの分析結果をもとに次の分析軸を決定する。
    Returns: (next_viz_plan_or_None, completion_flag)
    completion_flag=True のとき分析終了。
    """
    cols = dataset["cols"]
    stat_id = dataset["stat_id"]
    non_value_cols = [c for c in cols if c != "value"]

    # 過去に使った x_col を収集
    used_x_cols = [f.get("x_col", "") for f in past_findings]

    # 未使用のカラムがあるか
    unused_cols = [c for c in non_value_cols if c not in used_x_cols]

    findings_summary = "\n".join(
        f"- 軸={f.get('x_col', '?')}: {f.get('headline', '')}" for f in past_findings
    )

    prompt = f"""統計データの分析を続けるか判断してください。

統計ID: {stat_id}
全カラム: {cols}
未使用カラム: {unused_cols}

【これまでの分析結果】
{findings_summary}

まだ重要な軸が残っているなら次の分析を提案し、十分なら完了としてください。
JSONのみで回答:
{{"done": true/false, "reason": "判断理由（1文）", "next_x_col": "次に分析するカラム名（doneがfalseの場合）", "next_title": "次のグラフタイトル"}}"""

    raw = _call_ollama(prompt, max_tokens=256)

    try:
        m = re.search(r"\{.*?\}", raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            done = bool(parsed.get("done", True))
            reason = parsed.get("reason", "")
            print(f"  [next_axis] done={done}  {reason[:60]}")
            if done:
                return None, True
            next_x_col = parsed.get("next_x_col", "")
            if next_x_col in cols:
                next_plan: dict[str, Any] = {
                    "chart_type": "bar",
                    "x_col": next_x_col,
                    "y_col": "value",
                    "title": parsed.get("next_title", f"{next_x_col}別分析"),
                    "limit": 20,
                    "description": reason,
                }
                return next_plan, False
    except Exception:
        pass

    # パース失敗 or 未使用カラムなし → 完了扱い
    return None, True


# ---------------------------------------------------------------------------
# パイプライン 4関数（runner.py が検証する署名を維持）
# ---------------------------------------------------------------------------


def _select_topic() -> dict[str, Any]:
    """catalog_index.db + STAT_CATALOG + lessons.md から未使用IDを選択。"""
    lessons_path = Path("docs/lessons.md")
    lessons = lessons_path.read_text(encoding="utf-8") if lessons_path.exists() else ""

    used_ids: set[str] = set()
    for line in lessons.splitlines():
        if "使用統計ID" in line:
            for sid in STAT_CATALOG:
                if sid in line:
                    used_ids.add(sid)

    available_ids = [k for k in STAT_CATALOG if k not in used_ids]
    if not available_ids:
        available_ids = list(STAT_CATALOG.keys())
    random.shuffle(available_ids)
    selected_ids = available_ids[: min(MAX_DATASETS, len(available_ids))]

    catalog_db_entries = _load_catalog_from_db()
    catalog_lines: list[str] = []
    for sid in selected_ids:
        desc = STAT_CATALOG[sid]
        db_info = catalog_db_entries.get(sid, {})
        if db_info:
            try:
                kw_list = json.loads(db_info.get("keywords", "[]"))
                kw_str = "、".join(kw_list[:5])
            except Exception:
                kw_str = ""
            catalog_lines.append(f"- {sid}: {desc}" + (f"（{kw_str}）" if kw_str else ""))
        else:
            catalog_lines.append(f"- {sid}: {desc}")

    used_text = "\n".join(f"- {sid}" for sid in used_ids) or "なし"
    prompt = f"""以下の統計を組み合わせて1本の記事テーマを作ってください。

【候補統計】
{chr(10).join(catalog_lines)}

【過去使用済み（繰り返し禁止）】
{used_text}

JSONのみで回答:
{{"theme": "記事テーマ（30字以内）", "angle": "切り口・問い（40字以内）"}}"""

    raw = _call_ollama(prompt, max_tokens=256)
    theme, angle = "複数統計の比較レポート", "地域差と時系列のパターンを探る"
    try:
        m = re.search(r"\{.*?\}", raw, re.DOTALL)
        if m:
            r = json.loads(m.group())
            theme = r.get("theme", theme)
            angle = r.get("angle", angle)
    except Exception:
        pass

    print(f"  [topic] {selected_ids}")
    print(f"          テーマ: {theme} / {angle}")
    return {"stat_ids": selected_ids, "theme": theme, "angle": angle}


def fetch_data() -> dict[str, Any]:
    """
    トピック選択 → 各データセットを1件ずつキャッシュ/取得。
    カタログ情報を付加して返す。
    """
    topic = _select_topic()
    datasets: list[dict[str, Any]] = []
    for stat_id in topic["stat_ids"]:
        info = _ensure_cached(stat_id, topic["theme"], topic["angle"])
        info["catalog_desc"] = STAT_CATALOG.get(stat_id, "")
        datasets.append(info)
    return {
        "theme": topic["theme"],
        "angle": topic["angle"],
        "datasets": datasets,
        "selected_ids": topic["stat_ids"],
    }


MAX_AXIS_ITER = 3  # 1データセットあたりの最大分析軸数


def analyze(info: dict[str, Any]) -> dict[str, Any]:
    """
    内部ループ型分析:
      データセットごとに「軸決定 → 可視化 → 分析 → 次軸判定」を繰り返す。
      Ollamaが「完了」と判断するか MAX_AXIS_ITER 回に達したら次のデータセットへ。
    """
    all_dataset_results: list[dict[str, Any]] = []

    for dataset in info["datasets"]:
        stat_id = dataset["stat_id"]
        print(f"\n  === [{stat_id}] 内部ループ開始 ===")

        if not dataset["cols"]:
            print(f"  [skip] {stat_id}: カラム情報なし")
            continue

        past_findings: list[dict[str, Any]] = []
        viz_plan: dict[str, Any] | None = None  # None = 初回（Ollamaが自由に決める）
        iteration = 0

        while iteration < MAX_AXIS_ITER:
            iteration += 1
            print(f"\n  --- {stat_id} / 軸イテレーション {iteration}/{MAX_AXIS_ITER} ---")

            # [3] 可視化方向決定（初回は自由選択、2回目以降は next_axis を使用）
            if viz_plan is None:
                viz_plan = _decide_viz_direction(dataset)
            print(f"  [viz] x={viz_plan.get('x_col')} / {viz_plan.get('title', '')[:30]}")

            # [4] 可視化コード生成
            viz_code = _generate_viz_code(dataset, viz_plan)

            # [5] コード実行 → PNG（イテレーション番号をファイル名に付与）
            chart_path = _execute_viz(dataset, viz_code, iteration)

            # [6] グラフ分析
            finding = _analyze_chart(dataset, viz_plan, chart_path)
            finding["iteration"] = iteration
            past_findings.append(finding)

            # [7] 次の分析軸を決定 → 完了判定
            next_plan, done = _decide_next_axis(dataset, past_findings)
            if done:
                print(f"  [done] {stat_id}: Ollamaが分析完了と判断")
                break
            viz_plan = next_plan

        all_dataset_results.append(
            {
                "stat_id": stat_id,
                "catalog_desc": STAT_CATALOG.get(stat_id, ""),
                "iterations": iteration,
                "findings": past_findings,
            }
        )

    return {
        "theme": info["theme"],
        "angle": info["angle"],
        "selected_ids": info["selected_ids"],
        "chart_results": all_dataset_results,
        "completion_flag": bool(all_dataset_results),
    }


def generate_story(analysis: dict[str, Any]) -> str:
    """全データセットの分析結果を統合してOllamaが記事を生成 → output/story.md。"""
    if not analysis.get("completion_flag") or not analysis.get("chart_results"):
        fallback = f"# {analysis.get('theme', '分析結果')}\n\nデータ分析が完了しませんでした。"
        Path("output/story.md").parent.mkdir(exist_ok=True)
        Path("output/story.md").write_text(fallback, encoding="utf-8")
        return fallback

    # 各データセットの分析ブロックを構築（多軸分析結果を統合）
    blocks: list[str] = []
    for ds in analysis["chart_results"]:
        stat_id = ds["stat_id"]
        desc = ds.get("catalog_desc", STAT_CATALOG.get(stat_id, ""))
        iter_blocks: list[str] = []
        for f in ds.get("findings", []):
            findings_text = "\n".join(f"  - {x}" for x in f.get("findings", []))
            chart_info = f"  グラフ: {f['chart_path']}" if f.get("chart_path") else ""
            iter_blocks.append(
                f"  [軸{f.get('iteration', '?')} x={f.get('x_col', '?')}]\n"
                f"  発見: {f.get('headline', '')}\n"
                f"{findings_text}\n"
                f"  示唆: {f.get('implication', '')}\n"
                f"  統計: {f.get('stats', '')}\n"
                f"{chart_info}"
            )
        blocks.append(
            f"【統計ID: {stat_id}】{desc}\n"
            f"分析軸数: {ds.get('iterations', 1)}\n" + "\n".join(iter_blocks)
        )

    datasets_text = "\n\n".join(blocks)
    selected_ids_str = ", ".join(analysis["selected_ids"])

    prompt = f"""あなたは日本のデータジャーナリストです。
以下の統計分析結果をもとに、一般市民が関心を持てる日本語の記事を書いてください。

【統計ID群】{selected_ids_str}（e-Stat実データ）
【テーマ】{analysis["theme"]}
【切り口】{analysis["angle"]}

【データセット別分析結果】
{datasets_text}

【記事の要件】
1. 読者を引き込む問いかけで書き始める
2. 上記の実数値を具体的に引用する（最低2つの統計IDから数値を引用）
3. 複数統計を比較して関係性・仮説を述べる
4. 数値の羅列でなくパターン・変化・異常値の洞察を書く
5. 800〜1200字程度、Markdown形式
6. 末尾に「データ出典: e-Stat 統計ID {selected_ids_str}」を明記

記事のみ出力（前置き不要）:"""

    content = _call_ollama(prompt, max_tokens=2048)

    if not content:
        content = f"# {analysis['theme']}\n\n記事生成に失敗しました。"

    story_path = Path("output/story.md")
    story_path.parent.mkdir(exist_ok=True)
    story_path.write_text(content, encoding="utf-8")
    print(f"  [story] output/story.md 保存完了（{len(content):,}文字）")

    return content


if __name__ == "__main__":
    df = fetch_data()
    analysis = analyze(df)
    generate_story(analysis)
