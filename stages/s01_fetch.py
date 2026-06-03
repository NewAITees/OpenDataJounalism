"""
ステージ1: データ取得（2データセット）
入力: なし
出力: work/01_raw_A.parquet, work/01_meta_A.json
      work/01_raw_B.parquet, work/01_meta_B.json
      work/01_common_axes.json  # 共通軸の情報
"""

from __future__ import annotations

from pathlib import Path

from infra.estat_api import fetch_values, pick_random_table
from infra.storage import save_dataframe, write_json


def _common_axes(meta_a: dict, meta_b: dict) -> dict:
    """2つのデータセットの共通軸（time/area）を検出する。"""
    cols_a = set(meta_a["columns"])
    cols_b = set(meta_b["columns"])
    common_cols = cols_a & cols_b

    axes = {
        "common_columns": sorted(common_cols),
        "has_time": "@time" in common_cols,
        "has_area": "@area" in common_cols,
    }

    # 共通の時間コードを抽出
    if axes["has_time"]:
        times_a = set(meta_a.get("time_values", []))
        times_b = set(meta_b.get("time_values", []))
        axes["common_times"] = sorted(times_a & times_b)

    return axes


def _fetch_one(label: str, work_dir: Path) -> dict:
    """1データセットを取得して保存。最大3回リトライ。"""
    for attempt in range(3):
        try:
            table = pick_random_table()
            print(f"  [{label}] 選択: [{table['field_name']}] {table['title']}")
            df, class_map = fetch_values(table["stats_data_id"])
            print(f"  [{label}] 取得: {len(df)}行 / 列: {df.columns.tolist()}")

            save_dataframe(work_dir / f"01_raw_{label}.parquet", df)

            meta = {
                "table": table,
                "class_map": class_map,
                "row_count": len(df),
                "columns": df.columns.tolist(),
                "time_values": sorted(df["@time"].astype(str).unique().tolist())
                if "@time" in df.columns
                else [],
                "area_values": sorted(df["@area"].astype(str).unique().tolist())
                if "@area" in df.columns
                else [],
            }
            write_json(work_dir / f"01_meta_{label}.json", meta)
            return meta
        except Exception as e:
            print(f"  [{label}] 試行{attempt + 1}失敗: {e}")

    raise RuntimeError(f"データセット{label}の取得に3回失敗しました。")


def run(work_dir: Path) -> dict:
    meta_a = _fetch_one("A", work_dir)
    meta_b = _fetch_one("B", work_dir)

    common = _common_axes(meta_a, meta_b)
    write_json(work_dir / "01_common_axes.json", common)
    print(f"  共通軸: {common['common_columns']}")

    return {"meta_a": meta_a, "meta_b": meta_b, "common_axes": common}
