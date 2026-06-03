"""
ステージ4: 分析実行 + グラフ生成（2データセット対応）
入力: work/01_raw_A.parquet, work/01_raw_B.parquet
      work/01_meta_A.json, work/01_meta_B.json
      work/03_plan.json
出力: work/04_result.json, work/charts/*.png
"""

from __future__ import annotations

import traceback
from pathlib import Path

import matplotlib
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd

from infra.storage import load_dataframe, read_json, write_json

matplotlib.use("Agg")

# 日本語グリフを含むフォントを優先順で探す
_JP_FONT_PRIORITY = [
    "BIZ UDGothic",
    "Meiryo",
    "Yu Gothic",
    "MS Gothic",
    "Noto Sans CJK JP",
    "HGGothicM",
]
_available = {f.name for f in fm.fontManager.ttflist}
_jp_font = next((f for f in _JP_FONT_PRIORITY if f in _available), None)
if _jp_font:
    plt.rcParams["font.family"] = _jp_font
plt.rcParams["axes.unicode_minus"] = False


# ---------------------------------------------------------------------------
# LLM生成コードの実行
# ---------------------------------------------------------------------------


def _exec_analysis(code: str, df_a: pd.DataFrame, df_b: pd.DataFrame) -> dict:
    ns: dict = {}
    exec(compile(code, "<analysis>", "exec"), ns)
    if "analyze" not in ns:
        raise RuntimeError("analyze 関数が定義されていません。")
    import inspect

    sig = inspect.signature(ns["analyze"])
    if len(sig.parameters) >= 2:
        return ns["analyze"](df_a, df_b)
    return ns["analyze"](df_a)


# ---------------------------------------------------------------------------
# data_facts: DataFrameから必ず実際の数値を抽出
# ---------------------------------------------------------------------------


def _extract_data_facts(df: pd.DataFrame, class_map: dict, label: str) -> dict:
    facts: dict = {}

    if "value" not in df.columns:
        return facts

    numeric = df["value"].dropna()
    facts[f"{label}_全体_最小値"] = round(float(numeric.min()), 2)
    facts[f"{label}_全体_最大値"] = round(float(numeric.max()), 2)
    facts[f"{label}_全体_平均値"] = round(float(numeric.mean()), 2)
    facts[f"{label}_全体_中央値"] = round(float(numeric.median()), 2)
    facts[f"{label}_全体_レコード数"] = len(numeric)
    facts["注意_各数値は個別レコードの値であり合計ではない"] = True

    if "@time" in df.columns:
        time_sorted = df.sort_values("@time")
        times = sorted(df["@time"].unique())
        facts[f"{label}_時系列_期間"] = f"{times[0]} ～ {times[-1]}"
        facts[f"{label}_時系列_時点数"] = len(times)
        time_means = time_sorted.groupby("@time")["value"].mean().dropna()
        facts[f"{label}_時系列_各時点の平均値"] = {
            str(k): round(float(v), 2) for k, v in time_means.items()
        }
        if len(time_means) >= 2:
            first, last = float(time_means.iloc[0]), float(time_means.iloc[-1])
            facts[f"{label}_時系列_最初の時点の平均値"] = round(first, 2)
            facts[f"{label}_時系列_最後の時点の平均値"] = round(last, 2)
            if first != 0:
                facts[f"{label}_時系列_変化率_pct"] = round((last - first) / first * 100, 2)

    for col in ["@cat01", "@cat02", "@area"]:
        if col not in df.columns:
            continue
        dim_id = col.lstrip("@")
        dim_name = {"cat01": "カテゴリ1", "cat02": "カテゴリ2", "area": "地域"}.get(dim_id, dim_id)
        code_to_name = class_map.get(dim_id, {})
        cat_means = df.groupby(col)["value"].mean().dropna().sort_values(ascending=False)
        facts[f"{label}_{dim_name}_平均値_上位5件"] = {
            code_to_name.get(str(k), str(k)): round(float(v), 2)
            for k, v in cat_means.head(5).items()
        }
        facts[f"{label}_{dim_name}_平均値_下位5件"] = {
            code_to_name.get(str(k), str(k)): round(float(v), 2)
            for k, v in cat_means.tail(5).items()
        }

    return facts


# ---------------------------------------------------------------------------
# グラフ生成: data_facts から必ず生成する
# ---------------------------------------------------------------------------


def _make_charts(
    data_facts: dict, result: dict, work_dir: Path, title_a: str, title_b: str
) -> list[str]:
    chart_dir = work_dir / "charts"
    chart_dir.mkdir(exist_ok=True)
    charts = []

    def _save(fig: plt.Figure, name: str) -> str:
        path = chart_dir / name
        fig.savefig(path, dpi=120, bbox_inches="tight")
        plt.close(fig)
        rel = str(path.relative_to(work_dir.parent.parent))
        charts.append(rel)
        return rel

    # --- 時系列グラフ（A・B両方あれば2系列、片方のみでも出す）---
    for label, title in [("A", title_a), ("B", title_b)]:
        key = f"{label}_時系列_各時点の平均値"
        if key not in data_facts:
            continue
        ts = data_facts[key]
        if len(ts) < 2:
            continue
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(list(ts.keys()), list(ts.values()), marker="o", linewidth=1.5)
        ax.set_title(f"[{label}] {title[:40]} 時系列推移", fontsize=11)
        ax.set_xlabel("時点")
        ax.set_ylabel("平均値")
        ax.tick_params(axis="x", rotation=45)
        _save(fig, f"timeseries_{label}.png")

    # --- カテゴリ棒グラフ（上位5件）---
    for label, title in [("A", title_a), ("B", title_b)]:
        for dim in ["カテゴリ1", "カテゴリ2", "地域"]:
            key = f"{label}_{dim}_平均値_上位5件"
            if key not in data_facts:
                continue
            cat_data = data_facts[key]
            if not cat_data:
                continue
            fig, ax = plt.subplots(figsize=(9, 4))
            names = [str(k)[:20] for k in cat_data.keys()]
            values = list(cat_data.values())
            bars = ax.barh(names[::-1], values[::-1])
            ax.set_title(f"[{label}] {dim} 別平均値（上位5件）", fontsize=11)
            ax.set_xlabel("平均値")
            _save(fig, f"bar_{label}_{dim}.png")
            break  # 1データセットにつき1カテゴリ軸のみ

    # --- LLM生成コードのseries_dataがあればそれも描く ---
    series = result.get("series_data")
    if series and isinstance(series, dict):
        fig, ax = plt.subplots(figsize=(10, 5))
        for lbl, vals in series.items():
            if isinstance(vals, list):
                ax.plot(range(len(vals)), vals, marker="o", label=str(lbl))
            elif isinstance(vals, dict):
                ax.bar(list(vals.keys()), list(vals.values()), label=str(lbl), alpha=0.7)
        ax.set_title(result.get("angle", "分析結果")[:50])
        ax.legend()
        _save(fig, "series_custom.png")

    return charts


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------


def run(work_dir: Path) -> dict:
    df_a = load_dataframe(work_dir / "01_raw_A.parquet")
    df_b = load_dataframe(work_dir / "01_raw_B.parquet")
    meta_a = read_json(work_dir / "01_meta_A.json")
    meta_b = read_json(work_dir / "01_meta_B.json")
    plan = read_json(work_dir / "03_plan.json")

    # data_facts は常に両データから抽出
    data_facts: dict = {}
    data_facts.update(_extract_data_facts(df_a, meta_a.get("class_map", {}), "A"))
    data_facts.update(_extract_data_facts(df_b, meta_b.get("class_map", {}), "B"))

    # LLM生成コードを実行（失敗してもdata_factsで継続）
    llm_result: dict = {}
    try:
        llm_result = _exec_analysis(
            plan.get("analysis_code", "def analyze(df_a, df_b=None):\n    return {}"), df_a, df_b
        )
    except Exception:
        print(f"  [WARN] LLM生成コード失敗（data_factsで継続）:\n{traceback.format_exc()[:300]}")

    title_a = meta_a["table"]["title"]
    title_b = meta_b["table"]["title"]

    result = {
        "angle": plan.get("angle", ""),
        "hypothesis": plan.get("hypothesis", ""),
        "success": bool(data_facts),
        "dataset_a": title_a,
        "dataset_b": title_b,
        "data_facts": data_facts,
        **{k: v for k, v in llm_result.items() if k not in ("angle", "hypothesis")},
    }

    charts = _make_charts(data_facts, result, work_dir, title_a, title_b)
    result["charts"] = charts

    write_json(work_dir / "04_result.json", result)
    status = "OK" if result["success"] else "NG"
    print(f"  分析実行: [{status}] / グラフ: {len(charts)}枚")
    return result
