"""
ステージ2: データを読む（A・B両方）
入力: work/01_raw_A.parquet, work/01_meta_A.json
      work/01_raw_B.parquet, work/01_meta_B.json
      work/01_common_axes.json
出力: work/02_reading.md
"""

from __future__ import annotations

from pathlib import Path

from infra.llm import call
from infra.storage import load_dataframe, read_json, write_text


def _describe_dataset(label: str, work_dir: Path) -> str:
    meta = read_json(work_dir / f"01_meta_{label}.json")
    df = load_dataframe(work_dir / f"01_raw_{label}.parquet")
    class_map = meta["class_map"]
    table = meta["table"]

    dims = []
    for col in df.columns:
        dim_id = col.lstrip("@")
        if dim_id in class_map:
            names = list(class_map[dim_id].values())[:5]
            dims.append(f"  - {col}: {names}")

    sample = df.head(5).to_string()
    value_stats = df["value"].describe().to_string() if "value" in df.columns else "数値列なし"

    return f"""### データセット{label}: {table["title"]}
分野: {table["field_name"]} / 行数: {meta["row_count"]}
列構成:
{chr(10).join(dims)}
サンプル:
{sample}
数値分布:
{value_stats}"""


def run(work_dir: Path) -> str:
    desc_a = _describe_dataset("A", work_dir)
    desc_b = _describe_dataset("B", work_dir)
    common = read_json(work_dir / "01_common_axes.json")

    prompt = f"""以下の2つのe-Statデータセットを読んで、日本語で説明してください。

{desc_a}

{desc_b}

【共通軸】
{common}

以下を答えてください:
1. それぞれのデータは何を測定しているか（各1〜2文）
2. 2つを組み合わせると何が分析できるか（共通軸がある場合）
3. 共通軸がない場合は、それぞれ単独で面白い観点を1つずつ
4. 各データの数値のスケール感

マークダウン形式で出力してください。"""

    reading = call(prompt, max_tokens=1200)
    write_text(work_dir / "02_reading.md", reading)
    print(f"  データ理解完了 ({len(reading)}文字)")
    return reading
