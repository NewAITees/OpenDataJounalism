"""
ステージ2: データを読む（A・B両方）+ 組み合わせ可否の判定
入力: work/01_raw_A.parquet, work/01_meta_A.json
      work/01_raw_B.parquet, work/01_meta_B.json
      work/01_common_axes.json
出力: work/02_reading.md
      work/02_verdict.json  # 組み合わせ可否の判定結果
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from infra.llm import call
from infra.storage import load_dataframe, read_json, write_json, write_text


def _describe_dataset(label: str, work_dir: Path) -> tuple[str, dict]:
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

    desc = f"""### データセット{label}: {table["title"]}
分野: {table["field_name"]} / 行数: {meta["row_count"]}
列構成:
{chr(10).join(dims)}
サンプル:
{sample}
数値分布:
{value_stats}"""

    return desc, {"title": table["title"], "field": table["field_name"], "rows": meta["row_count"]}


def run(work_dir: Path) -> str:
    desc_a, info_a = _describe_dataset("A", work_dir)
    desc_b, info_b = _describe_dataset("B", work_dir)
    common = read_json(work_dir / "01_common_axes.json")

    prompt = f"""以下の2つのe-Statデータセットについて答えてください。

{desc_a}

{desc_b}

【共通軸】{common}

## 質問1: データの内容説明
それぞれのデータは何を測定しているか（各1〜2文）

## 質問2: 組み合わせ判定
この2つのデータを組み合わせて「1本の記事」として分析することは意味があるか？

判定基準:
- 共通の時間軸・地域軸があり、かつテーマに関連性がある → 組み合わせ可
- 共通軸はなくてもテーマが補完し合う関係にある → 組み合わせ可
- 全く異なる分野・時代・対象で関連性が見出せない → 組み合わせ不可

組み合わせ不可の場合は、より行数が多い・より面白い方を「強いデータ」として選ぶこと。

マークダウンで説明した後、末尾に以下のJSONを出力してください:
```json
{{
  "combination_viable": true または false,
  "reason": "判断理由（1文）",
  "stronger_dataset": "A" または "B"（組み合わせ不可の場合のみ、より有望な方）,
  "analysis_angle": "組み合わせ可の場合の分析切り口、不可の場合は強いデータ単独の切り口（1文）"
}}
```"""

    raw = call(prompt, max_tokens=1200)

    # マークダウン部分とJSON部分を分離
    reading = raw
    verdict: dict = {
        "combination_viable": bool(common.get("has_time") or common.get("has_area")),
        "reason": "共通軸の有無に基づくデフォルト判定",
        "stronger_dataset": "A" if info_a["rows"] >= info_b["rows"] else "B",
        "analysis_angle": "",
    }
    try:
        m = re.search(r"```json\s*(\{.*?\})\s*```", raw, re.DOTALL)
        if m:
            verdict.update(json.loads(m.group(1)))
            # JSONブロックをreadingから除去
            reading = raw[: raw.rfind("```json")].strip()
    except Exception as e:
        print(f"  [WARN] 判定JSONパース失敗: {e}")

    write_text(work_dir / "02_reading.md", reading)
    write_json(work_dir / "02_verdict.json", verdict)

    viable = (
        "組み合わせ可"
        if verdict["combination_viable"]
        else f"組み合わせ不可→{verdict['stronger_dataset']}単独"
    )
    print(f"  データ理解完了 ({len(reading)}文字) / 判定: {viable}")
    return reading
