"""
ステージ3: 分析方針を決める（2データセット対応）
入力: work/01_meta_A.json, work/01_meta_B.json, work/01_common_axes.json, work/02_reading.md
出力: work/03_plan.json
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from infra.llm import call
from infra.storage import read_json, read_text, write_json


def _col_examples(meta: dict) -> str:
    return "\n".join(
        f"  df['{c}']  # 値例: {list(meta['class_map'].get(c.lstrip('@'), {}).values())[:3]}"
        for c in meta["columns"]
    )


def run(work_dir: Path) -> dict:
    meta_a = read_json(work_dir / "01_meta_A.json")
    meta_b = read_json(work_dir / "01_meta_B.json")
    common = read_json(work_dir / "01_common_axes.json")
    reading = read_text(work_dir / "02_reading.md")

    use_both = bool(common.get("has_time") or common.get("has_area"))

    if use_both:
        analysis_target = """2つのデータセットを組み合わせた分析を行う。
df_a と df_b の両方が引数として渡される。
共通軸（@time や @area）でマージまたは並列比較して分析すること。"""
        func_sig = "def analyze(df_a, df_b):"
        df_desc = f"""データセットA列: {meta_a["columns"]}
データセットB列: {meta_b["columns"]}
共通軸: {common["common_columns"]}"""
    else:
        analysis_target = """共通軸がないため、より行数が多いデータセットを単独で分析する。
df_a のみが引数として渡される。"""
        func_sig = "def analyze(df_a, df_b=None):"
        df_desc = f"""データセットA列: {meta_a["columns"]}
データセットB列: {meta_b["columns"]}（参考のみ）"""

    prompt = f"""あなたはデータ分析エンジニアです。

【データ理解】
{reading}

【{analysis_target}】

【列名（@プレフィックスに注意）】
データセットA:
{_col_examples(meta_a)}

データセットB:
{_col_examples(meta_b)}

{df_desc}

以下の2ブロックを順番に出力してください:

```json
{{"angle": "分析の切り口（1文）", "hypothesis": "検証する仮説（1文）"}}
```

```python
{func_sig}
    # pandasコード
    return {{}}
```

制約:
- 列名は必ず @付きをそのまま使う（'cat01' ではなく '@cat01'）
- 数値列は 'value' を使う（'$' ではない）
- フィルタはコード値（数字文字列）で行う。日本語カテゴリ名でフィルタしない
- 戻り値は JSON化できる dict のみ（DataFrame禁止、to_dict()等で変換）
- matplotlib は使わない
- import は関数内に書く"""

    raw = call(prompt, max_tokens=2000)

    plan: dict = {
        "angle": "",
        "hypothesis": "",
        "analysis_code": f"{func_sig}\n    return {{}}",
        "use_both_datasets": use_both,
    }
    try:
        m_json = re.search(r"```json\s*(\{.*?\})\s*```", raw, re.DOTALL)
        if m_json:
            plan.update(json.loads(m_json.group(1)))
        m_code = re.search(r"```python\s*(def analyze.*?)```", raw, re.DOTALL)
        if m_code:
            plan["analysis_code"] = m_code.group(1).strip()
    except Exception as e:
        print(f"  [WARN] パース失敗: {e}")

    write_json(work_dir / "03_plan.json", plan)
    print(f"  分析方針: {plan['angle']}")
    return plan
