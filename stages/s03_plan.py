"""
ステージ3: 分析方針を決める
入力: work/01_meta_A.json, work/01_meta_B.json
      work/01_common_axes.json, work/02_reading.md, work/02_verdict.json
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

    # s02のAI判定を優先、なければ共通軸の有無で判断
    verdict_path = work_dir / "02_verdict.json"
    verdict = read_json(verdict_path) if verdict_path.exists() else {}
    use_both = verdict.get(
        "combination_viable", bool(common.get("has_time") or common.get("has_area"))
    )
    stronger = verdict.get("stronger_dataset", "A")
    ai_angle = verdict.get("analysis_angle", "")

    if use_both:
        analysis_target = (
            "2つのデータセットを組み合わせて分析する。df_a と df_b の両方が引数として渡される。"
        )
        func_sig = "def analyze(df_a, df_b):"
        primary_meta = meta_a
        secondary_note = (
            f"\nデータセットB列: {meta_b['columns']}\n共通軸: {common.get('common_columns', [])}"
        )
    else:
        # 強いデータ1本に絞る
        primary_meta = meta_a if stronger == "A" else meta_b
        secondary_meta = meta_b if stronger == "A" else meta_a
        analysis_target = (
            f"データセット{stronger}（{primary_meta['table']['title']}）を単独で分析する。"
            f"\nデータセット{'B' if stronger == 'A' else 'A'}（{secondary_meta['table']['title']}）は"
            f"組み合わせ不可と判定されたため使用しない。"
            f"\ndf_a には{stronger}のデータが入っている。df_b は無視してよい。"
        )
        func_sig = "def analyze(df_a, df_b=None):"
        secondary_note = ""

    angle_hint = f"\n【推奨する分析の切り口】{ai_angle}" if ai_angle else ""

    prompt = f"""あなたはデータ分析エンジニアです。
{angle_hint}
【データ理解】
{reading[:800]}

【分析対象】
{analysis_target}

【列名（@プレフィックスに注意・必ずこの名前を使うこと）】
データセット{"A または B" if use_both else stronger}:
{_col_examples(primary_meta)}
{secondary_note}

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
        "primary_dataset": "AB" if use_both else stronger,
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
    mode = "2DS組み合わせ" if use_both else f"{stronger}単独"
    print(f"  分析方針: [{mode}] {plan['angle']}")
    return plan
