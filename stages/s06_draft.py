"""
ステージ6: ドラフト記事生成（2データセット対応）
入力: work/01_meta_A.json, work/01_meta_B.json
      work/04_result.json, work/05_eval.json
出力: work/06_draft.md
"""

from __future__ import annotations

import json
from pathlib import Path

from infra.llm import call
from infra.storage import read_json, write_text


def run(work_dir: Path) -> str:
    meta_a = read_json(work_dir / "01_meta_A.json")
    meta_b = read_json(work_dir / "01_meta_B.json")
    result = read_json(work_dir / "04_result.json")
    evaluation = read_json(work_dir / "05_eval.json")

    data_facts = result.get("data_facts", {})
    charts = result.get("charts", [])

    chart_note = ""
    if charts:
        chart_note = (
            "\n【グラフファイル（記事末尾に ![グラフ名]({}) 形式で埋め込むこと）】\n"
            + "\n".join(f"- {c}" for c in charts)
        )

    prompt = f"""以下の分析結果に基づいて、データジャーナリズムの記事ドラフトを書いてください。

【使用データセット】
A: {meta_a["table"]["title"]}（分野: {meta_a["table"]["field_name"]}）
B: {meta_b["table"]["title"]}（分野: {meta_b["table"]["field_name"]}）

【分析の切り口】{result.get("angle", "")}
【面白い点】{evaluation.get("what_is_interesting", "")}

【実際のデータから抽出した数値（これだけを使うこと）】
{json.dumps(data_facts, ensure_ascii=False, indent=2, default=str)[:3000]}
{chart_note}

制約:
- 上記の数値以外を記事に書いてはならない（架空の数字は絶対禁止）
- キー名（例: A_時系列_変化率_pct）は記事に書かず、値と意味だけを自然な日本語で書く
- グラフがある場合は ![グラフの説明](グラフのパス) 形式で記事内に埋め込む
- 日本語、マークダウン形式
- タイトル・導入・本論・結論の構成
- 1000字程度
- 出典として使用した2つの統計表名をe-Statと共に末尾に記載"""

    draft = call(prompt, max_tokens=2000)
    write_text(work_dir / "06_draft.md", draft)
    print(f"  ドラフト生成完了 ({len(draft)}文字)")
    return draft
