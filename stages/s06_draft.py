"""
ステージ6: ドラフト記事生成（2データセット対応）
入力: work/01_meta_A.json, work/01_meta_B.json
      work/04_result.json, work/05_eval.json
      work/06_data_interpretation.json  # s05後に生成される数値解釈
出力: work/06_draft.md
"""

from __future__ import annotations

import json
from pathlib import Path

from infra.llm import call
from infra.storage import read_json, write_text


def run(work_dir: Path, ng_reason: str = "") -> str:
    meta_a = read_json(work_dir / "01_meta_A.json")
    meta_b = read_json(work_dir / "01_meta_B.json")
    result = read_json(work_dir / "04_result.json")
    evaluation = read_json(work_dir / "05_eval.json")

    data_facts = result.get("data_facts", {})
    charts = result.get("charts", [])

    # 数値の意味解釈（s05後に生成済みのはず）
    interp_path = work_dir / "06_data_interpretation.json"
    interpretation = read_json(interp_path) if interp_path.exists() else {}

    chart_note = ""
    if charts:
        chart_note = (
            "\n【グラフファイル（記事内に ![グラフの説明](パス) 形式で埋め込むこと）】\n"
            + "\n".join(f"- {c}" for c in charts)
        )

    ng_note = ""
    if ng_reason:
        ng_note = f"""
【前回の検証でNGになった理由（この問題を必ず修正すること）】
{ng_reason}
"""

    interp_note = ""
    if interpretation:
        interp_note = f"""
【各数値の正確な意味（必ずこの解釈に従って記事を書くこと）】
{json.dumps(interpretation, ensure_ascii=False, indent=2)}
"""

    prompt = f"""以下の分析結果に基づいて、データジャーナリズムの記事ドラフトを書いてください。

【使用データセット】
A: {meta_a["table"]["title"]}（分野: {meta_a["table"]["field_name"]}）
B: {meta_b["table"]["title"]}（分野: {meta_b["table"]["field_name"]}）

【分析の切り口】{result.get("angle", "")}
【面白い点】{evaluation.get("what_is_interesting", "")}
{ng_note}{interp_note}
【実際のデータから抽出した数値（これだけを使うこと）】
{json.dumps(data_facts, ensure_ascii=False, indent=2, default=str)[:2500]}
{chart_note}

制約:
- 上記の数値以外を記事に書いてはならない（架空の数字は絶対禁止）
- 各数値の意味は【各数値の正確な意味】に従うこと
- 「平均値」を「合計」や「総数」と解釈してはならない
- キー名は記事に書かず、値と意味だけを自然な日本語で書く
- グラフがある場合は記事内に埋め込む
- 日本語、マークダウン形式
- タイトル・導入・本論・結論の構成
- 1000字程度
- 出典として使用した2つの統計表名をe-Statと共に末尾に記載"""

    draft = call(prompt, max_tokens=2000)
    write_text(work_dir / "06_draft.md", draft)
    print(f"  ドラフト生成完了 ({len(draft)}文字)")
    return draft
