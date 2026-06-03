"""
ステージ5: 結果評価（go / no-go）
入力: work/02_reading.md, work/03_plan.json, work/04_result.json
出力: work/05_eval.json
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from infra.llm import call
from infra.storage import read_json, read_text, write_json

MAX_RETRIES = 3


def run(work_dir: Path, attempt: int = 1) -> dict:
    reading = read_text(work_dir / "02_reading.md")
    plan = read_json(work_dir / "03_plan.json")
    result = read_json(work_dir / "04_result.json")

    data_facts = result.get("data_facts", {})

    # data_facts に実際の数値があればコードレベルでGO判定できる
    if not data_facts:
        eval_result = {
            "go": False,
            "reason": "data_facts が空です。データ取得またはs04に問題があります。",
            "attempt": attempt,
        }
        write_json(work_dir / "05_eval.json", eval_result)
        return eval_result

    prompt = f"""データ分析の結果を評価してください。回答はJSONのみ。

【分析の切り口】{plan["angle"]}
【検証仮説】{plan["hypothesis"]}

【実際に取得できた数値（data_facts）】
{json.dumps(data_facts, ensure_ascii=False, indent=2, default=str)[:2000]}

評価基準:
- data_facts の数値で記事として書ける内容があるか（仮説の完全な検証は不要）
- 最低限「何かの差・傾向・変化」が数値として存在するか
- data_facts に数値があれば基本的に go: true でよい

以下のJSONで回答:
{{
  "go": true または false,
  "reason": "判断理由（1〜2文）",
  "what_is_interesting": "data_factsの中で最も面白い点（goの場合のみ）"
}}"""

    raw = call(prompt, max_tokens=500)

    eval_result: dict = {"go": False, "reason": "評価取得失敗", "attempt": attempt}
    try:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            eval_result.update(json.loads(m.group()))
    except Exception as e:
        eval_result["reason"] = f"JSONパース失敗: {e}"

    eval_result["attempt"] = attempt
    write_json(work_dir / "05_eval.json", eval_result)
    verdict = "GO" if eval_result.get("go") else "NO-GO"
    print(f"  評価: [{verdict}] {eval_result.get('reason', '')[:80]}")
    return eval_result
