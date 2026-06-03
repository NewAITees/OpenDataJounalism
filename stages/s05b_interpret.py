"""
ステージ5b: data_factsの数値をAIが解釈する
入力: work/04_result.json, work/01_meta_A.json, work/01_meta_B.json
出力: work/06_data_interpretation.json

各数値が「何の平均値か」「単位は何か」「合計ではないこと」を
AIが自然言語で解釈してJSONに書き出す。
s06はこれを読んで数値を正しく記事に使う。
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from infra.llm import call
from infra.storage import read_json, write_json


def run(work_dir: Path) -> dict:
    result = read_json(work_dir / "04_result.json")
    meta_a = read_json(work_dir / "01_meta_A.json")
    meta_b = read_json(work_dir / "01_meta_B.json")

    data_facts = result.get("data_facts", {})
    if not data_facts:
        write_json(work_dir / "06_data_interpretation.json", {})
        return {}

    prompt = f"""以下のdata_factsは、e-Statの統計データから自動抽出した数値です。
各キーと値について、「この数値は何を意味するか」を正確に解釈してください。

【データセットA】{meta_a["table"]["title"]}
【データセットB】{meta_b["table"]["title"]}

【data_facts（自動抽出された数値）】
{json.dumps(data_facts, ensure_ascii=False, indent=2, default=str)[:3000]}

以下のルールで解釈してください:
- "_平均値_" を含むキーは「各カテゴリ・地域・時点の平均値」であり「合計」や「総数」ではない
- "_全体_平均値" は全レコードの平均値であり全国合計ではない
- "_時系列_各時点の平均値" は各時点における全地域・カテゴリの平均値
- 単位はデータセット名から推測して補足する

回答はJSONのみ（コードブロックなし）:
{{
  "key名": "この数値の正確な意味（1〜2文）",
  ...
}}

重要なキー（_平均値_上位5件、_全体_平均値、_変化率_pct）だけ解釈すれば十分。"""

    raw = call(prompt, max_tokens=1500)

    interpretation: dict = {}
    try:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            interpretation = json.loads(m.group())
    except Exception as e:
        print(f"  [WARN] 解釈JSONパース失敗: {e}")

    write_json(work_dir / "06_data_interpretation.json", interpretation)
    print(f"  数値解釈完了 ({len(interpretation)}件)")
    return interpretation
