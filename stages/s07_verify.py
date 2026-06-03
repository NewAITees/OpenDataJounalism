"""
ステージ7: ドラフトとデータの整合性確認
入力: work/04_result.json, work/06_draft.md
出力: work/07_verify.json
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from infra.llm import call
from infra.storage import read_json, read_text, write_json


def run(work_dir: Path) -> dict:
    result = read_json(work_dir / "04_result.json")
    draft = read_text(work_dir / "06_draft.md")

    prompt = f"""ドラフト記事が分析結果の数値と整合しているか確認してください。回答はJSONのみ。

【分析結果（正しい数値）】
{json.dumps(result, ensure_ascii=False, indent=2, default=str)[:1500]}

【ドラフト記事】
{draft[:2000]}

確認事項:
1. 記事に登場する数値は分析結果に存在するか
2. 記事の主張は数値で裏付けられているか
3. 誤解を招く表現や誇張はないか

以下のJSONで回答:
{{
  "ok": true または false,
  "issues": ["問題点（なければ空リスト）"],
  "fix_instructions": "修正指示（okの場合は空文字）"
}}"""

    raw = call(prompt, max_tokens=600)

    verify: dict = {"ok": False, "issues": ["検証取得失敗"], "fix_instructions": ""}
    try:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            verify.update(json.loads(m.group()))
    except Exception as e:
        verify["issues"] = [f"JSONパース失敗: {e}"]

    write_json(work_dir / "07_verify.json", verify)
    verdict = "OK" if verify.get("ok") else "NG"
    issues = verify.get("issues", [])
    print(f"  検証: [{verdict}] {issues[0] if issues else '問題なし'}")
    return verify
