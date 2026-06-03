"""
ステージ8: 最終レポート出力（2データセット対応・画像埋め込み）
入力: work/06_draft.md, work/07_verify.json, work/04_result.json
      work/01_meta_A.json, work/01_meta_B.json
出力: work/08_report.md, output/report_YYYYMMDD_HHMMSS.md
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from infra.llm import call
from infra.storage import OUTPUT_DIR, read_json, read_text, write_text


def run(work_dir: Path) -> str:
    draft = read_text(work_dir / "06_draft.md")
    verify = read_json(work_dir / "07_verify.json")
    result = read_json(work_dir / "04_result.json")
    meta_a = read_json(work_dir / "01_meta_A.json")
    meta_b = read_json(work_dir / "01_meta_B.json")

    fix_instructions = verify.get("fix_instructions", "")

    if fix_instructions:
        prompt = f"""以下のドラフト記事を修正指示に従って書き直してください。

【修正指示】
{fix_instructions}

【ドラフト記事】
{draft}

【分析結果（参照用数値のみ）】
{json.dumps(result.get("data_facts", {}), ensure_ascii=False, indent=2, default=str)[:1500]}

修正後の記事をそのまま出力してください（説明不要）。"""
        final = call(prompt, max_tokens=2000)
    else:
        final = draft

    # グラフ画像の埋め込み（draft内に ![](path) がなければ末尾に追加）
    charts = result.get("charts", [])
    for chart_path in charts:
        chart_name = Path(chart_path).stem
        img_tag = f"![{chart_name}]({chart_path})"
        if img_tag not in final and chart_path not in final:
            if "## グラフ" not in final:
                final += "\n\n## グラフ\n"
            final += f"\n{img_tag}\n"

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    footer = (
        f"\n\n---\n"
        f"*出典: e-Stat*\n"
        f"*A: {meta_a['table']['title']}*\n"
        f"*B: {meta_b['table']['title']}*\n"
        f"*生成: {ts}*"
    )
    final += footer

    write_text(work_dir / "08_report.md", final)

    OUTPUT_DIR.mkdir(exist_ok=True)
    out_path = OUTPUT_DIR / f"report_{ts}.md"
    write_text(out_path, final)

    print(f"  レポート出力: {out_path.relative_to(OUTPUT_DIR.parent)}")
    return final
