"""
自律型データジャーナリズム ループオーケストレーター
WARNING: このファイルは手動で管理します。エージェントは触れません。

使い方:
  uv run python runner.py          # 無限ループ（Ctrl+C で停止）
  uv run python runner.py --cycles 5   # 指定回数だけ実行
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

# --- 設定 ---
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "qwen3:8b"
PIPELINE_TIMEOUT = 1800  # 秒：pipeline.py 1回の最大実行時間（30分）

PROJECT = Path(__file__).parent
PIPELINE = PROJECT / "pipeline.py"
PROGRAM_MD = PROJECT / "program.md"
LESSONS_MD = PROJECT / "lessons.md"
OUTPUT_DIR = PROJECT / "output"
RUNS_DIR = PROJECT / "runs"
BEST_PIPELINE = PROJECT / "pipeline_best.py"


# ---------------------------------------------------------------------------
# Ollama ユーティリティ
# ---------------------------------------------------------------------------

def _ollama(prompt: str, max_tokens: int = 8192) -> str:
    """Ollamaに問い合わせてレスポンスを返す。"""
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False,
                "think": False,
                "options": {"num_predict": max_tokens, "num_ctx": 8192, "temperature": 0.7},
            },
            timeout=600,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")
    except Exception as e:
        print(f"  [Ollama ERROR] {e}")
        return ""


# ---------------------------------------------------------------------------
# ステップ 1: pipeline.py 実行
# ---------------------------------------------------------------------------

def run_pipeline(run_dir: Path) -> tuple[bool, str, float]:
    """
    pipeline.py をサブプロセスで実行。
    Returns: (success, story_or_error_text, elapsed_seconds)
    """
    shutil.copy(PIPELINE, run_dir / "pipeline.py")

    # 前サイクルの story.md を削除して「古い記事を成功とみなす」バグを防ぐ
    story_path = OUTPUT_DIR / "story.md"
    if story_path.exists():
        story_path.unlink()

    start = time.time()

    try:
        result = subprocess.run(
            [sys.executable, str(PIPELINE)],
            capture_output=True,
            text=True,
            timeout=PIPELINE_TIMEOUT,
            cwd=PROJECT,
        )
        elapsed = time.time() - start
        stdout = result.stdout
        stderr = result.stderr

        if story_path.exists() and story_path.stat().st_size > 0:
            story = story_path.read_text(encoding="utf-8")
            shutil.copy(story_path, run_dir / "story.md")
            success = True
        else:
            # story.md がない → 実行自体は通ったがパイプラインが不完全
            log = stdout + ("\n[STDERR]\n" + stderr if stderr else "")
            story = f"[output/story.md が生成されませんでした]\n\n{log}"
            success = False

        if result.returncode != 0:
            success = False
            story = f"[returncode={result.returncode}]\n{stdout}\n{stderr}"

    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        success = False
        story = f"[タイムアウト: {PIPELINE_TIMEOUT}秒超過]"
    except Exception as e:
        elapsed = time.time() - start
        success = False
        story = f"[実行エラー: {e}]"

    # 実行ログを保存
    (run_dir / "run_log.txt").write_text(
        f"success={success}\nelapsed={elapsed:.1f}s\n\n{story[:3000]}",
        encoding="utf-8",
    )
    return success, story, elapsed


# ---------------------------------------------------------------------------
# ステップ 2: 評価
# ---------------------------------------------------------------------------

def evaluate(story: str, run_dir: Path) -> dict[str, Any]:
    """Ollamaで記事を評価してスコアと改善案を返す。"""
    program = PROGRAM_MD.read_text(encoding="utf-8")

    prompt = f"""あなたはデータジャーナリズムの編集長です。
以下の記事を評価してください。回答はJSONのみ（他のテキスト一切不要）。

【評価基準（program.md）】
{program}

【評価対象の記事】
{story[:4000]}

以下のJSON形式で評価:
{{
  "score": 0から100の整数,
  "strengths": ["良い点（1文ずつ）"],
  "improvements": ["改善点（1文ずつ）"],
  "data_quality": "データの実在性・使い方の評価（1文）",
  "story_quality": "物語・洞察の質の評価（1文）",
  "suggestions": ["次サイクルで試すべき具体的な戦略（1文ずつ、2〜3件）"]
}}"""

    raw = _ollama(prompt, max_tokens=1024)

    evaluation: dict[str, Any] = {
        "score": 0,
        "strengths": [],
        "improvements": ["評価の取得に失敗"],
        "data_quality": "",
        "story_quality": "",
        "suggestions": [],
        "raw": raw,
    }

    try:
        # コードフェンス内のJSONを優先的に抽出、なければ裸のJSONを探す
        m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, re.DOTALL)
        if m:
            evaluation.update(json.loads(m.group(1)))
        else:
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            if m:
                evaluation.update(json.loads(m.group()))
    except Exception as e:
        evaluation["improvements"] = [f"JSON解析エラー: {e}", raw[:200]]

    with open(run_dir / "evaluation.json", "w", encoding="utf-8") as f:
        json.dump(evaluation, f, ensure_ascii=False, indent=2)

    return evaluation


# ---------------------------------------------------------------------------
# ステップ 3: lessons.md 更新
# ---------------------------------------------------------------------------

def update_lessons(cycle: int, elapsed: float, evaluation: dict[str, Any], story: str) -> None:
    """lessons.md にサイクルの知見を追記する。"""
    score = evaluation.get("score", 0)
    strengths = evaluation.get("strengths", [])
    improvements = evaluation.get("improvements", [])
    suggestions = evaluation.get("suggestions", [])

    # 記事の見出し（テーマ）を抽出
    first_line = story.strip().split("\n")[0].lstrip("#").strip() if story else "不明"

    # pipeline.py から使用した統計IDを抽出
    current = PIPELINE.read_text(encoding="utf-8")
    stat_ids = re.findall(r'statsDataId["\s:=]+["\']?(\d{10,})', current)
    stat_ids_str = ", ".join(set(stat_ids)) if stat_ids else "サンプルデータ"

    entry = (
        f"\n## サイクル {cycle}  "
        f"({datetime.now().strftime('%Y-%m-%d %H:%M')})  スコア={score}/100  "
        f"実行={elapsed:.0f}秒\n"
        f"**テーマ**: {first_line}\n"
        f"**使用統計ID**: {stat_ids_str}\n"
    )
    if strengths:
        entry += "**良かった点:**\n" + "".join(f"- {s}\n" for s in strengths)
    if improvements:
        entry += "**改善点:**\n" + "".join(f"- {s}\n" for s in improvements)
    if suggestions:
        entry += "**次サイクルの戦略:**\n" + "".join(f"- {s}\n" for s in suggestions)
    entry += "\n"

    with open(LESSONS_MD, "a", encoding="utf-8") as f:
        f.write(entry)


# ---------------------------------------------------------------------------
# ステップ 4: pipeline.py 書き換え
# ---------------------------------------------------------------------------

REQUIRED_SIGNATURES = [
    "def fetch_data(",
    "def analyze(",
    "def generate_story(",
    "output/story.md",
    "import pandas",
    '__name__ == "__main__"',
]


def validate(code: str) -> bool:
    """生成コードが最低限の制約を満たすか確認する。"""
    if not all(sig in code for sig in REQUIRED_SIGNATURES):
        return False
    # think=False が options の外（トップレベル）にあるか確認
    # options ブロック内だけにある場合は NG
    if '"think": False' in code or "'think': False" in code:
        # options の外にもあるかチェック（簡易: options行より前に出現するか）
        options_idx = code.find('"options"')
        think_idx = min(
            (code.find('"think": False') if '"think": False' in code else len(code)),
            (code.find("'think': False") if "'think': False" in code else len(code)),
        )
        if options_idx != -1 and think_idx > options_idx:
            print("  [WARN] think=False が options の中に入っています（トップレベルに移動が必要）")
            return False
    return True


def rewrite_pipeline(story: str, evaluation: dict[str, Any], cycle: int) -> str | None:
    """
    Ollamaに pipeline.py の改善版を生成させる。
    成功時は新コード文字列、失敗時は None を返す。
    """
    current = PIPELINE.read_text(encoding="utf-8")
    lessons = LESSONS_MD.read_text(encoding="utf-8") if LESSONS_MD.exists() else ""
    program = PROGRAM_MD.read_text(encoding="utf-8")
    score = evaluation.get("score", 0)

    # 過去に試したテーマ・統計IDをlessonsから抽出
    used_themes = re.findall(r"\*\*テーマ\*\*: (.+)", lessons)
    used_stat_ids = re.findall(r"\*\*使用統計ID\*\*: (.+)", lessons)
    diversity_note = ""
    if used_themes:
        diversity_note = f"""
【過去に試したテーマ（繰り返し禁止）】
{chr(10).join(f'- {t}' for t in used_themes[-5:])}

【過去に使った統計ID（できるだけ異なるIDを使うこと）】
{chr(10).join(f'- {s}' for s in used_stat_ids[-5:])}

→ 上記と異なるテーマ・データソース・分析手法・記事の切り口を選ぶこと。
"""

    prompt = f"""あなたはデータジャーナリズムシステムのコードエージェントです。
pipeline.py を書き直して、前回と異なる観点の記事を生成してください。

【研究目標・評価基準】
{program}

【過去の学習ログ】
{lessons[-2000:]}
{diversity_note}
【評価フィードバック（スコア {score}/100）】
- 良い点: {evaluation.get('strengths', [])}
- 改善点: {evaluation.get('improvements', [])}
- 推奨する次の戦略: {evaluation.get('suggestions', [])}

【前回の記事（参考）】
{story[:800]}

【現在の pipeline.py（参考）】
```python
{current[:2000]}
```

【絶対に守る制約】
0. ファイル先頭のdocstringや変数に具体的なテーマ・統計IDを書かないこと
   （テーマはselect_topic()がOllamaに動的に選ばせる）
1. 以下3関数を必ず含めること（引数名は自由）:
   def fetch_data(  ← これで始まる関数
   def analyze(     ← これで始まる関数
   def generate_story(  ← これで始まる関数
2. generate_story() は必ず output/story.md に書き込むこと
3. Ollama呼び出し（この形式を一字一句コピーすること）:
   response = requests.post(
       "http://localhost:11434/api/generate",
       json={{
           "model": "qwen3:8b",
           "prompt": your_prompt_here,
           "stream": False,
           "think": False,
           "options": {{"temperature": 0.7}},
       }},
       timeout=480,
   )
   content = response.json().get("response", "")
   ※ "think": False は必ず json の第一階層（optionsの外）に置くこと。optionsの中に入れると応答が空になる
4. e-Stat API: https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData

改善した pipeline.py を ```python\n...\n``` で出力（コードのみ、説明不要）。
末尾に必ず以下を含めること:
```
if __name__ == "__main__":
    df = fetch_data()
    analysis = analyze(df)
    generate_story(analysis)
```"""

    raw = _ollama(prompt, max_tokens=6000)

    # 1. ```python ... ``` 形式
    m = re.search(r"```python\n(.*?)```", raw, re.DOTALL)
    # 2. ``` ... ``` 形式（言語指定なし）
    if not m:
        m = re.search(r"```\n(.*?)```", raw, re.DOTALL)

    if m:
        code = m.group(1)
        if validate(code):
            return code
        missing = [s for s in REQUIRED_SIGNATURES if s not in code]
        print(f"  [WARN] 生成コードが制約を満たしません。不足: {missing}")
        return None

    print(f"  [WARN] コードブロックが見つかりません（生成文字数: {len(raw)}）")
    return None


# ---------------------------------------------------------------------------
# メインループ
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="自律型データジャーナリズムループ")
    parser.add_argument("--cycles", type=int, default=0, help="実行サイクル数（0=無限）")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True)
    RUNS_DIR.mkdir(exist_ok=True)
    if not LESSONS_MD.exists():
        LESSONS_MD.write_text("# Lessons Log\n\n---\n", encoding="utf-8")

    best_score = 0
    best_pipeline_code = PIPELINE.read_text(encoding="utf-8")
    cycle_times: list[float] = []
    cycle = 0

    print("=" * 60)
    print("  自律型データジャーナリズムシステム")
    print(f"  モデル: {MODEL}")
    print(f"  サイクル上限: {'無限' if args.cycles == 0 else args.cycles}")
    print("  Ctrl+C で停止")
    print("=" * 60)

    try:
        while args.cycles == 0 or cycle < args.cycles:
            cycle += 1
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            run_dir = RUNS_DIR / f"{ts}_cycle{cycle:03d}"
            run_dir.mkdir(exist_ok=True)

            print(f"\n{'─'*60}")
            print(f"  サイクル {cycle}  [{datetime.now().strftime('%H:%M:%S')}]")

            # --- 1. 実行 ---
            print("  [1/4] pipeline.py 実行中...")
            success, story, elapsed = run_pipeline(run_dir)
            cycle_times.append(elapsed)
            status = "✓" if success else "✗"
            print(f"  {status} 完了 ({elapsed:.1f}秒)")

            if not success:
                print(f"       エラー概要: {story[:120]}")
                # 前バージョンに戻す
                PIPELINE.write_text(best_pipeline_code, encoding="utf-8")
                print("  → ベストバージョンに復元しました")
                _save_meta(run_dir, cycle, elapsed, 0, False)
                continue

            # --- 2. 評価 ---
            print("  [2/4] 記事を評価中...")
            evaluation = evaluate(story, run_dir)
            score = evaluation.get("score", 0)
            print(f"  スコア: {score}/100  |  {evaluation.get('story_quality', '')[:60]}")

            if score > best_score:
                best_score = score
                best_pipeline_code = PIPELINE.read_text(encoding="utf-8")
                shutil.copy(PIPELINE, BEST_PIPELINE)
                shutil.copy(run_dir / "story.md", OUTPUT_DIR / "story_best.md")
                print(f"  ★ ベスト更新! {best_score}/100 → pipeline_best.py に保存")

            # --- 3. lessons.md 更新 ---
            update_lessons(cycle, elapsed, evaluation, story)

            # --- 4. pipeline.py 書き換え ---
            print("  [3/4] pipeline.py を改善中...")
            new_code = rewrite_pipeline(story, evaluation, cycle)

            if new_code:
                PIPELINE.write_text(new_code, encoding="utf-8")
                shutil.copy(PIPELINE, run_dir / "pipeline_next.py")
                print("  ✓ pipeline.py 更新完了")
            else:
                print("  → 書き換え失敗。現バージョンを維持します")

            _save_meta(run_dir, cycle, elapsed, score, True)

            avg_time = sum(cycle_times) / len(cycle_times)
            print(f"  [4/4] サイクル完了  平均実行時間: {avg_time:.0f}秒/サイクル")

    except KeyboardInterrupt:
        pass

    # --- 終了サマリー ---
    print(f"\n{'='*60}")
    print(f"  停止しました")
    print(f"  総サイクル数 : {cycle}")
    print(f"  ベストスコア : {best_score}/100")
    if cycle_times:
        print(f"  平均サイクル時間: {sum(cycle_times)/len(cycle_times):.0f}秒")
        print(f"  最短: {min(cycle_times):.0f}秒  最長: {max(cycle_times):.0f}秒")
    print(f"  ベストpipeline: pipeline_best.py")
    print(f"  ベスト記事    : output/story_best.md")
    print("=" * 60)


def _save_meta(
    run_dir: Path, cycle: int, elapsed: float, score: int, success: bool
) -> None:
    meta = {
        "cycle": cycle,
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "score": score,
        "success": success,
    }
    with open(run_dir / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
