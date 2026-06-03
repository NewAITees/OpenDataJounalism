"""
オーケストレーター: ステージ間の制御フロー。

使い方:
  uv run python orchestrator.py           # 1サイクル
  uv run python orchestrator.py --cycles 5
"""

from __future__ import annotations

import argparse
import sys
import traceback
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).parent
sys.path.insert(0, str(PROJECT))

import stages.s01_fetch as s01  # noqa: E402
import stages.s02_read as s02  # noqa: E402
import stages.s03_plan as s03  # noqa: E402
import stages.s04_analyze as s04  # noqa: E402
import stages.s05_evaluate as s05  # noqa: E402
import stages.s06_draft as s06  # noqa: E402
import stages.s07_verify as s07  # noqa: E402
import stages.s08_report as s08  # noqa: E402
from infra.storage import new_work_dir  # noqa: E402

MAX_PLAN_RETRIES = 3  # 05がNO-GOのとき03に戻る最大回数
MAX_DRAFT_RETRIES = 2  # 07がNGのとき06に戻る最大回数


def run_cycle() -> bool:
    work_dir = new_work_dir()
    print(f"\n作業ディレクトリ: {work_dir.name}")

    # --- 01: データ取得 ---
    print("[01] データ取得")
    try:
        s01.run(work_dir)
    except Exception as e:
        print(f"  [FATAL] {e}")
        return False

    # --- 02: データを読む ---
    print("[02] データを読む")
    try:
        s02.run(work_dir)
    except Exception as e:
        print(f"  [FATAL] {e}")
        return False

    # --- 03→04→05 ループ（分析方針→実行→評価）---
    go = False
    for plan_attempt in range(1, MAX_PLAN_RETRIES + 1):
        print(f"[03] 分析方針を決める (試行 {plan_attempt}/{MAX_PLAN_RETRIES})")
        try:
            s03.run(work_dir)
        except Exception as e:
            print(f"  [ERROR] {e}")
            continue

        print("[04] 分析実行")
        try:
            s04.run(work_dir)
        except Exception as e:
            print(f"  [ERROR] {e}")
            continue

        print("[05] 結果評価")
        try:
            eval_result = s05.run(work_dir, attempt=plan_attempt)
        except Exception as e:
            print(f"  [ERROR] {e}")
            continue

        if eval_result.get("go"):
            go = True
            break
        print(f"  → NO-GO: {eval_result.get('reason', '')} / 再計画します")

    if not go:
        print("  [ABORT] 分析方針が確定しませんでした。このサイクルをスキップします。")
        return False

    # --- 06→07 ループ（ドラフト→検証）---
    verified = False
    for draft_attempt in range(1, MAX_DRAFT_RETRIES + 1):
        print(f"[06] ドラフト記事生成 (試行 {draft_attempt}/{MAX_DRAFT_RETRIES})")
        try:
            s06.run(work_dir)
        except Exception as e:
            print(f"  [ERROR] {e}")
            continue

        print("[07] 整合性確認")
        try:
            verify = s07.run(work_dir)
        except Exception as e:
            print(f"  [ERROR] {e}")
            continue

        if verify.get("ok"):
            verified = True
            break
        print("  → NG: 書き直します")

    # 検証NGでも08には進む（fix_instructionsで修正する）
    print("[08] 最終レポート出力")
    try:
        s08.run(work_dir)
    except Exception as e:
        print(f"  [ERROR] {e}")
        return False

    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycles", type=int, default=1)
    args = parser.parse_args()

    print("=" * 50)
    print("  自律型データジャーナリズムシステム")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    success = 0
    for i in range(1, args.cycles + 1):
        print(f"\n{'─' * 50}")
        print(f"  サイクル {i}/{args.cycles}")
        print(f"{'─' * 50}")
        ok = run_cycle()
        if ok:
            success += 1

    print(f"\n{'=' * 50}")
    print(f"  完了: {success}/{args.cycles} サイクル成功")
    print("=" * 50)


if __name__ == "__main__":
    main()
