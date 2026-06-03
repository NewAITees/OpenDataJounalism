"""
作業ディレクトリとファイルI/O。変更しない安定インフラ。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT = Path(__file__).parent.parent
WORK_BASE = PROJECT / "work"
OUTPUT_DIR = PROJECT / "output"


def new_work_dir() -> Path:
    """タイムスタンプ付きの作業ディレクトリを作成して返す。"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    d = WORK_BASE / ts
    d.mkdir(parents=True, exist_ok=True)
    (d / "charts").mkdir(exist_ok=True)
    return d


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def save_dataframe(path: Path, df: pd.DataFrame) -> None:
    df.to_parquet(path, index=False)


def load_dataframe(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)
