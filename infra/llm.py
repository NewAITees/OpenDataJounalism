"""
LLM呼び出しの抽象層。Ollamaへの呼び出しをラップする。
"""

from __future__ import annotations

import os

import requests

OLLAMA_URL: str = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "qwen3.5:9b")


def call(prompt: str, max_tokens: int = 2000, temperature: float = 0.7) -> str:
    """Ollamaにプロンプトを送り、レスポンス文字列を返す。"""
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "think": False,
                "options": {"temperature": temperature, "num_predict": max_tokens},
            },
            timeout=480,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")
    except Exception as e:
        raise RuntimeError(f"LLM呼び出し失敗: {e}") from e
