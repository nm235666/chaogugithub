from __future__ import annotations

from .common import run_python_commands


def run_cn_news_pipeline() -> dict:
    commands = [
        {"script": "fetch_cn_news_sina_7x24.py", "args": ["--limit", "60", "--timeout", "30"]},
    ]
    return run_python_commands(commands)
