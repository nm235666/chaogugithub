from __future__ import annotations

from .common import run_python_script


def run_daily_summary_refresh(
    *,
    summary_date: str,
    model: str = "auto",
    importance: str = "极高,高,中",
) -> dict:
    return run_python_script(
        "llm_summarize_daily_important_news.py",
        "--date",
        summary_date,
        "--model",
        model,
        "--importance",
        importance,
        "--max-news",
        "30",
        "--min-news",
        "8",
        "--max-prompt-chars",
        "9000",
        "--request-timeout",
        "180",
        "--max-retries",
        "2",
        "--retry-backoff",
        "2",
        timeout_s=900,
        meta={"summary_date": summary_date, "model": model, "importance": importance},
    )
