from __future__ import annotations

from collectors.stock_news import (
    run_stock_news_backfill_missing,
    run_stock_news_expand_focus,
    run_stock_news_score_refresh,
)


def get_stock_news_job_target(job_key: str) -> dict:
    registry = {
        "stock_news_score_refresh": {
            "job_key": "stock_news_score_refresh",
            "category": "stock_news",
            "runner_type": "collector",
            "target": "collectors.stock_news.run_stock_news_score_refresh",
        },
        "stock_news_backfill_missing": {
            "job_key": "stock_news_backfill_missing",
            "category": "stock_news",
            "runner_type": "collector",
            "target": "collectors.stock_news.run_stock_news_backfill_missing",
        },
        "stock_news_expand_focus": {
            "job_key": "stock_news_expand_focus",
            "category": "stock_news",
            "runner_type": "collector",
            "target": "collectors.stock_news.run_stock_news_expand_focus",
        },
    }
    if job_key not in registry:
        raise KeyError(job_key)
    return registry[job_key]


def run_stock_news_job(job_key: str) -> dict:
    if job_key == "stock_news_score_refresh":
        return run_stock_news_score_refresh()
    if job_key == "stock_news_backfill_missing":
        return run_stock_news_backfill_missing()
    if job_key == "stock_news_expand_focus":
        return run_stock_news_expand_focus()
    raise KeyError(job_key)
