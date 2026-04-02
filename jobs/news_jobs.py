from __future__ import annotations

from datetime import datetime, timedelta, timezone

from collectors.news import (
    run_cn_news_pipeline,
    run_daily_summary_refresh,
    run_international_news_pipeline,
    run_news_sentiment_refresh,
    run_news_stock_map_refresh,
)


def cn_today() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y-%m-%d")


def get_news_job_target(job_key: str) -> dict:
    registry = {
        "intl_news_pipeline": {
            "job_key": "intl_news_pipeline",
            "category": "news",
            "runner_type": "collector",
            "target": "collectors.news.run_international_news_pipeline",
        },
        "cn_news_pipeline": {
            "job_key": "cn_news_pipeline",
            "category": "news",
            "runner_type": "collector",
            "target": "collectors.news.run_cn_news_pipeline",
        },
        "news_stock_map_refresh": {
            "job_key": "news_stock_map_refresh",
            "category": "news",
            "runner_type": "collector",
            "target": "collectors.news.run_news_stock_map_refresh",
        },
        "news_sentiment_refresh": {
            "job_key": "news_sentiment_refresh",
            "category": "news",
            "runner_type": "collector",
            "target": "collectors.news.run_news_sentiment_refresh",
        },
        "news_daily_summary_refresh": {
            "job_key": "news_daily_summary_refresh",
            "category": "reports",
            "runner_type": "collector",
            "target": "collectors.news.run_daily_summary_refresh",
        },
    }
    if job_key not in registry:
        raise KeyError(job_key)
    return registry[job_key]


def run_news_job(job_key: str) -> dict:
    if job_key == "intl_news_pipeline":
        return run_international_news_pipeline()
    if job_key == "cn_news_pipeline":
        return run_cn_news_pipeline()
    if job_key == "news_stock_map_refresh":
        return run_news_stock_map_refresh()
    if job_key == "news_sentiment_refresh":
        return run_news_sentiment_refresh()
    if job_key == "news_daily_summary_refresh":
        return run_daily_summary_refresh(summary_date=cn_today(), model="auto")
    raise KeyError(job_key)
