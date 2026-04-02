from __future__ import annotations

from services.reporting.daily_summaries import get_daily_summary_task, query_daily_summaries, start_daily_summary_generation


def build_reporting_runtime_deps(
    *,
    query_news_daily_summaries,
    start_async_daily_summary_job,
    get_async_daily_summary_job,
) -> dict:
    return {
        "query_news_daily_summaries": query_news_daily_summaries,
        "start_async_daily_summary_job": start_async_daily_summary_job,
        "get_async_daily_summary_job": get_async_daily_summary_job,
        "query_daily_summaries": query_daily_summaries,
        "start_daily_summary_generation": start_daily_summary_generation,
        "get_daily_summary_task": get_daily_summary_task,
    }
