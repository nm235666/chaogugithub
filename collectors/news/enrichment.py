from __future__ import annotations

from .common import run_python_script


def run_news_stock_map_refresh(*, limit: int = 300, days: int = 7) -> dict:
    return run_python_script(
        "map_news_items_to_stocks.py",
        "--limit",
        str(limit),
        "--days",
        str(days),
        timeout_s=900,
        meta={"limit": limit, "days": days, "kind": "stock_map_refresh"},
    )


def run_news_sentiment_refresh(
    *,
    target: str = "news",
    limit: int = 5,
    retry: int = 0,
    sleep: float = 0.05,
) -> dict:
    return run_python_script(
        "llm_score_sentiment.py",
        "--target",
        target,
        "--limit",
        str(limit),
        "--retry",
        str(retry),
        "--sleep",
        str(sleep),
        timeout_s=300,
        meta={
            "target": target,
            "limit": limit,
            "retry": retry,
            "sleep": sleep,
            "kind": "sentiment_refresh",
        },
    )
