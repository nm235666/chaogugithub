from __future__ import annotations

from collectors.news.common import run_python_script


def run_market_expectations_refresh() -> dict:
    return run_python_script(
        "fetch_market_expectations_polymarket.py",
        timeout_s=1800,
        meta={"kind": "market_expectations_refresh"},
    )


def run_market_news_refresh() -> dict:
    return run_python_script(
        "fetch_news_marketscreener.py",
        timeout_s=1800,
        meta={"kind": "market_news_refresh"},
    )
