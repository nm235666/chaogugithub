from __future__ import annotations

from collectors.market import run_market_expectations_refresh, run_market_news_refresh


def get_market_job_target(job_key: str) -> dict:
    registry = {
        "market_expectations_refresh": {
            "job_key": "market_expectations_refresh",
            "category": "market",
            "runner_type": "collector",
            "target": "collectors.market.run_market_expectations_refresh",
        },
        "market_news_refresh": {
            "job_key": "market_news_refresh",
            "category": "market",
            "runner_type": "collector",
            "target": "collectors.market.run_market_news_refresh",
        },
    }
    if job_key not in registry:
        raise KeyError(job_key)
    return registry[job_key]


def run_market_job(job_key: str) -> dict:
    if job_key == "market_expectations_refresh":
        return run_market_expectations_refresh()
    if job_key == "market_news_refresh":
        return run_market_news_refresh()
    raise KeyError(job_key)
