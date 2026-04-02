from __future__ import annotations

from .common import run_python_commands


def run_international_news_pipeline() -> dict:
    commands = [
        {"script": "fetch_news_rss.py", "args": ["--limit", "15", "--timeout", "30"]},
        {"script": "fetch_news_marketscreener.py", "args": ["--limit", "20", "--timeout", "30"]},
        {"script": "fetch_news_marketscreener_live.py", "args": ["--limit", "30", "--timeout", "30"]},
        {"script": "llm_score_news.py", "args": ["--limit", "20", "--retry", "1", "--sleep", "0.1"]},
        {"script": "llm_score_sentiment.py", "args": ["--target", "news", "--limit", "20", "--retry", "1", "--sleep", "0.1"]},
        {"script": "map_news_items_to_stocks.py", "args": ["--limit", "200", "--days", "7"]},
    ]
    return run_python_commands(commands)
