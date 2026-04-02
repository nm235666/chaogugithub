from __future__ import annotations

from collectors.news.common import run_python_commands, run_python_script


def run_chatroom_analysis_pipeline() -> dict:
    commands = [
        {
            "script": "llm_analyze_chatroom_investment_bias.py",
            "args": ["--days", "7", "--limit", "20", "--primary-category", "投资交易", "--retry", "2", "--sleep", "0.5"],
            "timeout_s": 1800,
            "meta": {"kind": "chatroom_analysis"},
        },
        {
            "script": "llm_score_chatroom_sentiment.py",
            "args": ["--limit", "20", "--retry", "1", "--sleep", "0.1"],
            "timeout_s": 1800,
            "meta": {"kind": "chatroom_sentiment"},
        },
        {
            "script": "build_chatroom_candidate_pool.py",
            "args": ["--min-room-count", "1"],
            "timeout_s": 1800,
            "meta": {"kind": "candidate_pool_build_1"},
        },
        {
            "script": "llm_resolve_stock_aliases.py",
            "args": ["--limit", "20", "--min-confidence", "0.88", "--retry", "2", "--sleep", "0.3"],
            "timeout_s": 1800,
            "meta": {"kind": "alias_resolve"},
        },
        {
            "script": "build_chatroom_candidate_pool.py",
            "args": ["--min-room-count", "1"],
            "timeout_s": 1800,
            "meta": {"kind": "candidate_pool_build_2"},
        },
    ]
    return run_python_commands(commands)


def run_chatroom_sentiment_refresh() -> dict:
    return run_python_script(
        "llm_score_chatroom_sentiment.py",
        "--limit",
        "30",
        "--retry",
        "1",
        "--sleep",
        "0.1",
        timeout_s=1800,
        meta={"kind": "chatroom_sentiment_refresh"},
    )


def run_monitored_chatlog_fetch() -> dict:
    return run_python_script(
        "fetch_monitored_chatlogs_once.py",
        timeout_s=1800,
        meta={"kind": "monitored_chatlog_fetch"},
    )


def run_chatroom_list_refresh() -> dict:
    return run_python_script(
        "fetch_chatroom_list_to_db.py",
        "--once",
        timeout_s=1800,
        meta={"kind": "chatroom_list_refresh"},
    )
