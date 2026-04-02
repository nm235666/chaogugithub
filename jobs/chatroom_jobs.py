from __future__ import annotations

from collectors.chatrooms import (
    run_chatroom_analysis_pipeline,
    run_chatroom_list_refresh,
    run_chatroom_sentiment_refresh,
    run_monitored_chatlog_fetch,
)


def get_chatroom_job_target(job_key: str) -> dict:
    registry = {
        "chatroom_analysis_pipeline": {
            "job_key": "chatroom_analysis_pipeline",
            "category": "chatrooms",
            "runner_type": "collector",
            "target": "collectors.chatrooms.run_chatroom_analysis_pipeline",
        },
        "chatroom_sentiment_refresh": {
            "job_key": "chatroom_sentiment_refresh",
            "category": "chatrooms",
            "runner_type": "collector",
            "target": "collectors.chatrooms.run_chatroom_sentiment_refresh",
        },
        "monitored_chatlog_fetch": {
            "job_key": "monitored_chatlog_fetch",
            "category": "chatrooms",
            "runner_type": "collector",
            "target": "collectors.chatrooms.run_monitored_chatlog_fetch",
        },
        "chatroom_list_refresh": {
            "job_key": "chatroom_list_refresh",
            "category": "chatrooms",
            "runner_type": "collector",
            "target": "collectors.chatrooms.run_chatroom_list_refresh",
        },
    }
    if job_key not in registry:
        raise KeyError(job_key)
    return registry[job_key]


def run_chatroom_job(job_key: str) -> dict:
    if job_key == "chatroom_analysis_pipeline":
        return run_chatroom_analysis_pipeline()
    if job_key == "chatroom_sentiment_refresh":
        return run_chatroom_sentiment_refresh()
    if job_key == "monitored_chatlog_fetch":
        return run_monitored_chatlog_fetch()
    if job_key == "chatroom_list_refresh":
        return run_chatroom_list_refresh()
    raise KeyError(job_key)
