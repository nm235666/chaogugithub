from __future__ import annotations

from pathlib import Path

import db_compat as sqlite3

from services.decision_service import refresh_trade_advisor_daily, run_decision_scheduled_job, run_strategy_selection_agent

ROOT_DIR = Path(__file__).resolve().parent.parent


def get_decision_job_target(job_key: str) -> dict:
    registry = {
        "decision_daily_snapshot": {
            "job_key": "decision_daily_snapshot",
            "category": "research",
            "runner_type": "service",
            "target": "services.decision_service.run_decision_scheduled_job",
        },
        "decision_trade_advisor_daily": {
            "job_key": "decision_trade_advisor_daily",
            "category": "agent",
            "runner_type": "service",
            "target": "services.decision_service.refresh_trade_advisor_daily",
        },
        "strategy_selection_daily": {
            "job_key": "strategy_selection_daily",
            "category": "agent",
            "runner_type": "service",
            "target": "services.decision_service.run_strategy_selection_agent",
        },
    }
    if job_key not in registry:
        raise KeyError(job_key)
    return registry[job_key]


def run_decision_job(job_key: str) -> dict:
    if job_key == "strategy_selection_daily":
        return run_strategy_selection_agent(
            sqlite3_module=sqlite3,
            db_path=str(ROOT_DIR / "stock_codes.db"),
        )
    if job_key == "decision_trade_advisor_daily":
        return refresh_trade_advisor_daily(
            sqlite3_module=sqlite3,
            db_path=str(ROOT_DIR / "stock_codes.db"),
        )
    return run_decision_scheduled_job(
        sqlite3_module=sqlite3,
        db_path=str(ROOT_DIR / "stock_codes.db"),
        job_key=job_key,
    )
