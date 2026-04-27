from __future__ import annotations

from pathlib import Path

import db_compat as sqlite3

from services.data_readiness_service import run_data_readiness_agent

ROOT_DIR = Path(__file__).resolve().parent.parent


def get_data_readiness_job_target(job_key: str) -> dict:
    registry = {
        "data_readiness_daily": {
            "job_key": "data_readiness_daily",
            "category": "agent",
            "runner_type": "service",
            "target": "services.data_readiness_service.run_data_readiness_agent",
        },
    }
    if job_key not in registry:
        raise KeyError(job_key)
    return registry[job_key]


def run_data_readiness_job(
    job_key: str,
    *,
    dry_run: bool = False,
    ai_enabled: bool = True,
    path_selection_enabled: bool = True,
) -> dict:
    if job_key != "data_readiness_daily":
        raise KeyError(job_key)
    return run_data_readiness_agent(
        sqlite3_module=sqlite3,
        db_path=str(ROOT_DIR / "stock_codes.db"),
        auto_fix=not dry_run,
        dry_run=dry_run,
        ai_enabled=ai_enabled,
        path_selection_enabled=path_selection_enabled,
    )
