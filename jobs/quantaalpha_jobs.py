from __future__ import annotations

from pathlib import Path

import db_compat as sqlite3

from services.quantaalpha_service import run_quantaalpha_scheduled_job

ROOT_DIR = Path(__file__).resolve().parent.parent


def get_quantaalpha_job_target(job_key: str) -> dict:
    registry = {
        "quantaalpha_health_check": {
            "job_key": "quantaalpha_health_check",
            "category": "quant",
            "runner_type": "service",
            "target": "services.quantaalpha_service.run_quantaalpha_scheduled_job",
        },
        "quantaalpha_mine_daily": {
            "job_key": "quantaalpha_mine_daily",
            "category": "quant",
            "runner_type": "service",
            "target": "services.quantaalpha_service.run_quantaalpha_scheduled_job",
        },
        "quantaalpha_backtest_daily": {
            "job_key": "quantaalpha_backtest_daily",
            "category": "quant",
            "runner_type": "service",
            "target": "services.quantaalpha_service.run_quantaalpha_scheduled_job",
        },
    }
    if job_key not in registry:
        raise KeyError(job_key)
    return registry[job_key]


def run_quantaalpha_job(job_key: str) -> dict:
    return run_quantaalpha_scheduled_job(
        sqlite3_module=sqlite3,
        db_path=str(ROOT_DIR / "stock_codes.db"),
        job_key=job_key,
    )
