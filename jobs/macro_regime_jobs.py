from __future__ import annotations

from services.macro_service import run_macro_regime_agent


def get_macro_regime_job_target(job_key: str) -> dict:
    registry = {
        "macro_regime_agent_daily": {
            "job_key": "macro_regime_agent_daily",
            "category": "agent",
            "runner_type": "service",
            "target": "services.macro_service.run_macro_regime_agent",
        },
    }
    if job_key not in registry:
        raise KeyError(job_key)
    return registry[job_key]


def run_macro_regime_job(job_key: str) -> dict:
    if job_key != "macro_regime_agent_daily":
        raise KeyError(job_key)
    return run_macro_regime_agent()
