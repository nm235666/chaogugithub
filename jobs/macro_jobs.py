from __future__ import annotations

from collectors.macro import run_macro_context_refresh, run_macro_series_akshare_refresh


def get_macro_job_target(job_key: str) -> dict:
    registry = {
        "macro_series_akshare_refresh": {
            "job_key": "macro_series_akshare_refresh",
            "category": "macro",
            "runner_type": "collector",
            "target": "collectors.macro.run_macro_series_akshare_refresh",
        },
        "macro_context_refresh": {
            "job_key": "macro_context_refresh",
            "category": "macro",
            "runner_type": "collector",
            "target": "collectors.macro.run_macro_context_refresh",
        },
    }
    if job_key not in registry:
        raise KeyError(job_key)
    return registry[job_key]


def run_macro_job(job_key: str) -> dict:
    if job_key == "macro_series_akshare_refresh":
        return run_macro_series_akshare_refresh()
    if job_key == "macro_context_refresh":
        return run_macro_context_refresh()
    raise KeyError(job_key)
