from __future__ import annotations

from collectors.news.common import run_python_commands, run_python_script


def run_macro_series_akshare_refresh() -> dict:
    return run_python_script(
        "backfill_macro_series_akshare.py",
        timeout_s=1800,
        meta={"kind": "macro_series_akshare_refresh"},
    )


def run_macro_context_refresh() -> dict:
    commands = [
        {
            "script": "backfill_fx_daily.py",
            "args": ["--lookback-days", "10", "--pause", "0.02"],
            "timeout_s": 1800,
            "meta": {"kind": "macro_fx_daily_refresh"},
        },
        {
            "script": "backfill_rate_curve_points.py",
            "args": ["--lookback-days", "10", "--pause", "0.02"],
            "timeout_s": 1800,
            "meta": {"kind": "macro_rate_curve_refresh"},
        },
        {
            "script": "backfill_spread_daily.py",
            "args": ["--lookback-days", "10"],
            "timeout_s": 1800,
            "meta": {"kind": "macro_spread_refresh"},
        },
    ]
    return run_python_commands(commands)
