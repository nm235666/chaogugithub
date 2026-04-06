#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db_compat as sqlite3

DB_PATH = ROOT_DIR / "stock_codes.db"


def _load_enabled_jobs() -> list[dict[str, str]]:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT job_key, category, schedule_expr
            FROM job_definitions
            WHERE COALESCE(enabled, 1) = 1
            ORDER BY category, job_key
            """
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _runner_for_category(category: str) -> str:
    if str(category or "").strip() == "market_data":
        return str(ROOT_DIR / "run_job_if_trade_day.sh")
    return str(ROOT_DIR / "run_job_always.sh")


def render_lines() -> list[str]:
    out: list[str] = []
    for job in _load_enabled_jobs():
        job_key = str(job.get("job_key") or "").strip()
        schedule_expr = str(job.get("schedule_expr") or "").strip()
        category = str(job.get("category") or "").strip()
        if not job_key or not schedule_expr:
            continue
        runner = _runner_for_category(category)
        out.append(f"{schedule_expr} /bin/bash {runner} {job_key} # zanbo_job:{job_key}")
    return out


def main() -> int:
    print("# BEGIN_ZANBO_JOBS")
    for line in render_lines():
        print(line)
    print("# END_ZANBO_JOBS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
