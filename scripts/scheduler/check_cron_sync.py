#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db_compat as sqlite3

DB_PATH = ROOT_DIR / "stock_codes.db"
UTC = timezone.utc
CST = timezone(timedelta(hours=8))


@dataclass
class JobCron:
    job_key: str
    category: str
    schedule_expr: str


def _parse_allowed(expr: str, *, low: int, high: int, sunday_alias_zero: bool = False) -> set[int]:
    expr = str(expr or "").strip()
    out: set[int] = set()
    if expr == "*":
        return set(range(low, high + 1))
    for part in expr.split(","):
        piece = part.strip()
        if not piece:
            continue
        step = 1
        base = piece
        if "/" in piece:
            base, step_raw = piece.split("/", 1)
            step = max(1, int(step_raw))
        if base == "*":
            start, end = low, high
        elif "-" in base:
            a, b = base.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = end = int(base)
        for value in range(start, end + 1, step):
            v = value
            if sunday_alias_zero and v == 7:
                v = 0
            if low <= v <= high:
                out.add(v)
    return out


def _matches_cron(expr: str, dt: datetime) -> bool:
    parts = str(expr or "").split()
    if len(parts) != 5:
        return False
    minute, hour, dom, month, dow = parts
    minute_ok = dt.minute in _parse_allowed(minute, low=0, high=59)
    hour_ok = dt.hour in _parse_allowed(hour, low=0, high=23)
    month_ok = dt.month in _parse_allowed(month, low=1, high=12)
    dom_ok = dt.day in _parse_allowed(dom, low=1, high=31)
    cron_dow = (dt.weekday() + 1) % 7
    dow_ok = cron_dow in _parse_allowed(dow, low=0, high=6, sunday_alias_zero=True)
    return minute_ok and hour_ok and month_ok and dom_ok and dow_ok


def _next_trigger(expr: str, now_utc: datetime, horizon_days: int = 14) -> datetime | None:
    cursor = (now_utc.replace(second=0, microsecond=0) + timedelta(minutes=1)).astimezone(UTC)
    max_steps = max(1, horizon_days) * 24 * 60
    for _ in range(max_steps):
        if _matches_cron(expr, cursor):
            return cursor
        cursor += timedelta(minutes=1)
    return None


def _load_enabled_jobs() -> list[JobCron]:
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
        return [JobCron(job_key=str(r["job_key"]), category=str(r["category"]), schedule_expr=str(r["schedule_expr"])) for r in rows]
    finally:
        conn.close()


def _expected_line(job: JobCron) -> str:
    runner = ROOT_DIR / ("run_job_if_trade_day.sh" if job.category == "market_data" else "run_job_always.sh")
    return f"{job.schedule_expr} /bin/bash {runner} {job.job_key} # zanbo_job:{job.job_key}"


def _current_crontab_lines() -> list[str]:
    proc = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        return []
    return [line.rstrip() for line in (proc.stdout or "").splitlines()]


def main() -> int:
    jobs = _load_enabled_jobs()
    expected = {_expected_line(job): job for job in jobs}
    expected_by_key = {job.job_key: _expected_line(job) for job in jobs}

    current_lines = _current_crontab_lines()
    current_job_lines = [line for line in current_lines if "# zanbo_job:" in line]
    current_by_key: dict[str, str] = {}
    for line in current_job_lines:
        marker = line.split("# zanbo_job:", 1)[-1].strip()
        if marker:
            current_by_key[marker] = line.strip()

    missing_keys = sorted(set(expected_by_key.keys()) - set(current_by_key.keys()))
    extra_keys = sorted(set(current_by_key.keys()) - set(expected_by_key.keys()))
    drift_keys = sorted(
        key
        for key in (set(expected_by_key.keys()) & set(current_by_key.keys()))
        if expected_by_key[key].strip() != current_by_key[key].strip()
    )

    print(f"expected_jobs={len(expected_by_key)} installed_jobs={len(current_by_key)}")
    print(f"missing={len(missing_keys)} extra={len(extra_keys)} drift={len(drift_keys)}")
    if missing_keys:
        print("missing_keys:", ",".join(missing_keys))
    if extra_keys:
        print("extra_keys:", ",".join(extra_keys))
    if drift_keys:
        print("drift_keys:", ",".join(drift_keys))

    now_utc = datetime.now(UTC)
    print("\nnext_trigger_report_utc_cst")
    for job in jobs:
        nxt = _next_trigger(job.schedule_expr, now_utc)
        next_utc = nxt.strftime("%Y-%m-%d %H:%M") if nxt else "-"
        next_cst = nxt.astimezone(CST).strftime("%Y-%m-%d %H:%M") if nxt else "-"
        mode = "trade_day_gate" if job.category == "market_data" else "always"
        print(f"{job.job_key}|cron={job.schedule_expr}|mode={mode}|next_utc={next_utc}|next_cst={next_cst}")

    return 0 if (not missing_keys and not extra_keys and not drift_keys) else 1


if __name__ == "__main__":
    raise SystemExit(main())
