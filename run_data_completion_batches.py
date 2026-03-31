#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3
from runtime_secrets import TUSHARE_TOKEN


ROOT = Path(__file__).resolve().parent
DEFAULT_TOKEN = TUSHARE_TOKEN


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="分批补全核心数据并输出缺口进度")
    parser.add_argument("--token", default=os.getenv("TUSHARE_TOKEN", DEFAULT_TOKEN))
    parser.add_argument("--governance-batch", type=int, default=100)
    parser.add_argument("--events-batch", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--skip-governance", action="store_true")
    parser.add_argument("--skip-events", action="store_true")
    parser.add_argument("--skip-flow", action="store_true")
    parser.add_argument("--skip-scores", action="store_true")
    parser.add_argument("--flow-lookback-days", type=int, default=365)
    parser.add_argument("--log-file", default="/tmp/data_completion_batches.log")
    return parser.parse_args()


def query_scalar(conn, sql: str):
    return conn.execute(sql).fetchone()[0]


def progress_snapshot() -> dict[str, object]:
    conn = sqlite3.connect(ROOT / "stocks.db")
    try:
        return {
            "events_rows": query_scalar(conn, "SELECT COUNT(*) FROM stock_events"),
            "governance_rows": query_scalar(conn, "SELECT COUNT(*) FROM company_governance"),
            "flow_rows": query_scalar(conn, "SELECT COUNT(*) FROM capital_flow_stock"),
            "scores_latest": query_scalar(conn, "SELECT MAX(score_date) FROM stock_scores_daily"),
            "miss_events": query_scalar(
                conn,
                "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' "
                "AND NOT EXISTS (SELECT 1 FROM stock_events e WHERE e.ts_code=s.ts_code)",
            ),
            "miss_governance": query_scalar(
                conn,
                "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' "
                "AND NOT EXISTS (SELECT 1 FROM company_governance g WHERE g.ts_code=s.ts_code)",
            ),
            "miss_flow": query_scalar(
                conn,
                "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' "
                "AND NOT EXISTS (SELECT 1 FROM capital_flow_stock c WHERE c.ts_code=s.ts_code)",
            ),
            "miss_minline": query_scalar(
                conn,
                "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' "
                "AND NOT EXISTS (SELECT 1 FROM stock_minline m WHERE m.ts_code=s.ts_code)",
            ),
        }
    finally:
        conn.close()


def log_line(path: str, text: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(text + "\n")


def run_step(log_file: str, name: str, argv: list[str]) -> int:
    log_line(log_file, f"[{utc_now()}] START {name}: {' '.join(argv)}")
    proc = subprocess.run(argv, cwd=str(ROOT))
    log_line(log_file, f"[{utc_now()}] END   {name}: rc={proc.returncode}")
    return proc.returncode


def main() -> int:
    args = parse_args()
    log_line(args.log_file, f"[{utc_now()}] ===== batch completion begin =====")
    before = progress_snapshot()
    log_line(args.log_file, f"[{utc_now()}] BEFORE {before}")

    env = os.environ.copy()
    env["TUSHARE_TOKEN"] = args.token

    for round_no in range(1, max(args.rounds, 1) + 1):
        log_line(args.log_file, f"[{utc_now()}] ROUND {round_no} begin")

        if not args.skip_governance and before["miss_governance"]:
            rc = run_step(
                args.log_file,
                f"governance_round_{round_no}",
                [
                    sys.executable,
                    str(ROOT / "backfill_company_governance.py"),
                    "--token",
                    args.token,
                    "--missing-only",
                    "--limit-stocks",
                    str(args.governance_batch),
                    "--pause",
                    "0.02",
                    "--retry",
                    "2",
                ],
            )
            if rc != 0:
                return rc

        if not args.skip_events and before["miss_events"]:
            rc = run_step(
                args.log_file,
                f"events_round_{round_no}",
                [
                    sys.executable,
                    str(ROOT / "backfill_stock_events.py"),
                    "--token",
                    args.token,
                    "--missing-only",
                    "--limit-stocks",
                    str(args.events_batch),
                    "--rows-per-source",
                    "120",
                    "--pause",
                    "0.02",
                    "--retry",
                    "2",
                ],
            )
            if rc != 0:
                return rc

        if round_no == 1 and not args.skip_flow and before["miss_flow"]:
            rc = run_step(
                args.log_file,
                "capital_flow_stock",
                [
                    sys.executable,
                    str(ROOT / "backfill_capital_flow_stock.py"),
                    "--token",
                    args.token,
                    "--lookback-days",
                    str(args.flow_lookback_days),
                    "--pause",
                    "0.01",
                    "--all-status",
                ],
            )
            if rc != 0:
                return rc

        if round_no == 1 and not args.skip_scores:
            rc = run_step(
                args.log_file,
                "stock_scores_refresh",
                [
                    sys.executable,
                    str(ROOT / "backfill_stock_scores_daily.py"),
                    "--truncate-date",
                ],
            )
            if rc != 0:
                return rc

        after = progress_snapshot()
        log_line(args.log_file, f"[{utc_now()}] AFTER ROUND {round_no} {after}")
        before = after
        time.sleep(1)

    log_line(args.log_file, f"[{utc_now()}] ===== batch completion end =====")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
