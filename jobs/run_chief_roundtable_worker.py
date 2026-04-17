#!/usr/bin/env python3
"""首席圆桌 Worker — 长驻进程，轮询并执行 chief_roundtable_jobs 队列。"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import db_compat as sqlite3
from backend import server as backend_server
from services.agent_service.chief_roundtable_v1 import ensure_roundtable_tables, run_roundtable_worker_loop

LOGGER = logging.getLogger("chief_roundtable_worker")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Chief Roundtable independent worker loop")
    parser.add_argument("--once", action="store_true", help="Process at most one queued job and exit")
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=float(os.getenv("CHIEF_ROUNDTABLE_WORKER_POLL_SECONDS", "2.0") or 2.0),
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=os.getenv("CHIEF_ROUNDTABLE_WORKER_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    LOGGER.info("starting chief roundtable worker db=%s", backend_server.DB_PATH)

    conn = sqlite3.connect()
    try:
        ensure_roundtable_tables(conn)
    finally:
        conn.close()

    try:
        run_roundtable_worker_loop(
            sqlite3_module=sqlite3,
            once=bool(args.once),
            poll_seconds=max(0.5, float(args.poll_seconds or 2.0)),
        )
    except KeyboardInterrupt:
        LOGGER.info("received interrupt, exiting")
        return 0
    except Exception:
        LOGGER.exception("chief roundtable worker crashed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
