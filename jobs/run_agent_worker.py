#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.agent_runtime import create_run, ensure_agent_tables, run_next_once
from services.agent_runtime.service import run_worker_loop


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Agent runtime worker")
    parser.add_argument("--once", action="store_true", help="Process at most one queued run and exit")
    parser.add_argument("--agent-key", default="", help="Create a run for this agent before processing")
    parser.add_argument("--job-key", default="", help="job_registry job_key stored on run metadata for traceability")
    parser.add_argument("--schedule-key", default="", help="Schedule de-dupe key for created runs")
    parser.add_argument("--trigger-source", default="worker", help="Run trigger source")
    parser.add_argument("--actor", default="agent-worker", help="Actor recorded on the created run")
    parser.add_argument("--poll-seconds", type=float, default=float(os.getenv("AGENT_WORKER_POLL_SECONDS", "1.0") or 1.0))
    args = parser.parse_args(argv)

    ensure_agent_tables()
    if args.agent_key.strip():
        meta: dict = {}
        jk = str(args.job_key or "").strip()
        if jk:
            meta["job_key"] = jk
        create_run(
            agent_key=args.agent_key.strip(),
            mode="auto",
            trigger_source=args.trigger_source.strip() or "worker",
            actor=args.actor.strip() or "agent-worker",
            goal={},
            schedule_key=args.schedule_key.strip(),
            dedupe=True,
            metadata=meta,
        )
    if args.once:
        run_next_once()
        return 0
    run_worker_loop(once=False, poll_seconds=args.poll_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
