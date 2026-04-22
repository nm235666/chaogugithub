#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from jobs.decision_jobs import get_decision_job_target, run_decision_job


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run decision board jobs")
    parser.add_argument("--job-key", required=True, help="任务标识，如 decision_daily_snapshot")
    args = parser.parse_args(argv)

    target = get_decision_job_target(args.job_key)
    print(json.dumps({"ok": True, "target": target}, ensure_ascii=False))
    result = run_decision_job(args.job_key)
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
