#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from jobs.macro_regime_jobs import get_macro_regime_job_target, run_macro_regime_job


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run macro regime agent jobs")
    parser.add_argument("--job-key", required=True, help="任务标识，如 macro_regime_agent_daily")
    parser.add_argument("--describe", action="store_true", help="仅输出任务目标，不实际运行")
    args = parser.parse_args(argv)

    target = get_macro_regime_job_target(args.job_key)
    if args.describe:
        print(json.dumps(target, ensure_ascii=False))
        return 0
    print(json.dumps({"ok": True, "target": target}, ensure_ascii=False))
    result = run_macro_regime_job(args.job_key)
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
