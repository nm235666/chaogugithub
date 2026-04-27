#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from jobs.data_readiness_jobs import get_data_readiness_job_target, run_data_readiness_job


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Data Readiness Agent jobs")
    parser.add_argument("--job-key", required=True, help="任务标识，如 data_readiness_daily")
    parser.add_argument("--describe", action="store_true", help="仅输出任务目标，不实际运行")
    parser.add_argument("--dry-run", action="store_true", help="只检查并规划补数动作，不执行补数脚本")
    parser.add_argument("--no-ai", action="store_true", help="禁用 AI 诊断，仅返回规则检查和启发式说明")
    parser.add_argument("--no-ai-path-selection", action="store_true", help="禁用 AI 补数路径选择，回退为规则顺序")
    args = parser.parse_args(argv)

    target = get_data_readiness_job_target(args.job_key)
    if args.describe:
        print(json.dumps(target, ensure_ascii=False))
        return 0
    print(json.dumps({"ok": True, "target": target}, ensure_ascii=False))
    ai_enabled = not args.no_ai
    result = run_data_readiness_job(
        args.job_key,
        dry_run=args.dry_run,
        ai_enabled=ai_enabled,
        path_selection_enabled=ai_enabled and not args.no_ai_path_selection,
    )
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
