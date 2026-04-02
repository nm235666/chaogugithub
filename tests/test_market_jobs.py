#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from jobs.market_jobs import get_market_job_target


ROOT_DIR = Path(__file__).resolve().parents[1]


class MarketJobsTest(unittest.TestCase):
    def test_all_market_job_targets_shape(self):
        for job_key in (
            "market_expectations_refresh",
            "market_news_refresh",
        ):
            target = get_market_job_target(job_key)
            self.assertEqual(target["job_key"], job_key)
            self.assertEqual(target["category"], "market")
            self.assertEqual(target["runner_type"], "collector")
            self.assertTrue(target["target"].startswith("collectors.market."))

    def test_run_market_job_describe(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT_DIR / "jobs" / "run_market_job.py"),
                "--job-key",
                "market_expectations_refresh",
                "--describe",
            ],
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout.strip())
        self.assertEqual(payload["job_key"], "market_expectations_refresh")
        self.assertEqual(payload["runner_type"], "collector")


if __name__ == "__main__":
    unittest.main()
