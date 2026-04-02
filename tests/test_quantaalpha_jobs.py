#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from jobs.quantaalpha_jobs import get_quantaalpha_job_target


ROOT_DIR = Path(__file__).resolve().parents[1]


class QuantaAlphaJobsTest(unittest.TestCase):
    def test_all_quantaalpha_job_targets_shape(self):
        for job_key in (
            "quantaalpha_health_check",
            "quantaalpha_mine_daily",
            "quantaalpha_backtest_daily",
        ):
            target = get_quantaalpha_job_target(job_key)
            self.assertEqual(target["job_key"], job_key)
            self.assertEqual(target["category"], "quant")
            self.assertEqual(target["runner_type"], "service")
            self.assertIn("quantaalpha_service", target["target"])

    def test_run_quantaalpha_job_describe(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT_DIR / "jobs" / "run_quantaalpha_job.py"),
                "--job-key",
                "quantaalpha_health_check",
                "--describe",
            ],
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout.strip())
        self.assertEqual(payload["job_key"], "quantaalpha_health_check")
        self.assertEqual(payload["runner_type"], "service")


if __name__ == "__main__":
    unittest.main()
