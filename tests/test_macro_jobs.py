#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from jobs.macro_jobs import get_macro_job_target


ROOT_DIR = Path(__file__).resolve().parents[1]


class MacroJobsTest(unittest.TestCase):
    def test_all_macro_job_targets_shape(self):
        for job_key in (
            "macro_series_akshare_refresh",
            "macro_context_refresh",
        ):
            target = get_macro_job_target(job_key)
            self.assertEqual(target["job_key"], job_key)
            self.assertEqual(target["category"], "macro")
            self.assertEqual(target["runner_type"], "collector")
            self.assertTrue(target["target"].startswith("collectors.macro."))

    def test_run_macro_job_describe(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT_DIR / "jobs" / "run_macro_job.py"),
                "--job-key",
                "macro_series_akshare_refresh",
                "--describe",
            ],
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout.strip())
        self.assertEqual(payload["job_key"], "macro_series_akshare_refresh")
        self.assertEqual(payload["runner_type"], "collector")


if __name__ == "__main__":
    unittest.main()
