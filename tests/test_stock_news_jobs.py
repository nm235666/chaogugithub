#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from jobs.stock_news_jobs import get_stock_news_job_target


ROOT_DIR = Path(__file__).resolve().parents[1]


class StockNewsJobsTest(unittest.TestCase):
    def test_all_stock_news_job_targets_shape(self):
        for job_key in (
            "stock_news_score_refresh",
            "stock_news_backfill_missing",
            "stock_news_expand_focus",
        ):
            target = get_stock_news_job_target(job_key)
            self.assertEqual(target["job_key"], job_key)
            self.assertEqual(target["category"], "stock_news")
            self.assertEqual(target["runner_type"], "collector")
            self.assertTrue(target["target"].startswith("collectors.stock_news."))

    def test_run_stock_news_job_describe(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT_DIR / "jobs" / "run_stock_news_job.py"),
                "--job-key",
                "stock_news_score_refresh",
                "--describe",
            ],
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout.strip())
        self.assertEqual(payload["job_key"], "stock_news_score_refresh")
        self.assertEqual(payload["runner_type"], "collector")
