#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from jobs.news_jobs import get_news_job_target
from collectors.news.common import run_python_commands


ROOT_DIR = Path(__file__).resolve().parents[1]


class NewsJobsTest(unittest.TestCase):
    def test_news_job_target_shape(self):
        target = get_news_job_target("news_daily_summary_refresh")
        self.assertEqual(target["job_key"], "news_daily_summary_refresh")
        self.assertEqual(target["runner_type"], "collector")
        self.assertIn("target", target)

    def test_all_news_job_targets_shape(self):
        for job_key in (
            "intl_news_pipeline",
            "cn_news_pipeline",
            "news_stock_map_refresh",
            "news_sentiment_refresh",
            "news_daily_summary_refresh",
        ):
            target = get_news_job_target(job_key)
            self.assertEqual(target["job_key"], job_key)
            self.assertIn(target["category"], {"news", "reports"})
            self.assertEqual(target["runner_type"], "collector")
            self.assertTrue(target["target"].startswith("collectors.news."))

    def test_run_python_commands_shape(self):
        result = run_python_commands(
            [{"script": "job_registry.py", "args": [], "timeout_s": 30, "meta": {"kind": "sample"}}],
            stop_on_error=False,
        )
        self.assertIn("ok", result)
        self.assertEqual(result["runner"], "python_pipeline")
        self.assertIn("command", result)
        self.assertIn("meta", result)

    def test_run_news_job_describe(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT_DIR / "jobs" / "run_news_job.py"),
                "--job-key",
                "news_sentiment_refresh",
                "--describe",
            ],
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout.strip())
        self.assertEqual(payload["job_key"], "news_sentiment_refresh")
        self.assertEqual(payload["runner_type"], "collector")
