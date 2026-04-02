#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from jobs.chatroom_jobs import get_chatroom_job_target


ROOT_DIR = Path(__file__).resolve().parents[1]


class ChatroomJobsTest(unittest.TestCase):
    def test_all_chatroom_job_targets_shape(self):
        for job_key in (
            "chatroom_analysis_pipeline",
            "chatroom_sentiment_refresh",
            "monitored_chatlog_fetch",
            "chatroom_list_refresh",
        ):
            target = get_chatroom_job_target(job_key)
            self.assertEqual(target["job_key"], job_key)
            self.assertEqual(target["category"], "chatrooms")
            self.assertEqual(target["runner_type"], "collector")
            self.assertTrue(target["target"].startswith("collectors.chatrooms."))

    def test_run_chatroom_job_describe(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT_DIR / "jobs" / "run_chatroom_job.py"),
                "--job-key",
                "chatroom_analysis_pipeline",
                "--describe",
            ],
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout.strip())
        self.assertEqual(payload["job_key"], "chatroom_analysis_pipeline")
        self.assertEqual(payload["runner_type"], "collector")
