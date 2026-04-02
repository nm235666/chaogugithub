#!/usr/bin/env python3
from __future__ import annotations

import threading
import unittest

from services.reporting.runtime_ops import run_async_daily_summary_job


class _Publisher:
    def __init__(self):
        self.events = []

    def __call__(self, **kwargs):
        self.events.append(kwargs)


class ReportingRuntimeOpsTest(unittest.TestCase):
    def test_run_async_daily_summary_job_done_notifies(self):
        jobs = {
            "job-1": {
                "job_id": "job-1",
                "status": "queued",
                "progress": 0,
                "stage": "queued",
                "message": "",
                "summary_date": "2026-04-02",
                "model": "mock-model",
            }
        }
        lock = threading.Lock()
        publisher = _Publisher()
        run_async_daily_summary_job(
            jobs=jobs,
            lock=lock,
            publish_app_event=publisher,
            generate_daily_summary_fn=lambda model, summary_date: {"stdout": "ok", "meta": {"attempts": []}},
            get_daily_summary_by_date_fn=lambda _date: {
                "summary_markdown": "日报内容",
                "model": "mock-model",
            },
            notify_fn=lambda **kwargs: {"ok": True, "meta": kwargs},
            job_id="job-1",
        )
        self.assertEqual(jobs["job-1"]["status"], "done")
        self.assertTrue(jobs["job-1"]["notification"]["ok"])

    def test_run_async_daily_summary_job_error_notifies(self):
        jobs = {
            "job-2": {
                "job_id": "job-2",
                "status": "queued",
                "progress": 0,
                "stage": "queued",
                "message": "",
                "summary_date": "2026-04-02",
                "model": "mock-model",
            }
        }
        lock = threading.Lock()
        publisher = _Publisher()
        run_async_daily_summary_job(
            jobs=jobs,
            lock=lock,
            publish_app_event=publisher,
            generate_daily_summary_fn=lambda model, summary_date: (_ for _ in ()).throw(RuntimeError("boom")),
            get_daily_summary_by_date_fn=lambda _date: None,
            notify_fn=lambda **kwargs: {"ok": True, "meta": kwargs},
            job_id="job-2",
        )
        self.assertEqual(jobs["job-2"]["status"], "error")
        self.assertTrue(jobs["job-2"]["notification"]["ok"])


if __name__ == "__main__":
    unittest.main()
