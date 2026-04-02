#!/usr/bin/env python3
from __future__ import annotations

import unittest

from services.reporting.daily_summaries import get_daily_summary_task, query_daily_summaries


class ReportingServiceTest(unittest.TestCase):
    def test_query_daily_summaries_enriches_items(self):
        deps = {
            "query_news_daily_summaries": lambda **kwargs: {
                "page": 1,
                "page_size": 10,
                "total": 1,
                "total_pages": 1,
                "items": [
                    {
                        "id": 1,
                        "summary_date": "2026-04-01",
                        "summary_markdown": "## 综合影响总结\n测试正文",
                        "news_count": 5,
                        "model": "mock-model",
                        "filter_importance": "高",
                        "source_filter": "",
                    }
                ],
            }
        }
        payload = query_daily_summaries(
            deps,
            summary_date="2026-04-01",
            source_filter="",
            model="",
            page=1,
            page_size=10,
        )
        self.assertEqual(payload["items"][0]["analysis_markdown"].strip(), "## 综合影响总结\n测试正文")
        self.assertEqual(payload["items"][0]["markdown_content"].strip(), "## 综合影响总结\n测试正文")
        self.assertIn("export_meta", payload["items"][0])
        self.assertEqual(payload["protocol"]["primary_markdown_field"], "analysis_markdown")

    def test_get_daily_summary_task_enriches_item(self):
        deps = {
            "get_async_daily_summary_job": lambda job_id: {
                "job_id": job_id,
                "status": "done",
                "item": {
                    "id": 2,
                    "summary_date": "2026-04-01",
                    "summary_markdown": "日报内容",
                    "news_count": 8,
                    "model": "mock-model",
                },
            }
        }
        task = get_daily_summary_task(deps, job_id="job-1")
        self.assertEqual(task["item"]["analysis_markdown"].strip(), "日报内容")
        self.assertIn("export_meta", task["item"])
