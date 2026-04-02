#!/usr/bin/env python3
from __future__ import annotations

import unittest

from services.reporting import query_research_reports


class _ConnStub:
    def __init__(self, responses):
        self.responses = list(responses)
        self.row_factory = None

    def execute(self, *_args, **_kwargs):
        return _CursorStub(self.responses.pop(0))

    def close(self):
        return None


class _CursorStub:
    def __init__(self, payload):
        self.payload = payload

    def fetchone(self):
        return self.payload

    def fetchall(self):
        return self.payload


class _SQLiteStub:
    Row = dict

    def __init__(self, responses):
        self.responses = responses

    def connect(self, _db_path):
        return _ConnStub(self.responses.copy())


class ReportingQueriesTest(unittest.TestCase):
    def test_query_research_reports_empty_table(self):
        sqlite_stub = _SQLiteStub([(0,)])
        payload = query_research_reports(
            sqlite3_module=sqlite_stub,
            db_path="mock.db",
            report_type="",
            keyword="",
            report_date="",
            page=1,
            page_size=20,
        )
        self.assertEqual(payload["total"], 0)
        self.assertIn("items", payload)
        self.assertEqual(payload["protocol"]["primary_markdown_field"], "analysis_markdown")
