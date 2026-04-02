#!/usr/bin/env python3
from __future__ import annotations

import unittest

from services.stock_news_service import build_fetch_response, build_score_response


class StockNewsServiceTest(unittest.TestCase):
    def test_build_fetch_response_uses_service_deps(self):
        deps = {
            "fetch_stock_news_now": lambda **kwargs: {"stdout": "fetch-ok", "stderr": ""},
            "score_stock_news_now": lambda **kwargs: {
                "stdout": "score-ok",
                "stderr": "",
                "meta": {"used_models": ["mock-score-model"], "items": [{"model": "mock-score-model"}]},
            },
            "query_stock_news_feed": lambda **kwargs: {"items": [{"id": 1, "ts_code": "000001.SZ"}], "total": 1},
        }
        payload = build_fetch_response(
            deps,
            ts_code="000001.SZ",
            company_name="平安银行",
            page_size=20,
            model="auto",
            score=True,
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["used_model"], "mock-score-model")
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["items"][0]["ts_code"], "000001.SZ")

    def test_build_score_response_preserves_meta(self):
        deps = {
            "score_stock_news_now": lambda **kwargs: {
                "stdout": "score-ok",
                "stderr": "",
                "meta": {"used_models": ["mock-score-model"], "items": [{"model": "mock-score-model"}]},
            }
        }
        payload = build_score_response(
            deps,
            ts_code="000001.SZ",
            model="auto",
            row_id=123,
            limit=1,
            force=True,
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["row_id"], 123)
        self.assertEqual(payload["used_model"], "mock-score-model")
        self.assertIn("meta", payload)
