#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from services.signals_service.queries import (
    query_investment_signals,
    query_signal_state_timeline,
)


def _resolve_signal_table(_conn, _scope: str):
    return "investment_signal_tracker", "7d"


class SignalsQueriesTest(unittest.TestCase):
    def _mk_db(self) -> str:
        fd, path = tempfile.mkstemp(prefix="signals-queries-", suffix=".db")
        os.close(fd)
        return path

    def test_query_investment_signals_returns_empty_when_table_missing(self):
        db_path = self._mk_db()
        try:
            payload = query_investment_signals(
                sqlite3_module=sqlite3,
                db_path=db_path,
                resolve_signal_table_fn=_resolve_signal_table,
                cache_get_json_fn=lambda _key: None,
                cache_set_json_fn=lambda _key, _value, _ttl: None,
                redis_cache_ttl_signals=60,
                keyword="",
                signal_type="",
                signal_group="",
                scope="",
                source_filter="",
                direction="",
                signal_status="",
                page=1,
                page_size=20,
            )
            self.assertEqual(payload["total"], 0)
            self.assertEqual(payload["items"], [])
            self.assertEqual(payload["scope"], "7d")
        finally:
            os.unlink(db_path)

    def test_query_investment_signals_supports_filters_and_pagination(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE investment_signal_tracker (
                    id INTEGER PRIMARY KEY,
                    signal_key TEXT,
                    signal_type TEXT,
                    subject_name TEXT,
                    ts_code TEXT,
                    direction TEXT,
                    signal_strength REAL,
                    confidence REAL,
                    evidence_count INTEGER,
                    news_count INTEGER,
                    stock_news_count INTEGER,
                    chatroom_count INTEGER,
                    signal_status TEXT,
                    latest_signal_date TEXT,
                    evidence_json TEXT,
                    source_summary_json TEXT,
                    created_at TEXT,
                    update_time TEXT
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO investment_signal_tracker (
                    signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                    evidence_count, news_count, stock_news_count, chatroom_count, signal_status,
                    latest_signal_date, evidence_json, source_summary_json, created_at, update_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("stock:贵州茅台", "stock", "贵州茅台", "600519.SH", "看多", 0.9, 0.8, 10, 5, 3, 2, "活跃", "2026-04-01", "{}", "{}", "2026-04-01", "2026-04-01"),
                    ("stock:中石油", "stock", "中石油", "601857.SH", "看空", 0.7, 0.6, 6, 2, 1, 1, "观察", "2026-04-01", "{}", "{}", "2026-04-01", "2026-04-01"),
                    ("theme:AI算力", "theme", "AI算力", "", "看多", 0.85, 0.7, 7, 3, 0, 1, "活跃", "2026-04-01", "{}", "{}", "2026-04-01", "2026-04-01"),
                ],
            )
            conn.commit()
        finally:
            conn.close()
        try:
            payload = query_investment_signals(
                sqlite3_module=sqlite3,
                db_path=db_path,
                resolve_signal_table_fn=_resolve_signal_table,
                cache_get_json_fn=lambda _key: None,
                cache_set_json_fn=lambda _key, _value, _ttl: None,
                redis_cache_ttl_signals=60,
                keyword="茅台",
                signal_type="stock",
                signal_group="",
                scope="7d",
                source_filter="",
                direction="看多",
                signal_status="活跃",
                page=1,
                page_size=1,
            )
            self.assertEqual(payload["total"], 1)
            self.assertEqual(payload["total_pages"], 1)
            self.assertEqual(len(payload["items"]), 1)
            self.assertEqual(payload["items"][0]["subject_name"], "贵州茅台")
            self.assertIn("signal_types", payload["filters"])
            self.assertIn("bullish_total", payload["summary"])
        finally:
            os.unlink(db_path)

    def test_query_signal_state_timeline_empty_tables(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE signal_state_tracker (
                    id INTEGER PRIMARY KEY,
                    signal_scope TEXT,
                    signal_key TEXT,
                    subject_name TEXT,
                    current_state TEXT,
                    update_time TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO signal_state_tracker (signal_scope, signal_key, subject_name, current_state, update_time)
                VALUES ('theme', 'theme:AI算力', 'AI算力', '活跃', '2026-04-01T10:00:00')
                """
            )
            conn.commit()
        finally:
            conn.close()
        try:
            payload = query_signal_state_timeline(
                sqlite3_module=sqlite3,
                db_path=db_path,
                signal_scope="theme",
                signal_key="theme:AI算力",
                page=1,
                page_size=20,
            )
            self.assertIsNotNone(payload["signal"])
            self.assertEqual(payload["signal"]["subject_name"], "AI算力")
            self.assertEqual(payload["events"], [])
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
