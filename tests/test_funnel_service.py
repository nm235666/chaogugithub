#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from services.funnel_service import service as funnel_service


def _table_exists_sqlite(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


class FunnelServiceStatusTest(unittest.TestCase):
    def _mk_db(self) -> str:
        fd, path = tempfile.mkstemp(prefix="funnel-", suffix=".db")
        os.close(fd)
        return path

    def test_metrics_marks_degraded_when_upstream_ready_but_funnel_missing(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("CREATE TABLE stock_scores_daily (score_date TEXT)")
            conn.execute("INSERT INTO stock_scores_daily (score_date) VALUES ('2026-04-23')")
            conn.commit()
        finally:
            conn.close()

        def connect_override():
            return sqlite3.connect(db_path)

        def apply_row_factory(conn):
            conn.row_factory = sqlite3.Row

        with patch.object(funnel_service._db, "connect", side_effect=connect_override), patch.object(
            funnel_service._db, "apply_row_factory", side_effect=apply_row_factory
        ), patch.object(funnel_service._db, "table_exists", side_effect=_table_exists_sqlite):
            metrics = funnel_service.get_funnel_metrics()
            listing = funnel_service.list_candidates()

        self.assertEqual(metrics["status"], "degraded")
        self.assertIn("funnel_candidates", metrics["missing_inputs"])
        self.assertEqual(metrics["upstream_scores"]["latest_count"], 1)
        self.assertEqual(listing["status"], "degraded")
        self.assertEqual(listing["total"], 0)
        self.assertEqual(listing["upstream_scores"]["latest_score_date"], "2026-04-23")

    def test_metrics_ready_when_funnel_has_rows(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("CREATE TABLE stock_scores_daily (score_date TEXT)")
            conn.execute("INSERT INTO stock_scores_daily (score_date) VALUES ('2026-04-23')")
            conn.execute(
                """
                CREATE TABLE funnel_candidates (
                    id TEXT PRIMARY KEY,
                    ts_code TEXT,
                    name TEXT,
                    source TEXT,
                    trigger_source TEXT,
                    reason TEXT,
                    evidence_ref TEXT,
                    state TEXT,
                    state_version INTEGER,
                    created_at TEXT,
                    updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO funnel_candidates (
                    id, ts_code, name, source, trigger_source, reason, evidence_ref, state, state_version, created_at, updated_at
                ) VALUES ('cid-1', '000001.SZ', '平安银行', 'decision_daily_snapshot', 'decision_action', 'test', 'snap:2026-04-23', 'decision_ready', 1, '2026-04-23T01:00:00Z', '2026-04-23T01:00:00Z')
                """
            )
            conn.commit()
        finally:
            conn.close()

        def connect_override():
            return sqlite3.connect(db_path)

        def apply_row_factory(conn):
            conn.row_factory = sqlite3.Row

        with patch.object(funnel_service._db, "connect", side_effect=connect_override), patch.object(
            funnel_service._db, "apply_row_factory", side_effect=apply_row_factory
        ), patch.object(funnel_service._db, "table_exists", side_effect=_table_exists_sqlite):
            metrics = funnel_service.get_funnel_metrics()
            listing = funnel_service.list_candidates(limit=10, offset=0)

        self.assertEqual(metrics["status"], "ready")
        self.assertEqual(metrics["total"], 1)
        self.assertEqual(listing["status"], "ready")
        self.assertEqual(listing["total"], 1)
        self.assertEqual(listing["items"][0]["ts_code"], "000001.SZ")


if __name__ == "__main__":
    unittest.main()
