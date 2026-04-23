#!/usr/bin/env python3
from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest import mock

from backend.routes.market import (
    _get_market_conclusion,
    _hydrate_conflict_sources_for_arbitration,
    _merge_distinct_conflict_rows,
    _score_conflict_resolution,
    query_market_conclusion_from_conn,
)


NOW = datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        rows = list(self._rows)
        return rows[0] if rows else None


class _FakeConn:
    def __init__(self, rows_by_table: dict[str, list[dict]]):
        self.rows_by_table = rows_by_table
        self.executed_sql: list[str] = []

    def execute(self, sql: str):
        self.executed_sql.append(sql)
        for table_name, rows in self.rows_by_table.items():
            if f"FROM {table_name}" in sql:
                return _FakeCursor(rows)
        raise AssertionError(f"unexpected SQL: {sql}")


class _FakeMarketConn:
    def __init__(self):
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple = ()):
        self.executed.append((sql, tuple(params or ())))
        if "SELECT MAX(scenario_date) AS dt FROM risk_scenarios" in sql:
            return _FakeCursor([{"dt": "20260423"}])
        if "FROM risk_scenarios" in sql and "WHERE scenario_date = ?" in sql:
            return _FakeCursor([])
        if "FROM risk_scenarios" in sql and "ORDER BY scenario_date DESC" in sql:
            return _FakeCursor(
                [
                    {
                        "scenario_name": "回撤压力测试",
                        "horizon": "1D",
                        "max_drawdown": -0.032,
                        "pnl_impact": -0.011,
                    }
                ]
            )
        # other source queries in _get_market_conclusion can safely return empty
        return _FakeCursor([])

    def close(self):
        return None


class MarketConclusionHydrateTest(unittest.TestCase):
    def test_merge_distinct_dedupes_by_source_and_subject(self):
        target: list[dict] = [{"source": "theme_hotspots", "theme_name": "AI"}]
        extra = [
            {"source": "theme_hotspots", "theme_name": "AI"},
            {"source": "theme_hotspots", "theme_name": "芯片"},
        ]
        added = _merge_distinct_conflict_rows(target, extra)
        self.assertEqual(added, 1)
        self.assertEqual(len(target), 2)

    def test_hydrate_pulls_wider_lookback_when_base_window_empty(self):
        """72h 无行时应在后续 widen 步合并到更宽窗口的数据（与 UI 可信度 0% 根因相关）。"""

        def theme_only(conn, hours: int) -> list[dict]:
            if int(hours) <= 72:
                return []
            if int(hours) == 144:
                return [
                    {
                        "source": "theme_hotspots",
                        "subject": "补位主题",
                        "direction": "看多",
                        "theme_strength": 0.55,
                        "confidence": 0.5,
                        "published_at": "2026-04-20T10:00:00Z",
                    }
                ]
            return []

        def empty_fetch(_conn, _hours):
            return []

        with mock.patch.multiple(
            "backend.routes.market",
            _fetch_theme_hotspot_rows=theme_only,
            _fetch_investment_signal_rows=empty_fetch,
            _fetch_news_summary_rows=empty_fetch,
            _fetch_stock_news_rows=empty_fetch,
            _fetch_macro_series_rows=empty_fetch,
            _fetch_risk_scenario_rows=empty_fetch,
        ):
            acc: list[dict] = []
            _hydrate_conflict_sources_for_arbitration(None, acc, base_lookback_hours=72)
            self.assertEqual(len(acc), 1)
            self.assertEqual(acc[0].get("subject"), "补位主题")

    def test_hydrate_skips_when_already_enough_rows(self):
        acc = [
            {"source": "theme_hotspots", "subject": f"t{i}", "direction": "看多"}
            for i in range(4)
        ]

        def boom(*_a, **_k):
            raise AssertionError("hydrate should not fetch when already >=4 rows")

        with mock.patch.multiple(
            "backend.routes.market",
            _fetch_theme_hotspot_rows=boom,
            _fetch_investment_signal_rows=boom,
            _fetch_news_summary_rows=boom,
            _fetch_stock_news_rows=boom,
            _fetch_macro_series_rows=boom,
            _fetch_risk_scenario_rows=boom,
        ):
            _hydrate_conflict_sources_for_arbitration(None, acc, base_lookback_hours=72)
        self.assertEqual(len(acc), 4)


class MarketConclusionScoringTest(unittest.TestCase):
    def test_single_source_returns_positive_composite(self):
        payload = _score_conflict_resolution(
            [
                {
                    "source": "theme_hotspots",
                    "direction": "看多",
                    "heat_level": "极高",
                    "theme_strength": 0.92,
                    "confidence": 0.88,
                    "published_at": "2026-04-20T11:00:00Z",
                }
            ],
            now=NOW,
        )
        self.assertEqual(payload["winner_source"], "theme_hotspots")
        self.assertEqual(payload["direction"], "看多")
        self.assertGreater(payload["confidence"], 0.7)
        self.assertFalse(payload["needs_review"])
        self.assertGreater(payload["score_breakdown"]["composite"], 0.0)

    def test_multi_source_uses_real_breakdown_and_consistency(self):
        payload = _score_conflict_resolution(
            [
                {
                    "source": "theme_hotspots",
                    "direction": "看多",
                    "heat_level": "高",
                    "theme_strength": 0.8,
                    "confidence": 0.7,
                    "published_at": "2026-04-20T10:00:00Z",
                },
                {
                    "source": "investment_signals",
                    "direction": "看多",
                    "signal_strength": 0.82,
                    "confidence": 0.76,
                    "updated_at": "2026-04-20T09:30:00Z",
                },
                {
                    "source": "news_daily_summaries",
                    "news_count": 16,
                    "summary_markdown": "市场整体偏多，看多科技与券商。",
                    "updated_at": "2026-04-20T09:00:00Z",
                },
                {
                    "source": "news_daily_summaries",
                    "news_count": 14,
                    "summary_markdown": "资金风格继续偏多，结论看多。",
                    "updated_at": "2026-04-20T08:00:00Z",
                },
            ],
            now=NOW,
        )
        self.assertEqual(payload["direction"], "看多")
        self.assertFalse(payload["needs_review"])
        self.assertEqual(payload["dissenting_sources"], [])
        self.assertGreater(payload["confidence"], 0.7)
        breakdown = {item["source"]: item for item in payload["score_breakdown"]["sources"]}
        self.assertIn("investment_signals", breakdown)
        self.assertIn("news_daily_summaries", breakdown)
        self.assertEqual(breakdown["news_daily_summaries"]["ai_consistency"], 1.0)
        self.assertGreater(breakdown["investment_signals"]["composite"], 0.0)

    def test_low_confidence_marks_needs_review_and_dissenting_sources(self):
        payload = _score_conflict_resolution(
            [
                {
                    "source": "investment_signals",
                    "direction": "看多",
                    "signal_strength": 0.05,
                    "confidence": 0.08,
                    "updated_at": "2026-04-15T08:00:00Z",
                },
                {
                    "source": "news_daily_summaries",
                    "news_count": 3,
                    "summary_markdown": "整体观点中性。",
                    "updated_at": "2026-04-18T06:00:00Z",
                },
                {
                    "source": "risk_scenarios",
                    "scenario_name": "回撤压力",
                    "pnl_impact": -0.01,
                    "max_drawdown": -0.02,
                    "var_95": -0.01,
                    "cvar_95": -0.015,
                    "updated_at": "2026-04-18T04:00:00Z",
                },
            ],
            now=NOW,
        )
        self.assertLess(payload["confidence"], 0.5)
        self.assertTrue(payload["needs_review"])
        self.assertIn("investment_signals", payload["dissenting_sources"])

    def test_risk_priority_can_win_over_bullish_sources(self):
        payload = _score_conflict_resolution(
            [
                {
                    "source": "theme_hotspots",
                    "direction": "看多",
                    "heat_level": "极高",
                    "theme_strength": 0.82,
                    "confidence": 0.75,
                    "published_at": "2026-04-20T11:00:00Z",
                },
                {
                    "source": "investment_signals",
                    "direction": "看多",
                    "signal_strength": 0.8,
                    "confidence": 0.72,
                    "updated_at": "2026-04-20T10:30:00Z",
                },
                {
                    "source": "risk_scenarios",
                    "scenario_name": "黑天鹅压力",
                    "pnl_impact": -0.08,
                    "max_drawdown": -0.18,
                    "var_95": -0.10,
                    "cvar_95": -0.14,
                    "updated_at": "2026-04-20T11:30:00Z",
                },
            ],
            now=NOW,
        )
        self.assertEqual(payload["winner_source"], "risk_scenarios")
        self.assertEqual(payload["direction"], "看空")
        self.assertFalse(payload["needs_review"])
        self.assertGreater(payload["confidence"], 0.7)

    def test_query_uses_postgres_windows_and_renamed_tables(self):
        conn = _FakeConn(
            {
                "theme_hotspot_tracker": [],
                "investment_signal_tracker": [],
                "news_daily_summaries": [],
                "stock_news_items": [],
                "macro_series": [],
                "risk_scenarios": [],
            }
        )
        payload = query_market_conclusion_from_conn(conn, lookback_hours=24, now=NOW)
        self.assertIn("conflict_resolution", payload)
        sql_blob = "\n".join(conn.executed_sql)
        self.assertIn("NOW() - INTERVAL '24 hours'", sql_blob)
        self.assertIn("FROM stock_news_items", sql_blob)
        self.assertIn("FROM macro_series", sql_blob)
        self.assertNotIn("datetime('now'", sql_blob)
        self.assertNotIn("FROM stock_news ", sql_blob)
        self.assertNotIn("FROM macro_indicators", sql_blob)

    def test_get_market_conclusion_uses_qmark_placeholder_for_risk_date_query(self):
        conn = _FakeMarketConn()

        def table_exists(_conn, table_name: str) -> bool:
            return table_name == "risk_scenarios"

        with mock.patch("backend.routes.market._db.connect", return_value=conn), mock.patch(
            "backend.routes.market._db.table_exists", side_effect=table_exists
        ), mock.patch("backend.routes.market._db.apply_row_factory"), mock.patch(
            "backend.routes.market._db.using_postgres", return_value=True
        ):
            payload = _get_market_conclusion(lookback_hours=72)

        self.assertIn("main_risks", payload)
        self.assertTrue(any("回撤压力测试" in item for item in payload["main_risks"]))
        sql_blob = "\n".join(sql for sql, _params in conn.executed)
        self.assertIn("WHERE scenario_date = ?", sql_blob)
        self.assertNotIn("%s", sql_blob)


if __name__ == "__main__":
    unittest.main()
