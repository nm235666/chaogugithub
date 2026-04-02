#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from services.stock_detail_service import query_stock_detail


def _init_schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE stock_codes (
            ts_code TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            area TEXT,
            industry TEXT,
            market TEXT,
            list_date TEXT,
            delist_date TEXT,
            list_status TEXT
        );
        CREATE TABLE stock_daily_prices (
            ts_code TEXT,
            trade_date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            pct_chg REAL,
            vol REAL,
            amount REAL
        );
        CREATE TABLE stock_minline (
            ts_code TEXT,
            trade_date TEXT,
            minute_time TEXT,
            price REAL,
            avg_price REAL,
            volume REAL,
            total_volume REAL
        );
        CREATE TABLE stock_scores_daily (
            ts_code TEXT,
            score_date TEXT,
            score_payload_json TEXT
        );
        CREATE TABLE stock_financials (
            ts_code TEXT,
            report_period TEXT,
            report_type TEXT,
            ann_date TEXT,
            revenue REAL,
            op_profit REAL,
            net_profit REAL,
            net_profit_excl_nr REAL,
            roe REAL,
            gross_margin REAL,
            debt_to_assets REAL,
            operating_cf REAL,
            free_cf REAL,
            eps REAL,
            bps REAL
        );
        CREATE TABLE stock_valuation_daily (
            ts_code TEXT,
            trade_date TEXT,
            pe REAL,
            pe_ttm REAL,
            pb REAL,
            ps REAL,
            ps_ttm REAL,
            dv_ratio REAL,
            dv_ttm REAL,
            total_mv REAL,
            circ_mv REAL
        );
        CREATE TABLE capital_flow_stock (
            ts_code TEXT,
            trade_date TEXT,
            net_inflow REAL,
            main_inflow REAL,
            super_large_inflow REAL,
            large_inflow REAL,
            medium_inflow REAL,
            small_inflow REAL
        );
        CREATE TABLE capital_flow_market (
            trade_date TEXT,
            flow_type TEXT,
            net_inflow REAL
        );
        CREATE TABLE stock_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT,
            event_type TEXT,
            event_date TEXT,
            ann_date TEXT,
            title TEXT,
            detail_json TEXT,
            source TEXT
        );
        CREATE TABLE company_governance (
            ts_code TEXT,
            asof_date TEXT,
            holder_structure_json TEXT,
            board_structure_json TEXT,
            mgmt_change_json TEXT,
            incentive_plan_json TEXT,
            governance_score REAL
        );
        CREATE TABLE risk_scenarios (
            ts_code TEXT,
            scenario_date TEXT,
            scenario_name TEXT,
            horizon TEXT,
            pnl_impact REAL,
            max_drawdown REAL,
            var_95 REAL,
            cvar_95 REAL,
            assumptions_json TEXT
        );
        CREATE TABLE stock_news_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT,
            pub_time TEXT,
            title TEXT,
            summary TEXT,
            link TEXT,
            llm_system_score REAL,
            llm_finance_impact_score REAL,
            llm_finance_importance TEXT,
            llm_summary TEXT,
            llm_impacts_json TEXT
        );
        CREATE TABLE chatroom_stock_candidate_pool (
            candidate_name TEXT,
            candidate_type TEXT,
            bullish_room_count INTEGER,
            bearish_room_count INTEGER,
            net_score REAL,
            dominant_bias TEXT,
            mention_count INTEGER,
            room_count INTEGER,
            latest_analysis_date TEXT
        );
        CREATE TABLE chatroom_investment_analysis (
            room_id TEXT,
            talker TEXT,
            analysis_date TEXT,
            latest_message_date TEXT,
            final_bias TEXT,
            targets_json TEXT,
            room_summary TEXT,
            update_time TEXT
        );
        """
    )
    conn.commit()


class StockDetailServiceTest(unittest.TestCase):
    def _mk_db(self) -> str:
        fd, path = tempfile.mkstemp(prefix="stock-detail-", suffix=".db")
        os.close(fd)
        return path

    def test_query_stock_detail_minimal_payload(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        try:
            _init_schema(conn)
            conn.execute(
                """
                INSERT INTO stock_codes (ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status)
                VALUES ('600519.SH', '600519', '贵州茅台', '贵州', '白酒', '主板', '20010827', '', 'L')
                """
            )
            conn.commit()
        finally:
            conn.close()
        try:
            payload = query_stock_detail(
                sqlite3_module=sqlite3,
                db_path=db_path,
                ts_code="600519.SH",
                keyword="",
                lookback=60,
            )
            self.assertEqual(payload["profile"]["name"], "贵州茅台")
            self.assertEqual(payload["recent_prices"], [])
            self.assertEqual(payload["financial_summary"], {})
            self.assertEqual(payload["stock_news_summary"], {})
        finally:
            os.unlink(db_path)

    def test_query_stock_detail_with_basic_data(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        try:
            _init_schema(conn)
            conn.execute(
                """
                INSERT INTO stock_codes (ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status)
                VALUES ('600519.SH', '600519', '贵州茅台', '贵州', '白酒', '主板', '20010827', '', 'L')
                """
            )
            conn.execute(
                """
                INSERT INTO stock_daily_prices (ts_code, trade_date, open, high, low, close, pct_chg, vol, amount)
                VALUES ('600519.SH', '2026-04-01', 1500, 1520, 1490, 1510, 1.2, 100000, 150000000)
                """
            )
            conn.execute(
                """
                INSERT INTO stock_minline (ts_code, trade_date, minute_time, price, avg_price, volume, total_volume)
                VALUES ('600519.SH', '2026-04-01', '14:59', 1510, 1508, 1200, 300000)
                """
            )
            conn.execute(
                """
                INSERT INTO stock_financials (ts_code, report_period, report_type, ann_date, revenue, op_profit, net_profit, net_profit_excl_nr, roe, gross_margin, debt_to_assets, operating_cf, free_cf, eps, bps)
                VALUES ('600519.SH', '2025Q4', '年报', '2026-03-20', 1000, 500, 400, 380, 22, 88, 20, 300, 250, 2.5, 25)
                """
            )
            conn.execute(
                """
                INSERT INTO stock_valuation_daily (ts_code, trade_date, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_mv, circ_mv)
                VALUES ('600519.SH', '2026-04-01', 30, 28, 8, 10, 9.8, 1.2, 1.1, 200000, 180000)
                """
            )
            conn.execute(
                """
                INSERT INTO stock_news_items (ts_code, pub_time, title, summary, link, llm_system_score, llm_finance_impact_score, llm_finance_importance, llm_summary, llm_impacts_json)
                VALUES ('600519.SH', '2026-04-01 10:00:00', '新闻标题', '新闻摘要', 'https://example.com', 0.8, 0.7, '高', 'LLM摘要', '{}')
                """
            )
            conn.commit()
        finally:
            conn.close()
        try:
            payload = query_stock_detail(
                sqlite3_module=sqlite3,
                db_path=db_path,
                ts_code="",
                keyword="茅台",
                lookback=80,
            )
            self.assertEqual(payload["profile"]["ts_code"], "600519.SH")
            self.assertEqual(len(payload["recent_prices"]), 1)
            self.assertEqual(payload["latest_minline"]["minute_time"], "14:59")
            self.assertEqual(payload["financial_summary"]["latest_report_period"], "2025Q4")
            self.assertEqual(payload["stock_news_summary"]["high_importance_count_recent_8"], 1)
            self.assertIn("current", payload["valuation_summary"])
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
