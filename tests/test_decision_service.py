#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.decision_service import service as decision_service


def _init_schema(conn: sqlite3.Connection) -> None:
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
        CREATE TABLE stock_scores_daily (
            score_date TEXT,
            ts_code TEXT,
            name TEXT,
            symbol TEXT,
            market TEXT,
            area TEXT,
            industry TEXT,
            industry_rank INTEGER,
            industry_count INTEGER,
            score_grade TEXT,
            industry_score_grade TEXT,
            total_score REAL,
            industry_total_score REAL,
            trend_score REAL,
            industry_trend_score REAL,
            financial_score REAL,
            industry_financial_score REAL,
            valuation_score REAL,
            industry_valuation_score REAL,
            capital_flow_score REAL,
            industry_capital_flow_score REAL,
            event_score REAL,
            industry_event_score REAL,
            news_score REAL,
            industry_news_score REAL,
            risk_score REAL,
            industry_risk_score REAL,
            latest_trade_date TEXT,
            latest_risk_date TEXT,
            score_payload_json TEXT,
            source TEXT,
            update_time TEXT
        );
        CREATE TABLE stock_news_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT,
            pub_time TEXT,
            title TEXT,
            summary TEXT,
            link TEXT,
            llm_finance_importance TEXT,
            llm_summary TEXT
        );
        CREATE TABLE chatroom_stock_candidate_pool (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_name TEXT,
            candidate_type TEXT,
            bullish_room_count INTEGER,
            bearish_room_count INTEGER,
            net_score REAL,
            dominant_bias TEXT,
            mention_count INTEGER,
            room_count INTEGER,
            latest_analysis_date TEXT,
            ts_code TEXT,
            sample_reasons_json TEXT
        );
        CREATE TABLE investment_signal_tracker_7d (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            source_summary_json TEXT
        );
        CREATE TABLE multi_role_v3_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            stage TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            lookback INTEGER NOT NULL DEFAULT 120,
            config_json TEXT NOT NULL DEFAULT '{}',
            state_json TEXT NOT NULL DEFAULT '{}',
            result_json TEXT NOT NULL DEFAULT '{}',
            decision_state_json TEXT NOT NULL DEFAULT '{}',
            metrics_json TEXT NOT NULL DEFAULT '{}',
            error TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT NOT NULL DEFAULT '',
            worker_id TEXT NOT NULL DEFAULT '',
            lease_until TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE multi_role_v3_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE TABLE chief_roundtable_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'queued',
            stage TEXT NOT NULL DEFAULT '',
            ts_code TEXT NOT NULL,
            trigger TEXT NOT NULL DEFAULT 'manual',
            source_job_id TEXT NOT NULL DEFAULT '',
            context_json TEXT NOT NULL DEFAULT '{}',
            positions_json TEXT NOT NULL DEFAULT '{}',
            synthesis_json TEXT NOT NULL DEFAULT '{}',
            error TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT NOT NULL DEFAULT '',
            worker_id TEXT NOT NULL DEFAULT '',
            owner TEXT NOT NULL DEFAULT ''
        );
        """
    )
    conn.commit()


class DecisionServiceTest(unittest.TestCase):
    def _mk_db(self) -> str:
        fd, path = tempfile.mkstemp(prefix="decision-", suffix=".db")
        os.close(fd)
        return path

    def test_board_stock_history_and_switch_flow(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        try:
            _init_schema(conn)
            conn.execute(
                """
                INSERT INTO stock_scores_daily (
                    score_date, ts_code, name, symbol, market, area, industry, industry_rank, industry_count,
                    score_grade, industry_score_grade, total_score, industry_total_score, trend_score,
                    industry_trend_score, financial_score, industry_financial_score, valuation_score,
                    industry_valuation_score, capital_flow_score, industry_capital_flow_score, event_score,
                    industry_event_score, news_score, industry_news_score, risk_score, industry_risk_score,
                    latest_trade_date, latest_risk_date, score_payload_json, source, update_time
                ) VALUES (
                    '2026-04-08', '000001.SZ', '平安银行', '000001', '主板', '深圳', '银行', 1, 8,
                    'A', 'A', 91.5, 88.2, 89.0, 86.0, 92.0, 90.0, 85.0, 83.0, 77.0, 74.0, 68.0, 63.0, 72.0, 70.0, 80.0, 78.0,
                    '2026-04-08', '2026-04-08', ?, 'unit_test', '2026-04-08T10:00:00Z'
                )
                """,
                (
                    json.dumps(
                        {
                            "score_summary": {
                                "trend": "趋势稳健",
                                "financial": "财务稳定",
                                "valuation": "估值合理",
                            }
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
            conn.commit()
        finally:
            conn.close()

        with patch.dict(os.environ, {"DECISION_STRATEGY_LLM_ENABLED": "0"}, clear=False):
            with patch.object(decision_service, "query_stock_detail") as mock_detail:
                mock_detail.return_value = {
                    "profile": {"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "market": "主板", "area": "深圳"},
                    "score": {"total_score": 91.5, "industry_total_score": 88.2, "score_grade": "A", "industry_score_grade": "A", "score_summary": {}},
                }

                board = decision_service.query_decision_board(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=5)
                self.assertEqual(board["summary"]["universe_size"], 1)
                self.assertEqual(board["market_regime"]["mode"], "aggressive")
                self.assertEqual(board["summary"]["top_score"], 91.5)
                self.assertEqual(board["shortlist"][0]["ts_code"], "000001.SZ")
                self.assertEqual(board["trade_plan"]["mode"], "aggressive")

                conn = sqlite3.connect(db_path)
                try:
                    conn.execute(
                        """
                        INSERT INTO stock_news_items (ts_code, pub_time, title, summary, link, llm_finance_importance, llm_summary)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        ("000001.SZ", "2026-04-08 11:00:00", "平安银行获资金关注", "news", "https://example.com/news", "高", "资金面改善"),
                    )
                    conn.execute(
                        """
                        INSERT INTO chatroom_stock_candidate_pool (
                            candidate_name, candidate_type, bullish_room_count, bearish_room_count, net_score,
                            dominant_bias, mention_count, room_count, latest_analysis_date, ts_code, sample_reasons_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        ("平安银行", "股票", 3, 0, 8.5, "看多", 5, 3, "2026-04-08", "000001.SZ", "[]"),
                    )
                    conn.execute(
                        """
                        INSERT INTO investment_signal_tracker_7d (
                            signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                            evidence_count, news_count, stock_news_count, chatroom_count, signal_status,
                            latest_signal_date, source_summary_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "stock:000001.SZ",
                            "stock",
                            "平安银行",
                            "000001.SZ",
                            "看多",
                            85.0,
                            78.0,
                            4,
                            2,
                            1,
                            1,
                            "活跃",
                            "2026-04-08",
                            json.dumps({"stock_news": 1, "chatroom": 1}, ensure_ascii=False),
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()

                plan = decision_service.query_decision_trade_plan(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=5)
                self.assertEqual(plan["mode"], "aggressive")
                self.assertEqual(plan["position_plan"]["base_position"], 30)
                self.assertGreaterEqual(len(plan["checklist"]), 3)
                self.assertEqual(plan["priority_stocks"][0]["ts_code"], "000001.SZ")
                self.assertGreaterEqual(len(plan["intraday_plan"]), 3)
                self.assertIn("开盘前", [item["stage"] for item in plan["intraday_plan"]])
                self.assertIsInstance(plan["theme_links"], list)
                self.assertIn("approval_flow", plan)
                self.assertEqual(plan["approval_flow"]["state"], "pending")

                strategy_lab = decision_service.query_decision_strategy_lab(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=5)
                self.assertEqual(strategy_lab["title"], "策略实验台")
                self.assertEqual(strategy_lab["source_mode"], "preview")
                self.assertGreaterEqual(len(strategy_lab["strategies"]), 3)
                self.assertEqual(strategy_lab["summary"]["best_strategy"], strategy_lab["strategies"][0]["name"])
                self.assertIn("右侧趋势确认策略", [item["name"] for item in strategy_lab["strategies"]])
                self.assertIn("llm_feasibility_score", strategy_lab["strategies"][0])

                runs_empty = decision_service.query_decision_strategy_runs(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=10)
                self.assertEqual(runs_empty["total"], 0)

                generated = decision_service.run_decision_strategy_generation(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=5)
                self.assertEqual(generated["title"], "策略实验台")
                self.assertEqual(generated["source_mode"], "generated")
                self.assertIn("generated_run", generated)
                self.assertGreaterEqual(len(generated["strategies"]), 3)
                self.assertIn("llm_feasibility_score", generated["strategies"][0])

                runs = decision_service.query_decision_strategy_runs(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=10)
                self.assertEqual(runs["total"], 1)
                self.assertEqual(runs["items"][0]["run_version"], 1)
                self.assertIn("comparison_to_previous", runs["items"][0])

                latest_strategy = decision_service.query_decision_strategy_lab(
                    sqlite3_module=sqlite3,
                    db_path=db_path,
                    page=1,
                    page_size=5,
                    run_id=generated["generated_run"]["run_id"],
                )
                self.assertEqual(latest_strategy["run"]["run_version"], 1)
                self.assertEqual(latest_strategy["source_mode"], "stored")

                stock = decision_service.query_decision_stock(sqlite3_module=sqlite3, db_path=db_path, ts_code="000001.SZ")
                self.assertEqual(stock["score"]["total_score"], 91.5)
                self.assertTrue(stock["trade_plan"]["allow_entry"])
                self.assertIn("平安银行", stock["detail"]["profile"]["name"])

                history_empty = decision_service.query_decision_history(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=10)
                self.assertEqual(history_empty["total"], 0)

                switch_state = decision_service.get_decision_kill_switch(sqlite3_module=sqlite3, db_path=db_path)
                self.assertEqual(int(switch_state["allow_trading"]), 1)

                updated = decision_service.set_decision_kill_switch(
                    sqlite3_module=sqlite3,
                    db_path=db_path,
                    allow_trading=False,
                    reason="unit test pause",
                )
                self.assertEqual(int(updated["allow_trading"]), 0)
                self.assertEqual(updated["reason"], "unit test pause")

                snapshot = decision_service.run_decision_snapshot(sqlite3_module=sqlite3, db_path=db_path, snapshot_date="2026-04-08")
                self.assertTrue(snapshot["ok"])
                self.assertEqual(snapshot["status"], "success")
                self.assertEqual(snapshot["source"], "decision_snapshot")
                self.assertEqual(snapshot["receipt"]["source"], "decision_snapshot")
                self.assertEqual(snapshot["receipt"]["trace"]["snapshot_id"], snapshot["snapshot_id"])
                self.assertEqual(snapshot["receipt"]["trace"]["run_id"], snapshot["run_id"])
                history = decision_service.query_decision_history(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=10)
                self.assertEqual(history["total"], 1)
                self.assertEqual(history["items"][0]["snapshot_date"], "2026-04-08")
                self.assertIn("summary", history["items"][0]["payload"])
                self.assertEqual(history["items"][0]["status"], "success")
                self.assertEqual(history["items"][0]["source"], "decision_snapshot")
                self.assertEqual(history["items"][0]["receipt"]["trace"]["snapshot_id"], history["items"][0]["trace"]["snapshot_id"])

                action = decision_service.record_decision_action(
                    sqlite3_module=sqlite3,
                    db_path=db_path,
                    action_type="confirm",
                    ts_code="000001.SZ",
                    stock_name="平安银行",
                    note="unit test confirm",
                    actor="tester",
                    snapshot_date="2026-04-08",
                    payload={"context": {"source": "unit_test"}},
                )
                self.assertEqual(action["action_type"], "confirm")
                self.assertEqual(action["status"], "success")
                self.assertEqual(action["source"], "unit_test")
                self.assertEqual(action["receipt"]["source"], "unit_test")
                self.assertEqual(action["receipt"]["trace"]["action_id"], action["action_id"])
                actions = decision_service.query_decision_actions(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=10)
                self.assertEqual(actions["total"], 1)
                self.assertEqual(actions["items"][0]["ts_code"], "000001.SZ")
                self.assertEqual(actions["items"][0]["payload"]["context"]["source"], "unit_test")
                self.assertEqual(actions["items"][0]["status"], "success")
                self.assertEqual(actions["items"][0]["source"], "unit_test")
                self.assertEqual(actions["items"][0]["receipt"]["trace"]["action_id"], actions["items"][0]["trace"]["action_id"])

                conn = sqlite3.connect(db_path)
                try:
                    conn.execute(
                        """
                        INSERT INTO multi_role_v3_jobs (
                            job_id, status, stage, ts_code, lookback, config_json, state_json, result_json,
                            decision_state_json, metrics_json, error, created_at, updated_at, finished_at, worker_id, lease_until
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "mr-001",
                            "pending_user_decision",
                            "await_user_decision",
                            "000001.SZ",
                            120,
                            "{}",
                            "{}",
                            "{}",
                            "{}",
                            json.dumps({"message": "等待人工裁决"}, ensure_ascii=False),
                            "",
                            "2026-04-08T11:20:00Z",
                            "2026-04-08T11:30:00Z",
                            "",
                            "worker-a",
                            "",
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO multi_role_v3_events (job_id, stage, event_type, payload_json, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        ("mr-001", "await_user_decision", "awaiting_user_decision", "{}", "2026-04-08T11:31:00Z"),
                    )
                    conn.execute(
                        """
                        INSERT INTO chief_roundtable_jobs (
                            job_id, status, stage, ts_code, trigger, source_job_id, context_json, positions_json,
                            synthesis_json, error, created_at, updated_at, finished_at, worker_id, owner
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "rt-001",
                            "running",
                            "chiefs",
                            "000001.SZ",
                            "manual",
                            "mr-001",
                            "{}",
                            "{}",
                            "{}",
                            "",
                            "2026-04-08T11:40:00Z",
                            "2026-04-08T11:50:00Z",
                            "",
                            "worker-b",
                            "tester",
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()

                multi_role_action = decision_service.record_decision_action(
                    sqlite3_module=sqlite3,
                    db_path=db_path,
                    action_type="confirm",
                    ts_code="000001.SZ",
                    stock_name="平安银行",
                    note="multi role confirm",
                    actor="tester",
                    snapshot_date="2026-04-08",
                    payload={"context": {"source": "multi_role_v3", "job_id": "mr-001", "direction": "bullish"}},
                )
                self.assertEqual(multi_role_action["source"], "multi_role_v3")
                self.assertEqual(multi_role_action["context"]["job_id"], "mr-001")
                self.assertEqual(multi_role_action["receipt"]["source"], "multi_role_v3")
                self.assertEqual(multi_role_action["receipt"]["context"]["job_id"], "mr-001")
                self.assertTrue(multi_role_action["receipt"]["trace"]["action_id"])

                roundtable_action = decision_service.record_decision_action(
                    sqlite3_module=sqlite3,
                    db_path=db_path,
                    action_type="defer",
                    ts_code="000001.SZ",
                    stock_name="平安银行",
                    note="roundtable defer",
                    actor="tester",
                    snapshot_date="2026-04-08",
                    payload={"context": {"source": "chief_roundtable", "job_id": "rt-001", "consensus": "split"}},
                )
                self.assertEqual(roundtable_action["source"], "chief_roundtable")
                self.assertEqual(roundtable_action["context"]["job_id"], "rt-001")
                self.assertEqual(roundtable_action["receipt"]["source"], "chief_roundtable")
                self.assertEqual(roundtable_action["receipt"]["context"]["job_id"], "rt-001")
                self.assertTrue(roundtable_action["receipt"]["trace"]["action_id"])

                actions = decision_service.query_decision_actions(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=10)
                self.assertEqual(actions["total"], 3)
                self.assertEqual(actions["items"][0]["source"], "chief_roundtable")
                self.assertEqual(actions["items"][0]["context"]["job_id"], "rt-001")
                self.assertEqual(actions["items"][0]["receipt"]["source"], "chief_roundtable")
                self.assertEqual(actions["items"][0]["receipt"]["context"]["job_id"], "rt-001")
                self.assertTrue(actions["items"][0]["receipt"]["trace"]["action_id"])
                self.assertEqual(actions["items"][0]["job_trace"]["job_id"], "rt-001")
                self.assertEqual(actions["items"][0]["job_trace"]["stage"], "chiefs")
                self.assertEqual(actions["items"][0]["job_trace"]["status"], "running")
                self.assertIn("阶段 chiefs", actions["items"][0]["job_trace"]["summary"])
                self.assertEqual(actions["items"][0]["job_trace"]["updated_at"], "2026-04-08T11:50:00Z")
                self.assertEqual(actions["items"][1]["source"], "multi_role_v3")
                self.assertEqual(actions["items"][1]["context"]["job_id"], "mr-001")
                self.assertEqual(actions["items"][1]["receipt"]["source"], "multi_role_v3")
                self.assertEqual(actions["items"][1]["receipt"]["context"]["job_id"], "mr-001")
                self.assertTrue(actions["items"][1]["receipt"]["trace"]["action_id"])
                self.assertEqual(actions["items"][1]["job_trace"]["job_id"], "mr-001")
                self.assertEqual(actions["items"][1]["job_trace"]["stage"], "await_user_decision")
                self.assertEqual(actions["items"][1]["job_trace"]["status"], "pending_user_decision")
                self.assertIn("等待人工裁决", actions["items"][1]["job_trace"]["summary"])
                self.assertEqual(actions["items"][1]["job_trace"]["updated_at"], "2026-04-08T11:31:00Z")

                plan_with_action = decision_service.query_decision_trade_plan(sqlite3_module=sqlite3, db_path=db_path, page=1, page_size=5, ts_code="000001.SZ")
                self.assertEqual(plan_with_action["approval_flow"]["state"], "deferred")
                self.assertGreaterEqual(len(plan_with_action["approval_flow"]["recent_actions"]), 1)

                scoreboard = decision_service.query_decision_scoreboard(sqlite3_module=sqlite3, db_path=db_path, page_size=5)
                self.assertEqual(scoreboard["macro_regime"]["mode"], "aggressive")
                self.assertGreaterEqual(len(scoreboard["stock_shortlist"]), 1)
                packet = scoreboard["reason_packets"]["000001.SZ"]
                self.assertEqual(packet["score"]["total_score"], 91.5)
                self.assertEqual(packet["news"]["count"], 1)
                self.assertEqual(packet["signals"]["count"], 1)
                self.assertEqual(packet["candidate_pool"]["dominant_bias"], "看多")
                self.assertEqual(packet["status"], "ok")

    def test_scheduled_job_uses_cn_date(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        try:
            _init_schema(conn)
            conn.commit()
        finally:
            conn.close()

        result = decision_service.run_decision_scheduled_job(sqlite3_module=sqlite3, db_path=db_path, job_key="decision_daily_snapshot")
        self.assertEqual(result["job_key"], "decision_daily_snapshot")
        self.assertRegex(result["snapshot_date"], r"^\d{4}-\d{2}-\d{2}$")


if __name__ == "__main__":
    unittest.main()
