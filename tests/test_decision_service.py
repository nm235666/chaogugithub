#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlparse

from backend.routes import decision as decision_routes
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
        CREATE TABLE data_readiness_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_key TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'check',
            before_json TEXT NOT NULL DEFAULT '{}',
            after_json TEXT NOT NULL DEFAULT '{}',
            actions_json TEXT NOT NULL DEFAULT '[]',
            summary_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    conn.execute(
        """
        INSERT INTO data_readiness_runs (
            run_key, status, mode, before_json, after_json, actions_json, summary_json, created_at, updated_at
        ) VALUES (
            'unit-ready', 'ready', 'dry_run', '{}', '{"status":"ready","issues":[]}', '[]',
            '{"status":"ready","ai_diagnosis":{"business_impact":"可以运行策略选股","can_run_strategy_agent":true}}',
            '2026-04-08T07:00:00Z', '2026-04-08T07:00:00Z'
        )
        """
    )
    conn.commit()


class _FakeHandler:
    def __init__(self) -> None:
        self.status = 200
        self.payload = None

    def _send_json(self, payload, status=200):
        self.status = status
        self.payload = payload


class DecisionServiceTest(unittest.TestCase):
    def _mk_db(self) -> str:
        fd, path = tempfile.mkstemp(prefix="decision-", suffix=".db")
        os.close(fd)
        return path

    def _insert_score(
        self,
        conn: sqlite3.Connection,
        *,
        ts_code: str,
        name: str,
        total_score: float,
        trend_score: float,
        risk_score: float = 70.0,
        industry: str = "银行",
    ) -> None:
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
                '2026-04-08', ?, ?, substr(?, 1, 6), '主板', '深圳', ?, 1, 8,
                'A', 'A', ?, ?, ?, ?, 80, 80, 75, 75, 72, 72, 70, 70, 76, 76, ?, ?,
                '2026-04-08', '2026-04-08', ?, 'unit_test', '2026-04-08T10:00:00Z'
            )
            """,
            (
                ts_code,
                name,
                ts_code,
                industry,
                total_score,
                total_score,
                trend_score,
                trend_score,
                risk_score,
                risk_score,
                json.dumps({"score_summary": {"trend": "趋势确认", "risk": "风险可控"}}, ensure_ascii=False),
            ),
        )

    def _set_data_readiness_run(self, conn: sqlite3.Connection, *, status: str, issues: list[dict] | None = None, impact: str = "") -> None:
        conn.execute("DELETE FROM data_readiness_runs")
        after = {"status": status, "issues": issues or []}
        summary = {
            "status": status,
            "ai_diagnosis": {
                "business_impact": impact,
                "degrade_strategy": "剔除缺失股票或降低相关数据权重" if status == "degraded" else "",
                "can_run_strategy_agent": status != "blocked",
            },
        }
        conn.execute(
            """
            INSERT INTO data_readiness_runs (
                run_key, status, mode, before_json, after_json, actions_json, summary_json, created_at, updated_at
            ) VALUES (?, ?, 'dry_run', '{}', ?, '[]', ?, '2026-04-08T07:00:00Z', '2026-04-08T07:00:00Z')
            """,
            (f"unit-{status}", status, json.dumps(after, ensure_ascii=False), json.dumps(summary, ensure_ascii=False)),
        )

    def test_build_stock_evidence_packet_reuses_score_news_signal_chatroom(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
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
                (json.dumps({"score_summary": {"trend": "趋势稳健"}}, ensure_ascii=False),),
            )
            conn.execute(
                """
                INSERT INTO stock_news_items (ts_code, pub_time, title, summary, link, llm_finance_importance, llm_summary)
                VALUES ('000001.SZ', '2026-04-08 11:00:00', '平安银行获资金关注', 'news', 'https://example.com/news', '高', '资金面改善')
                """
            )
            conn.execute(
                """
                INSERT INTO investment_signal_tracker_7d (
                    signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                    evidence_count, news_count, stock_news_count, chatroom_count, signal_status,
                    latest_signal_date, source_summary_json
                ) VALUES (
                    'stock:000001.SZ', 'stock', '平安银行', '000001.SZ', '看多', 85, 78, 4, 2, 1, 1, '活跃', '2026-04-08', '{}'
                )
                """
            )
            conn.execute(
                """
                INSERT INTO chatroom_stock_candidate_pool (
                    candidate_name, candidate_type, bullish_room_count, bearish_room_count, net_score,
                    dominant_bias, mention_count, room_count, latest_analysis_date, ts_code, sample_reasons_json
                ) VALUES ('平安银行', '股票', 3, 0, 8.5, '看多', 5, 3, '2026-04-08', '000001.SZ', '[]')
                """
            )
            conn.commit()

            packet = decision_service.build_stock_evidence_packet(conn, ts_code="000001.SZ", name="平安银行")
        finally:
            conn.close()

        self.assertTrue(packet["evidence_chain_complete"])
        self.assertEqual(packet["evidence_status"], "complete")
        self.assertEqual(packet["score"]["total_score"], 91.5)
        self.assertEqual(packet["news"]["count"], 1)
        self.assertEqual(packet["signals"]["count"], 1)
        self.assertEqual(packet["candidate_pool"]["matched_count"], 1)

    def test_record_decision_action_stores_evidence_packet_snapshot(self):
        db_path = self._mk_db()
        packet = {
            "ts_code": "000001.SZ",
            "evidence_chain_complete": False,
            "missing_evidence": ["signals"],
            "score": {"status": "ok", "total_score": 88.0},
        }
        result = decision_service.record_decision_action(
            sqlite3_module=sqlite3,
            db_path=db_path,
            action_type="confirm",
            ts_code="000001.SZ",
            stock_name="平安银行",
            payload={
                "evidence_packet": packet,
                "missing_evidence": ["signals"],
                "evidence_chain_complete": False,
            },
        )
        self.assertEqual(result["payload"]["evidence_packet"]["ts_code"], "000001.SZ")
        self.assertFalse(result["payload"]["evidence_chain_complete"])
        self.assertEqual(result["payload"]["missing_evidence"], ["signals"])

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
                self.assertIn("entry_trigger", board["shortlist"][0])
                self.assertIn("invalidation", board["shortlist"][0])
                self.assertIn("position_hint", board["shortlist"][0])
                self.assertIn("risk_budget_source", board["shortlist"][0])
                self.assertIn("pipeline_health", board)
                self.assertIn(board["pipeline_health"]["status"], {"empty", "degraded", "ready"})
                self.assertEqual(board["pipeline_health"]["score_date"], "2026-04-08")

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
                self.assertIn("entry_trigger", stock["trade_plan"])
                self.assertIn("invalidation", stock["trade_plan"])
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
                funnel_sync = decision_service.sync_decision_action_to_funnel(
                    sqlite3_module=sqlite3,
                    db_path=db_path,
                    action_type="confirm",
                    ts_code="000001.SZ",
                    stock_name="平安银行",
                    note="unit test confirm",
                    actor="tester",
                    snapshot_date="2026-04-08",
                    action_id=action["action_id"],
                )
                self.assertTrue(funnel_sync["ok"])
                self.assertEqual(funnel_sync["state"], "confirmed")

                funnel_sync_defer = decision_service.sync_decision_action_to_funnel(
                    sqlite3_module=sqlite3,
                    db_path=db_path,
                    action_type="defer",
                    ts_code="000001.SZ",
                    stock_name="平安银行",
                    note="unit test defer",
                    actor="tester",
                    snapshot_date="2026-04-08",
                    action_id="manual-defer-1",
                )
                self.assertTrue(funnel_sync_defer["ok"])
                self.assertEqual(funnel_sync_defer["state"], "deferred")

                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                try:
                    candidate_row = conn.execute(
                        f"SELECT id, ts_code, state, state_version FROM {decision_service.FUNNEL_CANDIDATES_TABLE} WHERE ts_code = ?",
                        ("000001.SZ",),
                    ).fetchone()
                    self.assertIsNotNone(candidate_row)
                    self.assertEqual(dict(candidate_row)["state"], "deferred")
                    transitions_count = conn.execute(
                        f"SELECT COUNT(1) AS c FROM {decision_service.FUNNEL_TRANSITIONS_TABLE} WHERE candidate_id = ?",
                        (dict(candidate_row)["id"],),
                    ).fetchone()
                    self.assertGreaterEqual(int(dict(transitions_count)["c"] or 0), 3)
                finally:
                    conn.close()

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

    def test_today_actions_returns_buy_reduce_close_with_reasons_and_quantity(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="000001.SZ", name="强势新买", total_score=91.0, trend_score=82.0, risk_score=75.0)
            self._insert_score(conn, ts_code="000002.SZ", name="趋势减弱", total_score=64.0, trend_score=52.0, risk_score=62.0)
            self._insert_score(conn, ts_code="000003.SZ", name="风险恶化", total_score=48.0, trend_score=38.0, risk_score=35.0)
            conn.executescript(
                """
                CREATE TABLE portfolio_positions (
                    id TEXT PRIMARY KEY,
                    ts_code TEXT,
                    name TEXT,
                    quantity INTEGER,
                    avg_cost REAL,
                    last_price REAL,
                    market_value REAL,
                    unrealized_pnl REAL,
                    order_no TEXT,
                    updated_at TEXT
                );
                CREATE TABLE stock_daily_prices (
                    ts_code TEXT,
                    trade_date TEXT,
                    close REAL
                );
                """
            )
            conn.executemany(
                """
                INSERT INTO portfolio_positions (id, ts_code, name, quantity, avg_cost, last_price, market_value, unrealized_pnl, order_no, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("pos-2", "000002.SZ", "趋势减弱", 1000, 10.0, 10.0, 10000.0, 0.0, "22222222", "2026-04-08T10:00:00Z"),
                    ("pos-3", "000003.SZ", "风险恶化", 800, 12.0, 12.0, 9600.0, -200.0, "33333333", "2026-04-08T10:00:00Z"),
                ],
            )
            conn.executemany(
                "INSERT INTO stock_daily_prices (ts_code, trade_date, close) VALUES (?, ?, ?)",
                [
                    ("000001.SZ", "2026-04-08", 10.0),
                    ("000002.SZ", "2026-04-08", 10.0),
                    ("000003.SZ", "2026-04-08", 12.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        with patch.dict(os.environ, {"DECISION_DEFAULT_ACCOUNT_EQUITY": "100000", "DECISION_LOT_SIZE": "100", "DECISION_TRADE_ADVISOR_AGENT_ENABLED": "0"}, clear=False):
            payload = decision_service.query_decision_today_actions(sqlite3_module=sqlite3, db_path=db_path, limit=20)

        self.assertTrue(payload["risk_gate"]["ok"], payload["risk_gate"])
        by_code = {item["ts_code"]: item for item in payload["items"]}
        self.assertEqual(by_code["000001.SZ"]["action_type"], "buy")
        self.assertEqual(by_code["000001.SZ"]["rule_tier"], "strong_buy")
        self.assertEqual(by_code["000001.SZ"]["quantity"], 900)
        self.assertTrue(by_code["000001.SZ"]["can_create_order"])
        self.assertIn("短线新开仓", by_code["000001.SZ"]["reason"]["summary"])
        self.assertEqual(by_code["000002.SZ"]["action_type"], "reduce")
        self.assertEqual(by_code["000002.SZ"]["quantity"], 500)
        self.assertEqual(by_code["000002.SZ"]["execution_flow"]["chain_mode"], "reuse")
        self.assertEqual(by_code["000002.SZ"]["order_payload"]["chain_order_no"], "22222222")
        self.assertEqual(by_code["000002.SZ"]["next_step"], "create_order_plan")
        self.assertIn("减仓", by_code["000002.SZ"]["reason"]["sell_reason"])
        self.assertEqual(by_code["000003.SZ"]["action_type"], "close")
        self.assertEqual(by_code["000003.SZ"]["quantity"], 800)
        self.assertEqual(by_code["000003.SZ"]["execution_flow"]["chain_mode"], "reuse")
        self.assertEqual(by_code["000003.SZ"]["order_payload"]["chain_order_no"], "33333333")
        self.assertIn("清仓", by_code["000003.SZ"]["reason"]["sell_reason"])
        self.assertGreaterEqual(payload["summary"]["executable"], 3)

    def test_today_actions_blocked_by_data_readiness_gate(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="000001.SZ", name="强势新买", total_score=91.0, trend_score=82.0, risk_score=75.0)
            self._set_data_readiness_run(
                conn,
                status="blocked",
                issues=[{"code": "risk_recent_missing", "severity": "blocked", "message": "最近 5 日风险情景缺口过大"}],
                impact="风险情景缺失，暂停新增买入建议",
            )
            conn.commit()
        finally:
            conn.close()

        with patch.dict(os.environ, {"DECISION_DEFAULT_ACCOUNT_EQUITY": "100000", "DECISION_LOT_SIZE": "100", "DECISION_TRADE_ADVISOR_AGENT_ENABLED": "0"}, clear=False):
            payload = decision_service.query_decision_today_actions(sqlite3_module=sqlite3, db_path=db_path, limit=10)

        self.assertFalse(payload["risk_gate"]["ok"])
        self.assertEqual(payload["risk_gate"]["data_readiness_status"], "blocked")
        self.assertEqual(payload["data_readiness_gate"]["status"], "blocked")
        self.assertTrue(any("数据就绪 Gate" in item for item in payload["risk_gate"]["blockers"]))
        self.assertTrue(payload["items"])
        self.assertTrue(all(not item["can_create_order"] for item in payload["items"]))

    def test_today_actions_degraded_data_readiness_warns_but_allows_actions(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="000001.SZ", name="强势新买", total_score=91.0, trend_score=82.0, risk_score=75.0)
            self._set_data_readiness_run(
                conn,
                status="degraded",
                issues=[{"code": "flow_recent_missing", "severity": "degraded", "message": "最近 5 日资金流覆盖不足"}],
                impact="短线策略降级运行",
            )
            conn.commit()
        finally:
            conn.close()

        with patch.dict(os.environ, {"DECISION_DEFAULT_ACCOUNT_EQUITY": "100000", "DECISION_LOT_SIZE": "100", "DECISION_TRADE_ADVISOR_AGENT_ENABLED": "0"}, clear=False):
            payload = decision_service.query_decision_today_actions(sqlite3_module=sqlite3, db_path=db_path, limit=10)

        self.assertTrue(payload["risk_gate"]["ok"], payload["risk_gate"])
        self.assertEqual(payload["risk_gate"]["data_readiness_status"], "degraded")
        self.assertEqual(payload["data_readiness_gate"]["status"], "degraded")
        self.assertTrue(any("降级" in item or "资金流" in item for item in payload["risk_gate"]["warnings"]))
        self.assertGreaterEqual(payload["summary"]["executable"], 1)

    def test_strategy_selection_agent_persists_candidates_and_today_actions_use_them(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="000001.SZ", name="强势策略股", total_score=91.0, trend_score=82.0, risk_score=75.0)
            self._insert_score(conn, ts_code="000002.SZ", name="普通观察股", total_score=70.0, trend_score=50.0, risk_score=60.0)
            conn.execute("CREATE TABLE stock_daily_prices (ts_code TEXT, trade_date TEXT, close REAL)")
            conn.execute("INSERT INTO stock_daily_prices VALUES ('000001.SZ', '2026-04-08', 10.0)")
            conn.execute("INSERT INTO stock_daily_prices VALUES ('000002.SZ', '2026-04-08', 10.0)")
            conn.commit()
        finally:
            conn.close()

        with patch.dict(os.environ, {"DECISION_DEFAULT_ACCOUNT_EQUITY": "100000", "DECISION_LOT_SIZE": "100", "DECISION_TRADE_ADVISOR_AGENT_ENABLED": "0"}, clear=False):
            result = decision_service.run_strategy_selection_agent(sqlite3_module=sqlite3, db_path=db_path, limit=10)
            payload = decision_service.query_decision_today_actions(sqlite3_module=sqlite3, db_path=db_path, limit=10)

        self.assertTrue(result["ok"])
        self.assertGreaterEqual(result["summary"]["candidate_count"], 1)
        self.assertEqual(payload["summary"]["candidate_source"], "strategy_selection")
        self.assertEqual(payload["summary"]["strategy_selection_count"], 1)
        item = payload["items"][0]
        self.assertEqual(item["ts_code"], "000001.SZ")
        self.assertEqual(item["evidence"]["source"], "strategy_selection")
        self.assertIn("策略来源", item["execution_flow"]["decision_path"][2])

    def test_strategy_selection_agent_blocked_by_data_readiness(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="000001.SZ", name="强势策略股", total_score=91.0, trend_score=82.0, risk_score=75.0)
            self._set_data_readiness_run(
                conn,
                status="blocked",
                issues=[{"code": "risk_recent_missing", "severity": "blocked", "message": "风险情景缺失"}],
                impact="暂停策略选股",
            )
            conn.commit()
        finally:
            conn.close()

        result = decision_service.run_strategy_selection_agent(sqlite3_module=sqlite3, db_path=db_path, limit=10)

        self.assertFalse(result["ok"])
        self.assertEqual(result["summary"]["candidate_count"], 0)
        self.assertEqual(result["data_readiness_gate"]["status"], "blocked")

    def test_today_actions_kill_switch_blocks_plan_creation(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="000001.SZ", name="强势新买", total_score=91.0, trend_score=82.0, risk_score=75.0)
            conn.commit()
        finally:
            conn.close()

        decision_service.set_decision_kill_switch(sqlite3_module=sqlite3, db_path=db_path, allow_trading=False, reason="暂停交易")
        with patch.dict(os.environ, {"DECISION_TRADE_ADVISOR_AGENT_ENABLED": "0"}, clear=False):
            payload = decision_service.query_decision_today_actions(sqlite3_module=sqlite3, db_path=db_path, limit=10)

        self.assertFalse(payload["risk_gate"]["ok"])
        self.assertIn("Kill Switch", payload["risk_gate"]["blockers"][0])
        self.assertTrue(payload["items"])
        self.assertTrue(all(not item["can_create_order"] for item in payload["items"]))
        self.assertTrue(all(item["non_executable_reasons"] for item in payload["items"]))
        self.assertEqual(payload["items"][0]["action_type"], "watch")
        self.assertEqual(payload["items"][0]["next_step"], "observe_only")

    def test_today_actions_trade_advisor_agent_can_suggest_probe_buy(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="600519.SH", name="贵州茅台", total_score=76.8, trend_score=73.4, risk_score=98.7, industry="白酒")
            conn.execute("CREATE TABLE stock_daily_prices (ts_code TEXT, trade_date TEXT, close REAL)")
            conn.execute("INSERT INTO stock_daily_prices (ts_code, trade_date, close) VALUES (?, ?, ?)", ("600519.SH", "2026-04-08", 1500.0))
            conn.commit()
        finally:
            conn.close()

        agent_json = json.dumps(
            {
                "action": "probe_buy",
                "confidence": 72,
                "target_position_pct": 2,
                "summary": "总分未到强买阈值，但趋势和风险较好，可小仓位试买。",
                "buy_reason": "低风险核心资产，趋势尚可。",
                "sell_reason": "",
                "risk_note": "白酒板块若继续走弱则不买。",
                "invalid_if": "跌破20日均线。",
                "requires_user_confirm": True,
                "can_override_rule": True,
            },
            ensure_ascii=False,
        )
        with patch.dict(os.environ, {"DECISION_TRADE_ADVISOR_AGENT_ENABLED": "1", "DECISION_TRADE_ADVISOR_AGENT_SYNC": "0", "DECISION_TRADE_ADVISOR_AGENT_LIMIT": "1"}, clear=False):
            with patch.object(decision_service, "chat_completion_text", return_value=agent_json):
                refreshed = decision_service.refresh_trade_advisor_opinion(sqlite3_module=sqlite3, db_path=db_path, ts_code="600519.SH", limit=5)
                payload = decision_service.query_decision_today_actions(sqlite3_module=sqlite3, db_path=db_path, limit=5)

        item = payload["items"][0]
        self.assertEqual(refreshed["opinion"]["source"], "llm")
        self.assertEqual(item["action_type"], "watch")
        self.assertFalse(item["can_create_order"])
        self.assertEqual(item["agent_opinion"]["source"], "llm")
        self.assertEqual(item["agent_opinion"]["action"], "probe_buy")
        self.assertTrue(item["agent_opinion"]["requires_user_confirm"])
        self.assertTrue(item["agent_opinion"]["can_override_rule"])
        self.assertFalse(item["agent_can_create_order"])
        self.assertNotIn("agent_order_payload", item)
        self.assertEqual(payload["summary"]["agent_reviewed"], 1)

    def test_today_actions_trade_advisor_reads_cache_and_does_not_call_llm(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="600519.SH", name="贵州茅台", total_score=79.8, trend_score=73.4, risk_score=98.7, industry="白酒")
            conn.commit()
        finally:
            conn.close()

        with patch.dict(os.environ, {"DECISION_TRADE_ADVISOR_AGENT_ENABLED": "1", "DECISION_TRADE_ADVISOR_AGENT_SYNC": "0", "DECISION_TRADE_ADVISOR_AGENT_LIMIT": "1"}, clear=False):
            with patch.object(decision_service, "chat_completion_text", side_effect=AssertionError("llm should not block today-actions")):
                payload = decision_service.query_decision_today_actions(sqlite3_module=sqlite3, db_path=db_path, limit=5)

        item = payload["items"][0]
        self.assertEqual(item["agent_opinion"]["source"], "pending_review")
        self.assertEqual(item["agent_opinion"]["action"], "pending_review")
        self.assertEqual(payload["summary"]["agent_pending"], 1)

    def test_trade_advisor_refresh_api_queues_without_calling_llm(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="600519.SH", name="贵州茅台", total_score=79.8, trend_score=73.4, risk_score=98.7, industry="白酒")
            conn.commit()
        finally:
            conn.close()

        class _FakeThread:
            def __init__(self, *, target, name, daemon):
                self.target = target
                self.name = name
                self.daemon = daemon

            def start(self):
                return None

        with patch.object(decision_service.threading, "Thread", _FakeThread):
            with patch.object(decision_service, "chat_completion_text", side_effect=AssertionError("refresh API should return before LLM")):
                result = decision_service.queue_trade_advisor_opinion_refresh(sqlite3_module=sqlite3, db_path=db_path, ts_code="600519.SH", limit=5)
                payload = decision_service.query_decision_today_actions(sqlite3_module=sqlite3, db_path=db_path, limit=5)

        self.assertTrue(result["queued"])
        item = payload["items"][0]
        self.assertEqual(item["agent_opinion"]["source"], "refresh_running")
        self.assertEqual(item["agent_opinion"]["cache_status"], "running")

    def test_trade_advisor_daily_refresh_persists_cached_opinion(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="600519.SH", name="贵州茅台", total_score=79.8, trend_score=73.4, risk_score=98.7, industry="白酒")
            conn.commit()
        finally:
            conn.close()

        agent_json = json.dumps(
            {
                "action": "probe_buy",
                "confidence": 70,
                "target_position_pct": 2,
                "summary": "盘前批量评估：可小仓位试买。",
                "buy_reason": "趋势和风险较好。",
                "risk_note": "跌破均线不买。",
                "invalid_if": "趋势转弱。",
                "requires_user_confirm": True,
                "can_override_rule": True,
            },
            ensure_ascii=False,
        )
        with patch.dict(os.environ, {"DECISION_TRADE_ADVISOR_AGENT_ENABLED": "1", "DECISION_TRADE_ADVISOR_AGENT_SYNC": "0", "DECISION_TRADE_ADVISOR_AGENT_LIMIT": "2"}, clear=False):
            with patch.object(decision_service, "chat_completion_text", return_value=agent_json) as chat_mock:
                result = decision_service.refresh_trade_advisor_daily(sqlite3_module=sqlite3, db_path=db_path, limit=2)
                payload = decision_service.query_decision_today_actions(sqlite3_module=sqlite3, db_path=db_path, limit=5)

        self.assertEqual(result["evaluated_count"], 1)
        self.assertEqual(chat_mock.call_count, 1)
        item = payload["items"][0]
        self.assertEqual(item["agent_opinion"]["source"], "llm")
        self.assertEqual(item["agent_opinion"]["action"], "probe_buy")
        self.assertTrue(item["agent_opinion"]["cached_at"])

    def test_today_actions_rule_engine_has_probe_buy_and_avoid_tiers(self):
        db_path = self._mk_db()
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            _init_schema(conn)
            self._insert_score(conn, ts_code="600519.SH", name="贵州茅台", total_score=79.8, trend_score=73.4, risk_score=98.7, industry="白酒")
            self._insert_score(conn, ts_code="000004.SZ", name="弱信号", total_score=50.0, trend_score=40.0, risk_score=55.0, industry="测试")
            conn.execute("CREATE TABLE stock_daily_prices (ts_code TEXT, trade_date TEXT, close REAL)")
            conn.executemany(
                "INSERT INTO stock_daily_prices (ts_code, trade_date, close) VALUES (?, ?, ?)",
                [
                    ("600519.SH", "2026-04-08", 1500.0),
                    ("000004.SZ", "2026-04-08", 10.0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        with patch.dict(os.environ, {"DECISION_DEFAULT_ACCOUNT_EQUITY": "100000", "DECISION_LOT_SIZE": "100", "DECISION_TRADE_ADVISOR_AGENT_ENABLED": "0"}, clear=False):
            payload = decision_service.query_decision_today_actions(sqlite3_module=sqlite3, db_path=db_path, limit=10)

        by_code = {item["ts_code"]: item for item in payload["items"]}
        self.assertEqual(by_code["600519.SH"]["action_type"], "buy")
        self.assertEqual(by_code["600519.SH"]["rule_tier"], "probe_buy")
        self.assertFalse(by_code["600519.SH"]["can_create_order"])
        self.assertEqual(by_code["600519.SH"]["quantity"], 0)
        self.assertEqual(by_code["600519.SH"]["target_position_pct"], 2.0)
        self.assertIn("A股买入必须至少 1 手", "；".join(by_code["600519.SH"]["non_executable_reasons"]))
        self.assertEqual(by_code["000004.SZ"]["action_type"], "avoid")
        self.assertEqual(by_code["000004.SZ"]["rule_tier"], "avoid")
        self.assertFalse(by_code["000004.SZ"]["can_create_order"])

    def test_today_actions_api_contract(self):
        handler = _FakeHandler()
        handled = decision_routes.dispatch_get(
            handler,
            urlparse("/api/decision/today-actions?limit=3"),
            {
                "query_decision_today_actions": lambda limit=30: {
                    "generated_at": "2026-04-08T10:00:00Z",
                    "summary": {"total": 1, "executable": 1},
                    "risk_gate": {"ok": True},
                    "items": [
                        {
                            "id": "short:buy:000001.SZ",
                            "action_type": "buy",
                            "ts_code": "000001.SZ",
                            "quantity": 100,
                            "reason": {"summary": "测试买入原因"},
                        }
                    ],
                    "limit_seen": limit,
                }
            },
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 200)
        self.assertTrue(handler.payload["ok"])
        self.assertEqual(handler.payload["items"][0]["action_type"], "buy")
        self.assertEqual(handler.payload["limit_seen"], 3)

    def test_trade_advisor_refresh_api_contract(self):
        handler = _FakeHandler()
        handled = decision_routes.dispatch_post(
            handler,
            urlparse("/api/decision/trade-advisor/refresh"),
            {"ts_code": "600519.SH"},
            {
                "auth_context": {"authenticated": True},
                "queue_trade_advisor_opinion_refresh": lambda ts_code: {
                    "queued": True,
                    "submitted_at": "2026-04-08T08:45:00Z",
                    "snapshot_date": "2026-04-08",
                    "opinion": {"source": "refresh_running", "action": "refresh_running"},
                    "item": {"ts_code": ts_code},
                },
            },
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 200)
        self.assertTrue(handler.payload["ok"])
        self.assertTrue(handler.payload["queued"])
        self.assertEqual(handler.payload["item"]["ts_code"], "600519.SH")

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
        self.assertIn("pipeline_sync", result)
        self.assertIn(result["pipeline_sync"]["scores_stage"], {"empty", "ready"})
        self.assertIn(result["pipeline_sync"]["decision_stage"], {"ready"})
        self.assertIn(result["pipeline_sync"]["funnel_stage"], {"degraded", "ready"})


if __name__ == "__main__":
    unittest.main()
