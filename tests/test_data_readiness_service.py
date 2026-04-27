from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.data_readiness_service import query_latest_data_readiness_report, run_data_readiness_agent
from jobs import run_data_readiness_job as data_readiness_cli

ROOT_DIR = Path(__file__).resolve().parents[1]


def _init_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE stock_codes (ts_code TEXT PRIMARY KEY, name TEXT, list_status TEXT);
            CREATE TABLE stock_daily_prices (ts_code TEXT, trade_date TEXT, close REAL);
            CREATE TABLE stock_scores_daily (ts_code TEXT, score_date TEXT, total_score REAL, trend_score REAL, risk_score REAL);
            CREATE TABLE stock_valuation_daily (ts_code TEXT, trade_date TEXT);
            CREATE TABLE capital_flow_stock (ts_code TEXT, trade_date TEXT);
            CREATE TABLE risk_scenarios (ts_code TEXT, scenario_date TEXT);
            CREATE TABLE stock_financials (ts_code TEXT, report_period TEXT);
            CREATE TABLE stock_daily_price_rollups (ts_code TEXT, window_days INTEGER);
            """
        )
        conn.executemany(
            "INSERT INTO stock_codes (ts_code, name, list_status) VALUES (?, ?, 'L')",
            [("000001.SZ", "平安银行"), ("600000.SH", "浦发银行")],
        )
        conn.commit()
    finally:
        conn.close()


def _fill_ready_data(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        for code in ("000001.SZ", "600000.SH"):
            conn.execute("INSERT INTO stock_daily_prices VALUES (?, '20260424', 10.0)", (code,))
            conn.execute("INSERT INTO stock_scores_daily VALUES (?, '20260424', 70, 60, 80)", (code,))
            conn.execute("INSERT INTO stock_valuation_daily VALUES (?, '20260424')", (code,))
            conn.execute("INSERT INTO capital_flow_stock VALUES (?, '20260424')", (code,))
            conn.execute("INSERT INTO risk_scenarios VALUES (?, '20260424')", (code,))
            conn.execute("INSERT INTO stock_financials VALUES (?, '20251231')", (code,))
            conn.execute("INSERT INTO stock_daily_price_rollups VALUES (?, 20)", (code,))
            conn.execute("INSERT INTO stock_daily_price_rollups VALUES (?, 30)", (code,))
        conn.commit()
    finally:
        conn.close()


class DataReadinessServiceTest(unittest.TestCase):
    def test_ready_snapshot_persists_report_without_actions(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "test.db")
            _init_schema(db_path)
            _fill_ready_data(db_path)

            result = run_data_readiness_agent(sqlite3_module=sqlite3, db_path=db_path, auto_fix=True, dry_run=False, ai_enabled=False)

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "ready")
            self.assertEqual(result["actions"], [])
            self.assertEqual(result["summary"]["ai_diagnosis"]["source"], "disabled")
            latest = query_latest_data_readiness_report(sqlite3_module=sqlite3, db_path=db_path)
            self.assertEqual(latest["latest_run"]["status"], "ready")

    def test_missing_scores_and_risk_plan_backfill_in_dry_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "test.db")
            _init_schema(db_path)
            for code in ("000001.SZ", "600000.SH"):
                conn = sqlite3.connect(db_path)
                try:
                    conn.execute("INSERT INTO stock_daily_prices VALUES (?, '20260424', 10.0)", (code,))
                    conn.commit()
                finally:
                    conn.close()

            result = run_data_readiness_agent(sqlite3_module=sqlite3, db_path=db_path, auto_fix=False, dry_run=True, ai_enabled=False)

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "blocked")
            action_keys = {item["action_key"] for item in result["actions"]}
            self.assertIn("refresh_risk_scenarios", action_keys)
            self.assertIn("build_price_rollups", action_keys)
            self.assertIn("refresh_scores", action_keys)
            self.assertTrue(all(item["status"] == "planned" for item in result["actions"]))

    def test_auto_fix_executes_planned_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "test.db")
            _init_schema(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("INSERT INTO stock_daily_prices VALUES ('000001.SZ', '20260424', 10.0)")
                conn.execute("INSERT INTO stock_daily_prices VALUES ('600000.SH', '20260424', 10.0)")
                conn.commit()
            finally:
                conn.close()

            completed = subprocess.CompletedProcess(args=["python3"], returncode=0, stdout="ok", stderr="")
            with mock.patch("services.data_readiness_service.service.subprocess.run", return_value=completed) as run_mock:
                result = run_data_readiness_agent(sqlite3_module=sqlite3, db_path=db_path, auto_fix=True, dry_run=False, ai_enabled=False)

            self.assertGreater(run_mock.call_count, 0)
            self.assertTrue(all(item["status"] == "done" for item in result["actions"]))

    def test_ai_diagnosis_success_is_persisted_in_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "test.db")
            _init_schema(db_path)
            _fill_ready_data(db_path)
            ai_payload = {
                "root_cause_summary": "数据齐全",
                "business_impact": "可以运行策略选股",
                "degrade_strategy": "无需降级",
                "repair_priority": "low",
                "next_actions": ["继续监控"],
                "manual_check_required": False,
                "can_run_strategy_agent": True,
            }
            with mock.patch("services.data_readiness_service.service.chat_completion_text", return_value=json.dumps(ai_payload, ensure_ascii=False)):
                result = run_data_readiness_agent(sqlite3_module=sqlite3, db_path=db_path, auto_fix=True, dry_run=False, ai_enabled=True)

            diagnosis = result["summary"]["ai_diagnosis"]
            self.assertEqual(diagnosis["source"], "llm")
            self.assertEqual(diagnosis["root_cause_summary"], "数据齐全")
            self.assertTrue(diagnosis["can_run_strategy_agent"])

    def test_ai_diagnosis_failure_falls_back_without_changing_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "test.db")
            _init_schema(db_path)
            _fill_ready_data(db_path)
            with mock.patch("services.data_readiness_service.service.chat_completion_text", side_effect=RuntimeError("llm timeout")):
                result = run_data_readiness_agent(sqlite3_module=sqlite3, db_path=db_path, auto_fix=True, dry_run=False, ai_enabled=True)

            self.assertEqual(result["status"], "ready")
            diagnosis = result["summary"]["ai_diagnosis"]
            self.assertEqual(diagnosis["source"], "heuristic_fallback")
            self.assertIn("llm timeout", diagnosis["error"])

    def test_ai_disabled_does_not_call_llm(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "test.db")
            _init_schema(db_path)
            _fill_ready_data(db_path)
            with mock.patch("services.data_readiness_service.service.chat_completion_text") as llm_mock:
                result = run_data_readiness_agent(sqlite3_module=sqlite3, db_path=db_path, auto_fix=False, dry_run=True, ai_enabled=False)

            llm_mock.assert_not_called()
            self.assertEqual(result["summary"]["ai_diagnosis"]["source"], "disabled")

    def test_ai_path_selection_can_reorder_allowed_actions(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "test.db")
            _init_schema(db_path)
            conn = sqlite3.connect(db_path)
            try:
                for code in ("000001.SZ", "600000.SH"):
                    conn.execute("INSERT INTO stock_daily_prices VALUES (?, '20260424', 10.0)", (code,))
                conn.commit()
            finally:
                conn.close()

            selector_payload = {
                "selected_action_keys": ["build_price_rollups", "refresh_risk_scenarios", "refresh_scores"],
                "rationale": "先补衍生汇总，再补风险，最后重算评分",
                "skipped_action_keys": [],
            }
            diagnosis_payload = {
                "root_cause_summary": "需要补 risk、rollup 和 score",
                "business_impact": "暂不运行策略",
                "degrade_strategy": "阻断新增选股",
                "repair_priority": "high",
                "next_actions": ["按 AI 选路补数"],
                "manual_check_required": True,
                "can_run_strategy_agent": False,
            }
            with mock.patch(
                "services.data_readiness_service.service.chat_completion_text",
                side_effect=[json.dumps(selector_payload, ensure_ascii=False), json.dumps(diagnosis_payload, ensure_ascii=False)],
            ):
                result = run_data_readiness_agent(sqlite3_module=sqlite3, db_path=db_path, auto_fix=False, dry_run=True, ai_enabled=True)

            action_keys = [item["action_key"] for item in result["actions"]]
            self.assertEqual(action_keys, ["build_price_rollups", "refresh_risk_scenarios", "refresh_scores"])
            self.assertTrue(all(item["selected_by"] == "llm" for item in result["actions"]))
            self.assertEqual(result["summary"]["action_selection"]["source"], "llm")

    def test_ai_path_selection_rejects_invalid_keys_and_falls_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "test.db")
            _init_schema(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("INSERT INTO stock_daily_prices VALUES ('000001.SZ', '20260424', 10.0)")
                conn.execute("INSERT INTO stock_daily_prices VALUES ('600000.SH', '20260424', 10.0)")
                conn.commit()
            finally:
                conn.close()

            bad_selector_payload = {"selected_action_keys": ["rm_rf_everything"], "rationale": "bad"}
            with mock.patch("services.data_readiness_service.service.chat_completion_text", side_effect=[json.dumps(bad_selector_payload), RuntimeError("diagnosis skipped")]):
                result = run_data_readiness_agent(sqlite3_module=sqlite3, db_path=db_path, auto_fix=False, dry_run=True, ai_enabled=True)

            self.assertEqual(result["summary"]["action_selection"]["source"], "rule_fallback")
            self.assertIn("refresh_scores", [item["action_key"] for item in result["actions"]])

    def test_path_selection_can_be_disabled_without_disabling_diagnosis(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "test.db")
            _init_schema(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("INSERT INTO stock_daily_prices VALUES ('000001.SZ', '20260424', 10.0)")
                conn.execute("INSERT INTO stock_daily_prices VALUES ('600000.SH', '20260424', 10.0)")
                conn.commit()
            finally:
                conn.close()

            diagnosis_payload = {
                "root_cause_summary": "路径选择关闭，仅诊断",
                "business_impact": "阻断",
                "degrade_strategy": "不运行策略",
                "repair_priority": "high",
                "next_actions": ["按规则补数"],
                "manual_check_required": True,
                "can_run_strategy_agent": False,
            }
            with mock.patch("services.data_readiness_service.service.chat_completion_text", return_value=json.dumps(diagnosis_payload, ensure_ascii=False)) as llm_mock:
                result = run_data_readiness_agent(
                    sqlite3_module=sqlite3,
                    db_path=db_path,
                    auto_fix=False,
                    dry_run=True,
                    ai_enabled=True,
                    path_selection_enabled=False,
                )

            self.assertEqual(llm_mock.call_count, 1)
            self.assertEqual(result["summary"]["action_selection"]["source"], "disabled")
            self.assertEqual(result["summary"]["ai_diagnosis"]["source"], "llm")


class DataReadinessJobsTest(unittest.TestCase):
    def test_run_data_readiness_job_describe(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT_DIR / "jobs" / "run_data_readiness_job.py"),
                "--job-key",
                "data_readiness_daily",
                "--describe",
            ],
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("data_readiness_daily", proc.stdout)

    def test_cli_no_ai_passes_flag_to_runner(self):
        with mock.patch.object(data_readiness_cli, "run_data_readiness_job", return_value={"ok": True, "status": "ready"}) as runner:
            rc = data_readiness_cli.main(["--job-key", "data_readiness_daily", "--dry-run", "--no-ai"])

        self.assertEqual(rc, 0)
        runner.assert_called_once_with("data_readiness_daily", dry_run=True, ai_enabled=False, path_selection_enabled=False)

    def test_cli_can_disable_only_ai_path_selection(self):
        with mock.patch.object(data_readiness_cli, "run_data_readiness_job", return_value={"ok": True, "status": "ready"}) as runner:
            rc = data_readiness_cli.main(["--job-key", "data_readiness_daily", "--dry-run", "--no-ai-path-selection"])

        self.assertEqual(rc, 0)
        runner.assert_called_once_with("data_readiness_daily", dry_run=True, ai_enabled=True, path_selection_enabled=False)


if __name__ == "__main__":
    unittest.main()
