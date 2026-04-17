#!/usr/bin/env python3
from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

import db_compat as sqlite3

from services.quantaalpha_service import service as qa_service


class QuantaAlphaServiceTest(unittest.TestCase):
    def _wait_task_terminal(self, *, db_path: str, task_id: str, timeout_seconds: float = 8.0, interval_seconds: float = 0.05):
        deadline = time.time() + timeout_seconds
        last = None
        while time.time() < deadline:
            qa_service.run_quantaalpha_worker_once(sqlite3_module=sqlite3, db_path=db_path)
            last = qa_service.get_quantaalpha_task(sqlite3_module=sqlite3, db_path=db_path, task_id=task_id)
            if last and str(last.get("status") or "").lower() in {"done", "error"}:
                return last
            time.sleep(interval_seconds)
        return last

    def test_tasks_converge_for_mine_backtest_and_auto_research(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "qa_test.db")
            original_choose_engine_route = qa_service._choose_engine_route
            original_run_business_engine = qa_service._run_business_engine
            original_run_research_engine = qa_service._run_research_engine
            try:
                qa_service._choose_engine_route = lambda payload, task_type: {
                    "engine_profile": "auto",
                    "mode": "legacy",
                    "primary": qa_service.BUSINESS_ENGINE,
                    "fallback": None,
                    "shadow": None,
                    "research_health": "ok",
                }

                def _fake_business(*, conn, task_type, payload, task_id):
                    metrics = {"ic": 0.12, "rank_ic": 0.09, "arr": 0.18, "mdd": 0.11, "calmar": 1.63}
                    artifacts = {
                        "engine": qa_service.ENGINE_NAME,
                        "engine_used": qa_service.BUSINESS_ENGINE,
                        "selection_reason": "unit_test",
                        "benchmark_delta": {"delta_arr": 0.01, "delta_calmar": 0.05, "delta_mdd": 0.0},
                        "candidate_factors": [{"factor_name": "f_alpha", "quality_score": 0.7}],
                        "selected_factors": [{"factor_name": "f_alpha", "quality_score": 0.7}],
                    }
                    if str(task_type or "") == "backtest":
                        artifacts["strategy_name"] = "unit_test_strategy"
                    return True, {
                        "stdout": f"[unit] task={task_id} type={task_type}",
                        "stderr": "",
                        "metrics": metrics,
                        "artifacts": artifacts,
                        "duration_seconds": 0.02,
                    }

                def _fake_research(*, conn, task_type, payload, task_id):
                    metrics = {"ic": 0.2, "rank_ic": 0.18, "arr": 0.24, "mdd": 0.1, "calmar": 2.4}
                    artifacts = {
                        "engine": qa_service.ENGINE_NAME,
                        "engine_used": qa_service.RESEARCH_ENGINE,
                        "selection_reason": "auto_research_topk_best",
                        "auto_research": bool(payload.get("auto_research")),
                        "benchmark_delta": {"delta_arr": 0.03, "delta_calmar": 0.12, "delta_mdd": 0.0},
                        "candidate_factors": [{"factor_name": "f_beta", "quality_score": 0.81}],
                        "selected_factors": [{"factor_name": "f_beta", "quality_score": 0.81}],
                    }
                    return True, {
                        "stdout": f"[unit] research task={task_id}",
                        "stderr": "",
                        "metrics": metrics,
                        "artifacts": artifacts,
                        "duration_seconds": 0.03,
                    }

                qa_service._run_business_engine = _fake_business
                qa_service._run_research_engine = _fake_research

                mine_task = qa_service.start_quantaalpha_mine_task(
                    sqlite3_module=sqlite3, db_path=db_path, direction="A股多因子挖掘", market_scope="A_share", lookback=60, config_profile="default", llm_profile="auto"
                )
                mine_id = str(mine_task.get("task_id") or "")
                self.assertTrue(mine_id)
                mine_current = self._wait_task_terminal(db_path=db_path, task_id=mine_id)
                self.assertIsNotNone(mine_current)
                self.assertEqual(str(mine_current.get("status")), "done")

                backtest_task = qa_service.start_quantaalpha_backtest_task(
                    sqlite3_module=sqlite3, db_path=db_path, direction="A股多因子回测", market_scope="A_share", lookback=80, config_profile="default", llm_profile="auto"
                )
                backtest_id = str(backtest_task.get("task_id") or "")
                self.assertTrue(backtest_id)
                backtest_current = self._wait_task_terminal(db_path=db_path, task_id=backtest_id)
                self.assertIsNotNone(backtest_current)
                self.assertEqual(str(backtest_current.get("status")), "done")

                qa_service._choose_engine_route = lambda payload, task_type: {
                    "engine_profile": "research",
                    "mode": "research",
                    "primary": qa_service.RESEARCH_ENGINE,
                    "fallback": None,
                    "shadow": None,
                    "research_health": "ok",
                }
                auto_task = qa_service.start_quantaalpha_auto_research_task(
                    sqlite3_module=sqlite3, db_path=db_path, direction="自动研究方向", market_scope="A_share", lookback=90, config_profile="default", llm_profile="auto"
                )
                auto_id = str(auto_task.get("task_id") or "")
                self.assertTrue(auto_id)
                auto_current = self._wait_task_terminal(db_path=db_path, task_id=auto_id)
                self.assertIsNotNone(auto_current)
                self.assertEqual(str(auto_current.get("status")), "done")

                mine_results = qa_service.query_quantaalpha_results(
                    sqlite3_module=sqlite3, db_path=db_path, task_type="mine", status="done", page=1, page_size=50
                )
                backtest_results = qa_service.query_quantaalpha_results(
                    sqlite3_module=sqlite3, db_path=db_path, task_type="backtest", status="done", page=1, page_size=50
                )
                self.assertGreaterEqual(int(mine_results.get("total") or 0), 2)
                self.assertGreaterEqual(int(backtest_results.get("total") or 0), 1)
            finally:
                qa_service._choose_engine_route = original_choose_engine_route
                qa_service._run_business_engine = original_run_business_engine
                qa_service._run_research_engine = original_run_research_engine

    def test_failed_scenarios_have_readable_error_codes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "qa_test.db")
            original_choose_engine_route = qa_service._choose_engine_route
            original_run_business_engine = qa_service._run_business_engine
            try:
                qa_service._choose_engine_route = lambda payload, task_type: {
                    "engine_profile": "business",
                    "mode": "legacy",
                    "primary": qa_service.BUSINESS_ENGINE,
                    "fallback": None,
                    "shadow": None,
                    "research_health": "ok",
                }

                def _fake_business_fail(*, conn, task_type, payload, task_id):
                    direction = str((payload or {}).get("direction") or "").strip()
                    mapping = {
                        "INVALID_PARAM": (qa_service.ERR_RUNNER_CONFIG_INVALID, "direction 参数非法"),
                        "DATA_SHORT": (qa_service.ERR_DATA_NOT_READY, "样本不足"),
                        "EXEC_FAIL": (qa_service.ERR_EXECUTION_EXCEPTION, "执行异常"),
                    }
                    code, message = mapping.get(direction, (qa_service.ERR_EXECUTION_EXCEPTION, "unknown"))
                    return False, {"error_code": code, "error_message": message, "stdout": "", "stderr": message}

                qa_service._run_business_engine = _fake_business_fail

                for direction, expected_code in [
                    ("INVALID_PARAM", qa_service.ERR_RUNNER_CONFIG_INVALID),
                    ("DATA_SHORT", qa_service.ERR_DATA_NOT_READY),
                    ("EXEC_FAIL", qa_service.ERR_EXECUTION_EXCEPTION),
                ]:
                    task = qa_service.start_quantaalpha_mine_task(
                        sqlite3_module=sqlite3, db_path=db_path, direction=direction, market_scope="A_share", lookback=30, config_profile="default", llm_profile="auto"
                    )
                    task_id = str(task.get("task_id") or "")
                    self.assertTrue(task_id)
                    current = self._wait_task_terminal(db_path=db_path, task_id=task_id)
                    self.assertIsNotNone(current)
                    self.assertEqual(str(current.get("status")), "error")
                    self.assertEqual(str(current.get("error_code")), expected_code)
                    self.assertTrue(str(current.get("error_message") or "").strip())
            finally:
                qa_service._choose_engine_route = original_choose_engine_route
                qa_service._run_business_engine = original_run_business_engine


if __name__ == "__main__":
    unittest.main()
