#!/usr/bin/env python3
from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

import db_compat as sqlite3

from services.quantaalpha_service import service as qa_service


class QuantaAlphaServiceTest(unittest.TestCase):
    def test_start_mine_task_and_query_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "qa_test.db")
            original_run_cli = qa_service._run_cli
            try:
                qa_service._run_cli = lambda task_type, payload: (
                    True,
                    {
                        "stdout": "IC: 0.12 RankIC: 0.08",
                        "stderr": "",
                        "metrics": {"ic": 0.12, "rank_ic": 0.08},
                        "artifacts": {"results_dir": str(Path(tmpdir) / "results")},
                        "duration_seconds": 0.2,
                    },
                )
                task = qa_service.start_quantaalpha_mine_task(
                    sqlite3_module=sqlite3,
                    db_path=db_path,
                    direction="A股多因子挖掘",
                    market_scope="A_share",
                    lookback=60,
                    config_profile="default",
                    llm_profile="auto",
                )
                task_id = str(task.get("task_id") or "")
                self.assertTrue(task_id)

                deadline = time.time() + 5
                current = None
                while time.time() < deadline:
                    current = qa_service.get_quantaalpha_task(sqlite3_module=sqlite3, db_path=db_path, task_id=task_id)
                    if current and str(current.get("status")) in {"done", "error"}:
                        break
                    time.sleep(0.05)

                self.assertIsNotNone(current)
                self.assertEqual(current.get("status"), "done")

                results = qa_service.query_quantaalpha_results(
                    sqlite3_module=sqlite3,
                    db_path=db_path,
                    task_type="mine",
                    status="done",
                    page=1,
                    page_size=20,
                )
                self.assertGreaterEqual(int(results.get("total") or 0), 1)
            finally:
                qa_service._run_cli = original_run_cli

    def test_error_mapping_when_cli_timeout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "qa_test.db")
            original_run_cli = qa_service._run_cli
            try:
                qa_service._run_cli = lambda task_type, payload: (
                    False,
                    {
                        "error_code": qa_service.ERR_PROCESS_TIMEOUT,
                        "error_message": "timeout",
                        "stdout": "",
                        "stderr": "timeout",
                    },
                )
                task = qa_service.start_quantaalpha_health_check_task(sqlite3_module=sqlite3, db_path=db_path)
                task_id = str(task.get("task_id") or "")
                self.assertTrue(task_id)
                deadline = time.time() + 5
                current = None
                while time.time() < deadline:
                    current = qa_service.get_quantaalpha_task(sqlite3_module=sqlite3, db_path=db_path, task_id=task_id)
                    if current and str(current.get("status")) in {"done", "error"}:
                        break
                    time.sleep(0.05)
                self.assertIsNotNone(current)
                self.assertEqual(current.get("status"), "error")
                self.assertEqual(current.get("error_code"), qa_service.ERR_PROCESS_TIMEOUT)
            finally:
                qa_service._run_cli = original_run_cli


if __name__ == "__main__":
    unittest.main()
