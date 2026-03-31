#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time
import unittest
import urllib.error
import urllib.request


class MinimalRegressionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = int(os.getenv("TEST_BACKEND_PORT", "18002"))
        cls.base_url = f"http://127.0.0.1:{cls.port}"
        cls.admin_token = "test-admin-token"
        db_path = "/home/zanbo/zanbotest/stock_codes.db"
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_codes (
                    ts_code TEXT PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    area TEXT,
                    industry TEXT,
                    market TEXT,
                    list_date TEXT,
                    delist_date TEXT,
                    list_status TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO stock_codes (
                    ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("000001.SZ", "000001", "平安银行", "深圳", "银行", "主板", "19910403", "", "L"),
            )
            conn.commit()
        finally:
            conn.close()
        env = os.environ.copy()
        env.setdefault("USE_POSTGRES", "0")
        env.setdefault("BACKEND_ADMIN_TOKEN", cls.admin_token)
        env["PORT"] = str(cls.port)
        cls.proc = subprocess.Popen(
            [
                "python3",
                "-c",
                (
                    "import backend.server as s; "
                    f"server=s.ThreadingHTTPServer(('127.0.0.1',{cls.port}), s.ApiHandler); "
                    "server.serve_forever()"
                ),
            ],
            cwd="/home/zanbo/zanbotest",
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(f"{cls.base_url}/api/health", timeout=2) as resp:
                    if resp.status == 200:
                        return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError("backend did not start in time")

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "proc", None):
            cls.proc.terminate()
            try:
                cls.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                cls.proc.kill()

    def _get_json(self, path: str, headers: dict[str, str] | None = None) -> tuple[int, dict]:
        req = urllib.request.Request(f"{self.base_url}{path}", headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            payload = exc.read().decode("utf-8", errors="ignore")
            try:
                body = json.loads(payload)
            except Exception:
                body = {"error": payload}
            return exc.code, body

    def test_health(self):
        status, body = self._get_json("/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(body.get("ok"))

    def test_core_query_stocks(self):
        status, body = self._get_json("/api/stocks?page=1&page_size=1")
        self.assertEqual(status, 200, body)
        self.assertIn("items", body)

    def test_job_dry_run(self):
        status, body = self._get_json(
            "/api/jobs/dry-run?job_key=db_health_check_refresh",
            headers={"X-Admin-Token": self.admin_token},
        )
        self.assertEqual(status, 200, body)
        self.assertTrue(body.get("ok"))
        self.assertTrue(body.get("commands"))


if __name__ == "__main__":
    unittest.main()
