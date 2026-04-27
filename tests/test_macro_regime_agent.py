from __future__ import annotations

import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.macro_service import regime

ROOT_DIR = Path(__file__).resolve().parents[1]


def _connect(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table_name,)).fetchone()
    return row is not None


class MacroRegimeAgentTest(unittest.TestCase):
    def test_suggest_regime_v2_uses_multi_signal_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "macro.db")
            conn = _connect(db_path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE theme_hotspot_tracker (
                        direction TEXT,
                        latest_evidence_time TEXT
                    );
                    CREATE TABLE stock_daily_prices (
                        ts_code TEXT,
                        trade_date TEXT,
                        close REAL,
                        pct_chg REAL,
                        amount REAL
                    );
                    CREATE TABLE capital_flow_stock (
                        ts_code TEXT,
                        trade_date TEXT,
                        net_inflow REAL
                    );
                    CREATE TABLE stock_scores_daily (
                        score_date TEXT,
                        ts_code TEXT,
                        industry TEXT,
                        total_score REAL,
                        trend_score REAL,
                        valuation_score REAL,
                        financial_score REAL
                    );
                    CREATE TABLE macro_series (
                        indicator_code TEXT,
                        indicator_name TEXT,
                        value REAL
                    );
                    """
                )
                for _ in range(8):
                    conn.execute("INSERT INTO theme_hotspot_tracker VALUES ('bullish', '2099-01-01')")
                for _ in range(2):
                    conn.execute("INSERT INTO theme_hotspot_tracker VALUES ('bearish', '2099-01-01')")
                for idx in range(10):
                    conn.execute(
                        "INSERT INTO stock_daily_prices VALUES (?, '20260424', ?, ?, ?)",
                        (f"0000{idx}.SZ", 10 + idx, 1.5 if idx < 8 else -0.8, 100000 + idx),
                    )
                    conn.execute(
                        "INSERT INTO capital_flow_stock VALUES (?, '20260424', ?)",
                        (f"0000{idx}.SZ", 1000000 if idx < 8 else -500000),
                    )
                    conn.execute(
                        "INSERT INTO stock_scores_daily VALUES ('20260424', ?, ?, 76, 72, 70, 75)",
                        (f"0000{idx}.SZ", "行业A" if idx < 5 else "行业B"),
                    )
                conn.execute("INSERT INTO macro_series VALUES ('macro_shipping_bdi.近6月涨跌幅', 'macro_shipping_bdi-近6月涨跌幅', 20)")
                conn.execute("INSERT INTO macro_series VALUES ('macro_rate.利率', '利率', -5)")
                conn.commit()
            finally:
                conn.close()

            with mock.patch.object(regime._db, "connect", side_effect=lambda: _connect(db_path)):
                with mock.patch.object(regime._db, "apply_row_factory", side_effect=lambda conn: setattr(conn, "row_factory", sqlite3.Row)):
                    with mock.patch.object(regime._db, "table_exists", side_effect=_table_exists):
                        result = regime.suggest_regime()

            self.assertTrue(result["ok"], result)
            suggestion = result["suggestion"]
            self.assertEqual(suggestion["short_term_state"], "expansion")
            self.assertIn(suggestion["medium_term_state"], {"recovery", "expansion"})
            self.assertIn(suggestion["long_term_state"], {"recovery", "expansion"})
            self.assertIn("signal_groups", suggestion)
            self.assertIn("market_breadth", [s["name"] for s in suggestion["signal_groups"]["short_term"]["signals"]])

    def test_agent_generates_draft_and_confirm_writes_regime(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "macro.db")
            with mock.patch.object(regime._db, "connect", side_effect=lambda: _connect(db_path)):
                with mock.patch.object(regime._db, "apply_row_factory", side_effect=lambda conn: setattr(conn, "row_factory", sqlite3.Row)):
                    with mock.patch.object(regime._db, "table_exists", side_effect=_table_exists):
                        with mock.patch.object(regime, "_ensure_outcome_columns", side_effect=lambda conn: None):
                            created = regime.run_macro_regime_agent()
                            self.assertTrue(created["ok"], created)
                            draft = created["draft"]
                            self.assertEqual(draft["status"], "draft")
                            self.assertEqual(draft["source"], "macro_regime_agent")
                            self.assertIn(draft["short_term_state"], regime.VALID_STATES)
                            self.assertFalse(draft["confirmed_regime_id"])

                            latest = regime.get_latest_macro_regime_draft()
                            self.assertEqual(latest["draft"]["id"], draft["id"])

                            confirmed = regime.confirm_macro_regime_draft(draft["id"], confirmed_by="tester")
                            self.assertTrue(confirmed["ok"], confirmed)
                            self.assertTrue(confirmed["regime_id"])

                            latest_after = regime.get_latest_macro_regime_draft()
                            self.assertEqual(latest_after["draft"]["status"], "confirmed")
                            self.assertEqual(latest_after["draft"]["confirmed_regime_id"], confirmed["regime_id"])

                            current = regime.get_latest_regime()
                            self.assertEqual(current["status"], "ready")
                            self.assertEqual(current["regime"]["id"], confirmed["regime_id"])

    def test_agent_suppresses_low_confidence_long_cycle_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "macro.db")
            with mock.patch.object(regime._db, "connect", side_effect=lambda: _connect(db_path)):
                with mock.patch.object(regime._db, "apply_row_factory", side_effect=lambda conn: setattr(conn, "row_factory", sqlite3.Row)):
                    with mock.patch.object(regime._db, "table_exists", side_effect=_table_exists):
                        with mock.patch.object(regime, "_ensure_outcome_columns", side_effect=lambda conn: None):
                            regime.record_regime(
                                short_term_state="volatile",
                                medium_term_state="volatile",
                                long_term_state="expansion",
                                created_by="seed",
                            )
                            suggestion = {
                                "ok": True,
                                "suggestion": {
                                    "short_term_state": "risk_rising",
                                    "short_term_confidence": 0.6,
                                    "medium_term_state": "slowdown",
                                    "medium_term_confidence": 0.5,
                                    "long_term_state": "contraction",
                                    "long_term_confidence": 0.6,
                                    "basis": "盘后证据不足",
                                    "data_points": 2,
                                },
                            }
                            with mock.patch.object(regime, "suggest_regime", return_value=suggestion):
                                created = regime.run_macro_regime_agent()

                            draft = created["draft"]
                            self.assertTrue(draft["short_term_changed"])
                            self.assertFalse(draft["medium_term_changed"])
                            self.assertFalse(draft["long_term_changed"])
                            self.assertTrue(draft["evidence"]["needs_human_confirmation"])
                            self.assertIn("长周期变化需要更高置信度", "；".join(draft["evidence"]["suppressed_changes"]))
                            self.assertEqual(draft["evidence"]["evaluation_timing"], "post_close")

    def test_agent_job_describe(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT_DIR / "jobs" / "run_macro_regime_job.py"),
                "--job-key",
                "macro_regime_agent_daily",
                "--describe",
            ],
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("macro_regime_agent_daily", proc.stdout)


if __name__ == "__main__":
    unittest.main()
