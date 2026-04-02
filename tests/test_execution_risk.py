#!/usr/bin/env python3
from __future__ import annotations

import unittest

from services.execution import OrderRecord, PaperAccount, TradeFill, pre_trade_check


class ExecutionRiskTest(unittest.TestCase):
    def test_pre_trade_check_blocks_when_risk_exceeds_limits(self):
        result = pre_trade_check(
            {
                "target_position_weight": 0.35,
                "max_drawdown": 0.08,
                "volatility": 0.52,
                "liquidity_score": 0.1,
            }
        )
        self.assertFalse(result["allowed"])
        self.assertGreaterEqual(len(result["reasons"]), 2)

    def test_paper_account_records_order_fill_and_snapshot(self):
        account = PaperAccount(account_id="paper-1", initial_cash=100000)
        account.record_order(
            OrderRecord(
                order_id="o1",
                symbol="600519.SH",
                side="buy",
                quantity=10,
                price=1500,
                status="filled",
            )
        )
        account.record_fill(
            TradeFill(
                trade_id="t1",
                order_id="o1",
                symbol="600519.SH",
                side="buy",
                quantity=10,
                price=1500,
            )
        )
        snapshot = account.snapshot({"600519.SH": 1520})
        self.assertEqual(snapshot.account_id, "paper-1")
        self.assertEqual(len(snapshot.positions), 1)
        self.assertAlmostEqual(snapshot.positions[0].market_value, 15200)
        self.assertLess(snapshot.cash, 100000)


if __name__ == "__main__":
    unittest.main()
