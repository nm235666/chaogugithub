#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlparse

# Force sqlite mode before loading service/db modules.
os.environ["USE_POSTGRES"] = "0"

from backend.routes import portfolio as portfolio_routes
from services.portfolio_service import service as portfolio_service


class _FakeHandler:
    def __init__(self) -> None:
        self.status = 200
        self.payload: dict = {}

    def _send_json(self, payload: dict, status: int = 200) -> None:
        self.status = status
        self.payload = payload


class PortfolioOrdersPatchTest(unittest.TestCase):
    def setUp(self) -> None:
        fd, db_path = tempfile.mkstemp(prefix="portfolio-orders-patch-", suffix=".db")
        os.close(fd)
        self.db_path = Path(db_path)

        self._orig_connect = portfolio_service._db.connect
        self._orig_using_postgres = portfolio_service._db.using_postgres

        portfolio_service._db.connect = lambda: sqlite3.connect(self.db_path, isolation_level=None)  # type: ignore[assignment]
        portfolio_service._db.using_postgres = lambda: False  # type: ignore[assignment]

        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            portfolio_service._ensure_portfolio_tables(conn)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS decision_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action_type TEXT NOT NULL,
                    ts_code TEXT NOT NULL DEFAULT '',
                    stock_name TEXT NOT NULL DEFAULT '',
                    note TEXT NOT NULL DEFAULT '',
                    actor TEXT NOT NULL DEFAULT '',
                    snapshot_date TEXT NOT NULL DEFAULT '',
                    action_payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                )
                """
            )

    def tearDown(self) -> None:
        portfolio_service._db.connect = self._orig_connect  # type: ignore[assignment]
        portfolio_service._db.using_postgres = self._orig_using_postgres  # type: ignore[assignment]
        if self.db_path.exists():
            self.db_path.unlink()

    def _insert_order(
        self,
        order_id: str,
        status: str = "planned",
        *,
        action_type: str = "buy",
        size: int = 100,
        ts_code: str = "600519.SH",
        planned_price: float = 10.0,
    ) -> None:
        now = "2026-04-20T00:00:00Z"
        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            conn.execute(
                """
                INSERT INTO portfolio_orders
                  (id, ts_code, action_type, planned_price, executed_price, size, status,
                   decision_action_id, note, executed_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    ts_code,
                    action_type,
                    planned_price,
                    None,
                    size,
                    status,
                    "",
                    "",
                    None,
                    now,
                    now,
                ),
            )

    def _fetch_position(self, ts_code: str = "600519.SH") -> tuple[int, float] | None:
        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            row = conn.execute(
                "SELECT quantity, avg_cost FROM portfolio_positions WHERE ts_code = ?",
                (ts_code,),
            ).fetchone()
        if row is None:
            return None
        return int(row[0]), float(row[1])

    def _review_count(self, order_id: str) -> int:
        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM portfolio_reviews WHERE order_id = ? AND review_tag = 'pending'",
                (order_id,),
            ).fetchone()
        return int(row[0] or 0)

    def _fetch_order(self, order_id: str) -> tuple[str, float | None, str | None]:
        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            row = conn.execute(
                "SELECT status, executed_price, executed_at FROM portfolio_orders WHERE id = ?",
                (order_id,),
            ).fetchone()
        self.assertIsNotNone(row)
        return row[0], row[1], row[2]

    def _insert_decision_action(self, *, ts_code: str = "600519.SH", note: str = "人工确认") -> int:
        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            cur = conn.execute(
                """
                INSERT INTO decision_actions
                    (action_type, ts_code, stock_name, note, actor, snapshot_date, action_payload_json, created_at)
                VALUES ('confirm', ?, '测试股票', ?, 'tester', '20260420', '{}', '2026-04-20T00:00:00Z')
                """,
                (ts_code, note),
            )
            return int(cur.lastrowid)

    def test_planned_to_executed_updates_fields(self):
        order_id = "order-executed"
        self._insert_order(order_id, status="planned")
        handler = _FakeHandler()

        handled = portfolio_routes.dispatch_patch(
            handler,
            urlparse(f"/api/portfolio/orders/{order_id}"),
            {"status": "executed", "executed_price": 10.5, "executed_at": "2026-04-20T12:30:00Z"},
            {},
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 200)
        self.assertTrue(handler.payload.get("ok"))

        status, executed_price, executed_at = self._fetch_order(order_id)
        self.assertEqual(status, "executed")
        self.assertEqual(executed_price, 10.5)
        self.assertEqual(executed_at, "2026-04-20T12:30:00Z")
        self.assertEqual(self._fetch_position(), (100, 10.5))
        self.assertEqual(self._review_count(order_id), 1)

    def test_planned_to_cancelled_updates_status(self):
        order_id = "order-cancelled"
        self._insert_order(order_id, status="planned")
        handler = _FakeHandler()

        handled = portfolio_routes.dispatch_patch(
            handler,
            urlparse(f"/api/portfolio/orders/{order_id}"),
            {"status": "cancelled"},
            {},
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 200)
        self.assertTrue(handler.payload.get("ok"))

        status, executed_price, executed_at = self._fetch_order(order_id)
        self.assertEqual(status, "cancelled")
        self.assertIsNone(executed_price)
        self.assertIsNone(executed_at)

    def test_invalid_status_returns_400_with_valid_list(self):
        order_id = "order-invalid-status"
        self._insert_order(order_id, status="planned")
        handler = _FakeHandler()

        handled = portfolio_routes.dispatch_patch(
            handler,
            urlparse(f"/api/portfolio/orders/{order_id}"),
            {"status": "done"},
            {},
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 400)
        self.assertFalse(handler.payload.get("ok"))
        self.assertIn("无效订单状态", str(handler.payload.get("error") or ""))
        self.assertEqual(handler.payload.get("valid"), sorted(portfolio_routes.VALID_ORDER_STATUSES))

    def test_invalid_executed_price_returns_400(self):
        order_id = "order-invalid-price"
        self._insert_order(order_id, status="planned")
        handler = _FakeHandler()

        handled = portfolio_routes.dispatch_patch(
            handler,
            urlparse(f"/api/portfolio/orders/{order_id}"),
            {"status": "executed", "executed_price": "abc"},
            {},
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 400)
        self.assertFalse(handler.payload.get("ok"))
        self.assertIn("executed_price 必须是数字", str(handler.payload.get("error") or ""))

    def test_sell_more_than_position_is_rejected(self):
        order_id = "order-sell-too-much"
        self._insert_order(order_id, action_type="sell", size=10)
        handler = _FakeHandler()

        handled = portfolio_routes.dispatch_patch(
            handler,
            urlparse(f"/api/portfolio/orders/{order_id}"),
            {"status": "executed", "executed_price": 11.0},
            {},
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 404)
        self.assertFalse(handler.payload.get("ok"))
        self.assertIn("持仓不足", str(handler.payload.get("error") or ""))
        status, _, _ = self._fetch_order(order_id)
        self.assertEqual(status, "planned")

    def test_close_clears_active_position(self):
        buy_id = "order-buy-before-close"
        close_id = "order-close"
        self._insert_order(buy_id, action_type="buy", size=100, planned_price=10.0)
        self.assertTrue(
            portfolio_service.update_order(buy_id, status="executed", executed_price=10.0).get("ok")
        )
        self.assertEqual(self._fetch_position(), (100, 10.0))
        self._insert_order(close_id, action_type="close", size=100, planned_price=10.0)

        result = portfolio_service.update_order(close_id, status="executed", executed_price=12.0)

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self._fetch_position(), (0, 0.0))
        self.assertEqual(self._review_count(close_id), 1)

    def test_close_order_reuses_position_order_no_chain(self):
        buy = portfolio_service.create_order(
            ts_code="600519.SH",
            action_type="buy",
            planned_price=10.0,
            size=100,
            decision_action_id="",
            note="",
            owner_key="alice",
        )
        self.assertTrue(buy.get("ok"), buy)
        buy_order_id = str(buy.get("id") or "")
        buy_order_no = str(buy.get("order_no") or "")
        self.assertEqual(len(buy_order_no), 8)
        self.assertTrue(portfolio_service.update_order(buy_order_id, status="executed", executed_price=10.0).get("ok"))

        close = portfolio_service.create_order(
            ts_code="600519.SH",
            action_type="close",
            planned_price=12.0,
            size=100,
            decision_action_id="",
            note="",
            owner_key="alice",
            chain_order_no=buy_order_no,
        )
        self.assertTrue(close.get("ok"), close)
        self.assertEqual(close.get("order_no"), buy_order_no)
        self.assertTrue(portfolio_service.update_order(str(close.get("id") or ""), status="executed", executed_price=12.0).get("ok"))

        reviews = portfolio_service.list_reviews(order_id=buy_order_no)
        self.assertEqual(reviews.get("total"), 2)
        self.assertEqual({item["order_no"] for item in reviews["items"]}, {buy_order_no})
        groups = portfolio_service.list_review_groups(order_id=buy_order_no)
        self.assertEqual(groups.get("total"), 1)
        self.assertEqual(groups["items"][0]["order_no"], buy_order_no)
        self.assertEqual(groups["items"][0]["review_count"], 2)

    def test_review_chains_group_buy_and_close_by_order_no(self):
        buy = portfolio_service.create_order(
            ts_code="000001.SZ",
            action_type="buy",
            planned_price=11.6,
            size=400,
            decision_action_id="",
            note="",
            owner_key="alice",
        )
        self.assertTrue(buy.get("ok"), buy)
        order_no = str(buy.get("order_no") or "")
        self.assertTrue(portfolio_service.update_order(str(buy.get("id") or ""), status="executed", executed_price=11.6).get("ok"))
        close = portfolio_service.create_order(
            ts_code="000001.SZ",
            action_type="close",
            planned_price=10.6,
            size=400,
            decision_action_id="",
            note="",
            owner_key="alice",
            chain_order_no=order_no,
        )
        self.assertTrue(close.get("ok"), close)
        self.assertTrue(portfolio_service.update_order(str(close.get("id") or ""), status="executed", executed_price=10.6).get("ok"))

        chains = portfolio_service.list_review_chains()

        self.assertEqual(chains.get("total"), 1)
        chain = chains["items"][0]
        self.assertEqual(chain["order_no"], order_no)
        self.assertEqual(chain["event_count"], 2)
        self.assertEqual(chain["action_summary"], "新买 -> 清仓")
        self.assertEqual(chain["chain_status"], "closed")

    def test_get_trade_chain_returns_timeline_and_metrics(self):
        buy = portfolio_service.create_order(
            ts_code="000001.SZ",
            action_type="buy",
            planned_price=10.0,
            size=100,
            decision_action_id="",
            note="入场原因",
            owner_key="alice",
        )
        self.assertTrue(buy.get("ok"), buy)
        order_no = str(buy.get("order_no") or "")
        self.assertTrue(portfolio_service.update_order(str(buy.get("id") or ""), status="executed", executed_price=10.0).get("ok"))
        close = portfolio_service.create_order(
            ts_code="000001.SZ",
            action_type="close",
            planned_price=12.0,
            size=100,
            decision_action_id="",
            note="出场原因",
            owner_key="alice",
            chain_order_no=order_no,
        )
        self.assertTrue(close.get("ok"), close)
        self.assertTrue(portfolio_service.update_order(str(close.get("id") or ""), status="executed", executed_price=12.0).get("ok"))

        detail = portfolio_service.get_trade_chain(order_no)

        self.assertTrue(detail.get("ok"), detail)
        self.assertEqual(detail.get("order_no"), order_no)
        self.assertEqual(detail.get("event_count"), 2)
        self.assertEqual(detail.get("remaining_quantity"), 0)
        self.assertEqual(detail.get("realized_pnl"), 200.0)
        self.assertEqual([event["action_type"] for event in detail["timeline"]], ["buy", "close"])

    def test_list_reviews_filters_by_order_id(self):
        first_id = "order-review-first"
        second_id = "order-review-second"
        self._insert_order(first_id, action_type="buy", size=100, planned_price=10.0)
        self._insert_order(second_id, action_type="buy", size=100, planned_price=11.0, ts_code="000001.SZ")
        self.assertTrue(portfolio_service.update_order(first_id, status="executed", executed_price=10.0).get("ok"))
        self.assertTrue(portfolio_service.update_order(second_id, status="executed", executed_price=11.0).get("ok"))

        result = portfolio_service.list_reviews(order_id=second_id)

        self.assertEqual(result.get("total"), 1)
        self.assertEqual(result["items"][0]["order_id"], second_id)

    def test_delete_review_removes_record(self):
        order_id = "order-review-delete"
        self._insert_order(order_id, action_type="buy", size=100, planned_price=10.0)
        self.assertTrue(portfolio_service.update_order(order_id, status="executed", executed_price=10.0).get("ok"))
        review_id = portfolio_service.list_reviews(order_id=order_id)["items"][0]["id"]

        result = portfolio_service.delete_review(review_id)

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(portfolio_service.list_reviews(order_id=order_id).get("total"), 0)

    def test_delete_review_route_returns_404_for_unknown_review(self):
        handler = _FakeHandler()

        handled = portfolio_routes.dispatch_delete(
            handler,
            urlparse("/api/portfolio/review/not-exists"),
            {},
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 404)
        self.assertFalse(handler.payload.get("ok"))

    def test_create_order_rejects_zero_size_trading_action(self):
        result = portfolio_service.create_order(
            ts_code="600519.SH",
            action_type="buy",
            planned_price=10.0,
            size=0,
            decision_action_id="",
            note="",
        )

        self.assertFalse(result.get("ok"))
        self.assertIn("size", str(result.get("error") or ""))

    def test_create_order_allows_zero_size_watch_action(self):
        result = portfolio_service.create_order(
            ts_code="600519.SH",
            action_type="watch",
            planned_price=None,
            size=0,
            decision_action_id="",
            note="watch only",
        )

        self.assertTrue(result.get("ok"), result)

    def test_create_real_order_from_decision_action(self):
        decision_action_id = self._insert_decision_action(note="突破后确认")
        handler = _FakeHandler()

        handled = portfolio_routes.dispatch_post(
            handler,
            urlparse("/api/portfolio/orders/from-decision"),
            {
                "decision_action_id": str(decision_action_id),
                "action_type": "buy",
                "planned_price": 18.5,
                "size": 200,
                "note": "分两笔执行",
            },
            {},
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 200)
        self.assertTrue(handler.payload.get("ok"), handler.payload)
        order_id = str(handler.payload.get("id") or "")
        self.assertTrue(order_id)
        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            row = conn.execute(
                """
                SELECT ts_code, action_type, planned_price, size, decision_action_id, note
                FROM portfolio_orders
                WHERE id = ?
                """,
                (order_id,),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "600519.SH")
        self.assertEqual(row[1], "buy")
        self.assertEqual(row[2], 18.5)
        self.assertEqual(row[3], 200)
        self.assertEqual(row[4], str(decision_action_id))
        self.assertIn("来自决策动作", row[5])

    def test_create_real_order_from_decision_respects_risk_gate(self):
        decision_action_id = self._insert_decision_action(note="突破后确认")
        handler = _FakeHandler()

        handled = portfolio_routes.dispatch_post(
            handler,
            urlparse("/api/portfolio/orders/from-decision"),
            {
                "decision_action_id": str(decision_action_id),
                "action_type": "buy",
                "planned_price": 18.5,
                "size": 200,
                "note": "分两笔执行",
            },
            {
                "get_decision_kill_switch": lambda: {
                    "allow_trading": 0,
                    "reason": "盘中风险检查暂停",
                },
            },
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 400)
        self.assertFalse(handler.payload.get("ok"))
        self.assertIn("风控检查", str(handler.payload.get("error") or ""))
        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            count = conn.execute("SELECT COUNT(*) FROM portfolio_orders").fetchone()[0]
        self.assertEqual(count, 0)

    def test_create_real_order_from_decision_requires_executable_size(self):
        decision_action_id = self._insert_decision_action()
        result = portfolio_service.create_order_from_decision_action(
            decision_action_id=str(decision_action_id),
            action_type="watch",
            planned_price=10.0,
            size=0,
            note="",
        )

        self.assertFalse(result.get("ok"))
        self.assertIn("可执行动作", str(result.get("error") or ""))

    def test_unknown_order_id_returns_404(self):
        handler = _FakeHandler()

        handled = portfolio_routes.dispatch_patch(
            handler,
            urlparse("/api/portfolio/orders/not-exists"),
            {"status": "executed", "executed_price": 10.5},
            {},
        )

        self.assertTrue(handled)
        self.assertEqual(handler.status, 404)
        self.assertFalse(handler.payload.get("ok"))
        self.assertIn("订单不存在", str(handler.payload.get("error") or ""))


if __name__ == "__main__":
    unittest.main()
