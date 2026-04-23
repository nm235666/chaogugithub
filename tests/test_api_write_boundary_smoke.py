#!/usr/bin/env python3
from __future__ import annotations

import unittest
from types import SimpleNamespace

from backend.routes import decision as decision_routes
from backend.routes import funnel as funnel_routes
from backend.routes import portfolio as portfolio_routes
from backend.routes import system as system_routes
from backend.server import ApiHandler


class _DummyHandler:
    def __init__(self) -> None:
        self.status = None
        self.payload = None

    def _send_json(self, payload, status=200):  # noqa: ANN001
        self.status = status
        self.payload = payload


class ApiWriteBoundarySmokeTest(unittest.TestCase):
    def test_api_contract_method_returns_405(self):
        handler = _DummyHandler()
        blocked = ApiHandler._enforce_api_contract_method(
            handler,  # type: ignore[arg-type]
            SimpleNamespace(path="/api/decision/scores"),
            "POST",
        )
        self.assertTrue(blocked)
        self.assertEqual(handler.status, 405)
        self.assertIn("Method Not Allowed", str((handler.payload or {}).get("error")))

    def test_decision_actions_blocked_when_write_scope_invalid(self):
        handler = _DummyHandler()
        parsed = SimpleNamespace(path="/api/decision/actions", query="")
        deps = {
            "assert_write_allowed": lambda **kwargs: (_ for _ in ()).throw(PermissionError("blocked by write policy")),
        }
        handled = decision_routes.dispatch_post(handler, parsed, {}, deps)
        self.assertTrue(handled)
        self.assertEqual(handler.status, 403)
        self.assertIn("blocked", str((handler.payload or {}).get("error")))

    def test_funnel_create_blocked_when_write_scope_invalid(self):
        handler = _DummyHandler()
        parsed = SimpleNamespace(path="/api/funnel/candidates", query="")
        deps = {
            "assert_write_allowed": lambda **kwargs: (_ for _ in ()).throw(PermissionError("blocked by write policy")),
        }
        handled = funnel_routes.dispatch_post(handler, parsed, {}, deps)
        self.assertTrue(handled)
        self.assertEqual(handler.status, 403)
        self.assertIn("blocked", str((handler.payload or {}).get("error")))

    def test_portfolio_order_blocked_when_write_scope_invalid(self):
        handler = _DummyHandler()
        parsed = SimpleNamespace(path="/api/portfolio/orders", query="")
        deps = {
            "assert_write_allowed": lambda **kwargs: (_ for _ in ()).throw(PermissionError("blocked by write policy")),
        }
        handled = portfolio_routes.dispatch_post(handler, parsed, {}, deps)
        self.assertTrue(handled)
        self.assertEqual(handler.status, 403)
        self.assertIn("blocked", str((handler.payload or {}).get("error")))

    def test_jobs_trigger_blocked_when_write_scope_invalid(self):
        handler = _DummyHandler()
        parsed = SimpleNamespace(path="/api/jobs/trigger", query="job_key=test_job")
        deps = {
            "assert_write_allowed": lambda **kwargs: (_ for _ in ()).throw(PermissionError("blocked by write policy")),
        }
        handled = system_routes.dispatch_get(handler, parsed, "127.0.0.1", deps)
        self.assertTrue(handled)
        self.assertEqual(handler.status, 403)
        self.assertIn("blocked", str((handler.payload or {}).get("error")))


if __name__ == "__main__":
    unittest.main()
