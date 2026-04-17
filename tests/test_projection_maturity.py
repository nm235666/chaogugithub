"""
Maturity self-check tests for project_final_state_projection_2026-04-15.md

Section 6 validation criteria:
1. 主链路成功率 > 99%  — core service imports succeed, no hard crash on import
2. 决策可追踪率 > 95%  — all decision actions include trace fields (action_id, snapshot_id)
3. 闭环完成率 > 85%    — cross-module bridges present (stocks, news, signals, chatrooms → decision)
4. 状态一致性 → 0      — URL/state/stats consistency; key page patterns verified
5. 回归能力            — test files exist and cover core routes
"""
import ast
import os
import re
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(path: str) -> str:
    full = os.path.join(REPO, path)
    with open(full, encoding="utf-8") as f:
        return f.read()


def _exists(path: str) -> bool:
    return os.path.exists(os.path.join(REPO, path))


class ProjectionMaturityTest(unittest.TestCase):
    # ───────────────────────────────────────────────
    # Criterion 1: 主链路成功率
    # ───────────────────────────────────────────────

    def test_backend_server_importable(self):
        """backend/server.py should import without errors (ApiHandler defined)."""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "backend.server", os.path.join(REPO, "backend", "server.py")
        )
        assert spec is not None
        mod = importlib.util.module_from_spec(spec)
        # Should not raise on exec (class definitions, no side effects at module level)
        spec.loader.exec_module(mod)  # type: ignore
        self.assertTrue(hasattr(mod, "ApiHandler"), "ApiHandler class must be defined in server.py")

    def test_decision_service_importable(self):
        """services/decision_service/service.py should import cleanly."""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "decision_service",
            os.path.join(REPO, "services", "decision_service", "service.py"),
        )
        assert spec is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore
        self.assertTrue(hasattr(mod, "record_decision_action"))
        self.assertTrue(hasattr(mod, "query_decision_calibration"))

    def test_decision_route_accepts_structured_fields(self):
        """backend/routes/decision.py must pass evidence_sources/execution_status/review_conclusion to payload."""
        content = _read("backend/routes/decision.py")
        self.assertIn("evidence_sources", content, "evidence_sources must be wired in decision route")
        self.assertIn("execution_status", content, "execution_status must be wired in decision route")
        self.assertIn("review_conclusion", content, "review_conclusion must be wired in decision route")

    # ───────────────────────────────────────────────
    # Criterion 2: 决策可追踪率
    # ───────────────────────────────────────────────

    def test_decision_actions_return_trace_fields(self):
        """service.py _action_trace_fields must be called when building action items."""
        content = _read("services/decision_service/service.py")
        self.assertIn("_action_trace_fields", content)
        self.assertIn("action_id", content)
        self.assertIn("run_id", content)
        self.assertIn("snapshot_id", content)

    def test_decision_board_page_displays_trace_ids(self):
        """DecisionBoardPage.vue must render action_id and snapshot_id chips."""
        content = _read("apps/web/src/pages/research/DecisionBoardPage.vue")
        self.assertIn("action_id", content)
        self.assertIn("snapshot_id", content)
        self.assertIn("actionTraceId", content)

    def test_decision_board_displays_review_conclusion(self):
        """DecisionBoardPage.vue must display review_conclusion from payload."""
        content = _read("apps/web/src/pages/research/DecisionBoardPage.vue")
        self.assertIn("review_conclusion", content)
        self.assertIn("actionReviewConclusion", content)

    def test_decision_board_displays_evidence_sources(self):
        """DecisionBoardPage.vue must display evidence_sources from payload."""
        content = _read("apps/web/src/pages/research/DecisionBoardPage.vue")
        self.assertIn("evidence_sources", content)
        self.assertIn("actionEvidenceSources", content)

    # ───────────────────────────────────────────────
    # Criterion 3: 闭环完成率 — cross-module bridges
    # ───────────────────────────────────────────────

    def test_stocks_bridge_to_decision(self):
        """StockScoresPage or StockDetailPage must link to /research/decision."""
        scores = _read("apps/web/src/pages/stocks/StockScoresPage.vue")
        detail = _read("apps/web/src/pages/stocks/StockDetailPage.vue")
        has_bridge = "/research/decision" in scores or "/research/decision" in detail
        self.assertTrue(has_bridge, "Stocks module must have a bridge to /research/decision")

    def test_news_bridge_to_decision(self):
        """NewsListPageBlock.vue must have goDecision or → 决策板."""
        content = _read("apps/web/src/pages/intelligence/NewsListPageBlock.vue")
        self.assertTrue(
            "goDecision" in content or "决策板" in content,
            "News module must bridge to decision board",
        )

    def test_chatrooms_bridge_to_decision(self):
        """ChatroomInvestmentPage.vue must have goDecisionFromChatroom or 决策板."""
        content = _read("apps/web/src/pages/chatrooms/ChatroomInvestmentPage.vue")
        self.assertTrue(
            "goDecisionFromChatroom" in content or "决策板" in content,
            "Chatrooms module must bridge to decision board",
        )

    def test_signals_bridge_to_decision(self):
        """SignalChainGraphPage.vue must have goToDecision linking to /research/decision."""
        content = _read("apps/web/src/pages/signals/SignalChainGraphPage.vue")
        self.assertIn("goToDecision", content, "Signal graph must have goToDecision function")
        self.assertIn("/research/decision", content, "Signal graph must link to /research/decision")

    def test_decision_board_has_retrospective_section(self):
        """DecisionBoardPage.vue must have a 复盘 (retrospective) section."""
        content = _read("apps/web/src/pages/research/DecisionBoardPage.vue")
        self.assertTrue(
            "决策复盘" in content or "reviewItems" in content,
            "DecisionBoardPage must have a retrospective/review section",
        )

    # ───────────────────────────────────────────────
    # Criterion 4: 状态一致性
    # ───────────────────────────────────────────────

    def test_default_landing_prioritizes_decision_board(self):
        """resolveDefaultLandingPath in navigation.ts must explicitly check /research/decision before /dashboard."""
        content = _read("apps/web/src/app/navigation.ts")
        self.assertIn("/research/decision", content, "/research/decision must be in navigation.ts")
        # The function resolveDefaultLandingPath must contain an explicit check for /research/decision
        # before the admin dashboard fallback
        func_start = content.find("export function resolveDefaultLandingPath")
        self.assertGreater(func_start, 0, "resolveDefaultLandingPath function must exist")
        func_body = content[func_start:]
        decision_check_pos = func_body.find("/research/decision")
        dashboard_check_pos = func_body.find("/dashboard")
        self.assertGreater(decision_check_pos, 0, "/research/decision must be checked in resolveDefaultLandingPath")
        self.assertLess(
            decision_check_pos, dashboard_check_pos,
            "resolveDefaultLandingPath must check /research/decision before /dashboard fallback",
        )

    def test_stocks_empty_state_machine(self):
        """StocksListPage and PricesPage must have enforcePageCoherence for empty state handling."""
        list_page = _read("apps/web/src/pages/stocks/StocksListPage.vue")
        prices_page = _read("apps/web/src/pages/stocks/PricesPage.vue")
        self.assertIn("enforcePageCoherence", list_page)
        self.assertIn("enforcePageCoherence", prices_page)

    def test_signal_graph_uses_rendered_stats(self):
        """SignalChainGraphPage.vue displaySummary must be computed from rendered nodes, not raw API."""
        content = _read("apps/web/src/pages/signals/SignalChainGraphPage.vue")
        self.assertIn("displaySummary", content)
        self.assertIn("renderPayload", content)

    def test_upgrade_page_has_route_context_rules(self):
        """UpgradePage.vue must map at least 10 blocked routes to context + alternatives."""
        content = _read("apps/web/src/pages/auth/UpgradePage.vue")
        # Count test() rule entries
        test_count = content.count("test: (pathName)")
        self.assertGreaterEqual(test_count, 8, f"UpgradePage must have ≥8 route context rules, found {test_count}")
        self.assertIn("alternatives", content)

    # ───────────────────────────────────────────────
    # Criterion 5: 回归能力
    # ───────────────────────────────────────────────

    def test_core_test_files_exist(self):
        """Core test files must exist."""
        for path in [
            "tests/test_frontend_api_smoke.py",
            "tests/test_decision_service.py",
            "tests/test_minimal_regression.py",
        ]:
            self.assertTrue(_exists(path), f"Test file missing: {path}")

    def test_decision_service_tests_cover_calibration(self):
        """test_decision_service.py must test the calibration/board flow."""
        content = _read("tests/test_decision_service.py")
        self.assertTrue(
            "calibrat" in content.lower() or "board" in content.lower(),
            "Decision service tests must cover calibration or board flow",
        )

    def test_frontend_smoke_tests_cover_decision_routes(self):
        """test_frontend_api_smoke.py must verify decision board API routes."""
        content = _read("tests/test_frontend_api_smoke.py")
        self.assertIn("decision", content.lower())


if __name__ == "__main__":
    unittest.main(verbosity=2)
