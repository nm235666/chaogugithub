#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from mcp_server.audit import AUDIT_TABLE
from services.agent_runtime import create_run, decide_run, get_run, run_next_once
from services.agent_runtime import agents as agents_mod
from services.agent_runtime import config as agent_config
from services.agent_runtime import governance
from services.agent_runtime import store
from services.agent_runtime.executor import execute_tool_step
from services.agent_runtime import planner
from services.agent_runtime.platform_allowlist import FUNNEL_AUTO_WRITE_TOOLS
from mcp_server import schemas
from mcp_server.tools import governance_tools, memory_tools


class AgentRuntimeTest(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(prefix="agent-runtime-", suffix=".db")
        os.close(fd)

    def tearDown(self):
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _patch_db(self):
        return patch("services.agent_runtime.store.db.connect", self._connect)

    def _patch_governance_db(self):
        return patch("services.agent_runtime.governance.db.connect", self._connect)

    def test_create_run_dedupes_schedule_key(self):
        with self._patch_db():
            first = create_run(agent_key="funnel_progress_agent", schedule_key="20260424")
            second = create_run(agent_key="funnel_progress_agent", schedule_key="20260424")
        self.assertEqual(first["id"], second["id"])
        self.assertTrue(second["deduped"])

    def test_create_run_persists_metadata(self):
        with self._patch_db():
            run = create_run(
                agent_key="funnel_progress_agent",
                metadata={"job_key": "agent.funnel_progress.daily"},
                correlation_id="corr-test",
                parent_run_id="parent-1",
                dedupe=False,
            )
        self.assertEqual(run.get("metadata", {}).get("job_key"), "agent.funnel_progress.daily")
        self.assertEqual(run.get("correlation_id"), "corr-test")
        self.assertEqual(run.get("parent_run_id"), "parent-1")

    def test_memory_items_record_and_search(self):
        with self._patch_db():
            result = store.record_memory_item(
                memory_type="review_rule_correction",
                source_run_id="run-1",
                source_agent_key="portfolio_review_agent",
                ts_code="600519.SH",
                scope="portfolio_review",
                summary="T+5 review suggests stricter entry rule",
                evidence={"order_id": "o1"},
                score=0.8,
            )
            found = store.search_memory_items(ts_code="600519.SH", memory_type="review_rule_correction")

        self.assertTrue(result["ok"])
        self.assertEqual(len(found["items"]), 1)
        self.assertEqual(found["items"][0]["evidence"]["order_id"], "o1")

    def test_memory_record_tool_requires_write_guard(self):
        args = schemas.MemoryRecordArgs(
            memory_type="failed_signal",
            summary="signal failed after review",
            dry_run=False,
            confirm=True,
            actor="agent:test",
            reason="record quality memory",
            idempotency_key="mem-1",
        )
        with self._patch_db(), patch("mcp_server.config.MCP_WRITE_ENABLED", True):
            out = memory_tools.record_item(args)
            loaded = store.list_memory_items(memory_type="failed_signal")

        self.assertTrue(out["ok"])
        self.assertFalse(out["dry_run"])
        self.assertEqual(len(loaded["items"]), 1)

    def test_memory_record_tool_rejects_missing_reason(self):
        args = schemas.MemoryRecordArgs(
            memory_type="failed_signal",
            summary="signal failed after review",
            dry_run=False,
            confirm=True,
            actor="agent:test",
            idempotency_key="mem-2",
        )
        with patch("mcp_server.config.MCP_WRITE_ENABLED", True), self.assertRaises(ValueError):
            memory_tools.record_item(args)

    def test_governance_quality_scores_runs_and_policy_decisions(self):
        with self._patch_db(), self._patch_governance_db(), patch("mcp_server.audit.db.connect", self._connect):
            ok = create_run(agent_key="funnel_progress_agent", dedupe=False)
            store.update_run(ok["id"], status="succeeded", result={"changed_count": 2}, finished=True)
            bad = create_run(agent_key="funnel_progress_agent", dedupe=False)
            store.update_run(bad["id"], status="failed", error_text="boom", finished=True)
            out = governance.compute_quality_snapshot(agent_key="funnel_progress_agent", persist=True)
            scores = store.list_quality_scores(agent_key="funnel_progress_agent")["items"]
            decision = governance.evaluate_action(
                agent_key="funnel_progress_agent",
                tool_name="business.repair_funnel_score_align",
                arguments={"dry_run": False},
                requested_dry_run=False,
                run_id=ok["id"],
                record=True,
            )
            policies = store.list_policy_decisions(agent_key="funnel_progress_agent")["items"]

        self.assertTrue(out["ok"])
        self.assertEqual(scores[0]["total_runs"], 2)
        self.assertGreater(scores[0]["failure_rate"], 0)
        self.assertIn(decision["decision"], {"allow", "dry_run_only"})
        self.assertEqual(len(policies), 1)

    def test_governance_evaluate_blocks_high_risk_and_write_disabled(self):
        with self._patch_db(), self._patch_governance_db(), patch("mcp_server.audit.db.connect", self._connect), patch(
            "mcp_server.config.MCP_WRITE_ENABLED", False
        ):
            high = governance.evaluate_action(
                agent_key="portfolio_reconcile_agent",
                tool_name="business.reconcile_portfolio_positions",
                arguments={"dry_run": False},
                requested_dry_run=False,
            )
            low = governance.evaluate_action(
                agent_key="funnel_progress_agent",
                tool_name="business.repair_funnel_score_align",
                arguments={"dry_run": False},
                requested_dry_run=False,
            )

        self.assertEqual(high["decision"], "requires_approval")
        self.assertEqual(low["decision"], "dry_run_only")

    def test_governance_upsert_rule_write_guard(self):
        args = schemas.GovernanceRuleUpsertArgs(
            rule_key="block-funnel",
            agent_key="funnel_progress_agent",
            tool_name="business.repair_funnel_score_align",
            decision="blocked",
            dry_run=False,
            confirm=True,
            actor="admin",
            idempotency_key="rule-1",
        )
        with patch("mcp_server.config.MCP_WRITE_ENABLED", True), self.assertRaises(ValueError):
            governance_tools.upsert_rule(args)

    def test_governance_rule_cannot_allow_high_risk_tool(self):
        with self._patch_db(), self._patch_governance_db(), patch("mcp_server.audit.db.connect", self._connect), patch(
            "mcp_server.config.MCP_WRITE_ENABLED", True
        ):
            store.upsert_governance_rule(
                rule_key="unsafe-allow-reconcile",
                tool_name="business.reconcile_portfolio_positions",
                decision="allow",
                risk_level="high",
                actor="test",
            )
            out = governance.evaluate_action(
                agent_key="portfolio_reconcile_agent",
                tool_name="business.reconcile_portfolio_positions",
                arguments={"dry_run": False},
                requested_dry_run=False,
            )

        self.assertEqual(out["decision"], "requires_approval")

    def test_active_failure_memory_degrades_auto_write(self):
        with self._patch_db(), self._patch_governance_db(), patch("mcp_server.audit.db.connect", self._connect), patch(
            "mcp_server.config.MCP_WRITE_ENABLED", True
        ):
            store.record_memory_item(
                memory_type="agent_failure_pattern",
                source_agent_key="funnel_progress_agent",
                scope="agent_runtime",
                summary="recent tool failure",
                status="active",
            )
            out = governance.evaluate_action(
                agent_key="funnel_progress_agent",
                tool_name="business.repair_funnel_score_align",
                arguments={"dry_run": False},
                requested_dry_run=False,
            )

        self.assertEqual(out["decision"], "dry_run_only")

    def test_funnel_write_tools_match_platform_allowlist(self):
        self.assertEqual(agents_mod.FUNNEL_WRITE_TOOLS, set(FUNNEL_AUTO_WRITE_TOOLS))
        with patch.dict(os.environ, {"AGENT_AUTO_WRITE_TOOL_ALLOWLIST": ""}):
            self.assertEqual(agent_config.auto_write_allowlist(), agents_mod.FUNNEL_WRITE_TOOLS)

    def test_planner_stub_builds_steps(self):
        out = planner.build_planner_preview(goal={"force_score_align": True}, closure_summary={"gaps": []})
        self.assertTrue(out.get("ok"))
        self.assertGreaterEqual(int(out.get("len") or 0), 2)

    def test_executor_records_step_and_mcp_audit(self):
        with self._patch_db(), patch("mcp_server.audit.db.connect", self._connect), patch(
            "mcp_server.tools.registry.call_tool", return_value={"ok": True, "value": 1}
        ):
            run = create_run(agent_key="funnel_progress_agent")
            out = execute_tool_step(
                run_id=run["id"],
                step_index=1,
                tool_name="system.health_snapshot",
                arguments={},
            )
            loaded = get_run(run["id"])

        self.assertTrue(out["ok"])
        self.assertGreater(out["audit_id"], 0)
        self.assertEqual(loaded["steps"][0]["tool_name"], "system.health_snapshot")
        conn = self._connect()
        try:
            row = conn.execute(f"SELECT tool_name, status FROM {AUDIT_TABLE} WHERE id = ?", (out["audit_id"],)).fetchone()
        finally:
            conn.close()
        self.assertEqual(row["tool_name"], "system.health_snapshot")
        self.assertEqual(row["status"], "success")

    def test_funnel_agent_auto_writes_only_allowlisted_tools(self):
        calls = []

        def fake_step(*, run_id, step_index, tool_name, arguments=None):
            calls.append((tool_name, dict(arguments or {})))
            if tool_name == "business.closure_gap_scan":
                return {
                    "ok": True,
                    "result": {
                        "ok": True,
                        "gaps": ["funnel_ingested_backlog"],
                        "funnel_by_state": {"ingested": 3, "confirmed": 1},
                    },
                }
            return {"ok": True, "result": {"ok": True, "changed_count": 1, "warnings": []}}

        with self._patch_db(), self._patch_governance_db(), patch("mcp_server.audit.db.connect", self._connect), patch(
            "services.agent_runtime.agents.execute_tool_step", side_effect=fake_step
        ):
            run = create_run(agent_key="funnel_progress_agent")
            out = run_next_once(worker_id="test-worker")
            loaded = get_run(run["id"])

        self.assertTrue(out["ok"])
        self.assertEqual(loaded["status"], "succeeded")
        write_calls = [(name, args) for name, args in calls if args.get("dry_run") is False]
        self.assertEqual({name for name, _ in write_calls}, {"business.repair_funnel_score_align", "business.repair_funnel_review_refresh"})
        for _, args in write_calls:
            self.assertTrue(args["confirm"])
            self.assertEqual(args["actor"], "agent:funnel_progress_agent")
            self.assertTrue(args["reason"])
            self.assertTrue(args["idempotency_key"])

    def test_auto_write_disabled_keeps_repairs_dry_run(self):
        calls = []

        def fake_step(*, run_id, step_index, tool_name, arguments=None):
            calls.append((tool_name, dict(arguments or {})))
            if tool_name == "business.closure_gap_scan":
                return {"ok": True, "result": {"ok": True, "gaps": ["funnel_ingested_backlog"], "funnel_by_state": {"ingested": 2}}}
            return {"ok": True, "result": {"ok": True, "planned_changes": [{}]}}

        with self._patch_db(), self._patch_governance_db(), patch("mcp_server.audit.db.connect", self._connect), patch.dict(os.environ, {"AGENT_AUTO_WRITE_ENABLED": "0"}), patch(
            "services.agent_runtime.agents.execute_tool_step", side_effect=fake_step
        ):
            run = create_run(agent_key="funnel_progress_agent")
            run_next_once(worker_id="test-worker")
            loaded = get_run(run["id"])

        self.assertEqual(loaded["status"], "succeeded")
        self.assertFalse([args for _, args in calls if args.get("dry_run") is False])
        self.assertIn("agent_auto_write_disabled", loaded["result"]["warnings"])

    def test_portfolio_agent_waits_for_approval_then_executes_pending_step(self):
        def fake_step(*, run_id, step_index, tool_name, arguments=None):
            if tool_name == "business.portfolio_closure_scan":
                return {"ok": True, "result": {"ok": True, "requires_position_reconcile": True, "executed_orders": 1, "positions": 0}}
            if tool_name == "business.reconcile_portfolio_positions":
                return {"ok": True, "result": {"ok": True, "dry_run": True, "planned_changes": [{"ts_code": "600519.SH"}], "warnings": []}}
            return {"ok": True, "result": {"ok": True}}

        with self._patch_db(), patch("services.agent_runtime.agents.execute_tool_step", side_effect=fake_step):
            run = create_run(agent_key="portfolio_reconcile_agent")
            run_next_once(worker_id="test-worker")
            waiting = get_run(run["id"])

        self.assertEqual(waiting["status"], "waiting_approval")
        pending = [s for s in waiting["steps"] if s["status"] == "pending_approval"]
        self.assertEqual(len(pending), 1)

        with self._patch_db(), patch("mcp_server.audit.db.connect", self._connect), patch(
            "mcp_server.tools.registry.call_tool", return_value={"ok": True, "changed_count": 1}
        ):
            decided = decide_run(run["id"], actor="admin", reason="approve reconcile", decision="approved")
            loaded = get_run(run["id"])

        self.assertTrue(decided["ok"])
        self.assertEqual(loaded["status"], "succeeded")
        self.assertEqual(loaded["result"]["changed_count"], 1)
        self.assertEqual([s for s in loaded["steps"] if s["status"] == "pending_approval"], [])

    def test_rejected_approval_does_not_execute_pending_step(self):
        with self._patch_db():
            run = create_run(agent_key="portfolio_review_agent")
            store.update_run(run["id"], status="waiting_approval", approval_required=True, result={"pending_write_steps": [{}]})
            store.insert_pending_step(
                run_id=run["id"],
                step_index=1,
                tool_name="business.generate_portfolio_order_reviews",
                args={"dry_run": False, "confirm": True},
            )
            decided = decide_run(run["id"], actor="admin", reason="not now", decision="rejected")
            loaded = get_run(run["id"])

        self.assertTrue(decided["ok"])
        self.assertEqual(loaded["status"], "cancelled")
        self.assertEqual(loaded["steps"][0]["status"], "pending_approval")

    def test_decision_orchestrator_creates_child_runs_with_correlation(self):
        def fake_step(*, run_id, step_index, tool_name, arguments=None):
            if tool_name == "business.closure_gap_scan":
                return {"ok": True, "result": {"ok": True, "gaps": ["funnel_ingested_backlog"], "funnel_by_state": {"ingested": 2}}}
            if tool_name == "business.portfolio_closure_scan":
                return {"ok": True, "result": {"ok": True, "requires_position_reconcile": True, "requires_review_generation": True}}
            if tool_name == "memory.search_relevant":
                return {"ok": True, "result": {"ok": True, "items": []}}
            return {"ok": True, "result": {"ok": True}}

        with self._patch_db(), self._patch_governance_db(), patch("mcp_server.audit.db.connect", self._connect), patch(
            "services.agent_runtime.agents.execute_tool_step", side_effect=fake_step
        ):
            run = create_run(agent_key="decision_orchestrator_agent", correlation_id="corr-orch", dedupe=False)
            run_next_once(worker_id="test-worker")
            loaded = get_run(run["id"])
            children = store.list_runs(parent_run_id=run["id"], limit=10)["items"]

        self.assertEqual(loaded["status"], "succeeded")
        self.assertEqual({child["agent_key"] for child in children}, {"funnel_progress_agent", "portfolio_reconcile_agent", "portfolio_review_agent"})
        self.assertEqual({child["correlation_id"] for child in children}, {"corr-orch"})
        self.assertEqual(len(loaded["result"]["created_child_runs"]), 3)

    def test_failed_run_generates_failure_memory(self):
        with self._patch_db(), patch("services.agent_runtime.agents.db.connect", self._connect):
            failed = create_run(agent_key="funnel_progress_agent", dedupe=False)
            store.update_run(failed["id"], status="failed", error_text="tool exploded", finished=True)
            refresh = create_run(agent_key="memory_refresh_agent", dedupe=False)
            run_next_once(worker_id="test-worker")
            loaded = get_run(refresh["id"])
            memories = store.list_memory_items(memory_type="agent_failure_pattern", status="active")["items"]

        self.assertEqual(loaded["status"], "succeeded")
        self.assertEqual(len(memories), 1)
        self.assertIn("tool exploded", memories[0]["summary"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
