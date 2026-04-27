from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import db_compat as db
from mcp_server import config as mcp_config

from . import config, governance, store
from .executor import execute_tool_step
from .platform_allowlist import FUNNEL_AUTO_WRITE_TOOLS


FUNNEL_AGENT_KEY = "funnel_progress_agent"
FUNNEL_WRITE_TOOLS = set(FUNNEL_AUTO_WRITE_TOOLS)
JOB_FAILURE_DIAG_AGENT_KEY = "job_failure_diag_agent"
DECISION_OPS_READ_AGENT_KEY = "decision_ops_read_agent"
PORTFOLIO_RECONCILE_AGENT_KEY = "portfolio_reconcile_agent"
PORTFOLIO_REVIEW_AGENT_KEY = "portfolio_review_agent"
DECISION_ORCHESTRATOR_AGENT_KEY = "decision_orchestrator_agent"
MEMORY_REFRESH_AGENT_KEY = "memory_refresh_agent"
GOVERNANCE_QUALITY_REFRESH_AGENT_KEY = "governance_quality_refresh_agent"


def _now_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _stable_key(*parts: Any) -> str:
    raw = "|".join(str(p or "") for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _state_count(closure: dict[str, Any], state: str) -> int:
    try:
        return int((closure.get("funnel_by_state") or {}).get(state) or 0)
    except Exception:
        return 0


def _planned_actions(goal: dict[str, Any], closure: dict[str, Any]) -> list[dict[str, Any]]:
    gaps = set(str(x) for x in closure.get("gaps", []) if str(x))
    actions: list[dict[str, Any]] = []
    ingested = _state_count(closure, "ingested")
    if "funnel_ingested_backlog" in gaps or ingested > 0 or goal.get("force_score_align"):
        actions.append(
            {
                "tool_name": "business.repair_funnel_score_align",
                "arguments": {
                    "score_date": str(goal.get("score_date") or ""),
                    "max_candidates": int(goal.get("max_candidates") or 10000),
                },
            }
        )
    review_candidates = sum(_state_count(closure, state) for state in ("confirmed", "executed", "reviewed"))
    if review_candidates > 0 or goal.get("force_review_refresh"):
        actions.append(
            {
                "tool_name": "business.repair_funnel_review_refresh",
                "arguments": {
                    "horizon_days": int(goal.get("horizon_days") or 5),
                    "limit": int(goal.get("review_limit") or 200),
                },
            }
        )
    return actions


def run_funnel_progress_agent(run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run.get("id") or "")
    goal = dict(run.get("goal") or {})
    actor = "agent:funnel_progress_agent"
    step_index = 1
    observations: dict[str, Any] = {}
    executed_actions: list[dict[str, Any]] = []
    skipped_actions: list[dict[str, Any]] = []
    requires_approval_actions: list[dict[str, Any]] = []
    policy_decisions: list[dict[str, Any]] = []
    degraded_actions: list[dict[str, Any]] = []
    warnings: list[str] = []

    def call(tool_name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        nonlocal step_index
        out = execute_tool_step(run_id=run_id, step_index=step_index, tool_name=tool_name, arguments=args or {})
        step_index += 1
        if not out.get("ok"):
            raise RuntimeError(f"{tool_name}: {out.get('error')}")
        result = out.get("result")
        return result if isinstance(result, dict) else {"result": result}

    observations["health"] = call("system.health_snapshot")
    closure = call("business.closure_gap_scan")
    observations["closure_gap_scan"] = closure
    observations["funnel_score_job_runs"] = call("jobs.list_runs", {"job_key": "funnel_ingested_score_align", "limit": 10})
    observations["funnel_review_job_runs"] = call("jobs.list_runs", {"job_key": "funnel_review_refresh", "limit": 10})

    planned_actions = _planned_actions(goal, closure)
    plan = {
        "agent_key": FUNNEL_AGENT_KEY,
        "observe_steps": [
            "system.health_snapshot",
            "business.closure_gap_scan",
            "jobs.list_runs:funnel_ingested_score_align",
            "jobs.list_runs:funnel_review_refresh",
        ],
        "planned_actions": planned_actions,
    }

    allowlist = config.auto_write_allowlist()
    can_auto_write = config.auto_write_enabled() and bool(mcp_config.MCP_WRITE_ENABLED)
    if not config.auto_write_enabled():
        warnings.append("agent_auto_write_disabled")
    if not mcp_config.MCP_WRITE_ENABLED:
        warnings.append("mcp_write_disabled")

    for action in planned_actions:
        tool_name = str(action.get("tool_name") or "")
        base_args = dict(action.get("arguments") or {})
        dry_args = {
            **base_args,
            "dry_run": True,
            "actor": actor,
            "reason": "funnel_progress_agent dry-run precheck",
            "idempotency_key": f"dryrun:{run_id}:{tool_name}",
        }
        dry_result = call(tool_name, dry_args)
        action_record = {"tool_name": tool_name, "dry_run": dry_result}
        if dry_result.get("ok") is False:
            action_record["reason"] = "dry_run_failed"
            skipped_actions.append(action_record)
            continue
        if tool_name not in FUNNEL_WRITE_TOOLS or tool_name not in allowlist:
            action_record["reason"] = "tool_not_auto_write_allowlisted"
            requires_approval_actions.append(action_record)
            continue
        if not can_auto_write:
            action_record["reason"] = "auto_write_disabled"
            skipped_actions.append(action_record)
            continue
        write_args = {
            **base_args,
            "dry_run": False,
            "confirm": True,
            "actor": actor,
            "reason": f"funnel_progress_agent auto repair via {tool_name}",
            "idempotency_key": f"agent:{FUNNEL_AGENT_KEY}:{tool_name}:{run.get('schedule_key') or _now_date()}:{_stable_key(base_args)}",
        }
        policy = governance.apply_policy_to_arguments(run=run, tool_name=tool_name, arguments=write_args, record=True)
        policy_decisions.append(policy)
        if policy.get("decision") == "blocked":
            skipped_actions.append({"tool_name": tool_name, "reason": policy.get("reason"), "policy_decision": policy})
            continue
        if policy.get("decision") == "requires_approval":
            requires_approval_actions.append({"tool_name": tool_name, "reason": policy.get("reason"), "policy_decision": policy, "dry_run": dry_result})
            continue
        if policy.get("decision") == "dry_run_only":
            degraded_actions.append({"tool_name": tool_name, "reason": policy.get("reason"), "policy_decision": policy})
            skipped_actions.append({"tool_name": tool_name, "reason": policy.get("reason"), "dry_run": dry_result})
            continue
        write_result = call(tool_name, dict(policy.get("arguments") or write_args))
        executed_actions.append({"tool_name": tool_name, "dry_run": dry_result, "write": write_result})

    if not planned_actions:
        skipped_actions.append({"reason": "no_funnel_gap_detected"})

    closure_status = "requires_attention" if requires_approval_actions or warnings else "closed_or_progressed"
    if not planned_actions:
        closure_status = "no_action_needed"

    try:
        from . import planner as _planner

        planner_preview = _planner.build_planner_preview(goal=goal, closure_summary=closure if isinstance(closure, dict) else {})
    except Exception as exc:
        planner_preview = {"ok": False, "error": str(exc)}

    return {
        "ok": True,
        "agent_key": FUNNEL_AGENT_KEY,
        "observations": observations,
        "plan": plan,
        "planned_actions": planned_actions,
        "executed_actions": executed_actions,
        "skipped_actions": skipped_actions,
        "requires_approval_actions": requires_approval_actions,
        "policy_decisions": policy_decisions,
        "degraded_actions": degraded_actions,
        "warnings": warnings,
        "closure_status": closure_status,
        "changed_count": sum(int((item.get("write") or {}).get("changed_count") or 0) for item in executed_actions),
        "planner_preview": planner_preview,
    }


def _approval_idempotency_key(run: dict[str, Any], tool_name: str, args: dict[str, Any]) -> str:
    return f"agent:{run.get('agent_key')}:{tool_name}:{run.get('schedule_key') or _now_date()}:{_stable_key(args)}"


def _portfolio_observations(run_id: str) -> tuple[dict[str, Any], int]:
    step_index = 1
    observations: dict[str, Any] = {}

    def call(tool_name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        nonlocal step_index
        out = execute_tool_step(run_id=run_id, step_index=step_index, tool_name=tool_name, arguments=args or {})
        step_index += 1
        if not out.get("ok"):
            raise RuntimeError(f"{tool_name}: {out.get('error')}")
        result = out.get("result")
        return result if isinstance(result, dict) else {"result": result}

    observations["health"] = call("system.health_snapshot")
    observations["closure_gap_scan"] = call("business.closure_gap_scan")
    observations["portfolio_closure_scan"] = call("business.portfolio_closure_scan")
    return observations, step_index


def run_portfolio_reconcile_agent(run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run.get("id") or "")
    goal = dict(run.get("goal") or {})
    observations, step_index = _portfolio_observations(run_id)
    scan = observations.get("portfolio_closure_scan") or {}
    planned_actions: list[dict[str, Any]] = []
    requires_approval_actions: list[dict[str, Any]] = []
    skipped_actions: list[dict[str, Any]] = []
    pending_write_steps: list[dict[str, Any]] = []
    warnings: list[str] = []

    if scan.get("requires_position_reconcile") or goal.get("force_reconcile"):
        base_args = {"limit": int(goal.get("limit") or 500)}
        dry_args = {
            **base_args,
            "dry_run": True,
            "actor": "agent:portfolio_reconcile_agent",
            "reason": "portfolio_reconcile_agent dry-run precheck",
            "idempotency_key": f"dryrun:{run_id}:business.reconcile_portfolio_positions",
        }
        out = execute_tool_step(
            run_id=run_id,
            step_index=step_index,
            tool_name="business.reconcile_portfolio_positions",
            arguments=dry_args,
        )
        step_index += 1
        if not out.get("ok"):
            raise RuntimeError(f"business.reconcile_portfolio_positions: {out.get('error')}")
        dry_result = out.get("result") if isinstance(out.get("result"), dict) else {}
        action = {"tool_name": "business.reconcile_portfolio_positions", "dry_run": dry_result}
        planned_actions.append({"tool_name": "business.reconcile_portfolio_positions", "arguments": base_args})
        if dry_result.get("requires_manual_review"):
            warnings.append("portfolio_reconcile_requires_manual_review")
            action["reason"] = "dry_run_conflicts"
            skipped_actions.append(action)
        elif dry_result.get("ok") is False:
            action["reason"] = "dry_run_failed"
            skipped_actions.append(action)
        else:
            write_args = {
                **base_args,
                "dry_run": False,
                "confirm": True,
                "actor": "agent:portfolio_reconcile_agent",
                "reason": "",
                "idempotency_key": _approval_idempotency_key(run, "business.reconcile_portfolio_positions", base_args),
            }
            step_id = store.insert_pending_step(
                run_id=run_id,
                step_index=step_index,
                tool_name="business.reconcile_portfolio_positions",
                args=write_args,
            )
            pending = {"step_id": step_id, "tool_name": "business.reconcile_portfolio_positions", "arguments": write_args, "dry_run": dry_result}
            requires_approval_actions.append(pending)
            pending_write_steps.append(pending)
    else:
        skipped_actions.append({"reason": "no_position_reconcile_gap_detected"})

    return {
        "ok": True,
        "agent_key": PORTFOLIO_RECONCILE_AGENT_KEY,
        "observations": observations,
        "plan": {"agent_key": PORTFOLIO_RECONCILE_AGENT_KEY, "planned_actions": planned_actions},
        "planned_actions": planned_actions,
        "executed_actions": [],
        "skipped_actions": skipped_actions,
        "requires_approval_actions": requires_approval_actions,
        "pending_write_steps": pending_write_steps,
        "warnings": warnings,
        "closure_status": "requires_approval" if pending_write_steps else ("requires_attention" if warnings else "no_action_needed"),
        "changed_count": 0,
    }


def run_portfolio_review_agent(run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run.get("id") or "")
    goal = dict(run.get("goal") or {})
    observations, step_index = _portfolio_observations(run_id)
    scan = observations.get("portfolio_closure_scan") or {}
    planned_actions: list[dict[str, Any]] = []
    requires_approval_actions: list[dict[str, Any]] = []
    skipped_actions: list[dict[str, Any]] = []
    pending_write_steps: list[dict[str, Any]] = []

    if scan.get("requires_review_generation") or goal.get("force_review_generation"):
        base_args = {
            "horizon_days": int(goal.get("horizon_days") or 5),
            "limit": int(goal.get("limit") or 200),
            "order_status": "executed",
        }
        dry_args = {
            **base_args,
            "dry_run": True,
            "actor": "agent:portfolio_review_agent",
            "reason": "portfolio_review_agent dry-run precheck",
            "idempotency_key": f"dryrun:{run_id}:business.generate_portfolio_order_reviews",
        }
        out = execute_tool_step(
            run_id=run_id,
            step_index=step_index,
            tool_name="business.generate_portfolio_order_reviews",
            arguments=dry_args,
        )
        step_index += 1
        if not out.get("ok"):
            raise RuntimeError(f"business.generate_portfolio_order_reviews: {out.get('error')}")
        dry_result = out.get("result") if isinstance(out.get("result"), dict) else {}
        planned_actions.append({"tool_name": "business.generate_portfolio_order_reviews", "arguments": base_args})
        action = {"tool_name": "business.generate_portfolio_order_reviews", "dry_run": dry_result}
        if dry_result.get("ok") is False or int(dry_result.get("changed_count") or 0) < 0:
            action["reason"] = "dry_run_failed"
            skipped_actions.append(action)
        elif not dry_result.get("planned_changes"):
            action["reason"] = "no_review_candidates"
            skipped_actions.append(action)
        else:
            write_args = {
                **base_args,
                "dry_run": False,
                "confirm": True,
                "actor": "agent:portfolio_review_agent",
                "reason": "",
                "idempotency_key": _approval_idempotency_key(run, "business.generate_portfolio_order_reviews", base_args),
            }
            step_id = store.insert_pending_step(
                run_id=run_id,
                step_index=step_index,
                tool_name="business.generate_portfolio_order_reviews",
                args=write_args,
            )
            pending = {"step_id": step_id, "tool_name": "business.generate_portfolio_order_reviews", "arguments": write_args, "dry_run": dry_result}
            requires_approval_actions.append(pending)
            pending_write_steps.append(pending)
    else:
        skipped_actions.append({"reason": "no_review_gap_detected"})

    return {
        "ok": True,
        "agent_key": PORTFOLIO_REVIEW_AGENT_KEY,
        "observations": observations,
        "plan": {"agent_key": PORTFOLIO_REVIEW_AGENT_KEY, "planned_actions": planned_actions},
        "planned_actions": planned_actions,
        "executed_actions": [],
        "skipped_actions": skipped_actions,
        "requires_approval_actions": requires_approval_actions,
        "pending_write_steps": pending_write_steps,
        "warnings": [],
        "closure_status": "requires_approval" if pending_write_steps else "no_action_needed",
        "changed_count": 0,
    }


def run_job_failure_diag_agent(run: dict[str, Any]) -> dict[str, Any]:
    """Read-only: recent job alerts + runs + health for triage."""
    run_id = str(run.get("id") or "")
    goal = dict(run.get("goal") or {})
    step_index = 1

    def call(tool_name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        nonlocal step_index
        out = execute_tool_step(run_id=run_id, step_index=step_index, tool_name=tool_name, arguments=args or {})
        step_index += 1
        if not out.get("ok"):
            raise RuntimeError(f"{tool_name}: {out.get('error')}")
        result = out.get("result")
        return result if isinstance(result, dict) else {"result": result}

    observations: dict[str, Any] = {}
    observations["health"] = call("system.health_snapshot")
    observations["alerts"] = call(
        "jobs.list_alerts",
        {
            "limit": int(goal.get("alert_limit") or 25),
            "unresolved_only": bool(goal.get("unresolved_only", True)),
            "job_key": str(goal.get("alert_job_key") or ""),
        },
    )
    jk = str(goal.get("job_key") or "").strip()
    run_args: dict[str, Any] = {"limit": int(goal.get("run_limit") or 30)}
    if jk:
        run_args["job_key"] = jk
    observations["job_runs"] = call("jobs.list_runs", run_args)
    alerts = observations.get("alerts") or {}
    runs = observations.get("job_runs") or {}
    summary = {
        "alert_count": len(alerts.get("items") or []),
        "job_run_count": len(runs.get("items") or []),
    }
    return {
        "ok": True,
        "agent_key": JOB_FAILURE_DIAG_AGENT_KEY,
        "observations": observations,
        "plan": {"agent_key": JOB_FAILURE_DIAG_AGENT_KEY, "readonly": True},
        "planned_actions": [],
        "executed_actions": [],
        "skipped_actions": [],
        "requires_approval_actions": [],
        "warnings": [],
        "closure_status": "readonly",
        "changed_count": 0,
        "summary": summary,
    }


def run_decision_ops_read_agent(run: dict[str, Any]) -> dict[str, Any]:
    """Read-only: closure scan plus recent agent job runs for funnel alignment."""
    run_id = str(run.get("id") or "")
    goal = dict(run.get("goal") or {})
    step_index = 1

    def call(tool_name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        nonlocal step_index
        out = execute_tool_step(run_id=run_id, step_index=step_index, tool_name=tool_name, arguments=args or {})
        step_index += 1
        if not out.get("ok"):
            raise RuntimeError(f"{tool_name}: {out.get('error')}")
        result = out.get("result")
        return result if isinstance(result, dict) else {"result": result}

    observations: dict[str, Any] = {}
    observations["health"] = call("system.health_snapshot")
    observations["closure_gap_scan"] = call("business.closure_gap_scan")
    lim = int(goal.get("agent_job_run_limit") or 5)
    observations["funnel_agent_job_runs"] = call("jobs.list_runs", {"job_key": "agent.funnel_progress.daily", "limit": lim})
    observations["portfolio_reconcile_agent_runs"] = call(
        "jobs.list_runs", {"job_key": "agent.portfolio_reconcile.daily", "limit": lim}
    )
    return {
        "ok": True,
        "agent_key": DECISION_OPS_READ_AGENT_KEY,
        "observations": observations,
        "plan": {"agent_key": DECISION_OPS_READ_AGENT_KEY, "readonly": True},
        "planned_actions": [],
        "executed_actions": [],
        "skipped_actions": [],
        "requires_approval_actions": [],
        "warnings": [],
        "closure_status": "readonly",
        "changed_count": 0,
    }


def _create_child_run(
    *,
    parent: dict[str, Any],
    agent_key: str,
    goal: dict[str, Any] | None = None,
    reason: str = "",
    requires_approval: bool = False,
) -> dict[str, Any]:
    correlation_id = str(parent.get("correlation_id") or "").strip()
    policy = governance.evaluate_action(
        agent_key=str(parent.get("agent_key") or ""),
        tool_name="agents.start_run",
        arguments={"agent_key": agent_key, "requires_approval_for_writes": requires_approval},
        run_id=str(parent.get("id") or ""),
        correlation_id=correlation_id,
        requested_dry_run=True,
        record=True,
    )
    if policy.get("decision") == "blocked":
        return {"ok": False, "blocked": True, "policy_decision": policy, "agent_key": agent_key}
    child = store.create_run(
        agent_key=agent_key,
        mode="auto",
        trigger_source="orchestrator",
        actor=f"agent:{DECISION_ORCHESTRATOR_AGENT_KEY}",
        goal=goal or {},
        schedule_key=f"{parent.get('id')}:{agent_key}",
        dedupe=True,
        metadata={
            "parent_agent_key": parent.get("agent_key"),
            "created_by": DECISION_ORCHESTRATOR_AGENT_KEY,
            "reason": reason,
            "requires_approval_for_writes": requires_approval,
        },
        correlation_id=correlation_id,
        parent_run_id=str(parent.get("id") or ""),
    )
    store.append_message(
        correlation_id=correlation_id,
        run_id=str(parent.get("id") or ""),
        parent_run_id=str(parent.get("id") or ""),
        source_agent_key=DECISION_ORCHESTRATOR_AGENT_KEY,
        target_agent_key=agent_key,
        message_type="child_run_created",
        payload={"child_run_id": child.get("id"), "reason": reason, "requires_approval_for_writes": requires_approval},
    )
    child["policy_decision"] = policy
    return child


def _scan_has_gap(scan: dict[str, Any], *keys: str) -> bool:
    gaps = set(str(item) for item in scan.get("gaps", []) if str(item))
    for key in keys:
        if bool(scan.get(key)) or key in gaps:
            return True
    return False


def run_decision_orchestrator_agent(run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run.get("id") or "")
    goal = dict(run.get("goal") or {})
    step_index = 1
    observations: dict[str, Any] = {}
    created_child_runs: list[dict[str, Any]] = []
    blocked_actions: list[dict[str, Any]] = []
    policy_decisions: list[dict[str, Any]] = []
    next_human_decisions: list[dict[str, Any]] = []
    warnings: list[str] = []

    def call(tool_name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        nonlocal step_index
        out = execute_tool_step(run_id=run_id, step_index=step_index, tool_name=tool_name, arguments=args or {})
        step_index += 1
        if not out.get("ok"):
            raise RuntimeError(f"{tool_name}: {out.get('error')}")
        result = out.get("result")
        return result if isinstance(result, dict) else {"result": result}

    observations["health"] = call("system.health_snapshot")
    closure = call("business.closure_gap_scan")
    observations["closure_gap_scan"] = closure
    portfolio_scan = call("business.portfolio_closure_scan")
    observations["portfolio_closure_scan"] = portfolio_scan
    memory_args = {
        "ts_code": str(goal.get("ts_code") or ""),
        "scope": str(goal.get("scope") or ""),
        "memory_type": str(goal.get("memory_type") or ""),
        "limit": int(goal.get("memory_limit") or 20),
    }
    memory = call("memory.search_relevant", memory_args)
    memory_hits = list(memory.get("items") or [])
    observations["memory_hits"] = memory_hits
    recent_runs = store.list_runs(limit=20)
    observations["recent_agent_runs"] = recent_runs.get("items") or []

    risky_memory = [
        item
        for item in memory_hits
        if str(item.get("memory_type") or "") in {"failed_signal", "agent_failure_pattern", "execution_slippage"}
    ]
    if risky_memory:
        blocked_actions.append(
            {
                "reason": "relevant_risk_memory_found",
                "memory_ids": [item.get("id") for item in risky_memory],
                "scope": goal.get("scope") or goal.get("ts_code") or "",
            }
        )
        warnings.append("risk_memory_found")

    ingested = _state_count(closure, "ingested")
    if _scan_has_gap(closure, "funnel_ingested_backlog") or ingested > 0 or goal.get("force_funnel"):
        child = _create_child_run(
            parent=run,
            agent_key=FUNNEL_AGENT_KEY,
            goal={"max_candidates": int(goal.get("max_candidates") or 10000)},
            reason="funnel backlog detected",
            requires_approval=False,
        )
        policy_decisions.append(dict(child.get("policy_decision") or {}))
        if child.get("blocked"):
            blocked_actions.append({"agent_key": FUNNEL_AGENT_KEY, "reason": "governance_blocked_child_run", "policy_decision": child.get("policy_decision")})
        else:
            created_child_runs.append({"agent_key": FUNNEL_AGENT_KEY, "run_id": child.get("id"), "status": child.get("status")})

    if portfolio_scan.get("requires_position_reconcile") or goal.get("force_portfolio_reconcile"):
        child = _create_child_run(
            parent=run,
            agent_key=PORTFOLIO_RECONCILE_AGENT_KEY,
            goal={"limit": int(goal.get("reconcile_limit") or 500)},
            reason="executed orders require position reconcile",
            requires_approval=True,
        )
        policy_decisions.append(dict(child.get("policy_decision") or {}))
        if child.get("blocked"):
            blocked_actions.append({"agent_key": PORTFOLIO_RECONCILE_AGENT_KEY, "reason": "governance_blocked_child_run", "policy_decision": child.get("policy_decision")})
        else:
            created_child_runs.append({"agent_key": PORTFOLIO_RECONCILE_AGENT_KEY, "run_id": child.get("id"), "status": child.get("status")})
            next_human_decisions.append({"run_id": child.get("id"), "agent_key": PORTFOLIO_RECONCILE_AGENT_KEY, "decision": "approve_or_reject_position_write"})

    if portfolio_scan.get("requires_review_generation") or goal.get("force_portfolio_review"):
        child = _create_child_run(
            parent=run,
            agent_key=PORTFOLIO_REVIEW_AGENT_KEY,
            goal={"horizon_days": int(goal.get("horizon_days") or 5), "limit": int(goal.get("review_limit") or 200)},
            reason="executed orders require T+N review",
            requires_approval=True,
        )
        policy_decisions.append(dict(child.get("policy_decision") or {}))
        if child.get("blocked"):
            blocked_actions.append({"agent_key": PORTFOLIO_REVIEW_AGENT_KEY, "reason": "governance_blocked_child_run", "policy_decision": child.get("policy_decision")})
        else:
            created_child_runs.append({"agent_key": PORTFOLIO_REVIEW_AGENT_KEY, "run_id": child.get("id"), "status": child.get("status")})
            next_human_decisions.append({"run_id": child.get("id"), "agent_key": PORTFOLIO_REVIEW_AGENT_KEY, "decision": "approve_or_reject_review_write"})

    chain_plan = {
        "low_risk_auto_children": [FUNNEL_AGENT_KEY],
        "approval_required_children": [PORTFOLIO_RECONCILE_AGENT_KEY, PORTFOLIO_REVIEW_AGENT_KEY],
        "blocked_high_risk_writes": ["order_create", "order_execute", "portfolio_overwrite", "decision_action_write"],
    }
    if not created_child_runs and not blocked_actions:
        next_human_decisions.append({"decision": "no_immediate_action", "reason": "no closure gap detected"})

    return {
        "ok": True,
        "agent_key": DECISION_ORCHESTRATOR_AGENT_KEY,
        "observations": observations,
        "plan": {"agent_key": DECISION_ORCHESTRATOR_AGENT_KEY, "chain_plan": chain_plan},
        "chain_plan": chain_plan,
        "created_child_runs": created_child_runs,
        "blocked_actions": blocked_actions,
        "policy_decisions": policy_decisions,
        "degraded_actions": [],
        "memory_hits": memory_hits,
        "next_human_decisions": next_human_decisions,
        "planned_actions": created_child_runs,
        "executed_actions": [],
        "skipped_actions": blocked_actions,
        "requires_approval_actions": [],
        "warnings": warnings,
        "closure_status": "planned_children" if created_child_runs else "readonly",
        "changed_count": 0,
        "correlation_id": run.get("correlation_id") or "",
    }


def _safe_json_loads(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw or ""))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _memory_exists(memory_type: str, source_run_id: str, summary: str) -> bool:
    existing = store.list_memory_items(memory_type=memory_type, source_agent_key="", status="", limit=200).get("items") or []
    digest = _stable_key(source_run_id, summary)
    for item in existing:
        evidence = item.get("evidence") or {}
        if evidence.get("dedupe_key") == digest:
            return True
    return False


def run_memory_refresh_agent(run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run.get("id") or "")
    goal = dict(run.get("goal") or {})
    limit = int(goal.get("limit") or 100)
    created: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    conn = db.connect()
    store.apply_row_factory(conn)
    try:
        store.ensure_agent_tables(conn)
        if db.table_exists(conn, "portfolio_reviews") and db.table_exists(conn, "portfolio_orders"):
            rows = conn.execute(
                """
                SELECT r.id, r.order_id, r.review_note, r.review_tag, r.slippage, r.created_at,
                       o.ts_code, o.decision_action_id
                FROM portfolio_reviews r
                LEFT JOIN portfolio_orders o ON o.id = r.order_id
                ORDER BY r.created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            for row in store.rows_to_dicts(rows):
                note = str(row.get("review_note") or "").strip()
                if not note:
                    skipped.append({"source": "portfolio_review", "id": row.get("id"), "reason": "empty_review_note"})
                    continue
                parsed = _safe_json_loads(note)
                hint = parsed.get("rule_correction_hint") or parsed.get("review_note") or note[:280]
                summary = str(hint or note[:280])
                dedupe_key = _stable_key(row.get("id"), summary)
                if _memory_exists("review_rule_correction", str(row.get("id") or ""), summary):
                    skipped.append({"source": "portfolio_review", "id": row.get("id"), "reason": "duplicate_memory"})
                    continue
                result = store.record_memory_item(
                    memory_type="review_rule_correction",
                    source_run_id=run_id,
                    source_agent_key=MEMORY_REFRESH_AGENT_KEY,
                    ts_code=str(row.get("ts_code") or ""),
                    scope="portfolio_review",
                    summary=summary,
                    evidence={**row, "parsed_review": parsed, "dedupe_key": dedupe_key},
                    score=0.7,
                    status="active",
                )
                created.append(result.get("item") or {})
        failed_runs = store.list_runs(status="failed", limit=limit).get("items") or []
        for item in failed_runs:
            err = str(item.get("error_text") or (item.get("result") or {}).get("error") or "").strip()
            if not err:
                continue
            summary = f"{item.get('agent_key')} failed: {err[:240]}"
            if _memory_exists("agent_failure_pattern", str(item.get("id") or ""), summary):
                skipped.append({"source": "agent_run", "id": item.get("id"), "reason": "duplicate_memory"})
                continue
            result = store.record_memory_item(
                memory_type="agent_failure_pattern",
                source_run_id=str(item.get("id") or ""),
                source_agent_key=str(item.get("agent_key") or ""),
                scope="agent_runtime",
                summary=summary,
                evidence={
                    "run_id": item.get("id"),
                    "agent_key": item.get("agent_key"),
                    "status": item.get("status"),
                    "error_text": err,
                    "dedupe_key": _stable_key(item.get("id"), summary),
                },
                score=0.6,
                status="active",
            )
            created.append(result.get("item") or {})
    finally:
        conn.close()

    store.append_message(
        correlation_id=str(run.get("correlation_id") or ""),
        run_id=run_id,
        source_agent_key=MEMORY_REFRESH_AGENT_KEY,
        message_type="memory_refresh_completed",
        payload={"created_count": len(created), "skipped_count": len(skipped)},
    )
    return {
        "ok": True,
        "agent_key": MEMORY_REFRESH_AGENT_KEY,
        "plan": {"agent_key": MEMORY_REFRESH_AGENT_KEY, "sources": ["portfolio_reviews", "failed_agent_runs"]},
        "planned_actions": [],
        "executed_actions": created,
        "skipped_actions": skipped,
        "requires_approval_actions": [],
        "warnings": [],
        "closure_status": "memory_refreshed",
        "changed_count": len(created),
    }


def run_governance_quality_refresh_agent(run: dict[str, Any]) -> dict[str, Any]:
    goal = dict(run.get("goal") or {})
    window_days = int(goal.get("window_days") or 7)
    agent_key = str(goal.get("agent_key") or "")
    snapshot = governance.compute_quality_snapshot(agent_key=agent_key, window_days=window_days, persist=True)
    store.append_message(
        correlation_id=str(run.get("correlation_id") or ""),
        run_id=str(run.get("id") or ""),
        source_agent_key=GOVERNANCE_QUALITY_REFRESH_AGENT_KEY,
        message_type="governance_quality_refresh_completed",
        payload={"item_count": len(snapshot.get("items") or []), "window_days": window_days},
    )
    return {
        "ok": True,
        "agent_key": GOVERNANCE_QUALITY_REFRESH_AGENT_KEY,
        "plan": {"agent_key": GOVERNANCE_QUALITY_REFRESH_AGENT_KEY, "window_days": window_days},
        "planned_actions": [],
        "executed_actions": snapshot.get("items") or [],
        "skipped_actions": [],
        "requires_approval_actions": [],
        "warnings": [],
        "closure_status": "governance_quality_refreshed",
        "changed_count": len(snapshot.get("items") or []),
    }
