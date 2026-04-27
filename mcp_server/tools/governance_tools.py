from __future__ import annotations

from typing import Any

from mcp_server import schemas
from services.agent_runtime import governance, store

from .common import require_write_allowed


def quality_snapshot(args: schemas.GovernanceQualityArgs) -> dict[str, Any]:
    return governance.quality_snapshot(agent_key=args.agent_key, window_days=args.window_days, refresh=bool(args.refresh))


def list_rules(args: schemas.GovernanceRuleListArgs) -> dict[str, Any]:
    return store.list_governance_rules(
        agent_key=args.agent_key,
        tool_name=args.tool_name,
        enabled=args.enabled,
        limit=args.limit,
    )


def upsert_rule(args: schemas.GovernanceRuleUpsertArgs) -> dict[str, Any]:
    decision = str(args.decision or "").strip()
    risk_level = str(args.risk_level or "").strip()
    if decision not in governance.DECISIONS:
        raise ValueError(f"invalid_governance_decision:{decision}")
    if risk_level not in governance.RISK_LEVELS:
        raise ValueError(f"invalid_risk_level:{risk_level}")
    planned = {
        "rule_key": args.rule_key,
        "agent_key": args.agent_key,
        "tool_name": args.tool_name,
        "risk_level": risk_level,
        "decision": decision,
        "enabled": args.enabled,
        "thresholds": args.thresholds,
    }
    if args.dry_run:
        return {"ok": True, "dry_run": True, "planned_changes": [planned], "changed_count": 0, "warnings": []}
    require_write_allowed(args)
    result = store.upsert_governance_rule(
        rule_key=args.rule_key,
        agent_key=args.agent_key,
        tool_name=args.tool_name,
        risk_level=risk_level,
        decision=decision,
        enabled=bool(args.enabled),
        thresholds=args.thresholds,
        reason=args.reason,
        actor=args.actor,
    )
    return {"ok": True, "dry_run": False, "planned_changes": [planned], "changed_count": 1, "warnings": [], "item": result.get("item")}


def evaluate_action(args: schemas.GovernanceEvaluateArgs) -> dict[str, Any]:
    return governance.evaluate_action(
        agent_key=args.agent_key,
        tool_name=args.tool_name,
        arguments=args.arguments,
        run_id=args.run_id,
        correlation_id=args.correlation_id,
        requested_dry_run=bool(args.requested_dry_run),
        record=False,
    )
