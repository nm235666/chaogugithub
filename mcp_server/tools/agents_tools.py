from __future__ import annotations

from typing import Any

from mcp_server import schemas
from services.agent_runtime import cancel_run as cancel_agent_run
from services.agent_runtime import create_run, get_run, list_runs
from services.agent_runtime import decide_run


def start_run(args: schemas.AgentStartRunArgs) -> dict[str, Any]:
    run = create_run(
        agent_key=args.agent_key,
        mode=args.mode,
        trigger_source=args.trigger_source or "mcp",
        actor=args.actor or "mcp",
        goal=args.goal,
        schedule_key=args.schedule_key,
        dedupe=bool(args.dedupe),
        metadata=args.metadata,
        correlation_id=args.correlation_id,
        parent_run_id=args.parent_run_id,
    )
    return {"ok": True, "run": run}


def get_agent_run(args: schemas.AgentGetRunArgs) -> dict[str, Any]:
    run = get_run(args.run_id)
    if not run:
        return {"ok": False, "error": "agent_run_not_found"}
    return {"ok": True, "run": run}


def list_agent_runs(args: schemas.AgentListRunsArgs) -> dict[str, Any]:
    return list_runs(agent_key=args.agent_key, status=args.status, limit=args.limit)


def cancel_agent_run_tool(args: schemas.AgentCancelRunArgs) -> dict[str, Any]:
    return cancel_agent_run(args.run_id, actor=args.actor, reason=args.reason)


def approve_agent_run(args: schemas.AgentApprovalArgs) -> dict[str, Any]:
    return decide_run(
        args.run_id,
        actor=args.actor,
        reason=args.reason,
        idempotency_key=args.idempotency_key,
        decision="approved",
    )


def reject_agent_run(args: schemas.AgentApprovalArgs) -> dict[str, Any]:
    return decide_run(
        args.run_id,
        actor=args.actor,
        reason=args.reason,
        idempotency_key=args.idempotency_key,
        decision="rejected",
    )
