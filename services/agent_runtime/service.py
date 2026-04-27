from __future__ import annotations

import socket
import time
from typing import Any

import db_compat as db

from . import config, store
from .agents import (
    DECISION_OPS_READ_AGENT_KEY,
    FUNNEL_AGENT_KEY,
    JOB_FAILURE_DIAG_AGENT_KEY,
    DECISION_ORCHESTRATOR_AGENT_KEY,
    GOVERNANCE_QUALITY_REFRESH_AGENT_KEY,
    MEMORY_REFRESH_AGENT_KEY,
    PORTFOLIO_RECONCILE_AGENT_KEY,
    PORTFOLIO_REVIEW_AGENT_KEY,
    run_decision_orchestrator_agent,
    run_decision_ops_read_agent,
    run_funnel_progress_agent,
    run_governance_quality_refresh_agent,
    run_job_failure_diag_agent,
    run_memory_refresh_agent,
    run_portfolio_reconcile_agent,
    run_portfolio_review_agent,
)
from .executor import execute_existing_step


def ensure_agent_tables(conn=None) -> None:
    if conn is not None:
        store.ensure_agent_tables(conn)
        return
    owned = db.connect()
    try:
        store.ensure_agent_tables(owned)
    finally:
        owned.close()


def create_run(
    *,
    agent_key: str,
    mode: str = "auto",
    trigger_source: str = "manual",
    actor: str = "",
    goal: dict[str, Any] | None = None,
    schedule_key: str = "",
    dedupe: bool = True,
    metadata: dict[str, Any] | None = None,
    correlation_id: str = "",
    parent_run_id: str = "",
) -> dict[str, Any]:
    return store.create_run(
        agent_key=agent_key,
        mode=mode,
        trigger_source=trigger_source,
        actor=actor,
        goal=goal,
        schedule_key=schedule_key,
        dedupe=dedupe,
        metadata=metadata,
        correlation_id=correlation_id,
        parent_run_id=parent_run_id,
    )


def list_runs(*, agent_key: str = "", status: str = "", limit: int = 50, correlation_id: str = "", parent_run_id: str = "") -> dict[str, Any]:
    return store.list_runs(
        agent_key=agent_key,
        status=status,
        limit=limit,
        correlation_id=correlation_id,
        parent_run_id=parent_run_id,
    )


def get_run(run_id: str) -> dict[str, Any] | None:
    return store.get_run(run_id)


def cancel_run(run_id: str, *, actor: str = "", reason: str = "") -> dict[str, Any]:
    return store.cancel_run(run_id, actor=actor, reason=reason)


def approve_run(run_id: str, *, actor: str, reason: str = "", idempotency_key: str = "") -> dict[str, Any]:
    return decide_run(run_id, actor=actor, reason=reason, idempotency_key=idempotency_key, decision="approved")


def reject_run(run_id: str, *, actor: str, reason: str = "", idempotency_key: str = "") -> dict[str, Any]:
    return decide_run(run_id, actor=actor, reason=reason, idempotency_key=idempotency_key, decision="rejected")


def decide_run(
    run_id: str,
    *,
    actor: str,
    reason: str = "",
    idempotency_key: str = "",
    decision: str = "approved",
) -> dict[str, Any]:
    decision = str(decision or "approved").strip().lower()
    if decision not in {"approved", "rejected"}:
        return {"ok": False, "error": "invalid_approval_decision"}
    run = store.get_run(run_id)
    if not run:
        return {"ok": False, "error": "agent_run_not_found"}
    if run.get("status") != "waiting_approval":
        return {"ok": False, "error": "agent_run_not_waiting_approval", "run": run}
    if not str(reason or "").strip():
        return {"ok": False, "error": "approval_reason_required", "run": run}
    store.record_approval(
        run_id,
        actor=actor,
        reason=reason,
        idempotency_key=idempotency_key,
        decision=decision,
    )
    if decision == "rejected":
        store.update_run(
            run_id,
            status="cancelled",
            result={**dict(run.get("result") or {}), "approval_decision": "rejected", "approval_reason": reason},
            approval_required=False,
            finished=True,
        )
        return {"ok": True, "run": store.get_run(run_id)}
    return resume_run(run_id, actor=actor, reason=reason)


def resume_run(run_id: str, *, actor: str = "", reason: str = "") -> dict[str, Any]:
    run = store.get_run(run_id)
    if not run:
        return {"ok": False, "error": "agent_run_not_found"}
    pending = store.list_pending_steps(run_id)
    if not pending:
        store.update_run(run_id, status="succeeded", approval_required=False, finished=True)
        return {"ok": True, "run": store.get_run(run_id), "resumed_steps": 0}
    executed: list[dict[str, Any]] = []
    errors: list[str] = []
    agent_key = str(run.get("agent_key") or "")
    for step in pending:
        args = dict(step.get("args") or {})
        args.update(
            {
                "dry_run": False,
                "confirm": True,
                "actor": f"agent:{agent_key}",
                "reason": str(reason or "approved agent write"),
            }
        )
        if not str(args.get("idempotency_key") or "").strip():
            args["idempotency_key"] = f"agent:{agent_key}:{step.get('tool_name')}:{run.get('schedule_key') or run_id}:{step.get('id')}"
        out = execute_existing_step(step=step, arguments=args)
        executed.append({"tool_name": step.get("tool_name"), **out})
        if not out.get("ok"):
            errors.append(str(out.get("error") or "step_failed"))
            break
    base_result = dict(run.get("result") or {})
    previous_executed = list(base_result.get("executed_actions") or [])
    base_result["executed_actions"] = previous_executed + executed
    base_result["approval_decision"] = "approved"
    base_result["approval_reason"] = reason
    base_result["changed_count"] = sum(
        int(((item.get("result") or {}) if isinstance(item, dict) else {}).get("changed_count") or 0)
        for item in base_result["executed_actions"]
        if isinstance(item, dict)
    )
    if errors:
        store.update_run(
            run_id,
            status="failed",
            result=base_result,
            error_text="; ".join(errors),
            approval_required=False,
            finished=True,
        )
        return {"ok": False, "error": "; ".join(errors), "run": store.get_run(run_id)}
    store.update_run(
        run_id,
        status="succeeded",
        result=base_result,
        approval_required=False,
        finished=True,
    )
    return {"ok": True, "run": store.get_run(run_id), "resumed_steps": len(executed)}


def run_one(run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run.get("id") or "")
    try:
        agent_key = str(run.get("agent_key") or "")
        if agent_key == FUNNEL_AGENT_KEY:
            result = run_funnel_progress_agent(run)
        elif agent_key == PORTFOLIO_RECONCILE_AGENT_KEY:
            result = run_portfolio_reconcile_agent(run)
        elif agent_key == PORTFOLIO_REVIEW_AGENT_KEY:
            result = run_portfolio_review_agent(run)
        elif agent_key == JOB_FAILURE_DIAG_AGENT_KEY:
            result = run_job_failure_diag_agent(run)
        elif agent_key == DECISION_OPS_READ_AGENT_KEY:
            result = run_decision_ops_read_agent(run)
        elif agent_key == DECISION_ORCHESTRATOR_AGENT_KEY:
            result = run_decision_orchestrator_agent(run)
        elif agent_key == MEMORY_REFRESH_AGENT_KEY:
            result = run_memory_refresh_agent(run)
        elif agent_key == GOVERNANCE_QUALITY_REFRESH_AGENT_KEY:
            result = run_governance_quality_refresh_agent(run)
        else:
            raise ValueError(f"unknown_agent_key:{run.get('agent_key')}")
        plan = result.get("plan") if isinstance(result.get("plan"), dict) else {}
        requires_approval = bool(result.get("requires_approval_actions"))
        status = "waiting_approval" if requires_approval else "succeeded"
        store.update_run(
            run_id,
            status=status,
            plan=plan,
            result=result,
            approval_required=requires_approval,
            finished=not requires_approval,
        )
        return {"ok": True, "run": store.get_run(run_id)}
    except Exception as exc:
        store.update_run(
            run_id,
            status="failed",
            error_text=str(exc),
            result={"ok": False, "error": str(exc)},
            finished=True,
        )
        return {"ok": False, "error": str(exc), "run": store.get_run(run_id)}


def run_next_once(*, worker_id: str = "") -> dict[str, Any]:
    worker = worker_id or f"{socket.gethostname()}:{id(object())}"
    run = store.claim_next_run(worker_id=worker)
    if not run:
        return {"ok": True, "processed": 0}
    result = run_one(run)
    result["processed"] = 1
    return result


def run_worker_loop(*, once: bool = False, poll_seconds: float | None = None) -> None:
    sleep_s = config.worker_poll_seconds() if poll_seconds is None else max(0.2, float(poll_seconds or 1.0))
    while True:
        out = run_next_once()
        if once:
            return
        if int(out.get("processed") or 0) <= 0:
            time.sleep(sleep_s)


def list_memory_items(**kwargs) -> dict[str, Any]:
    return store.list_memory_items(**kwargs)


def search_memory_items(**kwargs) -> dict[str, Any]:
    return store.search_memory_items(**kwargs)


def get_correlation_timeline(correlation_id: str, *, limit: int = 200) -> dict[str, Any]:
    correlation_id = str(correlation_id or "").strip()
    if not correlation_id:
        return {"ok": False, "error": "correlation_id_required"}
    runs = store.list_runs(correlation_id=correlation_id, limit=limit).get("items") or []
    messages = store.list_messages(correlation_id=correlation_id, limit=limit).get("items") or []
    events: list[dict[str, Any]] = []
    for run in runs:
        events.append(
            {
                "type": "run",
                "at": run.get("created_at") or "",
                "run_id": run.get("id"),
                "agent_key": run.get("agent_key"),
                "status": run.get("status"),
                "parent_run_id": run.get("parent_run_id") or "",
                "payload": run,
            }
        )
        for step in run.get("steps") or []:
            events.append(
                {
                    "type": "step",
                    "at": step.get("created_at") or run.get("created_at") or "",
                    "run_id": run.get("id"),
                    "agent_key": run.get("agent_key"),
                    "tool_name": step.get("tool_name"),
                    "status": step.get("status"),
                    "audit_id": step.get("audit_id"),
                    "payload": step,
                }
            )
    for msg in messages:
        events.append(
            {
                "type": "message",
                "at": msg.get("created_at") or "",
                "run_id": msg.get("run_id") or "",
                "agent_key": msg.get("source_agent_key") or "",
                "target_agent_key": msg.get("target_agent_key") or "",
                "message_type": msg.get("message_type") or "",
                "payload": msg,
            }
        )
    events.sort(key=lambda item: str(item.get("at") or ""))
    return {"ok": True, "correlation_id": correlation_id, "runs": runs, "messages": messages, "events": events[-limit:]}
