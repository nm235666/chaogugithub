from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import db_compat as db
from mcp_server import config as mcp_config
from mcp_server.audit import AUDIT_TABLE, ensure_audit_table

from . import store
from .platform_allowlist import FUNNEL_AUTO_WRITE_TOOLS


LOW_RISK_AUTO_TOOLS = set(FUNNEL_AUTO_WRITE_TOOLS) | {"memory.record_item"}
HIGH_RISK_TOOLS = {
    "business.reconcile_portfolio_positions",
    "business.generate_portfolio_order_reviews",
    "business.run_decision_snapshot",
    "orders.create",
    "orders.execute",
    "portfolio.overwrite",
    "decision.action.write",
}
DECISIONS = {"allow", "dry_run_only", "requires_approval", "blocked"}
RISK_LEVELS = {"low", "medium", "high", "critical"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _today() -> str:
    return _utc_now().strftime("%Y%m%d")


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text[: len(fmt)], fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _pct(num: int, den: int) -> float:
    return round(float(num) / float(den), 4) if den else 0.0


def _risk_status(risk_score: float) -> str:
    if risk_score >= 0.85:
        return "blocked"
    if risk_score >= 0.35:
        return "degraded"
    return "healthy"


def _risk_level_for_tool(tool_name: str) -> str:
    if tool_name in HIGH_RISK_TOOLS:
        return "high"
    if tool_name in LOW_RISK_AUTO_TOOLS:
        return "low"
    if tool_name.startswith("business."):
        return "medium"
    return "low"


def compute_quality_snapshot(*, agent_key: str = "", window_days: int = 7, persist: bool = True) -> dict[str, Any]:
    conn = db.connect()
    store.apply_row_factory(conn)
    try:
        store.ensure_agent_tables(conn)
        ensure_audit_table(conn)
        since = (_utc_now() - timedelta(days=max(1, int(window_days or 7)))).strftime("%Y-%m-%dT%H:%M:%SZ")
        params: list[Any] = [since]
        where = "created_at >= ?"
        if str(agent_key or "").strip():
            where += " AND agent_key = ?"
            params.append(str(agent_key).strip())
        rows = conn.execute(
            f"SELECT * FROM {store.RUN_TABLE} WHERE {where} ORDER BY created_at DESC",
            tuple(params),
        ).fetchall()
        runs = [store.row_to_dict(row) for row in rows]
        grouped: dict[str, list[dict[str, Any]]] = {}
        for run in runs:
            grouped.setdefault(str(run.get("agent_key") or ""), []).append(run)

        output: list[dict[str, Any]] = []
        for key, items in grouped.items():
            total = len(items)
            succeeded = sum(1 for item in items if item.get("status") == "succeeded")
            failed = sum(1 for item in items if item.get("status") == "failed")
            waiting = sum(1 for item in items if item.get("status") == "waiting_approval")
            changed_count = 0
            durations: list[float] = []
            for item in items:
                result = store.loads(item.get("result_json"), {})
                changed_count += int(result.get("changed_count") or 0)
                start = _parse_ts(item.get("created_at"))
                end = _parse_ts(item.get("finished_at")) or _parse_ts(item.get("updated_at"))
                if start and end and end >= start:
                    durations.append((end - start).total_seconds())
            approvals = conn.execute(
                f"""
                SELECT decision, COUNT(*) AS cnt
                FROM {store.APPROVAL_TABLE}
                WHERE run_id IN ({','.join(['?'] * len(items))})
                GROUP BY decision
                """
                if items
                else f"SELECT decision, 0 AS cnt FROM {store.APPROVAL_TABLE} WHERE 1=0",
                tuple(str(item.get("id") or "") for item in items),
            ).fetchall()
            approval_counts = {str(store.row_to_dict(row).get("decision") or ""): int(store.row_to_dict(row).get("cnt") or 0) for row in approvals}
            conflicts = conn.execute(
                f"""
                SELECT COUNT(*) AS cnt
                FROM {AUDIT_TABLE}
                WHERE actor = ? AND created_at >= ? AND (
                    error_text LIKE '%conflict%' OR error_text LIKE '%requires_manual_review%'
                    OR result_json LIKE '%requires_manual_review%'
                )
                """,
                (f"agent:{key}", since),
            ).fetchone()
            conflict_count = int(store.row_to_dict(conflicts).get("cnt") or 0)
            failure_memories = store.list_memory_items(
                memory_type="agent_failure_pattern",
                source_agent_key=key,
                status="active",
                limit=20,
            ).get("items") or []
            success_rate = _pct(succeeded, total)
            failure_rate = _pct(failed, total)
            approval_total = int(approval_counts.get("approved") or 0) + int(approval_counts.get("rejected") or 0)
            approval_pass_rate = _pct(int(approval_counts.get("approved") or 0), approval_total)
            risk_score = min(
                1.0,
                failure_rate
                + (0.25 if total >= 3 and success_rate < 0.8 else 0)
                + (0.2 if conflict_count else 0)
                + (0.2 if failure_memories else 0),
            )
            item = {
                "agent_key": key,
                "metric_date": _today(),
                "window_days": int(window_days or 7),
                "total_runs": total,
                "succeeded_runs": succeeded,
                "failed_runs": failed,
                "waiting_approval_runs": waiting,
                "approval_approved": int(approval_counts.get("approved") or 0),
                "approval_rejected": int(approval_counts.get("rejected") or 0),
                "changed_count": changed_count,
                "conflict_count": conflict_count,
                "avg_duration_seconds": round(sum(durations) / len(durations), 3) if durations else 0,
                "success_rate": success_rate,
                "failure_rate": failure_rate,
                "approval_pass_rate": approval_pass_rate,
                "risk_score": round(risk_score, 4),
                "risk_status": _risk_status(risk_score),
                "evidence": {
                    "failure_memory_count": len(failure_memories),
                    "recent_failure_memory_ids": [item.get("id") for item in failure_memories[:5]],
                },
            }
            if persist:
                stored = store.upsert_quality_score(item)
                output.append(stored.get("item") or item)
            else:
                output.append(item)
        return {"ok": True, "items": output, "window_days": int(window_days or 7)}
    finally:
        conn.close()


def quality_snapshot(*, agent_key: str = "", window_days: int = 7, refresh: bool = False) -> dict[str, Any]:
    if refresh:
        return compute_quality_snapshot(agent_key=agent_key, window_days=window_days, persist=True)
    items = store.list_quality_scores(agent_key=agent_key, metric_date=_today(), limit=100).get("items") or []
    if not items:
        return compute_quality_snapshot(agent_key=agent_key, window_days=window_days, persist=True)
    return {"ok": True, "items": items, "window_days": int(window_days or 7)}


def _matching_rules(agent_key: str, tool_name: str) -> list[dict[str, Any]]:
    rules = store.list_governance_rules(agent_key=agent_key, tool_name=tool_name, enabled="1", limit=100).get("items") or []
    exact = []
    generic = []
    for rule in rules:
        a = str(rule.get("agent_key") or "")
        t = str(rule.get("tool_name") or "")
        if a == agent_key and t == tool_name:
            exact.append(rule)
        else:
            generic.append(rule)
    return exact + generic


def evaluate_action(
    *,
    agent_key: str,
    tool_name: str,
    arguments: dict[str, Any] | None = None,
    run_id: str = "",
    step_id: str = "",
    correlation_id: str = "",
    requested_dry_run: bool = True,
    record: bool = False,
) -> dict[str, Any]:
    agent_key = str(agent_key or "").strip()
    tool_name = str(tool_name or "").strip()
    args = dict(arguments or {})
    risk_level = _risk_level_for_tool(tool_name)
    decision = "allow"
    reason = "low_risk_allowlisted"
    evidence: dict[str, Any] = {"tool_risk_level": risk_level}

    if tool_name in HIGH_RISK_TOOLS:
        decision = "requires_approval"
        reason = "high_risk_tool_requires_approval"
    elif not requested_dry_run and not bool(mcp_config.MCP_WRITE_ENABLED):
        decision = "dry_run_only"
        reason = "mcp_write_disabled"
    elif not requested_dry_run and tool_name not in LOW_RISK_AUTO_TOOLS:
        decision = "requires_approval"
        reason = "tool_not_low_risk_auto_allowlisted"

    snapshot = quality_snapshot(agent_key=agent_key, refresh=False)
    score = (snapshot.get("items") or [{}])[0] if snapshot.get("items") else {}
    evidence["quality_score"] = score
    if decision == "allow" and score.get("risk_status") == "blocked":
        decision = "blocked"
        reason = "agent_quality_blocked"
    elif decision == "allow" and score.get("risk_status") == "degraded" and not requested_dry_run:
        decision = "dry_run_only"
        reason = "agent_quality_degraded"

    failures = store.list_memory_items(
        memory_type="agent_failure_pattern",
        source_agent_key=agent_key,
        status="active",
        limit=10,
    ).get("items") or []
    if failures and decision == "allow" and not requested_dry_run:
        decision = "dry_run_only"
        reason = "active_agent_failure_memory"
        evidence["failure_memory_ids"] = [item.get("id") for item in failures[:5]]

    for rule in _matching_rules(agent_key, tool_name):
        rule_decision = str(rule.get("decision") or "").strip()
        if rule_decision in DECISIONS:
            decision = rule_decision
            risk_level = str(rule.get("risk_level") or risk_level)
            reason = f"governance_rule:{rule.get('rule_key')}"
            evidence["matched_rule"] = rule
            break

    if tool_name in HIGH_RISK_TOOLS and decision == "allow":
        decision = "requires_approval"
        reason = "high_risk_tool_requires_approval"
    if not requested_dry_run and not bool(mcp_config.MCP_WRITE_ENABLED) and decision == "allow":
        decision = "dry_run_only"
        reason = "mcp_write_disabled"

    out = {
        "ok": True,
        "agent_key": agent_key,
        "tool_name": tool_name,
        "requested_dry_run": requested_dry_run,
        "decision": decision,
        "risk_level": risk_level,
        "reason": reason,
        "evidence": evidence,
        "effective_arguments": {**args, "dry_run": True} if decision == "dry_run_only" else args,
    }
    if record:
        rec = store.record_policy_decision(
            run_id=run_id,
            step_id=step_id,
            correlation_id=correlation_id,
            agent_key=agent_key,
            tool_name=tool_name,
            requested_dry_run=requested_dry_run,
            decision=decision,
            risk_level=risk_level,
            reason=reason,
            evidence=evidence,
        )
        out["policy_decision_id"] = rec.get("id")
    return out


def apply_policy_to_arguments(
    *,
    run: dict[str, Any],
    tool_name: str,
    arguments: dict[str, Any],
    record: bool = True,
) -> dict[str, Any]:
    args = dict(arguments or {})
    decision = evaluate_action(
        agent_key=str(run.get("agent_key") or ""),
        tool_name=tool_name,
        arguments=args,
        run_id=str(run.get("id") or ""),
        correlation_id=str(run.get("correlation_id") or ""),
        requested_dry_run=bool(args.get("dry_run", True)),
        record=record,
    )
    effective = dict(args)
    if decision.get("decision") == "dry_run_only":
        effective["dry_run"] = True
        effective["confirm"] = False
    return {**decision, "arguments": effective}
