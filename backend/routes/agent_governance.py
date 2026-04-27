from __future__ import annotations

from urllib.parse import parse_qs

from services.agent_runtime import governance, store


def _actor(deps: dict) -> str:
    auth = deps.get("auth_context") or {}
    user = auth.get("user") or {}
    return str(user.get("username") or auth.get("auth_mode") or "api").strip()


def _limit(params, default: int = 100) -> int:
    return max(1, min(int(params.get("limit", [str(default)])[0] or default), 500))


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if not parsed.path.startswith("/api/agent-governance"):
        return False
    params = parse_qs(parsed.query)
    if parsed.path == "/api/agent-governance/quality":
        try:
            window_days = int(params.get("window_days", ["7"])[0] or 7)
        except ValueError:
            handler._send_json({"ok": False, "error": "window_days must be integer"}, status=400)
            return True
        result = governance.quality_snapshot(
            agent_key=params.get("agent_key", [""])[0].strip(),
            window_days=window_days,
            refresh=params.get("refresh", [""])[0].strip().lower() in {"1", "true", "yes", "on"},
        )
        handler._send_json(result)
        return True
    if parsed.path == "/api/agent-governance/rules":
        handler._send_json(
            store.list_governance_rules(
                agent_key=params.get("agent_key", [""])[0].strip(),
                tool_name=params.get("tool_name", [""])[0].strip(),
                enabled=params.get("enabled", [""])[0].strip(),
                limit=_limit(params, 100),
            )
        )
        return True
    if parsed.path == "/api/agent-governance/policy-decisions":
        handler._send_json(
            store.list_policy_decisions(
                agent_key=params.get("agent_key", [""])[0].strip(),
                tool_name=params.get("tool_name", [""])[0].strip(),
                decision=params.get("decision", [""])[0].strip(),
                run_id=params.get("run_id", [""])[0].strip(),
                limit=_limit(params, 100),
            )
        )
        return True
    return False


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if not parsed.path.startswith("/api/agent-governance"):
        return False
    if parsed.path == "/api/agent-governance/recompute":
        result = governance.compute_quality_snapshot(
            agent_key=str(payload.get("agent_key") or "").strip(),
            window_days=int(payload.get("window_days") or 7),
            persist=True,
        )
        handler._send_json(result)
        return True
    if parsed.path == "/api/agent-governance/rules":
        result = store.upsert_governance_rule(
            rule_key=str(payload.get("rule_key") or "").strip(),
            agent_key=str(payload.get("agent_key") or "").strip(),
            tool_name=str(payload.get("tool_name") or "").strip(),
            risk_level=str(payload.get("risk_level") or "low").strip(),
            decision=str(payload.get("decision") or "allow").strip(),
            enabled=bool(payload.get("enabled", True)),
            thresholds=payload.get("thresholds") if isinstance(payload.get("thresholds"), dict) else {},
            reason=str(payload.get("reason") or "api governance rule update").strip(),
            actor=str(payload.get("actor") or _actor(deps)),
        )
        handler._send_json(result, status=200 if result.get("ok") else 400)
        return True
    return False
