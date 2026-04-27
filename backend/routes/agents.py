from __future__ import annotations

import re
from urllib.parse import parse_qs

from mcp_server.audit import list_recent_tool_audits

from services.agent_runtime.health import build_agent_stack_health_summary
from services.agent_runtime import (
    decide_run,
    cancel_run,
    create_run,
    get_correlation_timeline,
    get_run,
    list_memory_items,
    list_runs,
    resume_run,
)


_RUN_RE = re.compile(r"^/api/agents/runs/([^/]+)(/(cancel|approve|resume))?$")


def _actor(deps: dict) -> str:
    auth = deps.get("auth_context") or {}
    user = auth.get("user") or {}
    return str(user.get("username") or auth.get("auth_mode") or "api").strip()


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if not parsed.path.startswith("/api/agents"):
        return False
    if parsed.path == "/api/agents/health":
        handler._send_json({"ok": True, **build_agent_stack_health_summary()})
        return True
    if parsed.path.startswith("/api/agents/mcp-audit"):
        params = parse_qs(parsed.query)
        try:
            limit = int(params.get("limit", ["100"])[0] or 100)
        except ValueError:
            handler._send_json({"ok": False, "error": "limit must be integer"}, status=400)
            return True
        dry_only = str(params.get("dry_run_only", [""])[0] or "").strip().lower() in {"1", "true", "yes", "on"}
        write_only = str(params.get("write_only", [""])[0] or "").strip().lower() in {"1", "true", "yes", "on"}
        handler._send_json(list_recent_tool_audits(limit=limit, dry_run_only=dry_only, write_only=write_only))
        return True
    if parsed.path == "/api/agents/runs":
        params = parse_qs(parsed.query)
        agent_key = params.get("agent_key", [""])[0].strip()
        status = params.get("status", [""])[0].strip()
        correlation_id = params.get("correlation_id", [""])[0].strip()
        parent_run_id = params.get("parent_run_id", [""])[0].strip()
        try:
            limit = int(params.get("limit", ["50"])[0] or 50)
        except ValueError:
            handler._send_json({"ok": False, "error": "limit must be integer"}, status=400)
            return True
        handler._send_json(list_runs(agent_key=agent_key, status=status, limit=limit, correlation_id=correlation_id, parent_run_id=parent_run_id))
        return True
    if parsed.path == "/api/agents/timeline":
        params = parse_qs(parsed.query)
        try:
            limit = int(params.get("limit", ["200"])[0] or 200)
        except ValueError:
            handler._send_json({"ok": False, "error": "limit must be integer"}, status=400)
            return True
        result = get_correlation_timeline(params.get("correlation_id", [""])[0], limit=limit)
        handler._send_json(result, status=200 if result.get("ok") else 400)
        return True
    if parsed.path == "/api/agents/memory":
        params = parse_qs(parsed.query)
        try:
            limit = int(params.get("limit", ["50"])[0] or 50)
        except ValueError:
            handler._send_json({"ok": False, "error": "limit must be integer"}, status=400)
            return True
        handler._send_json(
            list_memory_items(
                memory_type=params.get("memory_type", [""])[0].strip(),
                ts_code=params.get("ts_code", [""])[0].strip(),
                scope=params.get("scope", [""])[0].strip(),
                source_agent_key=params.get("source_agent_key", [""])[0].strip(),
                status=params.get("status", ["active"])[0].strip(),
                limit=limit,
            )
        )
        return True
    match = _RUN_RE.match(parsed.path)
    if match and not match.group(2):
        run = get_run(match.group(1))
        if not run:
            handler._send_json({"ok": False, "error": "agent_run_not_found"}, status=404)
            return True
        handler._send_json({"ok": True, "run": run})
        return True
    return False


def dispatch_post(handler, parsed, payload: dict, deps: dict) -> bool:
    if not parsed.path.startswith("/api/agents"):
        return False
    if parsed.path == "/api/agents/runs":
        agent_key = str(payload.get("agent_key") or "funnel_progress_agent").strip()
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        run = create_run(
            agent_key=agent_key,
            mode=str(payload.get("mode") or "auto").strip(),
            trigger_source=str(payload.get("trigger_source") or "manual").strip(),
            actor=str(payload.get("actor") or _actor(deps)),
            goal=payload.get("goal") if isinstance(payload.get("goal"), dict) else {},
            schedule_key=str(payload.get("schedule_key") or "").strip(),
            dedupe=bool(payload.get("dedupe", True)),
            metadata=metadata,
            correlation_id=str(payload.get("correlation_id") or "").strip(),
            parent_run_id=str(payload.get("parent_run_id") or "").strip(),
        )
        handler._send_json({"ok": True, "run": run})
        return True
    match = _RUN_RE.match(parsed.path)
    if match and match.group(3) == "cancel":
        result = cancel_run(match.group(1), actor=_actor(deps), reason=str(payload.get("reason") or "api cancel"))
        handler._send_json(result, status=200 if result.get("ok") else 404)
        return True
    if match and match.group(3) == "approve":
        result = decide_run(
            match.group(1),
            actor=str(payload.get("actor") or _actor(deps)),
            reason=str(payload.get("reason") or ""),
            idempotency_key=str(payload.get("idempotency_key") or ""),
            decision=str(payload.get("decision") or "approved"),
        )
        err = str(result.get("error") or "")
        handler._send_json(result, status=200 if result.get("ok") else (400 if err != "agent_run_not_found" else 404))
        return True
    if match and match.group(3) == "resume":
        result = resume_run(
            match.group(1),
            actor=str(payload.get("actor") or _actor(deps)),
            reason=str(payload.get("reason") or "manual resume"),
        )
        handler._send_json(result, status=200 if result.get("ok") else 404)
        return True
    return False
