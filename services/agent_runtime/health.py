"""Lightweight probes for Agent stack observability (HTTP API + optional MCP)."""

from __future__ import annotations

import os
import urllib.error
import urllib.request
from typing import Any


def probe_mcp_health(*, timeout_s: float = 3.0) -> dict[str, Any]:
    base = str(os.getenv("MCP_LAN_BASE_URL") or os.getenv("MCP_PUBLIC_BASE_URL") or "").strip().rstrip("/")
    token = str(os.getenv("MCP_ADMIN_TOKEN") or "").strip()
    if not base or not token:
        return {"ok": False, "skipped": True, "reason": "mcp_base_or_token_unset"}
    url = f"{base}/mcp/health"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            code = int(getattr(resp, "status", 200) or 200)
            return {"ok": code < 400, "http_status": code, "url": url}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "http_status": int(exc.code or 0), "url": url, "error": str(exc)[:500]}
    except Exception as exc:
        return {"ok": False, "url": url, "error": str(exc)[:500]}


def build_agent_stack_health_summary() -> dict[str, Any]:
    mcp = probe_mcp_health()
    return {
        "ok": True,
        "mcp_probe": mcp,
        "agent_worker_poll_seconds": os.getenv("AGENT_WORKER_POLL_SECONDS", "1.0"),
        "agent_auto_write_enabled": os.getenv("AGENT_AUTO_WRITE_ENABLED", "1"),
        "mcp_write_enabled": os.getenv("MCP_WRITE_ENABLED", "0"),
    }
