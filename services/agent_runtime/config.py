from __future__ import annotations

import os

from .platform_allowlist import FUNNEL_AUTO_WRITE_TOOLS


def _csv_set(value: str) -> set[str]:
    return {item.strip() for item in str(value or "").split(",") if item.strip()}


def auto_write_enabled() -> bool:
    return os.getenv("AGENT_AUTO_WRITE_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}


def funnel_auto_write_tools_frozen() -> frozenset[str]:
    return frozenset(FUNNEL_AUTO_WRITE_TOOLS)


def auto_write_allowlist() -> set[str]:
    base = set(FUNNEL_AUTO_WRITE_TOOLS)
    env_raw = _csv_set(os.getenv("AGENT_AUTO_WRITE_TOOL_ALLOWLIST", ""))
    if not env_raw:
        return base
    allowed = {t for t in env_raw if t in base}
    return allowed or base


def worker_poll_seconds() -> float:
    try:
        return max(0.2, float(os.getenv("AGENT_WORKER_POLL_SECONDS", "1.0") or 1.0))
    except Exception:
        return 1.0
