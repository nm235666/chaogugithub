from __future__ import annotations

from typing import Any

from mcp_server import schemas
from services.agent_runtime import store

from .common import require_write_allowed


MEMORY_TYPES = {
    "effective_signal",
    "failed_signal",
    "execution_slippage",
    "review_rule_correction",
    "agent_failure_pattern",
}

MEMORY_STATUSES = {"active", "muted", "superseded"}


def _validate_type(value: str) -> str:
    text = str(value or "").strip()
    if text not in MEMORY_TYPES:
        raise ValueError(f"invalid_memory_type:{text}")
    return text


def _validate_status(value: str) -> str:
    text = str(value or "active").strip() or "active"
    if text not in MEMORY_STATUSES:
        raise ValueError(f"invalid_memory_status:{text}")
    return text


def list_items(args: schemas.MemoryListArgs) -> dict[str, Any]:
    return store.list_memory_items(
        memory_type=args.memory_type,
        ts_code=args.ts_code,
        scope=args.scope,
        source_agent_key=args.source_agent_key,
        status=args.status,
        limit=args.limit,
    )


def search_relevant(args: schemas.MemorySearchArgs) -> dict[str, Any]:
    return store.search_memory_items(
        ts_code=args.ts_code,
        scope=args.scope,
        memory_type=args.memory_type,
        limit=args.limit,
    )


def record_item(args: schemas.MemoryRecordArgs) -> dict[str, Any]:
    memory_type = _validate_type(args.memory_type)
    status = _validate_status(args.status)
    summary = str(args.summary or "").strip()
    if not summary:
        raise ValueError("memory_summary_required")
    planned = {
        "memory_type": memory_type,
        "source_run_id": args.source_run_id,
        "source_agent_key": args.source_agent_key,
        "ts_code": args.ts_code,
        "scope": args.scope,
        "summary": summary,
        "score": args.score,
        "status": status,
    }
    if args.dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "planned_changes": [planned],
            "changed_count": 0,
            "skipped_count": 0,
            "warnings": [],
        }
    require_write_allowed(args)
    result = store.record_memory_item(
        memory_type=memory_type,
        source_run_id=args.source_run_id,
        source_agent_key=args.source_agent_key,
        ts_code=args.ts_code,
        scope=args.scope,
        summary=summary,
        evidence=args.evidence,
        score=args.score,
        status=status,
    )
    return {
        "ok": True,
        "dry_run": False,
        "planned_changes": [planned],
        "changed_count": 1,
        "skipped_count": 0,
        "warnings": [],
        "item": result.get("item"),
    }
