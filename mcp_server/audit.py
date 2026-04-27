from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import db_compat as db


AUDIT_TABLE = "mcp_tool_audit_logs"


def _row_to_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return {k: row[k] for k in row.keys()}  # type: ignore[attr-defined]
    except Exception:
        return {}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_audit_table(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL DEFAULT '',
            actor TEXT NOT NULL DEFAULT '',
            tool_name TEXT NOT NULL,
            args_json TEXT NOT NULL DEFAULT '{{}}',
            dry_run INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL,
            result_json TEXT NOT NULL DEFAULT '{{}}',
            error_text TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{AUDIT_TABLE}_tool_time ON {AUDIT_TABLE}(tool_name, created_at DESC)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{AUDIT_TABLE}_request ON {AUDIT_TABLE}(request_id)")
    try:
        conn.commit()
    except Exception:
        pass


def list_recent_tool_audits(*, limit: int = 100, dry_run_only: bool = False, write_only: bool = False) -> dict[str, Any]:
    """Return recent MCP tool audit rows for ops export (read-only)."""
    limit = max(1, min(int(limit or 100), 500))
    conn = db.connect()
    try:
        ensure_audit_table(conn)
        where = []
        params: list[Any] = []
        if dry_run_only:
            where.append("dry_run = 1")
        if write_only:
            where.append("dry_run = 0")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        rows = conn.execute(
            f"""
            SELECT id, request_id, actor, tool_name, args_json, dry_run, status, result_json, error_text, created_at
            FROM {AUDIT_TABLE}
            {where_sql}
            ORDER BY id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        items = []
        for row in rows:
            d = _row_to_dict(row)
            items.append(
                {
                    "id": int(d.get("id") or 0),
                    "request_id": str(d.get("request_id") or ""),
                    "actor": str(d.get("actor") or ""),
                    "tool_name": str(d.get("tool_name") or ""),
                    "dry_run": bool(int(d.get("dry_run") or 0)),
                    "status": str(d.get("status") or ""),
                    "created_at": str(d.get("created_at") or ""),
                    "error_text": str(d.get("error_text") or "")[:500],
                }
            )
        return {"ok": True, "items": items, "total_returned": len(items)}
    finally:
        conn.close()


def record_tool_audit(
    *,
    request_id: str,
    actor: str,
    tool_name: str,
    args: dict[str, Any],
    dry_run: bool,
    status: str,
    result: dict[str, Any] | None = None,
    error_text: str = "",
) -> int:
    conn = db.connect()
    try:
        ensure_audit_table(conn)
        conn.execute(
            f"""
            INSERT INTO {AUDIT_TABLE}
                (request_id, actor, tool_name, args_json, dry_run, status, result_json, error_text, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                actor,
                tool_name,
                json.dumps(args or {}, ensure_ascii=False, default=str),
                1 if dry_run else 0,
                status,
                json.dumps(result or {}, ensure_ascii=False, default=str),
                str(error_text or "")[:4000],
                utc_now(),
            ),
        )
        row = conn.execute(f"SELECT MAX(id) FROM {AUDIT_TABLE}").fetchone()
        try:
            conn.commit()
        except Exception:
            pass
        return int(row[0] or 0) if row else 0
    finally:
        conn.close()

