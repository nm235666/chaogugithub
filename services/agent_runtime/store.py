from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

import db_compat as db


RUN_TABLE = "agent_runs"
STEP_TABLE = "agent_steps"
APPROVAL_TABLE = "agent_approvals"
MESSAGE_TABLE = "agent_messages"
MEMORY_TABLE = "agent_memory_items"
QUALITY_TABLE = "agent_quality_scores"
GOVERNANCE_RULE_TABLE = "agent_governance_rules"
POLICY_DECISION_TABLE = "agent_policy_decisions"

ACTIVE_STATUSES = {"queued", "running", "waiting_approval"}
TERMINAL_STATUSES = {"succeeded", "failed", "cancelled"}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def dumps(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, default=str)


def loads(raw: Any, default: Any):
    try:
        parsed = json.loads(str(raw or ""))
        return parsed if isinstance(parsed, type(default)) else default
    except Exception:
        return default


def row_to_dict(row) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def rows_to_dicts(rows) -> list[dict[str, Any]]:
    return [row_to_dict(row) for row in rows]


def apply_row_factory(conn) -> None:
    if isinstance(conn, sqlite3.Connection):
        return
    db.apply_row_factory(conn)


def ensure_agent_tables(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RUN_TABLE} (
            id TEXT PRIMARY KEY,
            agent_key TEXT NOT NULL,
            status TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'auto',
            trigger_source TEXT NOT NULL DEFAULT '',
            schedule_key TEXT NOT NULL DEFAULT '',
            actor TEXT NOT NULL DEFAULT '',
            goal_json TEXT NOT NULL DEFAULT '{{}}',
            plan_json TEXT NOT NULL DEFAULT '{{}}',
            result_json TEXT NOT NULL DEFAULT '{{}}',
            metadata_json TEXT NOT NULL DEFAULT '{{}}',
            correlation_id TEXT NOT NULL DEFAULT '',
            parent_run_id TEXT NOT NULL DEFAULT '',
            error_text TEXT NOT NULL DEFAULT '',
            approval_required INTEGER NOT NULL DEFAULT 0,
            worker_id TEXT NOT NULL DEFAULT '',
            lease_until TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {STEP_TABLE} (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            step_index INTEGER NOT NULL,
            tool_name TEXT NOT NULL,
            args_json TEXT NOT NULL DEFAULT '{{}}',
            dry_run INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL,
            audit_id INTEGER NOT NULL DEFAULT 0,
            result_json TEXT NOT NULL DEFAULT '{{}}',
            error_text TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {APPROVAL_TABLE} (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            step_id TEXT NOT NULL DEFAULT '',
            actor TEXT NOT NULL DEFAULT '',
            decision TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            idempotency_key TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MESSAGE_TABLE} (
            id TEXT PRIMARY KEY,
            correlation_id TEXT NOT NULL DEFAULT '',
            run_id TEXT NOT NULL DEFAULT '',
            parent_run_id TEXT NOT NULL DEFAULT '',
            source_agent_key TEXT NOT NULL DEFAULT '',
            target_agent_key TEXT NOT NULL DEFAULT '',
            message_type TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{{}}',
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MEMORY_TABLE} (
            id TEXT PRIMARY KEY,
            memory_type TEXT NOT NULL,
            source_run_id TEXT NOT NULL DEFAULT '',
            source_agent_key TEXT NOT NULL DEFAULT '',
            ts_code TEXT NOT NULL DEFAULT '',
            scope TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL DEFAULT '',
            evidence_json TEXT NOT NULL DEFAULT '{{}}',
            score REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUALITY_TABLE} (
            id TEXT PRIMARY KEY,
            agent_key TEXT NOT NULL,
            metric_date TEXT NOT NULL,
            window_days INTEGER NOT NULL DEFAULT 7,
            total_runs INTEGER NOT NULL DEFAULT 0,
            succeeded_runs INTEGER NOT NULL DEFAULT 0,
            failed_runs INTEGER NOT NULL DEFAULT 0,
            waiting_approval_runs INTEGER NOT NULL DEFAULT 0,
            approval_approved INTEGER NOT NULL DEFAULT 0,
            approval_rejected INTEGER NOT NULL DEFAULT 0,
            changed_count INTEGER NOT NULL DEFAULT 0,
            conflict_count INTEGER NOT NULL DEFAULT 0,
            avg_duration_seconds REAL NOT NULL DEFAULT 0,
            success_rate REAL NOT NULL DEFAULT 0,
            failure_rate REAL NOT NULL DEFAULT 0,
            approval_pass_rate REAL NOT NULL DEFAULT 0,
            risk_score REAL NOT NULL DEFAULT 0,
            risk_status TEXT NOT NULL DEFAULT 'healthy',
            evidence_json TEXT NOT NULL DEFAULT '{{}}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {GOVERNANCE_RULE_TABLE} (
            id TEXT PRIMARY KEY,
            rule_key TEXT NOT NULL,
            agent_key TEXT NOT NULL DEFAULT '',
            tool_name TEXT NOT NULL DEFAULT '',
            risk_level TEXT NOT NULL DEFAULT 'low',
            decision TEXT NOT NULL DEFAULT 'allow',
            enabled INTEGER NOT NULL DEFAULT 1,
            thresholds_json TEXT NOT NULL DEFAULT '{{}}',
            reason TEXT NOT NULL DEFAULT '',
            created_by TEXT NOT NULL DEFAULT '',
            updated_by TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {POLICY_DECISION_TABLE} (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL DEFAULT '',
            step_id TEXT NOT NULL DEFAULT '',
            correlation_id TEXT NOT NULL DEFAULT '',
            agent_key TEXT NOT NULL DEFAULT '',
            tool_name TEXT NOT NULL DEFAULT '',
            requested_dry_run INTEGER NOT NULL DEFAULT 1,
            decision TEXT NOT NULL,
            risk_level TEXT NOT NULL DEFAULT 'low',
            reason TEXT NOT NULL DEFAULT '',
            evidence_json TEXT NOT NULL DEFAULT '{{}}',
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{RUN_TABLE}_status ON {RUN_TABLE}(status, created_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{RUN_TABLE}_agent_schedule ON {RUN_TABLE}(agent_key, schedule_key, created_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{RUN_TABLE}_correlation ON {RUN_TABLE}(correlation_id, created_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{RUN_TABLE}_parent ON {RUN_TABLE}(parent_run_id, created_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{STEP_TABLE}_run ON {STEP_TABLE}(run_id, step_index)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{APPROVAL_TABLE}_run ON {APPROVAL_TABLE}(run_id, created_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{MESSAGE_TABLE}_correlation ON {MESSAGE_TABLE}(correlation_id, created_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{MESSAGE_TABLE}_run ON {MESSAGE_TABLE}(run_id, created_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{MEMORY_TABLE}_lookup ON {MEMORY_TABLE}(status, memory_type, ts_code, scope, updated_at)")
    conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{QUALITY_TABLE}_agent_date ON {QUALITY_TABLE}(agent_key, metric_date)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{QUALITY_TABLE}_status ON {QUALITY_TABLE}(risk_status, updated_at)")
    conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{GOVERNANCE_RULE_TABLE}_rule_key ON {GOVERNANCE_RULE_TABLE}(rule_key)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{GOVERNANCE_RULE_TABLE}_lookup ON {GOVERNANCE_RULE_TABLE}(enabled, agent_key, tool_name)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{POLICY_DECISION_TABLE}_run ON {POLICY_DECISION_TABLE}(run_id, created_at)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{POLICY_DECISION_TABLE}_lookup ON {POLICY_DECISION_TABLE}(agent_key, tool_name, decision, created_at)")
    try:
        conn.commit()
    except Exception:
        pass
    _ensure_run_compat_columns(conn)


def _ensure_run_compat_columns(conn) -> None:
    for column, ddl in (
        ("metadata_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("correlation_id", "TEXT NOT NULL DEFAULT ''"),
        ("parent_run_id", "TEXT NOT NULL DEFAULT ''"),
    ):
        try:
            conn.execute(f"ALTER TABLE {RUN_TABLE} ADD COLUMN {column} {ddl}")
            try:
                conn.commit()
            except Exception:
                pass
        except Exception:
            pass


def _hydrate_run(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    out = dict(row)
    out["goal"] = loads(out.pop("goal_json", "{}"), {})
    out["plan"] = loads(out.pop("plan_json", "{}"), {})
    out["result"] = loads(out.pop("result_json", "{}"), {})
    meta_raw = out.pop("metadata_json", None)
    if meta_raw is None:
        out["metadata"] = {}
    else:
        out["metadata"] = loads(meta_raw, {})
    out["approval_required"] = bool(out.get("approval_required"))
    return out


def _hydrate_step(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["args"] = loads(out.pop("args_json", "{}"), {})
    out["result"] = loads(out.pop("result_json", "{}"), {})
    out["dry_run"] = bool(out.get("dry_run"))
    return out


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
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        agent_key = str(agent_key or "").strip()
        if not agent_key:
            raise ValueError("agent_key_required")
        schedule_key = str(schedule_key or "").strip()
        if dedupe and schedule_key:
            row = conn.execute(
                f"""
                SELECT *
                FROM {RUN_TABLE}
                WHERE agent_key = ?
                  AND schedule_key = ?
                  AND status IN ('queued', 'running', 'waiting_approval', 'succeeded')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (agent_key, schedule_key),
            ).fetchone()
            existing = _hydrate_run(row_to_dict(row))
            if existing:
                existing["deduped"] = True
                return existing
        now = utc_now()
        run_id = f"agent_run_{uuid.uuid4().hex}"
        correlation_id = str(correlation_id or "").strip() or f"corr_{uuid.uuid4().hex}"
        conn.execute(
            f"""
            INSERT INTO {RUN_TABLE} (
                id, agent_key, status, mode, trigger_source, schedule_key, actor,
                goal_json, plan_json, result_json, metadata_json, correlation_id, parent_run_id, error_text, approval_required,
                worker_id, lease_until, created_at, updated_at, finished_at
            ) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, '{{}}', '{{}}', ?, ?, ?, '', 0, '', '', ?, ?, '')
            """,
            (
                run_id,
                agent_key,
                str(mode or "auto"),
                str(trigger_source or ""),
                schedule_key,
                str(actor or ""),
                dumps(goal),
                dumps(metadata),
                correlation_id,
                str(parent_run_id or "").strip(),
                now,
                now,
            ),
        )
        try:
            conn.commit()
        except Exception:
            pass
        return get_run(run_id) or {"id": run_id, "agent_key": agent_key, "status": "queued"}
    finally:
        conn.close()


def list_runs(*, agent_key: str = "", status: str = "", limit: int = 50, correlation_id: str = "", parent_run_id: str = "") -> dict[str, Any]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        where: list[str] = []
        params: list[Any] = []
        if str(agent_key or "").strip():
            where.append("agent_key = ?")
            params.append(str(agent_key).strip())
        if str(status or "").strip():
            where.append("status = ?")
            params.append(str(status).strip())
        if str(correlation_id or "").strip():
            where.append("correlation_id = ?")
            params.append(str(correlation_id).strip())
        if str(parent_run_id or "").strip():
            where.append("parent_run_id = ?")
            params.append(str(parent_run_id).strip())
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM {RUN_TABLE} {where_sql} ORDER BY created_at DESC LIMIT ?",
            (*params, max(1, min(int(limit or 50), 200))),
        ).fetchall()
        return {"ok": True, "items": [_hydrate_run(row_to_dict(row)) for row in rows]}
    finally:
        conn.close()


def get_run(run_id: str, *, include_steps: bool = True) -> dict[str, Any] | None:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        row = conn.execute(f"SELECT * FROM {RUN_TABLE} WHERE id = ?", (str(run_id or "").strip(),)).fetchone()
        run = _hydrate_run(row_to_dict(row))
        if not run:
            return None
        if include_steps:
            steps = conn.execute(
                f"SELECT * FROM {STEP_TABLE} WHERE run_id = ? ORDER BY step_index, created_at",
                (run["id"],),
            ).fetchall()
            run["steps"] = [_hydrate_step(row_to_dict(step)) for step in steps]
        return run
    finally:
        conn.close()


def claim_next_run(*, worker_id: str) -> dict[str, Any] | None:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        row = conn.execute(
            f"SELECT * FROM {RUN_TABLE} WHERE status = 'queued' ORDER BY created_at LIMIT 1"
        ).fetchone()
        run = _hydrate_run(row_to_dict(row))
        if not run:
            return None
        now = utc_now()
        conn.execute(
            f"UPDATE {RUN_TABLE} SET status = 'running', worker_id = ?, updated_at = ? WHERE id = ? AND status = 'queued'",
            (worker_id, now, run["id"]),
        )
        try:
            conn.commit()
        except Exception:
            pass
        return get_run(run["id"], include_steps=False)
    finally:
        conn.close()


def update_run(
    run_id: str,
    *,
    status: str | None = None,
    plan: dict[str, Any] | None = None,
    result: dict[str, Any] | None = None,
    error_text: str | None = None,
    approval_required: bool | None = None,
    finished: bool = False,
) -> None:
    conn = db.connect()
    try:
        ensure_agent_tables(conn)
        fields = ["updated_at = ?"]
        params: list[Any] = [utc_now()]
        if status is not None:
            fields.append("status = ?")
            params.append(status)
        if plan is not None:
            fields.append("plan_json = ?")
            params.append(dumps(plan))
        if result is not None:
            fields.append("result_json = ?")
            params.append(dumps(result))
        if error_text is not None:
            fields.append("error_text = ?")
            params.append(str(error_text or "")[:4000])
        if approval_required is not None:
            fields.append("approval_required = ?")
            params.append(1 if approval_required else 0)
        if finished:
            fields.append("finished_at = ?")
            params.append(utc_now())
        params.append(str(run_id or ""))
        conn.execute(f"UPDATE {RUN_TABLE} SET {', '.join(fields)} WHERE id = ?", tuple(params))
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()


def insert_step(*, run_id: str, step_index: int, tool_name: str, args: dict[str, Any], dry_run: bool) -> str:
    conn = db.connect()
    try:
        ensure_agent_tables(conn)
        now = utc_now()
        step_id = f"agent_step_{uuid.uuid4().hex}"
        conn.execute(
            f"""
            INSERT INTO {STEP_TABLE} (
                id, run_id, step_index, tool_name, args_json, dry_run, status,
                audit_id, result_json, error_text, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'running', 0, '{{}}', '', ?, ?)
            """,
            (step_id, run_id, int(step_index), tool_name, dumps(args), 1 if dry_run else 0, now, now),
        )
        try:
            conn.commit()
        except Exception:
            pass
        return step_id
    finally:
        conn.close()


def insert_pending_step(*, run_id: str, step_index: int, tool_name: str, args: dict[str, Any]) -> str:
    conn = db.connect()
    try:
        ensure_agent_tables(conn)
        now = utc_now()
        step_id = f"agent_step_{uuid.uuid4().hex}"
        conn.execute(
            f"""
            INSERT INTO {STEP_TABLE} (
                id, run_id, step_index, tool_name, args_json, dry_run, status,
                audit_id, result_json, error_text, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 0, 'pending_approval', 0, '{{}}', '', ?, ?)
            """,
            (step_id, run_id, int(step_index), tool_name, dumps(args), now, now),
        )
        try:
            conn.commit()
        except Exception:
            pass
        return step_id
    finally:
        conn.close()


def finish_step(
    step_id: str,
    *,
    status: str,
    result: dict[str, Any] | None = None,
    error_text: str = "",
    audit_id: int = 0,
) -> None:
    conn = db.connect()
    try:
        ensure_agent_tables(conn)
        conn.execute(
            f"""
            UPDATE {STEP_TABLE}
            SET status = ?, result_json = ?, error_text = ?, audit_id = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, dumps(result), str(error_text or "")[:4000], int(audit_id or 0), utc_now(), step_id),
        )
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()


def update_step_args(step_id: str, *, args: dict[str, Any], status: str | None = None) -> None:
    conn = db.connect()
    try:
        ensure_agent_tables(conn)
        fields = ["args_json = ?", "dry_run = ?", "updated_at = ?"]
        params: list[Any] = [dumps(args), 1 if bool(args.get("dry_run", True)) else 0, utc_now()]
        if status is not None:
            fields.append("status = ?")
            params.append(str(status or ""))
        params.append(str(step_id or ""))
        conn.execute(f"UPDATE {STEP_TABLE} SET {', '.join(fields)} WHERE id = ?", tuple(params))
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()


def get_step(step_id: str) -> dict[str, Any] | None:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        row = conn.execute(f"SELECT * FROM {STEP_TABLE} WHERE id = ?", (str(step_id or ""),)).fetchone()
        data = row_to_dict(row)
        return _hydrate_step(data) if data else None
    finally:
        conn.close()


def list_pending_steps(run_id: str) -> list[dict[str, Any]]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        rows = conn.execute(
            f"SELECT * FROM {STEP_TABLE} WHERE run_id = ? AND status = 'pending_approval' ORDER BY step_index",
            (str(run_id or ""),),
        ).fetchall()
        return [_hydrate_step(row_to_dict(row)) for row in rows]
    finally:
        conn.close()


def cancel_run(run_id: str, *, actor: str = "", reason: str = "") -> dict[str, Any]:
    run = get_run(run_id, include_steps=False)
    if not run:
        return {"ok": False, "error": "agent_run_not_found"}
    if run.get("status") in TERMINAL_STATUSES:
        return {"ok": True, "run": run, "skipped": True}
    update_run(run_id, status="cancelled", result={"cancelled_by": actor, "reason": reason}, finished=True)
    return {"ok": True, "run": get_run(run_id)}


def record_approval(run_id: str, *, actor: str, reason: str = "", idempotency_key: str = "", decision: str = "approved") -> dict[str, Any]:
    run = get_run(run_id, include_steps=False)
    if not run:
        return {"ok": False, "error": "agent_run_not_found"}
    conn = db.connect()
    try:
        ensure_agent_tables(conn)
        conn.execute(
            f"""
            INSERT INTO {APPROVAL_TABLE} (id, run_id, step_id, actor, decision, reason, idempotency_key, created_at)
            VALUES (?, ?, '', ?, ?, ?, ?, ?)
            """,
            (f"agent_approval_{uuid.uuid4().hex}", run_id, str(actor or ""), str(decision or "approved"), str(reason or ""), str(idempotency_key or ""), utc_now()),
        )
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()
    return {"ok": True, "run": get_run(run_id)}


def approve_run(run_id: str, *, actor: str, reason: str = "", idempotency_key: str = "", decision: str = "approved") -> dict[str, Any]:
    return record_approval(run_id, actor=actor, reason=reason, idempotency_key=idempotency_key, decision=decision)


def list_steps(run_id: str) -> list[dict[str, Any]]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        rows = conn.execute(f"SELECT * FROM {STEP_TABLE} WHERE run_id = ? ORDER BY step_index", (run_id,)).fetchall()
        return [_hydrate_step(row) for row in rows_to_dicts(rows)]
    finally:
        conn.close()


def append_message(
    *,
    correlation_id: str = "",
    run_id: str = "",
    parent_run_id: str = "",
    source_agent_key: str = "",
    target_agent_key: str = "",
    message_type: str = "",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    conn = db.connect()
    try:
        ensure_agent_tables(conn)
        message_id = f"agent_msg_{uuid.uuid4().hex}"
        now = utc_now()
        conn.execute(
            f"""
            INSERT INTO {MESSAGE_TABLE} (
                id, correlation_id, run_id, parent_run_id, source_agent_key,
                target_agent_key, message_type, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                str(correlation_id or ""),
                str(run_id or ""),
                str(parent_run_id or ""),
                str(source_agent_key or ""),
                str(target_agent_key or ""),
                str(message_type or ""),
                dumps(payload),
                now,
            ),
        )
        try:
            conn.commit()
        except Exception:
            pass
        return {"ok": True, "id": message_id}
    finally:
        conn.close()


def list_messages(*, correlation_id: str = "", run_id: str = "", limit: int = 200) -> dict[str, Any]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        where: list[str] = []
        params: list[Any] = []
        if str(correlation_id or "").strip():
            where.append("correlation_id = ?")
            params.append(str(correlation_id).strip())
        if str(run_id or "").strip():
            where.append("run_id = ?")
            params.append(str(run_id).strip())
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM {MESSAGE_TABLE} {where_sql} ORDER BY created_at DESC LIMIT ?",
            (*params, max(1, min(int(limit or 200), 500))),
        ).fetchall()
        items = []
        for row in rows_to_dicts(rows):
            item = dict(row)
            item["payload"] = loads(item.pop("payload_json", "{}"), {})
            items.append(item)
        return {"ok": True, "items": items}
    finally:
        conn.close()


def record_memory_item(
    *,
    memory_type: str,
    source_run_id: str = "",
    source_agent_key: str = "",
    ts_code: str = "",
    scope: str = "",
    summary: str = "",
    evidence: dict[str, Any] | None = None,
    score: float = 0,
    status: str = "active",
    memory_id: str = "",
) -> dict[str, Any]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        now = utc_now()
        mid = str(memory_id or "").strip() or f"agent_memory_{uuid.uuid4().hex}"
        conn.execute(
            f"""
            INSERT INTO {MEMORY_TABLE} (
                id, memory_type, source_run_id, source_agent_key, ts_code, scope,
                summary, evidence_json, score, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                mid,
                str(memory_type or "").strip(),
                str(source_run_id or "").strip(),
                str(source_agent_key or "").strip(),
                str(ts_code or "").strip(),
                str(scope or "").strip(),
                str(summary or "").strip(),
                dumps(evidence),
                float(score or 0),
                str(status or "active").strip() or "active",
                now,
                now,
            ),
        )
        try:
            conn.commit()
        except Exception:
            pass
        return {"ok": True, "item": get_memory_item(mid)}
    finally:
        conn.close()


def get_memory_item(memory_id: str) -> dict[str, Any] | None:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        row = conn.execute(f"SELECT * FROM {MEMORY_TABLE} WHERE id = ?", (str(memory_id or "").strip(),)).fetchone()
        data = row_to_dict(row)
        if not data:
            return None
        data["evidence"] = loads(data.pop("evidence_json", "{}"), {})
        return data
    finally:
        conn.close()


def list_memory_items(
    *,
    memory_type: str = "",
    ts_code: str = "",
    scope: str = "",
    source_agent_key: str = "",
    status: str = "active",
    limit: int = 50,
) -> dict[str, Any]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        where: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("memory_type", memory_type),
            ("ts_code", ts_code),
            ("scope", scope),
            ("source_agent_key", source_agent_key),
            ("status", status),
        ):
            if str(value or "").strip():
                where.append(f"{column} = ?")
                params.append(str(value).strip())
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM {MEMORY_TABLE} {where_sql} ORDER BY updated_at DESC, created_at DESC LIMIT ?",
            (*params, max(1, min(int(limit or 50), 500))),
        ).fetchall()
        items = []
        for row in rows_to_dicts(rows):
            item = dict(row)
            item["evidence"] = loads(item.pop("evidence_json", "{}"), {})
            items.append(item)
        return {"ok": True, "items": items}
    finally:
        conn.close()


def search_memory_items(*, ts_code: str = "", scope: str = "", memory_type: str = "", limit: int = 20) -> dict[str, Any]:
    return list_memory_items(memory_type=memory_type, ts_code=ts_code, scope=scope, status="active", limit=limit)


def upsert_quality_score(item: dict[str, Any]) -> dict[str, Any]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        now = utc_now()
        agent_key = str(item.get("agent_key") or "").strip()
        metric_date = str(item.get("metric_date") or "").strip()
        if not agent_key or not metric_date:
            raise ValueError("agent_key_and_metric_date_required")
        existing = conn.execute(
            f"SELECT id FROM {QUALITY_TABLE} WHERE agent_key = ? AND metric_date = ? LIMIT 1",
            (agent_key, metric_date),
        ).fetchone()
        score_id = str(row_to_dict(existing).get("id") or "") if existing else f"agent_quality_{uuid.uuid4().hex}"
        payload = {
            "window_days": int(item.get("window_days") or 7),
            "total_runs": int(item.get("total_runs") or 0),
            "succeeded_runs": int(item.get("succeeded_runs") or 0),
            "failed_runs": int(item.get("failed_runs") or 0),
            "waiting_approval_runs": int(item.get("waiting_approval_runs") or 0),
            "approval_approved": int(item.get("approval_approved") or 0),
            "approval_rejected": int(item.get("approval_rejected") or 0),
            "changed_count": int(item.get("changed_count") or 0),
            "conflict_count": int(item.get("conflict_count") or 0),
            "avg_duration_seconds": float(item.get("avg_duration_seconds") or 0),
            "success_rate": float(item.get("success_rate") or 0),
            "failure_rate": float(item.get("failure_rate") or 0),
            "approval_pass_rate": float(item.get("approval_pass_rate") or 0),
            "risk_score": float(item.get("risk_score") or 0),
            "risk_status": str(item.get("risk_status") or "healthy"),
            "evidence_json": dumps(item.get("evidence") or {}),
        }
        if existing:
            conn.execute(
                f"""
                UPDATE {QUALITY_TABLE}
                SET window_days = ?, total_runs = ?, succeeded_runs = ?, failed_runs = ?,
                    waiting_approval_runs = ?, approval_approved = ?, approval_rejected = ?,
                    changed_count = ?, conflict_count = ?, avg_duration_seconds = ?,
                    success_rate = ?, failure_rate = ?, approval_pass_rate = ?, risk_score = ?,
                    risk_status = ?, evidence_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    payload["window_days"],
                    payload["total_runs"],
                    payload["succeeded_runs"],
                    payload["failed_runs"],
                    payload["waiting_approval_runs"],
                    payload["approval_approved"],
                    payload["approval_rejected"],
                    payload["changed_count"],
                    payload["conflict_count"],
                    payload["avg_duration_seconds"],
                    payload["success_rate"],
                    payload["failure_rate"],
                    payload["approval_pass_rate"],
                    payload["risk_score"],
                    payload["risk_status"],
                    payload["evidence_json"],
                    now,
                    score_id,
                ),
            )
        else:
            conn.execute(
                f"""
                INSERT INTO {QUALITY_TABLE} (
                    id, agent_key, metric_date, window_days, total_runs, succeeded_runs,
                    failed_runs, waiting_approval_runs, approval_approved, approval_rejected,
                    changed_count, conflict_count, avg_duration_seconds, success_rate,
                    failure_rate, approval_pass_rate, risk_score, risk_status, evidence_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    score_id,
                    agent_key,
                    metric_date,
                    payload["window_days"],
                    payload["total_runs"],
                    payload["succeeded_runs"],
                    payload["failed_runs"],
                    payload["waiting_approval_runs"],
                    payload["approval_approved"],
                    payload["approval_rejected"],
                    payload["changed_count"],
                    payload["conflict_count"],
                    payload["avg_duration_seconds"],
                    payload["success_rate"],
                    payload["failure_rate"],
                    payload["approval_pass_rate"],
                    payload["risk_score"],
                    payload["risk_status"],
                    payload["evidence_json"],
                    now,
                    now,
                ),
            )
        try:
            conn.commit()
        except Exception:
            pass
        return {"ok": True, "item": get_quality_score(score_id)}
    finally:
        conn.close()


def get_quality_score(score_id: str) -> dict[str, Any] | None:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        row = conn.execute(f"SELECT * FROM {QUALITY_TABLE} WHERE id = ?", (str(score_id or ""),)).fetchone()
        data = row_to_dict(row)
        if not data:
            return None
        data["evidence"] = loads(data.pop("evidence_json", "{}"), {})
        return data
    finally:
        conn.close()


def list_quality_scores(*, agent_key: str = "", metric_date: str = "", limit: int = 50) -> dict[str, Any]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        where: list[str] = []
        params: list[Any] = []
        if str(agent_key or "").strip():
            where.append("agent_key = ?")
            params.append(str(agent_key).strip())
        if str(metric_date or "").strip():
            where.append("metric_date = ?")
            params.append(str(metric_date).strip())
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM {QUALITY_TABLE} {where_sql} ORDER BY metric_date DESC, risk_score DESC LIMIT ?",
            (*params, max(1, min(int(limit or 50), 500))),
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows_to_dicts(rows):
            item = dict(row)
            item["evidence"] = loads(item.pop("evidence_json", "{}"), {})
            items.append(item)
        return {"ok": True, "items": items}
    finally:
        conn.close()


def upsert_governance_rule(
    *,
    rule_key: str,
    agent_key: str = "",
    tool_name: str = "",
    risk_level: str = "low",
    decision: str = "allow",
    enabled: bool = True,
    thresholds: dict[str, Any] | None = None,
    reason: str = "",
    actor: str = "",
) -> dict[str, Any]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        now = utc_now()
        key = str(rule_key or "").strip()
        if not key:
            raise ValueError("rule_key_required")
        existing = conn.execute(f"SELECT id, created_by, created_at FROM {GOVERNANCE_RULE_TABLE} WHERE rule_key = ? LIMIT 1", (key,)).fetchone()
        existing_dict = row_to_dict(existing)
        rule_id = str(existing_dict.get("id") or "") if existing_dict else f"agent_rule_{uuid.uuid4().hex}"
        if existing_dict:
            conn.execute(
                f"""
                UPDATE {GOVERNANCE_RULE_TABLE}
                SET agent_key = ?, tool_name = ?, risk_level = ?, decision = ?, enabled = ?,
                    thresholds_json = ?, reason = ?, updated_by = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    str(agent_key or ""),
                    str(tool_name or ""),
                    str(risk_level or "low"),
                    str(decision or "allow"),
                    1 if enabled else 0,
                    dumps(thresholds),
                    str(reason or ""),
                    str(actor or ""),
                    now,
                    rule_id,
                ),
            )
        else:
            conn.execute(
                f"""
                INSERT INTO {GOVERNANCE_RULE_TABLE} (
                    id, rule_key, agent_key, tool_name, risk_level, decision, enabled,
                    thresholds_json, reason, created_by, updated_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rule_id,
                    key,
                    str(agent_key or ""),
                    str(tool_name or ""),
                    str(risk_level or "low"),
                    str(decision or "allow"),
                    1 if enabled else 0,
                    dumps(thresholds),
                    str(reason or ""),
                    str(actor or ""),
                    str(actor or ""),
                    now,
                    now,
                ),
            )
        try:
            conn.commit()
        except Exception:
            pass
        return {"ok": True, "item": get_governance_rule(rule_id)}
    finally:
        conn.close()


def get_governance_rule(rule_id: str) -> dict[str, Any] | None:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        row = conn.execute(f"SELECT * FROM {GOVERNANCE_RULE_TABLE} WHERE id = ?", (str(rule_id or ""),)).fetchone()
        data = row_to_dict(row)
        if not data:
            return None
        data["enabled"] = bool(data.get("enabled"))
        data["thresholds"] = loads(data.pop("thresholds_json", "{}"), {})
        return data
    finally:
        conn.close()


def list_governance_rules(*, agent_key: str = "", tool_name: str = "", enabled: str = "", limit: int = 100) -> dict[str, Any]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        where: list[str] = []
        params: list[Any] = []
        if str(agent_key or "").strip():
            where.append("(agent_key = ? OR agent_key = '')")
            params.append(str(agent_key).strip())
        if str(tool_name or "").strip():
            where.append("(tool_name = ? OR tool_name = '')")
            params.append(str(tool_name).strip())
        if str(enabled or "").strip():
            where.append("enabled = ?")
            params.append(1 if str(enabled).strip().lower() in {"1", "true", "yes", "on"} else 0)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM {GOVERNANCE_RULE_TABLE} {where_sql} ORDER BY updated_at DESC LIMIT ?",
            (*params, max(1, min(int(limit or 100), 500))),
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows_to_dicts(rows):
            item = dict(row)
            item["enabled"] = bool(item.get("enabled"))
            item["thresholds"] = loads(item.pop("thresholds_json", "{}"), {})
            items.append(item)
        return {"ok": True, "items": items}
    finally:
        conn.close()


def record_policy_decision(
    *,
    run_id: str = "",
    step_id: str = "",
    correlation_id: str = "",
    agent_key: str = "",
    tool_name: str = "",
    requested_dry_run: bool = True,
    decision: str,
    risk_level: str = "low",
    reason: str = "",
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    conn = db.connect()
    try:
        ensure_agent_tables(conn)
        decision_id = f"agent_policy_{uuid.uuid4().hex}"
        conn.execute(
            f"""
            INSERT INTO {POLICY_DECISION_TABLE} (
                id, run_id, step_id, correlation_id, agent_key, tool_name,
                requested_dry_run, decision, risk_level, reason, evidence_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_id,
                str(run_id or ""),
                str(step_id or ""),
                str(correlation_id or ""),
                str(agent_key or ""),
                str(tool_name or ""),
                1 if requested_dry_run else 0,
                str(decision or ""),
                str(risk_level or "low"),
                str(reason or ""),
                dumps(evidence),
                utc_now(),
            ),
        )
        try:
            conn.commit()
        except Exception:
            pass
        return {"ok": True, "id": decision_id}
    finally:
        conn.close()


def list_policy_decisions(*, agent_key: str = "", tool_name: str = "", decision: str = "", run_id: str = "", limit: int = 100) -> dict[str, Any]:
    conn = db.connect()
    apply_row_factory(conn)
    try:
        ensure_agent_tables(conn)
        where: list[str] = []
        params: list[Any] = []
        for column, value in (("agent_key", agent_key), ("tool_name", tool_name), ("decision", decision), ("run_id", run_id)):
            if str(value or "").strip():
                where.append(f"{column} = ?")
                params.append(str(value).strip())
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM {POLICY_DECISION_TABLE} {where_sql} ORDER BY created_at DESC LIMIT ?",
            (*params, max(1, min(int(limit or 100), 500))),
        ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows_to_dicts(rows):
            item = dict(row)
            item["requested_dry_run"] = bool(item.get("requested_dry_run"))
            item["evidence"] = loads(item.pop("evidence_json", "{}"), {})
            items.append(item)
        return {"ok": True, "items": items}
    finally:
        conn.close()
