from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import db_compat as _db

FUNNEL_CANDIDATES_TABLE = "funnel_candidates"
FUNNEL_TRANSITIONS_TABLE = "funnel_transitions"

VALID_STATES = {
    "ingested",
    "amplified",
    "ai_screen_passed",
    "shortlisted",
    "decision_ready",
    "confirmed",
    "rejected",
    "deferred",
    "executed",
    "reviewed",
}

TERMINAL_STATES = {"confirmed", "rejected", "executed", "reviewed"}

# Priority rank: higher number = higher authority
TRIGGER_SOURCE_PRIORITY: dict[str, int] = {
    "decision_action": 5,
    "researcher": 4,
    "execution_feedback": 3,
    "system_rule": 2,
    "ai_screen": 1,
    "signal": 0,
}

# Valid transitions: from_state -> set of valid to_states
VALID_TRANSITIONS: dict[str, set[str]] = {
    "ingested": {"amplified", "rejected"},
    "amplified": {"ai_screen_passed", "rejected"},
    "ai_screen_passed": {"shortlisted", "rejected"},
    "shortlisted": {"decision_ready", "deferred", "rejected"},
    "decision_ready": {"confirmed", "rejected", "deferred"},
    "confirmed": {"executed"},
    "deferred": {"executed", "rejected", "shortlisted"},
    "executed": {"reviewed"},
    "rejected": set(),
    "reviewed": set(),
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_json(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def _load_upstream_scores_hint(conn) -> dict[str, Any]:
    if not _db.table_exists(conn, "stock_scores_daily"):
        return {"status": "missing_source", "latest_score_date": "", "latest_count": 0}
    latest_row = conn.execute("SELECT MAX(score_date) AS dt FROM stock_scores_daily").fetchone()
    latest_score_date = str(_row_to_dict(latest_row).get("dt") if latest_row else "").strip()
    if not latest_score_date:
        return {"status": "empty", "latest_score_date": "", "latest_count": 0}
    count_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM stock_scores_daily WHERE score_date = ?",
        (latest_score_date,),
    ).fetchone()
    latest_count = int(_row_to_dict(count_row).get("cnt") if count_row else 0)
    return {
        "status": "ready" if latest_count > 0 else "empty",
        "latest_score_date": latest_score_date,
        "latest_count": latest_count,
    }


def _derive_funnel_status(*, total: int, table_exists: bool, upstream_hint: dict[str, Any], error: str = "") -> dict[str, Any]:
    missing_inputs: list[str] = []
    if not table_exists:
        missing_inputs.append(FUNNEL_CANDIDATES_TABLE)
    if str(upstream_hint.get("status") or "") in {"missing_source", "empty"}:
        missing_inputs.append("stock_scores_daily")
    if error:
        status = "degraded"
        status_reason = "漏斗查询异常，建议检查数据库连接与表结构。"
    elif total > 0:
        status = "ready"
        status_reason = "漏斗候选已就绪，可继续执行筛选与状态流转。"
    elif upstream_hint.get("latest_count"):
        status = "degraded"
        status_reason = "上游评分已就绪，但漏斗候选仍为空，请检查入池同步任务。"
    elif table_exists:
        status = "empty"
        status_reason = "漏斗表存在但暂无候选。"
    else:
        status = "not_initialized"
        status_reason = "漏斗表尚未初始化。"
    return {
        "status": status,
        "status_reason": status_reason,
        "missing_inputs": missing_inputs,
    }


def list_candidates(
    *,
    state: str = "",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    try:
        conn = _db.connect()
        try:
            _db.apply_row_factory(conn)
            table_exists = _db.table_exists(conn, FUNNEL_CANDIDATES_TABLE)
            upstream_hint = _load_upstream_scores_hint(conn)
            if not table_exists:
                status_payload = _derive_funnel_status(total=0, table_exists=False, upstream_hint=upstream_hint)
                return {"items": [], "total": 0, "limit": limit, "offset": offset, "upstream_scores": upstream_hint, **status_payload}
            where_clause = ""
            params: list[Any] = []
            if state and state in VALID_STATES:
                where_clause = "WHERE state = ?"
                params.append(state)
            count_sql = f"SELECT COUNT(*) FROM {FUNNEL_CANDIDATES_TABLE} {where_clause}"
            total_row = conn.execute(count_sql, params).fetchone()
            total = int(total_row[0] or 0) if total_row else 0

            sql = f"""
                SELECT id, ts_code, name, source, trigger_source, reason, evidence_ref,
                       state, state_version, created_at, updated_at
                FROM {FUNNEL_CANDIDATES_TABLE}
                {where_clause}
                ORDER BY updated_at DESC
                LIMIT ? OFFSET ?
            """
            params_page = params + [limit, offset]
            rows = conn.execute(sql, params_page).fetchall()
            items = [_row_to_dict(r) for r in rows]
            status_payload = _derive_funnel_status(total=total, table_exists=True, upstream_hint=upstream_hint)
            return {
                "items": items,
                "total": total,
                "limit": limit,
                "offset": offset,
                "upstream_scores": upstream_hint,
                **status_payload,
            }
        finally:
            conn.close()
    except Exception as exc:
        return {
            "items": [],
            "total": 0,
            "limit": limit,
            "offset": offset,
            "upstream_scores": {"status": "unknown", "latest_score_date": "", "latest_count": 0},
            **_derive_funnel_status(total=0, table_exists=False, upstream_hint={"status": "unknown"}, error=str(exc)),
            "error": str(exc),
        }


def get_candidate(candidate_id: str) -> dict[str, Any] | None:
    try:
        conn = _db.connect()
        try:
            _db.apply_row_factory(conn)
            if not _db.table_exists(conn, FUNNEL_CANDIDATES_TABLE):
                return None
            row = conn.execute(
                f"""
                SELECT id, ts_code, name, source, trigger_source, reason, evidence_ref,
                       state, state_version, created_at, updated_at
                FROM {FUNNEL_CANDIDATES_TABLE}
                WHERE id = ?
                LIMIT 1
                """,
                (candidate_id,),
            ).fetchone()
            if not row:
                return None
            candidate = _row_to_dict(row)
            # Load transition history
            transitions: list[dict] = []
            if _db.table_exists(conn, FUNNEL_TRANSITIONS_TABLE):
                t_rows = conn.execute(
                    f"""
                    SELECT id, candidate_id, from_state, to_state, reason, evidence_ref,
                           trigger_source, operator, idempotency_key, created_at
                    FROM {FUNNEL_TRANSITIONS_TABLE}
                    WHERE candidate_id = ?
                    ORDER BY id ASC
                    """,
                    (candidate_id,),
                ).fetchall()
                transitions = [_row_to_dict(r) for r in t_rows]
            candidate["transitions"] = transitions
            return candidate
        finally:
            conn.close()
    except Exception:
        return None


def create_candidate(
    *,
    ts_code: str,
    name: str,
    source: str,
    trigger_source: str,
    reason: str,
    evidence_ref: str,
) -> dict[str, Any]:
    candidate_id = str(uuid.uuid4())
    now = _utc_now()
    try:
        conn = _db.connect()
        try:
            _db.apply_row_factory(conn)
            if not _db.table_exists(conn, FUNNEL_CANDIDATES_TABLE):
                _ensure_funnel_tables(conn)
            conn.execute(
                f"""
                INSERT INTO {FUNNEL_CANDIDATES_TABLE}
                    (id, ts_code, name, source, trigger_source, reason, evidence_ref,
                     state, state_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'ingested', 1, ?, ?)
                """,
                (candidate_id, ts_code, name, source, trigger_source, reason, evidence_ref, now, now),
            )
            # Record initial ingestion transition
            _record_transition(
                conn,
                candidate_id=candidate_id,
                from_state="",
                to_state="ingested",
                reason=reason,
                evidence_ref=evidence_ref,
                trigger_source=trigger_source,
                operator="system",
                idempotency_key="",
            )
        finally:
            conn.close()
        return {"ok": True, "id": candidate_id, "state": "ingested"}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _record_transition(
    conn,
    *,
    candidate_id: str,
    from_state: str,
    to_state: str,
    reason: str,
    evidence_ref: str,
    trigger_source: str,
    operator: str,
    idempotency_key: str,
) -> None:
    now = _utc_now()
    conn.execute(
        f"""
        INSERT INTO {FUNNEL_TRANSITIONS_TABLE}
            (id, candidate_id, from_state, to_state, reason, evidence_ref,
             trigger_source, operator, idempotency_key, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            candidate_id,
            from_state,
            to_state,
            reason,
            evidence_ref,
            trigger_source,
            operator,
            idempotency_key,
            now,
        ),
    )


def transition_candidate(
    candidate_id: str,
    *,
    to_state: str,
    reason: str,
    evidence_ref: str,
    trigger_source: str,
    operator: str,
    idempotency_key: str,
    state_version: int,
) -> dict[str, Any]:
    if to_state not in VALID_STATES:
        return {"ok": False, "error": f"无效目标状态: {to_state}"}
    try:
        conn = _db.connect()
        try:
            _db.apply_row_factory(conn)
            if not _db.table_exists(conn, FUNNEL_CANDIDATES_TABLE):
                return {"ok": False, "error": "候选标的表不存在"}
            # Idempotency check
            if idempotency_key and _db.table_exists(conn, FUNNEL_TRANSITIONS_TABLE):
                existing = conn.execute(
                    f"SELECT id FROM {FUNNEL_TRANSITIONS_TABLE} WHERE idempotency_key = ? LIMIT 1",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    return {"ok": True, "idempotent": True, "candidate_id": candidate_id}
            # Load current candidate
            row = conn.execute(
                f"SELECT state, state_version FROM {FUNNEL_CANDIDATES_TABLE} WHERE id = ? LIMIT 1",
                (candidate_id,),
            ).fetchone()
            if not row:
                return {"ok": False, "error": "候选标的不存在"}
            current = _row_to_dict(row)
            current_state = str(current.get("state") or "")
            current_version = int(current.get("state_version") or 0)
            # Optimistic lock check
            if state_version and state_version != current_version:
                return {
                    "ok": False,
                    "error": f"状态版本冲突: 期望 {state_version}, 当前 {current_version}",
                    "current_state": current_state,
                    "current_version": current_version,
                }
            # Terminal state protection: only higher-priority sources can overwrite
            if current_state in TERMINAL_STATES:
                incoming_priority = TRIGGER_SOURCE_PRIORITY.get(trigger_source, 0)
                # Find the priority of the trigger_source that last set the terminal state
                last_terminal_source = trigger_source  # default
                if _db.table_exists(conn, FUNNEL_TRANSITIONS_TABLE):
                    last_row = conn.execute(
                        f"SELECT trigger_source FROM {FUNNEL_TRANSITIONS_TABLE} "
                        f"WHERE candidate_id = ? AND to_state = ? ORDER BY id DESC LIMIT 1",
                        (candidate_id, current_state),
                    ).fetchone()
                    if last_row:
                        last_terminal_source = str(_row_to_dict(last_row).get("trigger_source") or "signal")
                last_priority = TRIGGER_SOURCE_PRIORITY.get(last_terminal_source, 0)
                if incoming_priority <= last_priority:
                    return {
                        "ok": False,
                        "error": f"终态保护：{trigger_source}(优先级{incoming_priority})不能覆盖已由{last_terminal_source}(优先级{last_priority})设定的终态 {current_state}",
                        "current_state": current_state,
                        "blocked_by_priority": True,
                    }
            # Validate transition
            allowed = VALID_TRANSITIONS.get(current_state, set())
            if to_state not in allowed:
                return {
                    "ok": False,
                    "error": f"不允许的状态转换: {current_state} -> {to_state}",
                    "current_state": current_state,
                    "allowed_transitions": sorted(allowed),
                }
            now = _utc_now()
            new_version = current_version + 1
            conn.execute(
                f"""
                UPDATE {FUNNEL_CANDIDATES_TABLE}
                SET state = ?, state_version = ?, updated_at = ?
                WHERE id = ?
                """,
                (to_state, new_version, now, candidate_id),
            )
            _record_transition(
                conn,
                candidate_id=candidate_id,
                from_state=current_state,
                to_state=to_state,
                reason=reason,
                evidence_ref=evidence_ref,
                trigger_source=trigger_source,
                operator=operator,
                idempotency_key=idempotency_key,
            )
            return {
                "ok": True,
                "candidate_id": candidate_id,
                "from_state": current_state,
                "to_state": to_state,
                "state_version": new_version,
                "trigger_source_priority": TRIGGER_SOURCE_PRIORITY.get(trigger_source, 0),
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def get_funnel_metrics() -> dict[str, Any]:
    try:
        conn = _db.connect()
        try:
            _db.apply_row_factory(conn)
            table_exists = _db.table_exists(conn, FUNNEL_CANDIDATES_TABLE)
            upstream_hint = _load_upstream_scores_hint(conn)
            if not table_exists:
                status_payload = _derive_funnel_status(total=0, table_exists=False, upstream_hint=upstream_hint)
                return {"state_counts": {}, "total": 0, "upstream_scores": upstream_hint, **status_payload}
            rows = conn.execute(
                f"SELECT state, COUNT(*) as cnt FROM {FUNNEL_CANDIDATES_TABLE} GROUP BY state"
            ).fetchall()
            state_counts: dict[str, int] = {}
            total = 0
            for row in rows:
                d = _row_to_dict(row)
                state = str(d.get("state") or "")
                cnt = int(d.get("cnt") or 0)
                state_counts[state] = cnt
                total += cnt
            status_payload = _derive_funnel_status(total=total, table_exists=True, upstream_hint=upstream_hint)
            return {"state_counts": state_counts, "total": total, "upstream_scores": upstream_hint, **status_payload}
        finally:
            conn.close()
    except Exception as exc:
        return {
            "state_counts": {},
            "total": 0,
            "upstream_scores": {"status": "unknown", "latest_score_date": "", "latest_count": 0},
            **_derive_funnel_status(total=0, table_exists=False, upstream_hint={"status": "unknown"}, error=str(exc)),
            "error": str(exc),
        }


def _ensure_funnel_tables(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {FUNNEL_CANDIDATES_TABLE} (
            id TEXT PRIMARY KEY,
            ts_code TEXT NOT NULL,
            name TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            trigger_source TEXT NOT NULL DEFAULT '',
            reason TEXT NOT NULL DEFAULT '',
            evidence_ref TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL DEFAULT 'ingested',
            state_version INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {FUNNEL_TRANSITIONS_TABLE} (
            id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL,
            from_state TEXT NOT NULL DEFAULT '',
            to_state TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            evidence_ref TEXT NOT NULL DEFAULT '',
            trigger_source TEXT NOT NULL DEFAULT '',
            operator TEXT NOT NULL DEFAULT '',
            idempotency_key TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        """
    )
