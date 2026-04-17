from __future__ import annotations

import json
import math
import os
import re
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, TypedDict

try:
    from langgraph.graph import END, StateGraph
except Exception:  # pragma: no cover
    END = "__end__"
    StateGraph = None


DEFAULT_ANALYST_ROLES = [
    "fundamentals",
    "news",
    "sentiment",
    "technical",
    "macro_flow",
]

TERMINAL_STATUSES = {"approved", "rejected", "deferred", "done_with_warnings", "error"}
V4_ROLLBACK_ENV = "MULTI_ROLE_V4_ROLLBACK_TO_V3"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _loads(raw: str, default):
    try:
        parsed = json.loads(raw or "")
        return parsed if isinstance(parsed, type(default)) else default
    except Exception:
        return default


def _safe_json(value: Any):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and (not math.isfinite(value)):
            return None
        return value
    if isinstance(value, dict):
        return {str(k): _safe_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe_json(x) for x in value]
    return str(value)


def _extract_json_blob(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    match = re.search(r"\{[\s\S]*\}", raw)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return {}
    return {}


def _to_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [text]


def _coerce_confidence(value: Any) -> int | None:
    try:
        score = int(float(value))
    except Exception:
        return None
    return max(0, min(100, score))


def _coerce_node_output(text: str, *, default_claim: str) -> dict[str, Any]:
    payload = _extract_json_blob(text)
    claim = str(payload.get("claim") or "").strip() or default_claim
    evidence = _to_list(payload.get("evidence"))
    risk = _to_list(payload.get("risk"))
    actions = _to_list(payload.get("actions"))
    conflicts = _to_list(payload.get("conflicts"))
    confidence = _coerce_confidence(payload.get("confidence"))
    return {
        "claim": claim,
        "evidence": evidence,
        "confidence": confidence,
        "risk": risk,
        "actions": actions,
        "conflicts": conflicts,
        "raw_text": str(text or ""),
    }


def ensure_multi_role_v3_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS multi_role_v3_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            stage TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            lookback INTEGER NOT NULL DEFAULT 120,
            config_json TEXT NOT NULL DEFAULT '{}',
            state_json TEXT NOT NULL DEFAULT '{}',
            result_json TEXT NOT NULL DEFAULT '{}',
            decision_state_json TEXT NOT NULL DEFAULT '{}',
            metrics_json TEXT NOT NULL DEFAULT '{}',
            error TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT NOT NULL DEFAULT '',
            worker_id TEXT NOT NULL DEFAULT '',
            lease_until TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_multi_role_v3_jobs_status ON multi_role_v3_jobs(status, updated_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_multi_role_v3_jobs_ts_code ON multi_role_v3_jobs(ts_code, created_at)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS multi_role_v3_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_multi_role_v3_events_job_id ON multi_role_v3_events(job_id, id)"
    )
    conn.commit()


def _append_event(conn, *, job_id: str, stage: str, event_type: str, payload: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO multi_role_v3_events (job_id, stage, event_type, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            str(job_id or ""),
            str(stage or ""),
            str(event_type or ""),
            json.dumps(_safe_json(payload), ensure_ascii=False, allow_nan=False),
            _now_iso(),
        ),
    )


def _hydrate_job_row(row: dict[str, Any]) -> dict[str, Any]:
    config = _loads(str(row.get("config_json") or ""), {})
    state = _loads(str(row.get("state_json") or ""), {})
    result = _loads(str(row.get("result_json") or ""), {})
    decision_state = _loads(str(row.get("decision_state_json") or ""), {})
    metrics = _loads(str(row.get("metrics_json") or ""), {})
    output = {
        "job_id": str(row.get("job_id") or ""),
        "version": "v3",
        "engine_version": str(result.get("engine_version") or "v4"),
        "status": str(row.get("status") or ""),
        "stage": str(row.get("stage") or ""),
        "progress": int(metrics.get("progress") or 0),
        "message": str(metrics.get("message") or row.get("status") or ""),
        "ts_code": str(row.get("ts_code") or ""),
        "lookback": int(row.get("lookback") or 120),
        "config": config,
        "workflow": state.get("workflow") or {},
        "node_runs": list(state.get("node_runs") or []),
        "state": state,
        "result": result,
        "decision_state": decision_state,
        "metrics": metrics,
        "error": str(row.get("error") or ""),
        "created_at": str(row.get("created_at") or ""),
        "updated_at": str(row.get("updated_at") or ""),
        "finished_at": str(row.get("finished_at") or ""),
    }
    # Backward-compatible view fields for current frontend rendering.
    output["analysis_markdown"] = str(result.get("analysis_markdown") or "")
    output["analysis"] = output["analysis_markdown"]
    output["role_outputs"] = list(result.get("role_outputs") or [])
    output["role_sections"] = list(result.get("role_outputs") or [])
    output["common_sections_markdown"] = str(result.get("common_sections_markdown") or "")
    output["decision_confidence"] = dict(result.get("decision_confidence") or {})
    output["risk_review"] = dict(result.get("risk_review") or {})
    output["portfolio_view"] = dict(result.get("portfolio_view") or {})
    output["used_model"] = str(result.get("used_model") or "")
    output["requested_model"] = str(result.get("requested_model") or "")
    output["attempts"] = list(result.get("attempts") or [])
    output["role_runs"] = list(result.get("role_runs") or [])
    output["aggregator_run"] = dict(result.get("aggregator_run") or {})
    output["v3_stage_timeline"] = list(state.get("stage_timeline") or [])
    output["v3_research_debate"] = dict(state.get("research_debate") or {})
    output["v3_risk_debate"] = dict(state.get("risk_debate") or {})
    output["v3_portfolio_review"] = dict(state.get("portfolio_review") or {})
    return output


def _queue_alert_threshold() -> int:
    raw = str(os.getenv("MULTI_ROLE_V3_QUEUE_ALERT_THRESHOLD", "3") or "3").strip()
    try:
        return max(1, int(raw))
    except Exception:
        return 3


def _queue_avg_wait_seconds() -> int:
    raw = str(os.getenv("MULTI_ROLE_V3_QUEUE_AVG_WAIT_SECONDS", "120") or "120").strip()
    try:
        return max(30, int(raw))
    except Exception:
        return 120


def _with_queue_info(conn, row: dict[str, Any], hydrated: dict[str, Any]) -> dict[str, Any]:
    job_id = str(row.get("job_id") or "")
    status = str(row.get("status") or "")
    try:
        job_pk = int(row.get("id") or 0)
    except Exception:
        job_pk = 0
    threshold = _queue_alert_threshold()
    avg_wait_seconds = _queue_avg_wait_seconds()

    queued_total_row = conn.execute("SELECT COUNT(*) AS c FROM multi_role_v3_jobs WHERE status = 'queued'").fetchone()
    running_total_row = conn.execute("SELECT COUNT(*) AS c FROM multi_role_v3_jobs WHERE status = 'running'").fetchone()
    active_worker_row = conn.execute(
        "SELECT COUNT(DISTINCT worker_id) AS c FROM multi_role_v3_jobs WHERE status = 'running' AND worker_id != ''"
    ).fetchone()
    queued_total = int((dict(queued_total_row).get("c") if queued_total_row else 0) or 0)
    running_total = int((dict(running_total_row).get("c") if running_total_row else 0) or 0)
    active_workers = int((dict(active_worker_row).get("c") if active_worker_row else 0) or 0)
    if active_workers <= 0:
        active_workers = 1

    queue_position = 0
    queued_ahead = 0
    if status == "queued" and job_pk > 0:
        pos_row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM multi_role_v3_jobs
            WHERE status = 'queued' AND id <= ?
            """,
            (job_pk,),
        ).fetchone()
        ahead_row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM multi_role_v3_jobs
            WHERE status = 'queued' AND id < ?
            """,
            (job_pk,),
        ).fetchone()
        queue_position = int((dict(pos_row).get("c") if pos_row else 0) or 0)
        queued_ahead = int((dict(ahead_row).get("c") if ahead_row else 0) or 0)

    estimated_wait_seconds = 0
    if status == "queued":
        full_batches = queued_ahead // active_workers
        workers_busy = running_total >= active_workers
        estimated_wait_seconds = full_batches * avg_wait_seconds
        if workers_busy:
            estimated_wait_seconds += avg_wait_seconds
    estimated_wait_minutes = max(1, int(math.ceil(estimated_wait_seconds / 60.0))) if estimated_wait_seconds > 0 else 0

    queue_info = {
        "job_id": job_id,
        "status": status,
        "threshold": threshold,
        "queued_total": queued_total,
        "running_total": running_total,
        "active_workers": active_workers,
        "queue_position": queue_position,
        "estimated_wait_seconds": estimated_wait_seconds,
        "estimated_wait_minutes": estimated_wait_minutes,
        "alert": bool(status == "queued" and queued_total >= threshold),
    }
    hydrated["queue_info"] = queue_info
    return hydrated


def create_multi_role_v3_job(*, sqlite3_module, db_path, payload: dict[str, Any]) -> dict[str, Any]:
    ts_code = str(payload.get("ts_code") or "").strip().upper()
    if not ts_code:
        raise ValueError("缺少 ts_code")
    lookback = int(payload.get("lookback") or 120)
    now = _now_iso()
    job_id = uuid.uuid4().hex
    analysts = payload.get("analysts_enabled")
    if not isinstance(analysts, list) or not analysts:
        analysts = list(DEFAULT_ANALYST_ROLES)
    config = {
        "accept_auto_degrade": bool(payload.get("accept_auto_degrade", True)),
        "max_research_debate_rounds": max(1, int(payload.get("max_research_debate_rounds") or 2)),
        "max_risk_debate_rounds": max(1, int(payload.get("max_risk_debate_rounds") or 2)),
        "early_stop": bool(payload.get("early_stop", True)),
        "analysts_enabled": [str(x).strip() for x in analysts if str(x).strip()],
    }
    stage_timeline = [
        {"stage": "analyst", "status": "queued"},
        {"stage": "research_debate", "status": "queued"},
        {"stage": "research_manager", "status": "queued"},
        {"stage": "trader", "status": "queued"},
        {"stage": "risk_debate", "status": "queued"},
        {"stage": "portfolio_manager", "status": "queued"},
    ]
    state = {
        "workflow": {"stage": "queued"},
        "stage_timeline": stage_timeline,
        "node_runs": [],
        "context": {},
        "analyst_outputs": {},
        "research_debate": {"rounds": [], "summary": {}},
        "research_manager": {},
        "trader_plan": {},
        "risk_debate": {"rounds": [], "summary": {}},
        "portfolio_review": {},
        "warnings": [],
    }
    result = {"engine_version": "v4"}
    decision_state = {
        "pending_user_decision": False,
        "pending_stage": "",
        "last_action": "",
        "last_error": "",
        "updated_at": now,
    }
    metrics = {
        "progress": 5,
        "message": "V3 任务已创建，等待 worker 执行",
        "context_build_ms": 0,
        "total_ms": 0,
    }

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        ensure_multi_role_v3_tables(conn)
        conn.execute(
            """
            INSERT INTO multi_role_v3_jobs (
                job_id, status, stage, ts_code, lookback, config_json, state_json, result_json,
                decision_state_json, metrics_json, error, created_at, updated_at, finished_at, worker_id, lease_until
            ) VALUES (?, 'queued', 'queued', ?, ?, ?, ?, ?, ?, ?, '', ?, ?, '', '', '')
            """,
            (
                job_id,
                ts_code,
                lookback,
                json.dumps(_safe_json(config), ensure_ascii=False, allow_nan=False),
                json.dumps(_safe_json(state), ensure_ascii=False, allow_nan=False),
                json.dumps(_safe_json(result), ensure_ascii=False, allow_nan=False),
                json.dumps(_safe_json(decision_state), ensure_ascii=False, allow_nan=False),
                json.dumps(_safe_json(metrics), ensure_ascii=False, allow_nan=False),
                now,
                now,
            ),
        )
        _append_event(conn, job_id=job_id, stage="queued", event_type="job_created", payload={"ts_code": ts_code, "lookback": lookback})
        conn.commit()
        row = conn.execute("SELECT * FROM multi_role_v3_jobs WHERE job_id = ? LIMIT 1", (job_id,)).fetchone()
        raw = dict(row)
        return _with_queue_info(conn, raw, _hydrate_job_row(raw))
    finally:
        conn.close()


def get_multi_role_v3_job(*, sqlite3_module, db_path, job_id: str) -> dict[str, Any] | None:
    target_job_id = str(job_id or "").strip()
    if not target_job_id:
        return None
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        ensure_multi_role_v3_tables(conn)
        row = conn.execute("SELECT * FROM multi_role_v3_jobs WHERE job_id = ? LIMIT 1", (target_job_id,)).fetchone()
        if not row:
            return None
        raw = dict(row)
        return _with_queue_info(conn, raw, _hydrate_job_row(raw))
    finally:
        conn.close()


def _update_job_row(conn, *, job_id: str, status: str, stage: str, state: dict, result: dict, decision_state: dict, metrics: dict, error: str = "", finished: bool = False, worker_id: str = "", lease_until: str = "") -> None:
    now = _now_iso()
    finished_at = now if finished else ""
    conn.execute(
        """
        UPDATE multi_role_v3_jobs
        SET status = ?, stage = ?, state_json = ?, result_json = ?, decision_state_json = ?, metrics_json = ?,
            error = ?, updated_at = ?, finished_at = CASE WHEN ? != '' THEN ? ELSE finished_at END,
            worker_id = ?, lease_until = ?
        WHERE job_id = ?
        """,
        (
            status,
            stage,
            json.dumps(_safe_json(state), ensure_ascii=False, allow_nan=False),
            json.dumps(_safe_json(result), ensure_ascii=False, allow_nan=False),
            json.dumps(_safe_json(decision_state), ensure_ascii=False, allow_nan=False),
            json.dumps(_safe_json(metrics), ensure_ascii=False, allow_nan=False),
            str(error or ""),
            now,
            finished_at,
            finished_at,
            str(worker_id or ""),
            str(lease_until or ""),
            str(job_id or ""),
        ),
    )


def control_multi_role_v3_job(*, sqlite3_module, db_path, job_id: str, action: str, stage: str = "") -> dict[str, Any]:
    target_job_id = str(job_id or "").strip()
    if not target_job_id:
        raise ValueError("job_id 不能为空")
    cmd = str(action or "").strip().lower()
    if cmd not in {"retry_stage", "re_aggregate", "abort", "resume", "retry", "degrade"}:
        raise ValueError("action 必须是 retry_stage|re_aggregate|abort|resume|retry|degrade")
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        ensure_multi_role_v3_tables(conn)
        row = conn.execute("SELECT * FROM multi_role_v3_jobs WHERE job_id = ? LIMIT 1", (target_job_id,)).fetchone()
        if not row:
            raise RuntimeError(f"任务不存在: {target_job_id}")
        raw = dict(row)
        state = _loads(str(raw.get("state_json") or ""), {})
        result = _loads(str(raw.get("result_json") or ""), {})
        decision_state = _loads(str(raw.get("decision_state_json") or ""), {})
        metrics = _loads(str(raw.get("metrics_json") or ""), {})
        status = str(raw.get("status") or "")
        current_stage = str(raw.get("stage") or "")

        if cmd == "abort":
            decision_state.update({"pending_user_decision": False, "last_action": "abort", "updated_at": _now_iso()})
            metrics["progress"] = 100
            metrics["message"] = "任务已终止"
            _update_job_row(
                conn,
                job_id=target_job_id,
                status="error",
                stage="error",
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                error="用户终止任务",
                finished=True,
            )
            _append_event(conn, job_id=target_job_id, stage="error", event_type="abort", payload={})
            conn.commit()
            row2 = conn.execute("SELECT * FROM multi_role_v3_jobs WHERE job_id = ? LIMIT 1", (target_job_id,)).fetchone()
            raw2 = dict(row2)
            return _with_queue_info(conn, raw2, _hydrate_job_row(raw2))

        if cmd == "degrade":
            decision_state.update(
                {
                    "pending_user_decision": False,
                    "last_action": "degrade",
                    "updated_at": _now_iso(),
                }
            )
            metrics["message"] = "用户选择降级，等待 worker 继续执行"
            metrics["progress"] = max(10, int(metrics.get("progress") or 10))
            _update_job_row(
                conn,
                job_id=target_job_id,
                status="queued",
                stage=current_stage if current_stage and current_stage != "error" else "queued",
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                error="",
                finished=False,
                worker_id="",
                lease_until="",
            )
            _append_event(conn, job_id=target_job_id, stage=current_stage or "queued", event_type="degrade", payload={})
            conn.commit()
            row2 = conn.execute("SELECT * FROM multi_role_v3_jobs WHERE job_id = ? LIMIT 1", (target_job_id,)).fetchone()
            raw2 = dict(row2)
            return _with_queue_info(conn, raw2, _hydrate_job_row(raw2))

        target_stage = str(stage or "").strip() or current_stage or "queued"
        if cmd == "re_aggregate":
            target_stage = "portfolio_manager"
        elif cmd == "retry_stage":
            target_stage = target_stage if target_stage != "error" else "analyst"
        elif cmd in {"resume", "retry"} and status in TERMINAL_STATUSES:
            target_stage = "analyst"

        decision_state.update(
            {
                "pending_user_decision": False,
                "last_action": cmd,
                "updated_at": _now_iso(),
            }
        )
        metrics["message"] = f"任务已进入队列，待执行阶段：{target_stage}"
        metrics["progress"] = min(int(metrics.get("progress") or 0), 95)
        _update_job_row(
            conn,
            job_id=target_job_id,
            status="queued",
            stage=target_stage,
            state=state,
            result=result,
            decision_state=decision_state,
            metrics=metrics,
            error="",
            finished=False,
            worker_id="",
            lease_until="",
        )
        _append_event(conn, job_id=target_job_id, stage=target_stage, event_type=cmd, payload={"target_stage": target_stage})
        conn.commit()
        row2 = conn.execute("SELECT * FROM multi_role_v3_jobs WHERE job_id = ? LIMIT 1", (target_job_id,)).fetchone()
        raw2 = dict(row2)
        return _with_queue_info(conn, raw2, _hydrate_job_row(raw2))
    finally:
        conn.close()


def _parse_iso_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _extract_worker_pid(worker_id: str) -> int | None:
    match = re.match(r"^v3w-(\d+)-", str(worker_id or "").strip())
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _pid_alive(pid: int | None) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def reclaim_stale_multi_role_v3_jobs(*, sqlite3_module, db_path, stale_after_seconds: int = 300) -> dict[str, Any]:
    threshold = max(60, int(stale_after_seconds or 300))
    now_dt = datetime.now(timezone.utc)
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    recovered_items: list[dict[str, Any]] = []
    scanned = 0
    try:
        ensure_multi_role_v3_tables(conn)
        rows = conn.execute(
            """
            SELECT *
            FROM multi_role_v3_jobs
            WHERE status = 'running'
            ORDER BY id ASC
            """
        ).fetchall()
        scanned = len(rows)
        for row in rows:
            raw = dict(row)
            job_id = str(raw.get("job_id") or "")
            stage = str(raw.get("stage") or "").strip() or "analyst"
            worker_id = str(raw.get("worker_id") or "").strip()
            updated_at = _parse_iso_datetime(str(raw.get("updated_at") or ""))
            age_seconds = int((now_dt - updated_at).total_seconds()) if updated_at else threshold + 1

            pid = _extract_worker_pid(worker_id)
            alive = _pid_alive(pid)
            stale_reason = ""
            if worker_id and (pid is not None) and (not alive) and age_seconds >= threshold:
                stale_reason = "worker_pid_dead"
            elif worker_id and (pid is None) and age_seconds >= threshold:
                stale_reason = "invalid_worker_id"
            elif (not worker_id) and age_seconds >= threshold:
                stale_reason = "missing_worker_id"
            elif age_seconds >= threshold and stage in {"research_debate", "risk_debate"}:
                stale_reason = "stage_timeout"
            if not stale_reason:
                continue

            state = _loads(str(raw.get("state_json") or ""), {})
            result = _loads(str(raw.get("result_json") or ""), {})
            decision_state = _loads(str(raw.get("decision_state_json") or ""), {})
            metrics = _loads(str(raw.get("metrics_json") or ""), {})
            decision_state.update(
                {
                    "pending_user_decision": False,
                    "last_action": "auto_recover_stale_running",
                    "updated_at": _now_iso(),
                }
            )
            metrics["message"] = f"检测到僵尸任务，已自动回收，等待从阶段 {stage} 继续执行"
            metrics["progress"] = min(95, max(10, int(metrics.get("progress") or _stage_progress(stage) or 10)))
            _update_job_row(
                conn,
                job_id=job_id,
                status="queued",
                stage=stage,
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                error="",
                finished=False,
                worker_id="",
                lease_until="",
            )
            _append_event(
                conn,
                job_id=job_id,
                stage=stage,
                event_type="auto_recover_stale_running",
                payload={
                    "stale_reason": stale_reason,
                    "age_seconds": age_seconds,
                    "previous_worker_id": worker_id,
                    "worker_pid": pid,
                    "worker_pid_alive": alive,
                },
            )
            recovered_items.append(
                {
                    "job_id": job_id,
                    "stage": stage,
                    "stale_reason": stale_reason,
                    "age_seconds": age_seconds,
                    "previous_worker_id": worker_id,
                }
            )
        conn.commit()
        return {
            "ok": True,
            "scanned_running": scanned,
            "recovered_count": len(recovered_items),
            "recovered_items": recovered_items,
            "stale_after_seconds": threshold,
        }
    finally:
        conn.close()


def _stage_progress(stage: str) -> int:
    mapping = {
        "queued": 5,
        "context": 12,
        "analyst": 30,
        "research_debate": 48,
        "research_manager": 58,
        "trader": 68,
        "risk_debate": 80,
        "portfolio_manager": 92,
        "done": 100,
        "error": 100,
        "pending_user_decision": 88,
    }
    return int(mapping.get(stage, 0))


def _persist_stage_checkpoint(
    conn,
    *,
    job_id: str,
    stage: str,
    state: dict[str, Any],
    result: dict,
    decision_state: dict,
    metrics: dict,
    worker_id: str = "",
    message: str = "",
    progress: int | None = None,
) -> None:
    if message:
        metrics["message"] = message
    if progress is None:
        progress = _stage_progress(stage)
    metrics["progress"] = max(0, min(99, int(progress)))
    _update_job_row(
        conn,
        job_id=job_id,
        status="running",
        stage=stage,
        state=state,
        result=result,
        decision_state=decision_state,
        metrics=metrics,
        error="",
        finished=False,
        worker_id=worker_id,
        lease_until=_now_iso() if worker_id else "",
    )
    conn.commit()


def _update_timeline(state: dict[str, Any], stage: str, status: str) -> None:
    timeline = list(state.get("stage_timeline") or [])
    found = False
    for item in timeline:
        if str(item.get("stage") or "") == stage:
            item["status"] = status
            found = True
            break
    if not found and stage:
        timeline.append({"stage": stage, "status": status})
    state["stage_timeline"] = timeline


def _stage_failure(*, conn, job_id: str, stage: str, error_text: str, state: dict, result: dict, decision_state: dict, metrics: dict, config: dict) -> str:
    if bool(config.get("accept_auto_degrade", True)):
        warnings = list(state.get("warnings") or [])
        warnings.append(f"{stage}: {error_text}")
        state["warnings"] = warnings
        _append_event(conn, job_id=job_id, stage=stage, event_type="stage_degraded", payload={"error": error_text})
        return "degraded"
    decision_state.update(
        {
            "pending_user_decision": True,
            "pending_stage": stage,
            "last_error": error_text,
            "updated_at": _now_iso(),
        }
    )
    metrics["progress"] = _stage_progress("pending_user_decision")
    metrics["message"] = f"{stage} 阶段失败，等待用户决策"
    _update_job_row(
        conn,
        job_id=job_id,
        status="pending_user_decision",
        stage=stage,
        state=state,
        result=result,
        decision_state=decision_state,
        metrics=metrics,
        error=error_text,
        finished=False,
    )
    _append_event(conn, job_id=job_id, stage=stage, event_type="pending_user_decision", payload={"error": error_text})
    conn.commit()
    return "pending"


def _llm_node(*, runtime: dict[str, Any], node_key: str, prompt: str, strong: bool, timeout_s: int, default_claim: str) -> dict[str, Any]:
    call_fn: Callable[..., dict[str, Any]] = runtime["llm_call"]
    started = time.time()
    raw = call_fn(node_key=node_key, prompt=prompt, strong=strong, timeout_s=timeout_s)
    text = str(raw.get("text") or "")
    normalized = _coerce_node_output(text, default_claim=default_claim)
    return {
        **normalized,
        "used_model": str(raw.get("used_model") or ""),
        "requested_model": str(raw.get("requested_model") or ""),
        "attempts": list(raw.get("attempts") or []),
        "latency_ms": int((time.time() - started) * 1000),
    }


def _context_digest(context: dict[str, Any]) -> str:
    profile = dict(context.get("company_profile") or {})
    latest = dict((context.get("price_summary") or {}).get("latest") or {})
    metrics = dict((context.get("price_summary") or {}).get("metrics") or {})
    stock_news = dict(context.get("stock_news_summary") or {})
    payload = {
        "company_profile": profile,
        "latest": latest,
        "metrics": metrics,
        "stock_news": {
            "latest_pub_time": stock_news.get("latest_pub_time"),
            "high_importance_count_recent_8": stock_news.get("high_importance_count_recent_8"),
        },
    }
    return json.dumps(_safe_json(payload), ensure_ascii=False, allow_nan=False)


def _render_markdown(state: dict[str, Any], ts_code: str, lookback: int) -> dict[str, Any]:
    analyst_outputs = dict(state.get("analyst_outputs") or {})
    research_manager = dict(state.get("research_manager") or {})
    trader_plan = dict(state.get("trader_plan") or {})
    risk_summary = dict((state.get("risk_debate") or {}).get("summary") or {})
    portfolio_review = dict(state.get("portfolio_review") or {})
    warnings = list(state.get("warnings") or [])

    role_outputs = []
    md_lines = []
    for role, block in analyst_outputs.items():
        claim = str((block or {}).get("claim") or "").strip()
        evidence = _to_list((block or {}).get("evidence"))
        risk = _to_list((block or {}).get("risk"))
        actions = _to_list((block or {}).get("actions"))
        content = [
            f"## {role}",
            claim or "暂无结论",
        ]
        if evidence:
            content.append("### 证据")
            content.extend([f"- {x}" for x in evidence])
        if risk:
            content.append("### 风险")
            content.extend([f"- {x}" for x in risk])
        if actions:
            content.append("### 行动建议")
            content.extend([f"- {x}" for x in actions])
        section = "\n".join(content).strip()
        md_lines.append(section)
        role_outputs.append({"role": role, "content": section, "matched": True, "logic_view": {}})

    consensus = _to_list(research_manager.get("actions")) + _to_list(trader_plan.get("actions"))
    conflicts = _to_list(research_manager.get("conflicts")) + _to_list(risk_summary.get("conflicts"))
    risk_items = _to_list(risk_summary.get("risk")) + warnings
    confidence = _coerce_confidence(portfolio_review.get("confidence"))
    portfolio_claim = str(portfolio_review.get("claim") or "").strip()
    decision = str(portfolio_review.get("decision") or "defer").strip().lower()

    common_lines = [
        "## 综合结论",
        portfolio_claim or "需结合角色观点综合判断。",
        "",
        "## 行动清单",
    ]
    if consensus:
        common_lines.extend([f"- {x}" for x in consensus[:8]])
    else:
        common_lines.append("- 暂无稳定行动清单，请先复核关键证据。")
    common_lines.extend(["", "## 关键分歧"])
    if conflicts:
        common_lines.extend([f"- {x}" for x in conflicts[:8]])
    else:
        common_lines.append("- 暂无显著分歧。")
    common_lines.extend(["", "## 非投资建议免责声明", "以上内容仅供研究参考，不构成任何投资建议。"])
    common_md = "\n".join(common_lines)
    md_lines.append(common_md)
    full_markdown = "\n\n".join([x for x in md_lines if x.strip()])

    decision_label = {"approve": "高", "reject": "低", "defer": "中"}.get(decision, "未显式给出")
    decision_summary = (
        f"Portfolio Manager 决策为 {decision.upper()}。"
        if decision in {"approve", "reject", "defer"}
        else "原始分析未稳定提供最终审批结论。"
    )
    decision_conf = {"score": confidence, "label": decision_label, "summary": decision_summary}
    risk_review = {"summary": "\n".join(risk_items[:10]) if risk_items else "暂无结构化风险段落", "source": "v3"}
    portfolio_view = {"summary": "\n".join(consensus[:10]) if consensus else "暂无结构化行动建议", "source": "v3"}
    aggregator_run = {
        "status": "done",
        "used_model": str(portfolio_review.get("used_model") or ""),
        "requested_model": str(portfolio_review.get("requested_model") or ""),
        "attempts": list(portfolio_review.get("attempts") or []),
        "error": "",
        "duration_ms": int(portfolio_review.get("latency_ms") or 0),
    }
    role_runs = []
    for role, block in analyst_outputs.items():
        role_runs.append(
            {
                "role": role,
                "status": "done",
                "requested_model": str(block.get("requested_model") or ""),
                "used_model": str(block.get("used_model") or ""),
                "attempts": list(block.get("attempts") or []),
                "retry_count": max(0, len(list(block.get("attempts") or [])) - 1),
                "duration_ms": int(block.get("latency_ms") or 0),
                "error": "",
                "output": str(block.get("raw_text") or ""),
            }
        )

    return {
        "analysis_markdown": full_markdown,
        "common_sections_markdown": common_md,
        "role_outputs": role_outputs,
        "decision_confidence": decision_conf,
        "risk_review": risk_review,
        "portfolio_view": portfolio_view,
        "used_model": str(portfolio_review.get("used_model") or ""),
        "requested_model": str(portfolio_review.get("requested_model") or ""),
        "attempts": list(portfolio_review.get("attempts") or []),
        "role_runs": role_runs,
        "aggregator_run": aggregator_run,
        "summary": {
            "ts_code": ts_code,
            "lookback": lookback,
            "decision": decision,
            "confidence": confidence,
            "portfolio_claim": portfolio_claim,
        },
    }


def _set_stage_running(*, state: dict[str, Any], metrics: dict[str, Any], stage: str, message: str) -> None:
    state["workflow"] = {"stage": stage, "status": "running"}
    _update_timeline(state, stage, "running")
    metrics["progress"] = _stage_progress(stage)
    metrics["message"] = message


def _set_stage_done(state: dict[str, Any], stage: str) -> None:
    _update_timeline(state, stage, "done")


def _add_node_run(state: dict[str, Any], *, stage: str, node: str, output: dict[str, Any]) -> None:
    node_runs = list(state.get("node_runs") or [])
    node_runs.append(
        {
            "stage": stage,
            "node": node,
            "used_model": str(output.get("used_model") or ""),
            "requested_model": str(output.get("requested_model") or ""),
            "attempts": list(output.get("attempts") or []),
            "latency_ms": int(output.get("latency_ms") or 0),
            "claim": str(output.get("claim") or ""),
            "confidence": output.get("confidence"),
            "error": "",
        }
    )
    state["node_runs"] = node_runs


def _apply_degrade_placeholder(stage: str) -> dict[str, Any]:
    return {
        "claim": f"{stage} 阶段降级占位：上游节点失败，已跳过。",
        "evidence": [],
        "confidence": None,
        "risk": [f"{stage} 结果缺失，需人工复核"],
        "actions": ["等待模型恢复后补跑该阶段"],
        "conflicts": [],
        "raw_text": "",
        "used_model": "",
        "requested_model": "",
        "attempts": [],
        "latency_ms": 0,
    }


def _build_analyst_prompt(analyst_key: str, context: dict[str, Any], ts_code: str, lookback: int) -> str:
    role_hint = {
        "fundamentals": "基本面分析师",
        "news": "新闻事件分析师",
        "sentiment": "情绪分析师",
        "technical": "技术分析师",
        "macro_flow": "宏观与资金流分析师",
    }.get(analyst_key, analyst_key)
    digest = _context_digest(context)
    return (
        f"你是{role_hint}。请围绕股票 {ts_code}（观察窗口 {lookback} 日）产出结构化结论。"
        "只输出 JSON 对象，字段必须包含：claim,evidence,confidence,risk,actions,conflicts。\n"
        f"输入上下文摘要：{digest}"
    )


def _build_research_prompt(side: str, analyst_outputs: dict[str, Any], debate_history: list[dict[str, Any]], round_idx: int) -> str:
    snapshot = {
        "analyst_outputs": analyst_outputs,
        "debate_history": debate_history[-4:],
        "round": round_idx,
    }
    side_text = "看多研究员" if side == "bull" else "看空研究员"
    return (
        f"你是{side_text}。请基于 analyst 证据进行辩论。"
        "只输出 JSON：claim,evidence,confidence,risk,actions,conflicts。\n"
        f"输入：{json.dumps(_safe_json(snapshot), ensure_ascii=False, allow_nan=False)}"
    )


def _build_risk_prompt(side: str, trader_plan: dict[str, Any], research_summary: dict[str, Any], risk_history: list[dict[str, Any]], round_idx: int) -> str:
    snapshot = {
        "trader_plan": trader_plan,
        "research_summary": research_summary,
        "history": risk_history[-6:],
        "round": round_idx,
    }
    role_map = {"aggressive": "激进风控", "neutral": "中性风控", "conservative": "保守风控"}
    role_text = role_map.get(side, side)
    return (
        f"你是{role_text}。请给出约束与反驳。"
        "只输出 JSON：claim,evidence,confidence,risk,actions,conflicts。\n"
        f"输入：{json.dumps(_safe_json(snapshot), ensure_ascii=False, allow_nan=False)}"
    )


def _build_manager_prompt(kind: str, payload: dict[str, Any]) -> str:
    schema = "claim,evidence,confidence,risk,actions,conflicts"
    extra = ""
    if kind == "portfolio_manager":
        extra = (
            "，并必须新增以下字段："
            "direction（bullish/bearish/neutral 三选一，必须明确，禁止模糊）、"
            "decision（approve/reject/defer）、"
            "invalidation（失效条件，一句话：什么情况下本判断需要推翻）。"
            "你的职责是裁决，不是平衡各方观点。"
            "claim 字段必须是一句话的核心押注，不超过50字，禁止使用'一方面...另一方面'句式。"
        )
    return (
        f"你是{kind}。请完成该阶段最终裁决。"
        f"只输出 JSON，字段必须包含 {schema}{extra}。\n"
        f"输入：{json.dumps(_safe_json(payload), ensure_ascii=False, allow_nan=False)}"
    )


def _run_pipeline_legacy(conn, row: dict[str, Any], runtime: dict[str, Any], worker_id: str) -> None:
    job_id = str(row.get("job_id") or "")
    ts_code = str(row.get("ts_code") or "")
    lookback = int(row.get("lookback") or 120)
    config = _loads(str(row.get("config_json") or ""), {})
    state = _loads(str(row.get("state_json") or ""), {})
    result = _loads(str(row.get("result_json") or ""), {})
    decision_state = _loads(str(row.get("decision_state_json") or ""), {})
    metrics = _loads(str(row.get("metrics_json") or ""), {})
    stage = str(row.get("stage") or "queued")
    started_ts = time.time()

    def save(stage_name: str, status: str = "running", message: str = ""):
        if message:
            metrics["message"] = message
        metrics["progress"] = _stage_progress(stage_name if status != "done" else "done")
        _update_job_row(
            conn,
            job_id=job_id,
            status=status,
            stage=stage_name,
            state=state,
            result=result,
            decision_state=decision_state,
            metrics=metrics,
            error="" if status != "error" else str(decision_state.get("last_error") or ""),
            finished=status in TERMINAL_STATUSES,
            worker_id=worker_id if status not in TERMINAL_STATUSES else "",
            lease_until="",
        )
        conn.commit()

    # Context stage
    if stage in {"queued", "context", "analyst"}:
        _set_stage_running(state=state, metrics=metrics, stage="context", message="正在构建 V3 上下文")
        save("context", "running")
        try:
            build_context_fn: Callable[..., dict[str, Any]] = runtime["build_context"]
            context_started = time.time()
            context = build_context_fn(ts_code=ts_code, lookback=lookback)
            state["context"] = _safe_json(context)
            metrics["context_build_ms"] = int((time.time() - context_started) * 1000)
            _append_event(conn, job_id=job_id, stage="context", event_type="context_ready", payload={"context_build_ms": metrics["context_build_ms"]})
            _set_stage_done(state, "context")
        except Exception as exc:
            decision_state["last_error"] = str(exc)
            _update_timeline(state, "context", "error")
            save("error", "error", "上下文构建失败")
            return
        save("analyst", "running", "Analyst 取证阶段执行中")

    context = dict(state.get("context") or {})

    # Analyst stage
    if stage in {"queued", "context", "analyst", "research_debate", "research_manager", "trader", "risk_debate", "portfolio_manager"}:
        _set_stage_running(state=state, metrics=metrics, stage="analyst", message="Analyst 取证中")
        save("analyst", "running")
        analyst_outputs = dict(state.get("analyst_outputs") or {})
        analysts = [str(x).strip() for x in list(config.get("analysts_enabled") or DEFAULT_ANALYST_ROLES) if str(x).strip()]
        for analyst in analysts:
            if analyst in analyst_outputs:
                continue
            try:
                output = _llm_node(
                    runtime=runtime,
                    node_key=f"analyst:{analyst}",
                    prompt=_build_analyst_prompt(analyst, context, ts_code, lookback),
                    strong=False,
                    timeout_s=45,
                    default_claim=f"{analyst} 观点生成失败，需人工复核",
                )
                analyst_outputs[analyst] = output
                _add_node_run(state, stage="analyst", node=analyst, output=output)
            except Exception as exc:
                flow = _stage_failure(
                    conn=conn,
                    job_id=job_id,
                    stage="analyst",
                    error_text=str(exc),
                    state=state,
                    result=result,
                    decision_state=decision_state,
                    metrics=metrics,
                    config=config,
                )
                if flow == "pending":
                    return
                analyst_outputs[analyst] = _apply_degrade_placeholder("analyst")
        state["analyst_outputs"] = analyst_outputs
        _set_stage_done(state, "analyst")
        _append_event(conn, job_id=job_id, stage="analyst", event_type="stage_done", payload={"nodes": list(analyst_outputs.keys())})
        save("research_debate", "running", "Bull/Bear 研究辩论中")

    # Research debate stage
    if stage in {"queued", "context", "analyst", "research_debate", "research_manager", "trader", "risk_debate", "portfolio_manager"}:
        _set_stage_running(state=state, metrics=metrics, stage="research_debate", message="研究辩论阶段执行中")
        save("research_debate", "running")
        research_debate = dict(state.get("research_debate") or {"rounds": [], "summary": {}})
        rounds = list(research_debate.get("rounds") or [])
        max_rounds = max(1, int(config.get("max_research_debate_rounds") or 2))
        early_stop = bool(config.get("early_stop", True))
        analyst_outputs = dict(state.get("analyst_outputs") or {})
        for idx in range(len(rounds) + 1, max_rounds + 1):
            bull = _llm_node(
                runtime=runtime,
                node_key="research:bull",
                prompt=_build_research_prompt("bull", analyst_outputs, rounds, idx),
                strong=True,
                timeout_s=60,
                default_claim="看多观点待补充",
            )
            bear = _llm_node(
                runtime=runtime,
                node_key="research:bear",
                prompt=_build_research_prompt("bear", analyst_outputs, rounds + [{"bull": bull}], idx),
                strong=True,
                timeout_s=60,
                default_claim="看空观点待补充",
            )
            rounds.append({"round": idx, "bull": bull, "bear": bear})
            _add_node_run(state, stage="research_debate", node=f"bull_r{idx}", output=bull)
            _add_node_run(state, stage="research_debate", node=f"bear_r{idx}", output=bear)
            _persist_stage_checkpoint(
                conn,
                job_id=job_id,
                stage="research_debate",
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                worker_id=worker_id,
                message=f"研究辩论第 {idx}/{max_rounds} 轮已完成",
                progress=min(65, 48 + idx * 8),
            )
            if early_stop:
                bull_conflicts = len(_to_list(bull.get("conflicts")))
                bear_conflicts = len(_to_list(bear.get("conflicts")))
                if bull_conflicts == 0 and bear_conflicts == 0:
                    break
        summary_payload = {"rounds": rounds, "analyst_outputs": analyst_outputs}
        try:
            summary = _llm_node(
                runtime=runtime,
                node_key="research:manager",
                prompt=_build_manager_prompt("research_manager", summary_payload),
                strong=True,
                timeout_s=70,
                default_claim="研究辩论已完成，但未形成稳定总结",
            )
        except Exception as exc:
            flow = _stage_failure(
                conn=conn,
                job_id=job_id,
                stage="research_debate",
                error_text=str(exc),
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                config=config,
            )
            if flow == "pending":
                return
            summary = _apply_degrade_placeholder("research_debate")
        research_debate["rounds"] = rounds
        research_debate["summary"] = summary
        state["research_debate"] = research_debate
        _set_stage_done(state, "research_debate")
        _persist_stage_checkpoint(
            conn,
            job_id=job_id,
            stage="research_debate",
            state=state,
            result=result,
            decision_state=decision_state,
            metrics=metrics,
            worker_id=worker_id,
            message="研究辩论总结完成，进入研究经理裁决",
            progress=68,
        )
        _append_event(conn, job_id=job_id, stage="research_debate", event_type="stage_done", payload={"rounds": len(rounds)})
        save("research_manager", "running", "Research Manager 裁决中")

    # Research manager decision
    if stage in {"queued", "context", "analyst", "research_debate", "research_manager", "trader", "risk_debate", "portfolio_manager"}:
        _set_stage_running(state=state, metrics=metrics, stage="research_manager", message="研究经理裁决中")
        save("research_manager", "running")
        payload = {
            "research_summary": (state.get("research_debate") or {}).get("summary") or {},
            "analyst_outputs": state.get("analyst_outputs") or {},
        }
        try:
            manager = _llm_node(
                runtime=runtime,
                node_key="decision:research_manager",
                prompt=_build_manager_prompt("research_manager", payload),
                strong=True,
                timeout_s=65,
                default_claim="研究经理未形成稳定裁决",
            )
            state["research_manager"] = manager
            _add_node_run(state, stage="research_manager", node="research_manager", output=manager)
        except Exception as exc:
            flow = _stage_failure(
                conn=conn,
                job_id=job_id,
                stage="research_manager",
                error_text=str(exc),
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                config=config,
            )
            if flow == "pending":
                return
            state["research_manager"] = _apply_degrade_placeholder("research_manager")
        _set_stage_done(state, "research_manager")
        _append_event(conn, job_id=job_id, stage="research_manager", event_type="stage_done", payload={})
        save("trader", "running", "Trader 交易方案生成中")

    # Trader stage
    if stage in {"queued", "context", "analyst", "research_debate", "research_manager", "trader", "risk_debate", "portfolio_manager"}:
        _set_stage_running(state=state, metrics=metrics, stage="trader", message="Trader 阶段执行中")
        save("trader", "running")
        payload = {
            "research_manager": state.get("research_manager") or {},
            "analyst_outputs": state.get("analyst_outputs") or {},
            "ts_code": ts_code,
            "lookback": lookback,
        }
        try:
            trader = _llm_node(
                runtime=runtime,
                node_key="decision:trader",
                prompt=_build_manager_prompt("trader", payload),
                strong=True,
                timeout_s=60,
                default_claim="交易方案待补充",
            )
            state["trader_plan"] = trader
            _add_node_run(state, stage="trader", node="trader", output=trader)
        except Exception as exc:
            flow = _stage_failure(
                conn=conn,
                job_id=job_id,
                stage="trader",
                error_text=str(exc),
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                config=config,
            )
            if flow == "pending":
                return
            state["trader_plan"] = _apply_degrade_placeholder("trader")
        _set_stage_done(state, "trader")
        _append_event(conn, job_id=job_id, stage="trader", event_type="stage_done", payload={})
        save("risk_debate", "running", "风险三方辩论中")

    # Risk debate stage
    if stage in {"queued", "context", "analyst", "research_debate", "research_manager", "trader", "risk_debate", "portfolio_manager"}:
        _set_stage_running(state=state, metrics=metrics, stage="risk_debate", message="风险辩论阶段执行中")
        save("risk_debate", "running")
        risk_debate = dict(state.get("risk_debate") or {"rounds": [], "summary": {}})
        rounds = list(risk_debate.get("rounds") or [])
        max_rounds = max(1, int(config.get("max_risk_debate_rounds") or 2))
        research_summary = (state.get("research_debate") or {}).get("summary") or {}
        trader_plan = state.get("trader_plan") or {}
        for idx in range(len(rounds) + 1, max_rounds + 1):
            aggressive = _llm_node(
                runtime=runtime,
                node_key="risk:aggressive",
                prompt=_build_risk_prompt("aggressive", trader_plan, research_summary, rounds, idx),
                strong=False,
                timeout_s=45,
                default_claim="激进风控观点待补充",
            )
            conservative = _llm_node(
                runtime=runtime,
                node_key="risk:conservative",
                prompt=_build_risk_prompt("conservative", trader_plan, research_summary, rounds + [{"aggressive": aggressive}], idx),
                strong=False,
                timeout_s=45,
                default_claim="保守风控观点待补充",
            )
            neutral = _llm_node(
                runtime=runtime,
                node_key="risk:neutral",
                prompt=_build_risk_prompt("neutral", trader_plan, research_summary, rounds + [{"aggressive": aggressive, "conservative": conservative}], idx),
                strong=True,
                timeout_s=55,
                default_claim="中性风控观点待补充",
            )
            rounds.append({"round": idx, "aggressive": aggressive, "conservative": conservative, "neutral": neutral})
            _add_node_run(state, stage="risk_debate", node=f"aggressive_r{idx}", output=aggressive)
            _add_node_run(state, stage="risk_debate", node=f"conservative_r{idx}", output=conservative)
            _add_node_run(state, stage="risk_debate", node=f"neutral_r{idx}", output=neutral)
            _persist_stage_checkpoint(
                conn,
                job_id=job_id,
                stage="risk_debate",
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                worker_id=worker_id,
                message=f"风险辩论第 {idx}/{max_rounds} 轮已完成",
                progress=min(88, 80 + idx * 4),
            )
        summary_payload = {
            "trader_plan": trader_plan,
            "research_summary": research_summary,
            "risk_rounds": rounds,
        }
        try:
            summary = _llm_node(
                runtime=runtime,
                node_key="risk:manager",
                prompt=_build_manager_prompt("risk_manager", summary_payload),
                strong=True,
                timeout_s=65,
                default_claim="风险辩论未形成稳定结论",
            )
        except Exception as exc:
            flow = _stage_failure(
                conn=conn,
                job_id=job_id,
                stage="risk_debate",
                error_text=str(exc),
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                config=config,
            )
            if flow == "pending":
                return
            summary = _apply_degrade_placeholder("risk_debate")
        risk_debate["rounds"] = rounds
        risk_debate["summary"] = summary
        state["risk_debate"] = risk_debate
        _set_stage_done(state, "risk_debate")
        _persist_stage_checkpoint(
            conn,
            job_id=job_id,
            stage="risk_debate",
            state=state,
            result=result,
            decision_state=decision_state,
            metrics=metrics,
            worker_id=worker_id,
            message="风险辩论总结完成，进入 Portfolio Manager 审批",
            progress=92,
        )
        _append_event(conn, job_id=job_id, stage="risk_debate", event_type="stage_done", payload={"rounds": len(rounds)})
        save("portfolio_manager", "running", "Portfolio Manager 审批中")

    # Portfolio manager
    _set_stage_running(state=state, metrics=metrics, stage="portfolio_manager", message="Portfolio Manager 审批中")
    save("portfolio_manager", "running")
    payload = {
        "trader_plan": state.get("trader_plan") or {},
        "risk_summary": (state.get("risk_debate") or {}).get("summary") or {},
        "research_summary": (state.get("research_debate") or {}).get("summary") or {},
        "warnings": state.get("warnings") or [],
    }
    try:
        review = _llm_node(
            runtime=runtime,
            node_key="decision:portfolio_manager",
            prompt=_build_manager_prompt("portfolio_manager", payload),
            strong=True,
            timeout_s=70,
            default_claim="审批结果未生成，默认 defer",
        )
    except Exception as exc:
        flow = _stage_failure(
            conn=conn,
            job_id=job_id,
            stage="portfolio_manager",
            error_text=str(exc),
            state=state,
            result=result,
            decision_state=decision_state,
            metrics=metrics,
            config=config,
        )
        if flow == "pending":
            return
        review = _apply_degrade_placeholder("portfolio_manager")
        review["decision"] = "defer"
    decision = str(_extract_json_blob(str(review.get("raw_text") or "")).get("decision") or "").strip().lower()
    if decision not in {"approve", "reject", "defer"}:
        decision = "defer"
    review["decision"] = decision
    state["portfolio_review"] = review
    _add_node_run(state, stage="portfolio_manager", node="portfolio_manager", output=review)
    _set_stage_done(state, "portfolio_manager")
    _append_event(conn, job_id=job_id, stage="portfolio_manager", event_type="stage_done", payload={"decision": decision})

    rendered = _render_markdown(state, ts_code, lookback)
    result.update(rendered)
    elapsed_ms = int((time.time() - started_ts) * 1000)
    metrics["total_ms"] = int(metrics.get("total_ms") or 0) + elapsed_ms
    metrics["progress"] = 100
    metrics["message"] = f"V3 分析完成 · total {metrics['total_ms']}ms"

    if decision == "approve":
        final_status = "approved"
    elif decision == "reject":
        final_status = "rejected"
    else:
        final_status = "deferred"
    if state.get("warnings"):
        final_status = "done_with_warnings" if final_status == "deferred" else final_status
    decision_state.update({"pending_user_decision": False, "pending_stage": "", "updated_at": _now_iso()})
    _update_job_row(
        conn,
        job_id=job_id,
        status=final_status,
        stage="done",
        state=state,
        result=result,
        decision_state=decision_state,
        metrics=metrics,
        error="",
        finished=True,
        worker_id="",
        lease_until="",
    )
    _append_event(conn, job_id=job_id, stage="done", event_type="job_done", payload={"status": final_status})
    conn.commit()


class _V4GraphState(TypedDict):
    job_id: str
    ts_code: str
    lookback: int
    config: dict[str, Any]
    state: dict[str, Any]
    result: dict[str, Any]
    decision_state: dict[str, Any]
    metrics: dict[str, Any]
    worker_id: str
    pending: bool
    pending_stage: str
    final_status: str


def _run_pipeline_v4_langgraph(conn, row: dict[str, Any], runtime: dict[str, Any], worker_id: str) -> None:
    if StateGraph is None:
        raise RuntimeError("LangGraph 未安装，无法使用 v4 引擎")
    job_id = str(row.get("job_id") or "")
    ts_code = str(row.get("ts_code") or "")
    lookback = int(row.get("lookback") or 120)
    config = _loads(str(row.get("config_json") or ""), {})
    state = _loads(str(row.get("state_json") or ""), {})
    result = _loads(str(row.get("result_json") or ""), {})
    result["engine_version"] = "v4"
    decision_state = _loads(str(row.get("decision_state_json") or ""), {})
    metrics = _loads(str(row.get("metrics_json") or ""), {})
    started_ts = time.time()

    def _save(stage_name: str, status: str, error_text: str = "", finished: bool = False) -> None:
        _update_job_row(
            conn,
            job_id=job_id,
            status=status,
            stage=stage_name,
            state=state,
            result=result,
            decision_state=decision_state,
            metrics=metrics,
            error=error_text,
            finished=finished,
            worker_id="" if finished else worker_id,
            lease_until="",
        )
        conn.commit()

    def _node_start(stage_name: str, message: str) -> None:
        _set_stage_running(state=state, metrics=metrics, stage=stage_name, message=message)
        _append_event(conn, job_id=job_id, stage=stage_name, event_type="node_start", payload={"message": message})
        _save(stage_name, "running")

    def _node_done(stage_name: str, payload: dict[str, Any] | None = None) -> None:
        _set_stage_done(state, stage_name)
        _append_event(conn, job_id=job_id, stage=stage_name, event_type="node_done", payload=payload or {})

    def _node_error(stage_name: str, error_text: str) -> None:
        _append_event(conn, job_id=job_id, stage=stage_name, event_type="node_error", payload={"error": error_text})

    def _handle_stage_error(stage_name: str, exc: Exception, placeholder_key: str = "") -> bool:
        error_text = str(exc)
        _node_error(stage_name, error_text)
        if bool(config.get("accept_auto_degrade", True)):
            warnings = list(state.get("warnings") or [])
            warnings.append(f"{stage_name}: {error_text}")
            state["warnings"] = warnings
            if placeholder_key:
                state[placeholder_key] = _apply_degrade_placeholder(stage_name)
            return False
        decision_state.update(
            {
                "pending_user_decision": True,
                "pending_stage": stage_name,
                "last_error": error_text,
                "updated_at": _now_iso(),
            }
        )
        metrics["progress"] = _stage_progress("pending_user_decision")
        metrics["message"] = f"{stage_name} 阶段失败，等待用户决策"
        return True

    def node_context(gs: _V4GraphState) -> _V4GraphState:
        _node_start("context", "正在构建 V4 上下文")
        try:
            build_context_fn: Callable[..., dict[str, Any]] = runtime["build_context"]
            started = time.time()
            ctx = build_context_fn(ts_code=ts_code, lookback=lookback)
            state["context"] = _safe_json(ctx)
            metrics["context_build_ms"] = int((time.time() - started) * 1000)
            _node_done("context", {"context_build_ms": metrics["context_build_ms"]})
        except Exception as exc:
            _node_error("context", str(exc))
            decision_state["last_error"] = str(exc)
            metrics["progress"] = 100
            metrics["message"] = "上下文构建失败"
            gs["final_status"] = "error"
            return gs
        return gs

    def node_analyst(gs: _V4GraphState) -> _V4GraphState:
        _node_start("analyst", "Analyst 取证中")
        analyst_outputs = dict(state.get("analyst_outputs") or {})
        analysts = [str(x).strip() for x in list(config.get("analysts_enabled") or DEFAULT_ANALYST_ROLES) if str(x).strip()]
        context = dict(state.get("context") or {})
        for analyst in analysts:
            if analyst in analyst_outputs:
                continue
            try:
                output = _llm_node(
                    runtime=runtime,
                    node_key=f"analyst:{analyst}",
                    prompt=_build_analyst_prompt(analyst, context, ts_code, lookback),
                    strong=False,
                    timeout_s=45,
                    default_claim=f"{analyst} 观点生成失败，需人工复核",
                )
                analyst_outputs[analyst] = output
                _add_node_run(state, stage="analyst", node=analyst, output=output)
            except Exception as exc:
                if _handle_stage_error("analyst", exc):
                    gs["pending"] = True
                    gs["pending_stage"] = "analyst"
                    return gs
                analyst_outputs[analyst] = _apply_degrade_placeholder("analyst")
        state["analyst_outputs"] = analyst_outputs
        _node_done("analyst", {"nodes": list(analyst_outputs.keys())})
        return gs

    def node_research_debate(gs: _V4GraphState) -> _V4GraphState:
        _node_start("research_debate", "Bull/Bear 研究辩论中")
        research_debate = dict(state.get("research_debate") or {"rounds": [], "summary": {}})
        rounds = list(research_debate.get("rounds") or [])
        max_rounds = max(1, int(config.get("max_research_debate_rounds") or 2))
        early_stop = bool(config.get("early_stop", True))
        analyst_outputs = dict(state.get("analyst_outputs") or {})
        for idx in range(len(rounds) + 1, max_rounds + 1):
            try:
                bull = _llm_node(
                    runtime=runtime,
                    node_key="research:bull",
                    prompt=_build_research_prompt("bull", analyst_outputs, rounds, idx),
                    strong=True,
                    timeout_s=60,
                    default_claim="看多观点待补充",
                )
                bear = _llm_node(
                    runtime=runtime,
                    node_key="research:bear",
                    prompt=_build_research_prompt("bear", analyst_outputs, rounds + [{"bull": bull}], idx),
                    strong=True,
                    timeout_s=60,
                    default_claim="看空观点待补充",
                )
            except Exception as exc:
                if _handle_stage_error("research_debate", exc, "research_debate"):
                    gs["pending"] = True
                    gs["pending_stage"] = "research_debate"
                    return gs
                break
            rounds.append({"round": idx, "bull": bull, "bear": bear})
            _add_node_run(state, stage="research_debate", node=f"bull_r{idx}", output=bull)
            _add_node_run(state, stage="research_debate", node=f"bear_r{idx}", output=bear)
            _persist_stage_checkpoint(
                conn,
                job_id=job_id,
                stage="research_debate",
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                worker_id=worker_id,
                message=f"研究辩论第 {idx}/{max_rounds} 轮已完成",
                progress=min(65, 48 + idx * 8),
            )
            if early_stop and not _to_list(bull.get("conflicts")) and not _to_list(bear.get("conflicts")):
                break
        try:
            summary = _llm_node(
                runtime=runtime,
                node_key="research:manager",
                prompt=_build_manager_prompt("research_manager", {"rounds": rounds, "analyst_outputs": analyst_outputs}),
                strong=True,
                timeout_s=70,
                default_claim="研究辩论已完成，但未形成稳定总结",
            )
        except Exception as exc:
            if _handle_stage_error("research_debate", exc):
                gs["pending"] = True
                gs["pending_stage"] = "research_debate"
                return gs
            summary = _apply_degrade_placeholder("research_debate")
        research_debate["rounds"] = rounds
        research_debate["summary"] = summary
        state["research_debate"] = research_debate
        _persist_stage_checkpoint(
            conn,
            job_id=job_id,
            stage="research_debate",
            state=state,
            result=result,
            decision_state=decision_state,
            metrics=metrics,
            worker_id=worker_id,
            message="研究辩论总结完成，进入研究经理裁决",
            progress=68,
        )
        _node_done("research_debate", {"rounds": len(rounds)})
        return gs

    def node_research_manager(gs: _V4GraphState) -> _V4GraphState:
        _node_start("research_manager", "Research Manager 裁决中")
        try:
            manager = _llm_node(
                runtime=runtime,
                node_key="decision:research_manager",
                prompt=_build_manager_prompt(
                    "research_manager",
                    {"research_summary": (state.get("research_debate") or {}).get("summary") or {}, "analyst_outputs": state.get("analyst_outputs") or {}},
                ),
                strong=True,
                timeout_s=65,
                default_claim="研究经理未形成稳定裁决",
            )
            state["research_manager"] = manager
            _add_node_run(state, stage="research_manager", node="research_manager", output=manager)
        except Exception as exc:
            if _handle_stage_error("research_manager", exc, "research_manager"):
                gs["pending"] = True
                gs["pending_stage"] = "research_manager"
                return gs
        _node_done("research_manager")
        return gs

    def node_trader(gs: _V4GraphState) -> _V4GraphState:
        _node_start("trader", "Trader 交易方案生成中")
        try:
            trader = _llm_node(
                runtime=runtime,
                node_key="decision:trader",
                prompt=_build_manager_prompt(
                    "trader",
                    {
                        "research_manager": state.get("research_manager") or {},
                        "analyst_outputs": state.get("analyst_outputs") or {},
                        "ts_code": ts_code,
                        "lookback": lookback,
                    },
                ),
                strong=True,
                timeout_s=60,
                default_claim="交易方案待补充",
            )
            state["trader_plan"] = trader
            _add_node_run(state, stage="trader", node="trader", output=trader)
        except Exception as exc:
            if _handle_stage_error("trader", exc, "trader_plan"):
                gs["pending"] = True
                gs["pending_stage"] = "trader"
                return gs
        _node_done("trader")
        return gs

    def node_risk_debate(gs: _V4GraphState) -> _V4GraphState:
        _node_start("risk_debate", "风险三方辩论中")
        risk_debate = dict(state.get("risk_debate") or {"rounds": [], "summary": {}})
        rounds = list(risk_debate.get("rounds") or [])
        max_rounds = max(1, int(config.get("max_risk_debate_rounds") or 2))
        research_summary = (state.get("research_debate") or {}).get("summary") or {}
        trader_plan = state.get("trader_plan") or {}
        for idx in range(len(rounds) + 1, max_rounds + 1):
            try:
                aggressive = _llm_node(
                    runtime=runtime,
                    node_key="risk:aggressive",
                    prompt=_build_risk_prompt("aggressive", trader_plan, research_summary, rounds, idx),
                    strong=False,
                    timeout_s=45,
                    default_claim="激进风控观点待补充",
                )
                conservative = _llm_node(
                    runtime=runtime,
                    node_key="risk:conservative",
                    prompt=_build_risk_prompt("conservative", trader_plan, research_summary, rounds + [{"aggressive": aggressive}], idx),
                    strong=False,
                    timeout_s=45,
                    default_claim="保守风控观点待补充",
                )
                neutral = _llm_node(
                    runtime=runtime,
                    node_key="risk:neutral",
                    prompt=_build_risk_prompt("neutral", trader_plan, research_summary, rounds + [{"aggressive": aggressive, "conservative": conservative}], idx),
                    strong=True,
                    timeout_s=55,
                    default_claim="中性风控观点待补充",
                )
            except Exception as exc:
                if _handle_stage_error("risk_debate", exc):
                    gs["pending"] = True
                    gs["pending_stage"] = "risk_debate"
                    return gs
                break
            rounds.append({"round": idx, "aggressive": aggressive, "conservative": conservative, "neutral": neutral})
            _add_node_run(state, stage="risk_debate", node=f"aggressive_r{idx}", output=aggressive)
            _add_node_run(state, stage="risk_debate", node=f"conservative_r{idx}", output=conservative)
            _add_node_run(state, stage="risk_debate", node=f"neutral_r{idx}", output=neutral)
            _persist_stage_checkpoint(
                conn,
                job_id=job_id,
                stage="risk_debate",
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                worker_id=worker_id,
                message=f"风险辩论第 {idx}/{max_rounds} 轮已完成",
                progress=min(88, 80 + idx * 4),
            )
        try:
            summary = _llm_node(
                runtime=runtime,
                node_key="risk:manager",
                prompt=_build_manager_prompt(
                    "risk_manager",
                    {"trader_plan": trader_plan, "research_summary": research_summary, "risk_rounds": rounds},
                ),
                strong=True,
                timeout_s=65,
                default_claim="风险辩论未形成稳定结论",
            )
        except Exception as exc:
            if _handle_stage_error("risk_debate", exc):
                gs["pending"] = True
                gs["pending_stage"] = "risk_debate"
                return gs
            summary = _apply_degrade_placeholder("risk_debate")
        risk_debate["rounds"] = rounds
        risk_debate["summary"] = summary
        state["risk_debate"] = risk_debate
        _persist_stage_checkpoint(
            conn,
            job_id=job_id,
            stage="risk_debate",
            state=state,
            result=result,
            decision_state=decision_state,
            metrics=metrics,
            worker_id=worker_id,
            message="风险辩论总结完成，进入 Portfolio Manager 审批",
            progress=92,
        )
        _node_done("risk_debate", {"rounds": len(rounds)})
        return gs

    def node_portfolio_manager(gs: _V4GraphState) -> _V4GraphState:
        _node_start("portfolio_manager", "Portfolio Manager 审批中")
        try:
            review = _llm_node(
                runtime=runtime,
                node_key="decision:portfolio_manager",
                prompt=_build_manager_prompt(
                    "portfolio_manager",
                    {
                        "trader_plan": state.get("trader_plan") or {},
                        "risk_summary": (state.get("risk_debate") or {}).get("summary") or {},
                        "research_summary": (state.get("research_debate") or {}).get("summary") or {},
                        "warnings": state.get("warnings") or [],
                    },
                ),
                strong=True,
                timeout_s=70,
                default_claim="审批结果未生成，默认 defer",
            )
        except Exception as exc:
            if _handle_stage_error("portfolio_manager", exc):
                gs["pending"] = True
                gs["pending_stage"] = "portfolio_manager"
                return gs
            review = _apply_degrade_placeholder("portfolio_manager")
            review["decision"] = "defer"
        decision = str(_extract_json_blob(str(review.get("raw_text") or "")).get("decision") or "").strip().lower()
        if decision not in {"approve", "reject", "defer"}:
            decision = "defer"
        review["decision"] = decision
        state["portfolio_review"] = review
        _add_node_run(state, stage="portfolio_manager", node="portfolio_manager", output=review)
        _node_done("portfolio_manager", {"decision": decision})
        _append_event(conn, job_id=job_id, stage="portfolio_manager", event_type="decision_applied", payload={"decision": decision})
        rendered = _render_markdown(state, ts_code, lookback)
        rendered["engine_version"] = "v4"
        result.update(rendered)
        elapsed_ms = int((time.time() - started_ts) * 1000)
        metrics["total_ms"] = int(metrics.get("total_ms") or 0) + elapsed_ms
        metrics["progress"] = 100
        metrics["message"] = f"V4 分析完成 · total {metrics['total_ms']}ms"
        if decision == "approve":
            gs["final_status"] = "approved"
        elif decision == "reject":
            gs["final_status"] = "rejected"
        else:
            gs["final_status"] = "deferred"
        if state.get("warnings"):
            gs["final_status"] = "done_with_warnings" if gs["final_status"] == "deferred" else gs["final_status"]
        return gs

    def _route_pending(gs: _V4GraphState) -> str:
        return "pending" if gs.get("pending") else "ok"

    graph = StateGraph(_V4GraphState)
    graph.add_node("context", node_context)
    graph.add_node("analyst", node_analyst)
    graph.add_node("research_debate", node_research_debate)
    graph.add_node("research_manager", node_research_manager)
    graph.add_node("trader", node_trader)
    graph.add_node("risk_debate", node_risk_debate)
    graph.add_node("portfolio_manager", node_portfolio_manager)
    graph.add_conditional_edges("context", _route_pending, {"ok": "analyst", "pending": END})
    graph.add_conditional_edges("analyst", _route_pending, {"ok": "research_debate", "pending": END})
    graph.add_conditional_edges("research_debate", _route_pending, {"ok": "research_manager", "pending": END})
    graph.add_conditional_edges("research_manager", _route_pending, {"ok": "trader", "pending": END})
    graph.add_conditional_edges("trader", _route_pending, {"ok": "risk_debate", "pending": END})
    graph.add_conditional_edges("risk_debate", _route_pending, {"ok": "portfolio_manager", "pending": END})
    graph.add_conditional_edges("portfolio_manager", _route_pending, {"ok": END, "pending": END})
    graph.set_entry_point("context")
    compiled = graph.compile()
    out: _V4GraphState = compiled.invoke(
        {
            "job_id": job_id,
            "ts_code": ts_code,
            "lookback": lookback,
            "config": config,
            "state": state,
            "result": result,
            "decision_state": decision_state,
            "metrics": metrics,
            "worker_id": worker_id,
            "pending": False,
            "pending_stage": "",
            "final_status": "",
        }
    )
    if out.get("pending"):
        pending_stage = str(out.get("pending_stage") or decision_state.get("pending_stage") or "unknown")
        _save(pending_stage, "pending_user_decision", str(decision_state.get("last_error") or ""))
        return
    final_status = str(out.get("final_status") or "")
    if final_status == "error":
        _save("error", "error", str(decision_state.get("last_error") or "v4 pipeline failed"), finished=True)
        return
    if not final_status:
        raise RuntimeError("v4 graph 执行结束但缺少 final_status")
    decision_state.update({"pending_user_decision": False, "pending_stage": "", "updated_at": _now_iso()})
    _update_job_row(
        conn,
        job_id=job_id,
        status=final_status,
        stage="done",
        state=state,
        result=result,
        decision_state=decision_state,
        metrics=metrics,
        error="",
        finished=True,
        worker_id="",
        lease_until="",
    )
    _append_event(conn, job_id=job_id, stage="done", event_type="job_done", payload={"status": final_status, "engine_version": "v4"})
    conn.commit()


def _run_pipeline(conn, row: dict[str, Any], runtime: dict[str, Any], worker_id: str) -> None:
    rollback = str(os.getenv(V4_ROLLBACK_ENV, "") or "").strip().lower() in {"1", "true", "yes", "on"}
    if rollback:
        _run_pipeline_legacy(conn, row, runtime, worker_id)
        return
    _run_pipeline_v4_langgraph(conn, row, runtime, worker_id)


def process_one_multi_role_v3_job(*, sqlite3_module, db_path, runtime: dict[str, Any], worker_id: str | None = None) -> bool:
    wid = str(worker_id or f"v3w-{os.getpid()}-{uuid.uuid4().hex[:6]}")
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        ensure_multi_role_v3_tables(conn)
        now = _now_iso()
        row = conn.execute(
            """
            UPDATE multi_role_v3_jobs
               SET status = 'running', worker_id = ?, updated_at = ?, lease_until = ?
             WHERE id = (
                   SELECT id FROM multi_role_v3_jobs
                    WHERE status = 'queued'
                    ORDER BY id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
             )
            RETURNING *
            """,
            (wid, now, now),
        ).fetchone()
        if not row:
            return False
        job = dict(row)
        _append_event(conn, job_id=str(job.get("job_id") or ""), stage=str(job.get("stage") or "context"), event_type="worker_claimed", payload={"worker_id": wid})
        conn.commit()
        row2 = conn.execute("SELECT * FROM multi_role_v3_jobs WHERE job_id = ? LIMIT 1", (str(job.get("job_id") or ""),)).fetchone()
        try:
            _run_pipeline(conn, dict(row2), runtime, wid)
        except Exception as exc:
            # 防止 worker 吞异常后把任务永久留在 running。
            raw2 = dict(row2)
            state = _loads(str(raw2.get("state_json") or ""), {})
            result = _loads(str(raw2.get("result_json") or ""), {})
            decision_state = _loads(str(raw2.get("decision_state_json") or ""), {})
            metrics = _loads(str(raw2.get("metrics_json") or ""), {})
            tb = traceback.format_exc()
            decision_state.update({"pending_user_decision": False, "last_error": str(exc), "updated_at": _now_iso()})
            metrics["message"] = f"worker 未捕获异常: {exc}"
            metrics["progress"] = max(10, int(metrics.get("progress") or 10))
            _update_job_row(
                conn,
                job_id=str(raw2.get("job_id") or ""),
                status="error",
                stage=str(raw2.get("stage") or "error"),
                state=state,
                result=result,
                decision_state=decision_state,
                metrics=metrics,
                error=str(exc),
                finished=True,
                worker_id="",
                lease_until="",
            )
            _append_event(
                conn,
                job_id=str(raw2.get("job_id") or ""),
                stage=str(raw2.get("stage") or "error"),
                event_type="worker_unhandled_exception",
                payload={"error": str(exc), "traceback": tb[-4000:]},
            )
            conn.commit()
            print(f"[multi-role-v3-worker] unhandled exception job_id={raw2.get('job_id')} error={exc}\n{tb}", flush=True)
        return True
    finally:
        conn.close()


def run_multi_role_v3_worker_loop(*, sqlite3_module, db_path, runtime: dict[str, Any], once: bool = False, poll_seconds: float = 1.0) -> None:
    while True:
        handled = False
        try:
            handled = process_one_multi_role_v3_job(
                sqlite3_module=sqlite3_module,
                db_path=db_path,
                runtime=runtime,
            )
        except Exception:
            handled = False
        if once:
            return
        if not handled:
            time.sleep(max(0.2, float(poll_seconds or 1.0)))
