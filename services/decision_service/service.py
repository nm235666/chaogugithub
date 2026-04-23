from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import uuid

from llm_gateway import DEFAULT_LLM_MODEL, LLMCallError, chat_completion_text
from services.stock_detail_service import query_stock_detail

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = ROOT_DIR / "stock_codes.db"
DECISION_SNAPSHOT_TABLE = "decision_snapshots"
DECISION_CONTROL_TABLE = "decision_controls"
DECISION_ACTION_TABLE = "decision_actions"
DECISION_STRATEGY_RUN_TABLE = "decision_strategy_runs"
DECISION_STRATEGY_CANDIDATE_TABLE = "decision_strategy_candidates"
FUNNEL_CANDIDATES_TABLE = "funnel_candidates"
FUNNEL_TRANSITIONS_TABLE = "funnel_transitions"
DEFAULT_CONTROL_KEY = "global_kill_switch"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today_cn() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _trace_token(prefix: str, value: Any) -> str:
    token = str(value or "").strip()
    return f"{prefix}-{token}" if token else ""


def _snapshot_trace_id(snapshot_id: Any = None, snapshot_date: str = "") -> str:
    return _trace_token("snapshot", snapshot_id) or _trace_token("snapshot", snapshot_date)


def _snapshot_run_id(snapshot_id: Any = None, snapshot_date: str = "") -> str:
    return _trace_token("snapshot-run", snapshot_id) or _trace_token("snapshot-run", snapshot_date)


def _action_trace_fields(
    payload: dict[str, Any] | None,
    *,
    action_row_id: Any = None,
    snapshot_date: str = "",
    snapshot_row_id: Any = None,
) -> dict[str, str]:
    raw_payload = payload if isinstance(payload, dict) else {}
    context = raw_payload.get("context") if isinstance(raw_payload.get("context"), dict) else {}
    raw_trace = raw_payload.get("trace") if isinstance(raw_payload.get("trace"), dict) else {}
    action_id = str(raw_trace.get("action_id") or raw_payload.get("action_id") or "").strip()
    run_id = str(raw_trace.get("run_id") or raw_payload.get("run_id") or context.get("run_id") or context.get("job_id") or "").strip()
    snapshot_id = str(raw_trace.get("snapshot_id") or raw_payload.get("snapshot_id") or "").strip()
    if not action_id and action_row_id not in (None, ""):
        action_id = _trace_token("action", action_row_id)
    if not snapshot_id:
        snapshot_id = _snapshot_trace_id(snapshot_row_id, snapshot_date)
    return {
        "action_id": action_id,
        "run_id": run_id,
        "snapshot_id": snapshot_id,
    }


def _snapshot_trace_fields(payload: dict[str, Any] | None, *, snapshot_row_id: Any = None, snapshot_date: str = "") -> dict[str, str]:
    raw_payload = payload if isinstance(payload, dict) else {}
    raw_trace = raw_payload.get("trace") if isinstance(raw_payload.get("trace"), dict) else {}
    snapshot_id = str(raw_trace.get("snapshot_id") or raw_payload.get("snapshot_id") or "").strip() or _snapshot_trace_id(snapshot_row_id, snapshot_date)
    run_id = str(raw_trace.get("run_id") or raw_payload.get("run_id") or "").strip() or _snapshot_run_id(snapshot_row_id, snapshot_date)
    action_id = str(raw_trace.get("action_id") or raw_payload.get("action_id") or "").strip()
    return {
        "action_id": action_id,
        "run_id": run_id,
        "snapshot_id": snapshot_id,
    }


def _receipt_source(payload: dict[str, Any] | None, default_source: str) -> str:
    raw_payload = payload if isinstance(payload, dict) else {}
    context = raw_payload.get("context") if isinstance(raw_payload.get("context"), dict) else {}
    source = str(raw_payload.get("source") or context.get("source") or "").strip()
    return source or default_source



def _decision_receipt(
    payload: dict[str, Any] | None,
    *,
    trace: dict[str, str],
    source: str,
    status: str = "success",
) -> dict[str, Any]:
    return {
        "status": str(status or "success").strip() or "success",
        "source": str(source or "").strip(),
        "trace": {
            "action_id": str(trace.get("action_id") or "").strip(),
            "run_id": str(trace.get("run_id") or "").strip(),
            "snapshot_id": str(trace.get("snapshot_id") or "").strip(),
        },
        "context": (payload or {}).get("context") if isinstance((payload or {}).get("context"), dict) else {},
    }


def _job_trace_summary(*, stage: str = "", status: str = "", message: str = "", error: str = "") -> str:
    normalized_stage = str(stage or "").strip()
    normalized_status = str(status or "").strip()
    normalized_message = str(message or "").strip()
    normalized_error = str(error or "").strip()
    parts: list[str] = []
    if normalized_stage:
        parts.append(f"阶段 {normalized_stage}")
    elif normalized_status:
        parts.append(f"状态 {normalized_status}")
    if normalized_message and normalized_message not in parts:
        parts.append(normalized_message)
    if normalized_error:
        parts.append(f"异常 {normalized_error}")
    return " · ".join(parts)


def _query_multi_role_job_trace(conn, job_id: str) -> dict[str, Any]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id or not _table_exists(conn, "multi_role_v3_jobs"):
        return {}
    row = conn.execute(
        """
        SELECT status, stage, metrics_json, error, updated_at, finished_at
        FROM multi_role_v3_jobs
        WHERE job_id = ?
        LIMIT 1
        """,
        (normalized_job_id,),
    ).fetchone()
    if not row:
        return {}
    item = dict(row)
    metrics = _parse_json(item.get("metrics_json"))
    message = str(metrics.get("message") or "").strip()
    updated_at = str(item.get("updated_at") or item.get("finished_at") or "").strip()
    if _table_exists(conn, "multi_role_v3_events"):
        event_row = conn.execute(
            """
            SELECT stage, event_type, created_at
            FROM multi_role_v3_events
            WHERE job_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (normalized_job_id,),
        ).fetchone()
        if event_row:
            event = dict(event_row)
            message = message or str(event.get("event_type") or "").strip()
            updated_at = str(event.get("created_at") or updated_at or "").strip()
            if not str(item.get("stage") or "").strip():
                item["stage"] = str(event.get("stage") or "").strip()
    return {
        "job_id": normalized_job_id,
        "status": str(item.get("status") or "").strip(),
        "stage": str(item.get("stage") or "").strip(),
        "updated_at": updated_at,
        "summary": _job_trace_summary(
            stage=str(item.get("stage") or ""),
            status=str(item.get("status") or ""),
            message=message,
            error=str(item.get("error") or ""),
        ),
    }


def _query_roundtable_job_trace(conn, job_id: str) -> dict[str, Any]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id or not _table_exists(conn, "chief_roundtable_jobs"):
        return {}
    row = conn.execute(
        """
        SELECT status, stage, updated_at, finished_at, error, source_job_id
        FROM chief_roundtable_jobs
        WHERE job_id = ?
        LIMIT 1
        """,
        (normalized_job_id,),
    ).fetchone()
    if not row:
        return {}
    item = dict(row)
    return {
        "job_id": normalized_job_id,
        "status": str(item.get("status") or "").strip(),
        "stage": str(item.get("stage") or "").strip(),
        "updated_at": str(item.get("updated_at") or item.get("finished_at") or "").strip(),
        "source_job_id": str(item.get("source_job_id") or "").strip(),
        "summary": _job_trace_summary(
            stage=str(item.get("stage") or ""),
            status=str(item.get("status") or ""),
            error=str(item.get("error") or ""),
        ),
    }


def _action_job_trace(conn, item: dict[str, Any]) -> dict[str, Any]:
    source = str(item.get("source") or "").strip()
    context = item.get("context") if isinstance(item.get("context"), dict) else {}
    job_id = str(context.get("job_id") or item.get("trace", {}).get("run_id") or "").strip()
    if not job_id:
        return {}
    if source == "multi_role_v3":
        return _query_multi_role_job_trace(conn, job_id)
    if source == "chief_roundtable":
        return _query_roundtable_job_trace(conn, job_id)
    return {}


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _parse_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw or ""))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _parse_json_list(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    try:
        parsed = json.loads(str(raw or ""))
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _score_grade(score: float) -> str:
    if score >= 85:
        return "A"
    if score >= 70:
        return "B"
    if score >= 55:
        return "C"
    if score >= 40:
        return "D"
    return "E"


def _position_label(score: float) -> str:
    if score >= 85:
        return "核心底仓"
    if score >= 75:
        return "重点观察"
    if score >= 65:
        return "观察名单"
    return "暂缓关注"


def _approval_state_label(state: str) -> str:
    mapping = {
        "pending": "待审",
        "approved": "已审",
        "rejected": "驳回",
        "deferred": "暂缓",
        "watching": "观察中",
        "halted": "暂停",
    }
    return mapping.get(str(state or "").strip(), "待审")


def _approval_state_from_action(action_type: str) -> str:
    normalized = str(action_type or "").strip().lower()
    if normalized == "confirm":
        return "approved"
    if normalized == "reject":
        return "rejected"
    if normalized == "defer":
        return "deferred"
    if normalized == "watch":
        return "watching"
    if normalized == "review":
        return "pending"
    return "pending"


def _strategy_feasibility_label(score: float) -> str:
    if score >= 85:
        return "高"
    if score >= 70:
        return "中高"
    if score >= 55:
        return "中"
    return "低"


def _strategy_llm_enabled() -> bool:
    return str(os.getenv("DECISION_STRATEGY_LLM_ENABLED", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}


def _strategy_llm_model() -> str:
    return str(os.getenv("DECISION_STRATEGY_LLM_MODEL", DEFAULT_LLM_MODEL) or DEFAULT_LLM_MODEL).strip() or DEFAULT_LLM_MODEL


def _strategy_run_key(run_version: int) -> str:
    return f"strategy-{_utc_now().replace(':', '').replace('-', '').replace('Z', '')}-{run_version:04d}-{uuid.uuid4().hex[:6]}"


def _strategy_run_summary_from_candidates(candidates: list[dict[str, Any]], *, market_regime: dict[str, Any], approval_state: str, source_mode: str) -> dict[str, Any]:
    llm_scores = [_as_float(item.get("llm_feasibility_score")) for item in candidates if item.get("llm_feasibility_score") is not None]
    return {
        "strategy_count": len(candidates),
        "best_strategy": candidates[0]["name"] if candidates else "",
        "best_fit_score": candidates[0]["fit_score"] if candidates else 0.0,
        "market_mode": market_regime.get("mode") or "neutral",
        "approval_state": approval_state,
        "llm_average_score": round(sum(llm_scores) / len(llm_scores), 2) if llm_scores else 0.0,
        "source_mode": source_mode,
    }


def _strategy_comparison(current_summary: dict[str, Any], previous_summary: dict[str, Any] | None) -> dict[str, Any]:
    if not previous_summary:
        return {
            "has_previous": False,
            "best_fit_delta": 0.0,
            "candidate_count_delta": 0,
            "llm_average_delta": 0.0,
            "best_strategy_changed": False,
        }
    return {
        "has_previous": True,
        "best_fit_delta": round(_as_float(current_summary.get("best_fit_score")) - _as_float(previous_summary.get("best_fit_score")), 2),
        "candidate_count_delta": int(current_summary.get("strategy_count") or 0) - int(previous_summary.get("strategy_count") or 0),
        "llm_average_delta": round(_as_float(current_summary.get("llm_average_score")) - _as_float(previous_summary.get("llm_average_score")), 2),
        "best_strategy_changed": str(current_summary.get("best_strategy") or "") != str(previous_summary.get("best_strategy") or ""),
    }


def _load_recent_actions(conn, *, ts_code: str = "", limit: int = 8) -> list[dict[str, Any]]:
    if not _table_exists(conn, DECISION_ACTION_TABLE):
        return []
    normalized_ts_code = str(ts_code or "").strip().upper()
    params: list[Any] = []
    where_sql = ""
    if normalized_ts_code:
        where_sql = "WHERE ts_code = ?"
        params.append(normalized_ts_code)
    rows = conn.execute(
        f"""
        SELECT id, action_type, ts_code, stock_name, note, actor, snapshot_date, action_payload_json, created_at
        FROM {DECISION_ACTION_TABLE}
        {where_sql}
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["payload"] = _parse_json(item.get("action_payload_json"))
        item["approval_state"] = _approval_state_from_action(item.get("action_type"))
        item["approval_state_label"] = _approval_state_label(item["approval_state"])
        items.append(item)
    return items


def _ensure_tables(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DECISION_SNAPSHOT_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL,
            snapshot_type TEXT NOT NULL DEFAULT 'daily',
            payload_json TEXT NOT NULL DEFAULT '{{}}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DECISION_CONTROL_TABLE} (
            control_key TEXT PRIMARY KEY,
            allow_trading INTEGER NOT NULL DEFAULT 1,
            reason TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DECISION_ACTION_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action_type TEXT NOT NULL,
            ts_code TEXT NOT NULL DEFAULT '',
            stock_name TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            actor TEXT NOT NULL DEFAULT '',
            snapshot_date TEXT NOT NULL DEFAULT '',
            action_payload_json TEXT NOT NULL DEFAULT '{{}}',
            idempotency_key TEXT DEFAULT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DECISION_STRATEGY_RUN_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_key TEXT NOT NULL UNIQUE,
            run_version INTEGER NOT NULL DEFAULT 1,
            ts_code TEXT NOT NULL DEFAULT '',
            keyword TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '策略实验台',
            status TEXT NOT NULL DEFAULT 'completed',
            source_mode TEXT NOT NULL DEFAULT 'preview',
            generator_mode TEXT NOT NULL DEFAULT 'rules+llm',
            llm_model TEXT NOT NULL DEFAULT '',
            llm_enabled INTEGER NOT NULL DEFAULT 0,
            market_mode TEXT NOT NULL DEFAULT 'neutral',
            approval_state TEXT NOT NULL DEFAULT 'candidate',
            summary_json TEXT NOT NULL DEFAULT '{{}}',
            board_snapshot_json TEXT NOT NULL DEFAULT '{{}}',
            generator_rules_json TEXT NOT NULL DEFAULT '{{}}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DECISION_STRATEGY_CANDIDATE_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            run_key TEXT NOT NULL,
            ts_code TEXT NOT NULL DEFAULT '',
            keyword TEXT NOT NULL DEFAULT '',
            rank INTEGER NOT NULL DEFAULT 0,
            priority INTEGER NOT NULL DEFAULT 0,
            name TEXT NOT NULL DEFAULT '',
            mode TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            fit_score REAL NOT NULL DEFAULT 0,
            llm_feasibility_score REAL NOT NULL DEFAULT 0,
            llm_feasibility_label TEXT NOT NULL DEFAULT '',
            llm_explanation TEXT NOT NULL DEFAULT '',
            llm_risk_note TEXT NOT NULL DEFAULT '',
            llm_name_hint TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL DEFAULT '',
            entry_rule TEXT NOT NULL DEFAULT '',
            exit_rule TEXT NOT NULL DEFAULT '',
            position_bias TEXT NOT NULL DEFAULT '',
            universe TEXT NOT NULL DEFAULT '',
            rationale TEXT NOT NULL DEFAULT '',
            risk_control TEXT NOT NULL DEFAULT '',
            linked_industries_json TEXT NOT NULL DEFAULT '[]',
            linked_stocks_json TEXT NOT NULL DEFAULT '[]',
            candidate_json TEXT NOT NULL DEFAULT '{{}}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{DECISION_SNAPSHOT_TABLE}_date ON {DECISION_SNAPSHOT_TABLE}(snapshot_date DESC, updated_at DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{DECISION_ACTION_TABLE}_created_at ON {DECISION_ACTION_TABLE}(created_at DESC, id DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{DECISION_ACTION_TABLE}_ts_code ON {DECISION_ACTION_TABLE}(ts_code, created_at DESC, id DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{DECISION_STRATEGY_RUN_TABLE}_created_at ON {DECISION_STRATEGY_RUN_TABLE}(created_at DESC, id DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{DECISION_STRATEGY_RUN_TABLE}_scope ON {DECISION_STRATEGY_RUN_TABLE}(ts_code, keyword, created_at DESC, id DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{DECISION_STRATEGY_CANDIDATE_TABLE}_run ON {DECISION_STRATEGY_CANDIDATE_TABLE}(run_id, rank ASC, id ASC)"
    )
    conn.commit()
    _ensure_decision_idempotency_column(conn)


def _ensure_decision_idempotency_column(conn) -> None:
    """Add idempotency_key column + unique index to decision_actions (idempotent).

    SQLite does not support ``ALTER TABLE ADD COLUMN IF NOT EXISTS``; detect via
    PRAGMA/information_schema first, then only issue a plain ADD COLUMN when missing.
    """
    try:
        has_column = False
        try:
            rows = conn.execute("PRAGMA table_info(decision_actions)").fetchall()
            for row in rows:
                name = row[1] if not isinstance(row, dict) else row.get("name")
                if name == "idempotency_key":
                    has_column = True
                    break
        except Exception:
            try:
                row = conn.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name='decision_actions' AND column_name='idempotency_key' LIMIT 1"
                ).fetchone()
                has_column = bool(row)
            except Exception:
                has_column = False
        if not has_column:
            try:
                conn.execute(
                    "ALTER TABLE decision_actions ADD COLUMN idempotency_key TEXT DEFAULT NULL"
                )
            except Exception:
                pass
        try:
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_decision_actions_idem_key
                ON decision_actions (idempotency_key)
                WHERE idempotency_key IS NOT NULL
                """
            )
        except Exception:
            pass
        conn.commit()
    except Exception:
        pass


def _get_control_row(conn) -> dict[str, Any]:
    _ensure_tables(conn)
    now = _utc_now()
    conn.execute(
        f"""
        INSERT INTO {DECISION_CONTROL_TABLE} (control_key, allow_trading, reason, updated_at)
        VALUES (?, 1, '', ?)
        ON CONFLICT(control_key) DO NOTHING
        """,
        (DEFAULT_CONTROL_KEY, now),
    )
    row = conn.execute(
        f"""
        SELECT control_key, allow_trading, reason, updated_at
        FROM {DECISION_CONTROL_TABLE}
        WHERE control_key = ?
        LIMIT 1
        """,
        (DEFAULT_CONTROL_KEY,),
    ).fetchone()
    return dict(row) if row else {"control_key": DEFAULT_CONTROL_KEY, "allow_trading": 1, "reason": "", "updated_at": now}


def get_decision_kill_switch(*, sqlite3_module, db_path: str) -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        return _get_control_row(conn)
    finally:
        conn.close()


def set_decision_kill_switch(*, sqlite3_module, db_path: str, allow_trading: bool, reason: str = "") -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        now = _utc_now()
        conn.execute(
            f"""
            INSERT INTO {DECISION_CONTROL_TABLE} (control_key, allow_trading, reason, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(control_key) DO UPDATE SET
                allow_trading = excluded.allow_trading,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (DEFAULT_CONTROL_KEY, 1 if allow_trading else 0, str(reason or "").strip(), now),
        )
        conn.commit()
        return _get_control_row(conn)
    finally:
        conn.close()


def _load_stock_score_rows(conn, page: int, page_size: int) -> tuple[str, int, list[dict[str, Any]]]:
    if not _table_exists(conn, "stock_scores_daily"):
        return "", 0, []
    latest_score_date = conn.execute("SELECT MAX(score_date) FROM stock_scores_daily").fetchone()[0]
    if not latest_score_date:
        return "", 0, []
    total = int(
        conn.execute(
            "SELECT COUNT(*) FROM stock_scores_daily WHERE score_date = ?",
            (latest_score_date,),
        ).fetchone()[0]
        or 0
    )
    offset = (max(page, 1) - 1) * max(page_size, 1)
    rows = conn.execute(
        """
        SELECT
            score_date, ts_code, name, symbol, market, area, industry,
            industry_rank, industry_count, score_grade, industry_score_grade,
            total_score, industry_total_score, trend_score, industry_trend_score,
            financial_score, industry_financial_score, valuation_score, industry_valuation_score,
            capital_flow_score, industry_capital_flow_score, event_score, industry_event_score,
            news_score, industry_news_score, risk_score, industry_risk_score,
            latest_trade_date, latest_risk_date, score_payload_json, source, update_time
        FROM stock_scores_daily
        WHERE score_date = ?
        ORDER BY COALESCE(industry_total_score, total_score, 0) DESC, COALESCE(total_score, 0) DESC, ts_code
        LIMIT ? OFFSET ?
        """,
        (latest_score_date, page_size, offset),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        payload = _parse_json(item.get("score_payload_json"))
        item["score_payload"] = payload
        item["score_summary"] = payload.get("score_summary", {})
        item["raw_metrics"] = payload.get("raw_metrics", {})
        item["position_label"] = _position_label(_as_float(item.get("total_score")))
        item["decision_reason"] = _build_stock_reason(item)
        item["decision_risk"] = _build_stock_risk(item)
        items.append(item)
    return str(latest_score_date), total, items


def _build_stock_reason(item: dict[str, Any]) -> str:
    reasons: list[str] = []
    score = _as_float(item.get("total_score"))
    industry_score = _as_float(item.get("industry_total_score"))
    if score:
        reasons.append(f"综合评分 {score:.1f}")
    if industry_score:
        reasons.append(f"行业内评分 {industry_score:.1f}")
    if item.get("industry_rank") is not None:
        rank = int(_as_float(item.get("industry_rank"), 0))
        count = int(_as_float(item.get("industry_count"), 0))
        if rank > 0 and count > 0:
            reasons.append(f"行业排名 {rank}/{count}")
    summary = item.get("score_summary") or {}
    for key in ("trend", "financial", "valuation", "capital_flow", "event", "news", "risk"):
        text = str(summary.get(key) or "").strip()
        if text:
            reasons.append(text)
    return "；".join(reasons[:4]) if reasons else "复用现有综合评分结果，暂无更多结构化解释"


def _build_stock_risk(item: dict[str, Any]) -> str:
    pieces: list[str] = []
    if _as_float(item.get("risk_score")) < 50:
        pieces.append("风险分偏低，注意回撤和事件冲击")
    if _as_float(item.get("valuation_score")) < 50:
        pieces.append("估值分偏低，注意估值修复不确定性")
    if _as_float(item.get("trend_score")) < 50:
        pieces.append("趋势分偏低，等待右侧确认")
    if not pieces:
        pieces.append("暂无明显结构性风险，仍需结合仓位控制")
    return "；".join(pieces)


def _build_actionable_fields(
    *,
    total_score: float,
    trend_score: float,
    risk_score: float,
    position_label: str,
    market_mode: str,
    trade_mode: str,
    allow_trading: bool,
) -> dict[str, str]:
    trigger = "总分≥65 且趋势分≥50，配合市场模式确认后再执行。"
    if total_score >= 80 and trend_score >= 60:
        trigger = "总分与趋势分均处于高位，可在分时确认后优先跟踪。"
    invalidation = "若趋势分跌破50或风险分跌破45，则撤销本次入场计划。"
    if risk_score < 45:
        invalidation = "当前风险分偏低，优先降级为观察，不执行新增仓位。"
    hint = f"{position_label} · 模式 {trade_mode or market_mode or 'unknown'}"
    budget_source = "risk_budget: 决策板 trade_plan 仓位上限 + kill_switch 风控开关。"
    if not allow_trading:
        trigger = "Kill Switch 关闭，仅保留观察与复核，不执行新增仓位。"
        invalidation = "风控开关关闭即失效，需先恢复交易权限。"
    return {
        "entry_trigger": trigger,
        "invalidation": invalidation,
        "position_hint": hint,
        "risk_budget_source": budget_source,
    }


def _aggregate_market_regime(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "label": "数据不足",
            "score": 50.0,
            "mode": "neutral",
            "summary": "当前没有可用的股票评分快照，先以观察为主。",
            "factors": [],
        }
    total_scores = [_as_float(row.get("total_score")) for row in rows]
    industry_scores = [_as_float(row.get("industry_total_score")) for row in rows if row.get("industry_total_score") is not None]
    strong = sum(1 for score in total_scores if score >= 70)
    active = sum(1 for score in total_scores if score >= 60)
    score = _clamp((sum(total_scores) / len(total_scores)) if total_scores else 50.0)
    breadth_bonus = (active / len(total_scores) - 0.5) * 20 if total_scores else 0.0
    strong_bonus = (strong / len(total_scores)) * 12 if total_scores else 0.0
    score = _clamp(score + breadth_bonus + strong_bonus)
    label = "进攻" if score >= 70 else "平衡" if score >= 55 else "防御"
    mode = "aggressive" if score >= 70 else "balanced" if score >= 55 else "defensive"
    factors = [
        f"整体均分 {sum(total_scores) / len(total_scores):.1f}" if total_scores else "缺少均分",
        f"高分样本占比 {strong}/{len(total_scores)}",
        f"可交易样本占比 {active}/{len(total_scores)}",
    ]
    if industry_scores:
        factors.append(f"行业内均分 {sum(industry_scores) / len(industry_scores):.1f}")
    summary = "；".join(factors)
    return {
        "label": label,
        "score": round(score, 2),
        "mode": mode,
        "summary": summary,
        "factors": factors,
    }


def _aggregate_industries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        industry = str(row.get("industry") or "未知行业").strip() or "未知行业"
        buckets[industry].append(row)
    out: list[dict[str, Any]] = []
    for industry, items in buckets.items():
        scores = [_as_float(item.get("industry_total_score") or item.get("total_score")) for item in items]
        top = sorted(items, key=lambda item: _as_float(item.get("industry_total_score") or item.get("total_score")), reverse=True)[:3]
        out.append(
            {
                "industry": industry,
                "score": round(sum(scores) / len(scores), 2) if scores else 0.0,
                "count": len(items),
                "top_stocks": [
                    {
                        "ts_code": str(item.get("ts_code") or ""),
                        "name": str(item.get("name") or ""),
                        "score": round(_as_float(item.get("total_score")), 2),
                    }
                    for item in top
                ],
            }
        )
    out.sort(key=lambda item: (-float(item.get("score") or 0.0), -int(item.get("count") or 0), str(item.get("industry") or "")))
    return out


def _validation_snapshot(conn) -> dict[str, Any]:
    if not _table_exists(conn, "quantaalpha_runs"):
        return {"status": "table_missing", "table_exists": False, "checked_at": _utc_now(), "done": 0, "error": 0, "latest": None}
    done = int(conn.execute("SELECT COUNT(*) FROM quantaalpha_runs WHERE status = 'done'").fetchone()[0] or 0)
    error = int(conn.execute("SELECT COUNT(*) FROM quantaalpha_runs WHERE status = 'error'").fetchone()[0] or 0)
    latest = conn.execute(
        """
        SELECT task_id, task_type, status, error_code, error_message, created_at, finished_at, metrics_json
        FROM quantaalpha_runs
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    latest_item = dict(latest) if latest else None
    if latest_item:
        latest_item["metrics"] = _parse_json(latest_item.get("metrics_json"))
    return {
        "source": "quantaalpha_service",
        "status": "ok" if done or error else "idle",
        "done": done,
        "error": error,
        "latest": latest_item,
    }


def _build_trade_plan(market_regime: dict[str, Any], industries: list[dict[str, Any]], shortlist: list[dict[str, Any]], kill_switch: dict[str, Any]) -> dict[str, Any]:
    regime_score = _as_float(market_regime.get("score"))
    allow_trading = bool(int(kill_switch.get("allow_trading") or 0))
    if not allow_trading:
        return {
            "mode": "halted",
            "headline": "当前全局 Kill Switch 已开启，暂停新增动作。",
            "base_position": 0,
            "floating_position": 0,
            "reserve_position": 0,
            "actions": [
                "暂停新增仓位。",
                "仅允许人工确认后的紧急处理。",
                "等待风险状态恢复后再重新评估。",
            ],
        }
    if regime_score >= 70:
        base_position = 30
        floating_position = 15
        reserve_position = 55
        mode = "aggressive"
        headline = "市场模式偏进攻，可优先跟踪高分行业与核心底仓。"
    elif regime_score >= 55:
        base_position = 20
        floating_position = 10
        reserve_position = 70
        mode = "balanced"
        headline = "市场模式偏平衡，以观察和确认优先。"
    else:
        base_position = 10
        floating_position = 5
        reserve_position = 85
        mode = "defensive"
        headline = "市场模式偏防御，优先控制回撤和等待结构确认。"
    industry_names = [str(item.get("industry") or "").strip() for item in industries[:3] if str(item.get("industry") or "").strip()]
    stock_names = [str(item.get("name") or item.get("ts_code") or "").strip() for item in shortlist[:5] if str(item.get("name") or item.get("ts_code") or "").strip()]
    actions = [
        "先看宏观模式，再看行业排序。",
        f"优先观察：{', '.join(industry_names) if industry_names else '暂无明显领跑行业'}。",
        f"候选标的：{', '.join(stock_names) if stock_names else '暂无进入短名单的个股'}。",
    ]
    return {
        "mode": mode,
        "headline": headline,
        "base_position": base_position,
        "floating_position": floating_position,
        "reserve_position": reserve_position,
        "actions": actions,
    }


def _build_trade_plan_packet(board: dict[str, Any], *, conn, ts_code: str = "", keyword: str = "") -> dict[str, Any]:
    market_regime = board.get("market_regime") or {}
    trade_plan = board.get("trade_plan") or {}
    industries = list(board.get("industries") or [])
    shortlist = list(board.get("shortlist") or [])
    validation = board.get("validation") or {}
    kill_switch = board.get("kill_switch") or {}
    focus_stock = board.get("focus_stock") if isinstance(board.get("focus_stock"), dict) else None
    do_now = list(trade_plan.get("actions") or [])
    do_not = [
        "不要在 Kill Switch 暂停时新增仓位。",
        "不要在趋势分/行业分明显偏弱时追高。",
        "不要把计划书当成自动执行指令，先完成人工确认。",
    ]
    if bool(int(kill_switch.get("allow_trading") or 0)) is False:
        do_not.insert(0, "当前 Kill Switch 已关闭，暂停新增动作。")
    checklist = [
        "先确认市场模式与仓位上限。",
        "再确认行业排序与候选短名单。",
        "选定单票后写入人工确认记录。",
        "生成快照，便于后续复盘。",
    ]
    if validation.get("status") not in {None, "", "ok", "idle"}:
        checklist.append("先核对验证层最新状态，再决定是否放行。")
    intraday_plan = [
        {
            "stage": "开盘前",
            "time_window": "09:00-09:30",
            "actions": [
                "检查 Kill Switch 是否开启",
                "确认市场模式与仓位上限",
                "复核重点行业和重点个股",
            ],
        },
        {
            "stage": "开盘后",
            "time_window": "09:30-11:30",
            "actions": [
                "只处理计划书里的优先候选",
                "观察趋势和行业同步性",
                "记录必要的人工确认动作",
            ],
        },
        {
            "stage": "午盘后",
            "time_window": "13:00-15:00",
            "actions": [
                "复核消息面与盘中变化",
                "检查是否需要调整浮动仓",
                "生成当日快照便于复盘",
            ],
        },
    ]
    theme_links = []
    for industry_item in industries[:5]:
        industry_name = str(industry_item.get("industry") or "").strip()
        if not industry_name:
            continue
        links = _related_signals_for_term(conn=conn, term=industry_name)
        if links:
            theme_links.append({"term": industry_name, "signals": links[:5]})
    priority_industries = industries[:5]
    priority_stocks = shortlist[:8]
    focus_name = ""
    focus_reason = ""
    if focus_stock:
        focus_name = str(focus_stock.get("detail", {}).get("profile", {}).get("name") or focus_stock.get("ts_code") or "").strip()
        focus_reason = str(focus_stock.get("reason") or focus_stock.get("trade_plan", {}).get("suggestion") or "").strip()
    approval_actions = _load_recent_actions(conn, ts_code=ts_code, limit=8)
    latest_action = approval_actions[0] if approval_actions else None
    approval_state = latest_action.get("approval_state") if latest_action else "pending"
    approval_flow = {
        "scope": "single" if ts_code else "global",
        "state": approval_state,
        "state_label": _approval_state_label(approval_state),
        "summary": "；".join(
            [
                f"当前状态 {_approval_state_label(approval_state)}",
                f"近期动作 {len(approval_actions)} 条",
                f"最近动作 {str(latest_action.get('action_type') or '-').upper()}" if latest_action else "暂无最近动作",
            ]
        ),
        "latest_action": latest_action,
        "recent_actions": approval_actions[:5],
        "next_actions": [
            "确认" if approval_state in {"pending", "watching"} else "复核",
            "暂缓" if approval_state in {"pending", "watching"} else "重新评估",
            "驳回" if approval_state not in {"rejected"} else "保留驳回结论",
        ],
    }
    return {
        "generated_at": board.get("generated_at") or _utc_now(),
        "snapshot_date": board.get("snapshot_date") or "",
        "title": trade_plan.get("headline") or "每日交易计划书",
        "mode": trade_plan.get("mode") or market_regime.get("mode") or "neutral",
        "summary": market_regime.get("summary") or "暂无市场模式摘要。",
        "market_regime": market_regime,
        "trade_plan": trade_plan,
        "position_plan": {
            "base_position": trade_plan.get("base_position", 0),
            "floating_position": trade_plan.get("floating_position", 0),
            "reserve_position": trade_plan.get("reserve_position", 0),
        },
        "do_now": do_now,
        "do_not": do_not,
        "checklist": checklist,
        "intraday_plan": intraday_plan,
        "theme_links": theme_links,
        "priority_industries": priority_industries,
        "priority_stocks": priority_stocks,
        "kill_switch": kill_switch,
        "validation": validation,
        "approval_flow": approval_flow,
        "focus_stock": focus_stock,
        "focus_name": focus_name,
        "focus_reason": focus_reason,
        "key_metrics": {
            "industry_count": len(industries),
            "shortlist_size": len(shortlist),
            "top_score": board.get("summary", {}).get("top_score", 0.0),
        },
        "notes": [
            "计划书复用当前评分、行业排序和短名单结果。",
            "此页用于日常阅读、确认和复盘，不替代人工风控判断。",
        ],
        "decision_flow": [
            "确认市场状态",
            "确认行业排序",
            "确认单票理由",
            "记录人工动作",
            "生成日快照",
        ],
        "ts_code": ts_code,
        "keyword": keyword,
    }


def _build_pipeline_health(conn, *, latest_score_date: str, universe_size: int, shortlist_size: int) -> dict[str, Any]:
    score_ready = bool(str(latest_score_date or "").strip()) and int(universe_size or 0) > 0
    snapshot_ready = False
    latest_snapshot_date = ""
    if _table_exists(conn, DECISION_SNAPSHOT_TABLE):
        snapshot_row = conn.execute(
            f"SELECT snapshot_date FROM {DECISION_SNAPSHOT_TABLE} ORDER BY snapshot_date DESC, id DESC LIMIT 1"
        ).fetchone()
        latest_snapshot_date = str(snapshot_row[0] if snapshot_row else "").strip()
        snapshot_ready = bool(latest_snapshot_date)

    funnel_total = 0
    if _table_exists(conn, FUNNEL_CANDIDATES_TABLE):
        funnel_row = conn.execute(f"SELECT COUNT(*) FROM {FUNNEL_CANDIDATES_TABLE}").fetchone()
        funnel_total = int(funnel_row[0] or 0) if funnel_row else 0
    funnel_ready = funnel_total > 0

    missing_inputs: list[str] = []
    if not score_ready:
        missing_inputs.append("stock_scores_daily")
    if not snapshot_ready:
        missing_inputs.append(DECISION_SNAPSHOT_TABLE)
    if not funnel_ready:
        missing_inputs.append(FUNNEL_CANDIDATES_TABLE)

    if score_ready and snapshot_ready and funnel_ready:
        status = "ready"
        status_reason = "评分、决策快照和漏斗候选均已就绪。"
    elif score_ready and (snapshot_ready or funnel_ready):
        status = "degraded"
        status_reason = "主评分已就绪，但决策快照或漏斗候选仍不完整。"
    else:
        status = "empty"
        status_reason = "主评分链路尚未就绪，先补齐 scores->decision->funnel。"

    return {
        "status": status,
        "status_reason": status_reason,
        "missing_inputs": missing_inputs,
        "score_date": str(latest_score_date or ""),
        "universe_size": int(universe_size or 0),
        "shortlist_size": int(shortlist_size or 0),
        "latest_snapshot_date": latest_snapshot_date,
        "funnel_total": funnel_total,
    }


def _build_strategy_candidates(
    board: dict[str, Any],
    *,
    conn,
    ts_code: str = "",
    keyword: str = "",
    page: int = 1,
    page_size: int = 12,
    source_mode: str = "preview",
    run_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    market_regime = board.get("market_regime") or {}
    trade_plan = board.get("trade_plan") or {}
    industries = list(board.get("industries") or [])
    shortlist = list(board.get("shortlist") or [])
    validation = board.get("validation") or {}
    kill_switch = board.get("kill_switch") or {}
    focus_stock = board.get("focus_stock") if isinstance(board.get("focus_stock"), dict) else None
    regime_score = _as_float(market_regime.get("score"))
    allow_trading = bool(int(kill_switch.get("allow_trading") or 0))
    top_industries = industries[:3]
    top_stocks = shortlist[:5]
    primary_industry = str(top_industries[0].get("industry") or "未知行业").strip() if top_industries else "未知行业"
    primary_stock_name = str(top_stocks[0].get("name") or top_stocks[0].get("ts_code") or "核心候选").strip() if top_stocks else "核心候选"
    focus_name = ""
    focus_score = 0.0
    if focus_stock:
        focus_name = str(focus_stock.get("name") or focus_stock.get("ts_code") or "").strip()
        focus_score = _as_float((focus_stock.get("score") or {}).get("total_score"))

    candidates: list[dict[str, Any]] = []

    if not allow_trading:
        candidates.append(
            {
                "name": "暂停观察策略",
                "mode": "halted",
                "fit_score": 100.0,
                "priority": 1,
                "status": "halted",
                "summary": "当前 Kill Switch 已关闭，策略生成器只输出观察与复核建议。",
                "entry_rule": "仅在人工恢复交易后再评估入场。",
                "exit_rule": "保持空仓或仅做计划复核，不新增风险暴露。",
                "position_bias": "0/100",
                "universe": "全市场",
                "rationale": "风控开关优先级高于策略生成。",
                "risk_control": "等待 Kill Switch 恢复并重新生成策略。",
                "linked_industries": [item.get("industry") for item in top_industries if item.get("industry")],
                "linked_stocks": [item.get("ts_code") for item in top_stocks if item.get("ts_code")],
            }
        )
    else:
        trend_fit = 70 + max(0.0, (regime_score - 55.0) * 1.1)
        if regime_score >= 70:
            trend_fit += 8
        if regime_score < 55:
            trend_fit -= 12
        candidates.append(
            {
                "name": "右侧趋势确认策略",
                "mode": "aggressive" if regime_score >= 70 else "balanced",
                "fit_score": round(_clamp(trend_fit), 2),
                "priority": 1,
                "status": "candidate",
                "summary": "适合市场模式偏进攻或平衡时使用，优先跟踪高分股票的右侧确认。",
                "entry_rule": "市场模式不弱、个股总分与行业分同步抬升后再入场。",
                "exit_rule": "趋势分回落或行业内排序明显退化时减仓/离场。",
                "position_bias": f"{trade_plan.get('base_position', 0)} / {trade_plan.get('floating_position', 0)}",
                "universe": "短名单 + 高分行业",
                "rationale": f"当前市场模式 {market_regime.get('label') or '-'}，更适合右侧确认而非左侧抄底。",
                "risk_control": "严格控制追高，配合分批建仓与人工确认。",
                "linked_industries": [item.get("industry") for item in top_industries if item.get("industry")],
                "linked_stocks": [item.get("ts_code") for item in top_stocks if item.get("ts_code")],
            }
        )

        rotation_fit = 62 + len([item for item in industries if _as_float(item.get("score")) >= 60]) * 2
        if regime_score >= 60:
            rotation_fit += 8
        candidates.append(
            {
                "name": "行业轮动优选策略",
                "mode": "balanced" if regime_score >= 55 else "defensive",
                "fit_score": round(_clamp(rotation_fit), 2),
                "priority": 2,
                "status": "candidate",
                "summary": "当行业内部差异明显时，优先选择龙头行业中的头部样本。",
                "entry_rule": "行业排序位于前列且代表股在短名单内时才考虑。",
                "exit_rule": "行业均分下降或代表股风险分下降时收缩仓位。",
                "position_bias": "行业分布式配置",
                "universe": "行业 Top 列表",
                "rationale": f"当前行业排序中，{primary_industry} 和 {primary_stock_name} 具有较强代表性。",
                "risk_control": "避免在行业扩散过度时追求全覆盖。",
                "linked_industries": [item.get("industry") for item in top_industries if item.get("industry")],
                "linked_stocks": [item.get("ts_code") for item in top_stocks if item.get("ts_code")],
            }
        )

        defensive_fit = 55 + (55.0 - min(regime_score, 55.0)) * 1.5
        if regime_score < 55:
            defensive_fit += 10
        candidates.append(
            {
                "name": "防御观察策略",
                "mode": "defensive",
                "fit_score": round(_clamp(defensive_fit), 2),
                "priority": 3,
                "status": "candidate",
                "summary": "市场走弱或波动放大时，优先保住节奏与回撤控制。",
                "entry_rule": "市场模式偏弱、验证层不稳定或行业轮动散乱时启用。",
                "exit_rule": "市场模式恢复平衡后切回趋势或轮动策略。",
                "position_bias": "低仓位 / 观察为主",
                "universe": "低波动 + 高确定性样本",
                "rationale": "用等待和复核替代贸然进攻，优先保障主链路稳定。",
                "risk_control": "只做确认度高的候选，不扩大试错范围。",
                "linked_industries": [item.get("industry") for item in top_industries if item.get("industry")],
                "linked_stocks": [item.get("ts_code") for item in top_stocks if item.get("ts_code")],
            }
        )

    if focus_stock and focus_name:
        candidates.append(
            {
                "name": "单票聚焦策略",
                "mode": "single-stock",
                "fit_score": round(_clamp(65.0 + focus_score / 3.0), 2),
                "priority": 0,
                "status": "candidate",
                "summary": "围绕当前聚焦标的做单票复核与执行建议。",
                "entry_rule": "聚焦股票总分和行业分都满足计划阈值后再评估。",
                "exit_rule": "焦点标的风险分转弱或审批状态变为驳回时退出。",
                "position_bias": "按单票权重动态调整",
                "universe": focus_name,
                "rationale": f"当前聚焦标的是 {focus_name}，适合作为单票策略模板。",
                "risk_control": "仅在人工审批通过后执行。",
                "linked_industries": [str(focus_stock.get("detail", {}).get("profile", {}).get("industry") or "").strip()] if isinstance(focus_stock, dict) else [],
                "linked_stocks": [str(focus_stock.get("ts_code") or "").strip()] if isinstance(focus_stock, dict) else [],
            }
        )

    candidates = [item for item in candidates if str(item.get("name") or "").strip()]
    candidates.sort(key=lambda item: (-_as_float(item.get("fit_score")), int(item.get("priority") or 99), str(item.get("name") or "")))
    for index, item in enumerate(candidates, start=1):
        item["rank"] = index
        item["approval_hint"] = _approval_state_label("pending") if item.get("status") == "candidate" else _approval_state_label(str(item.get("status") or "pending"))
        item["linked_industries"] = [str(x).strip() for x in (item.get("linked_industries") or []) if str(x or "").strip()]
        item["linked_stocks"] = [str(x).strip().upper() for x in (item.get("linked_stocks") or []) if str(x or "").strip()]

        item.update(_score_strategy_candidate_with_llm(board=board, candidate=item))
        item["candidate_key"] = f"{item['rank']}::{item['name']}"
        item["candidate_json"] = {
            "name": item.get("name"),
            "mode": item.get("mode"),
            "status": item.get("status"),
            "fit_score": item.get("fit_score"),
            "llm_feasibility_score": item.get("llm_feasibility_score"),
            "llm_feasibility_label": item.get("llm_feasibility_label"),
            "linked_industries": item.get("linked_industries"),
            "linked_stocks": item.get("linked_stocks"),
        }

    summary = _strategy_run_summary_from_candidates(
        candidates,
        market_regime=market_regime,
        approval_state="paused" if not allow_trading else "candidate",
        source_mode=source_mode,
    )
    page = max(page, 1)
    page_size = min(max(page_size, 1), 100)
    offset = (page - 1) * page_size
    paged_candidates = candidates[offset : offset + page_size]
    run_payload = dict(run_meta or {})
    return {
        "generated_at": run_payload.get("generated_at") or board.get("generated_at") or _utc_now(),
        "snapshot_date": board.get("snapshot_date") or "",
        "title": "策略实验台",
        "source_mode": source_mode,
        "run": run_payload,
        "summary": summary,
        "generator_rules": [
            "只基于现有评分、短名单、行业排序和验证层输出策略候选。",
            "规则与参数搜索负责主排序，LLM 仅给出辅助可行性评分与解释。",
            "不自动下结论为实盘策略，必须经过人工确认和后续验证。",
            "Kill Switch 关闭时只输出观察策略。",
        ],
        "market_regime": market_regime,
        "validation": validation,
        "kill_switch": kill_switch,
        "focus_name": focus_name,
        "focus_stock": focus_stock,
        "strategies": paged_candidates,
        "all_strategies": candidates,
        "ts_code": ts_code,
        "keyword": keyword,
    }


def _related_signals_for_term(*, conn, term: str) -> list[dict[str, Any]]:
    normalized = str(term or "").strip()
    if not normalized:
        return []
    local_conn = conn
    if local_conn is None:
        return []
    try:
        table_candidates = ["investment_signal_tracker_7d", "investment_signal_tracker"]
        rows: list[dict[str, Any]] = []
        for table_name in table_candidates:
            exists = local_conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()[0]
            if not exists:
                continue
            matched = local_conn.execute(
                f"""
                SELECT signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                       evidence_count, news_count, stock_news_count, chatroom_count, signal_status,
                       latest_signal_date, source_summary_json
                FROM {table_name}
                WHERE subject_name LIKE ? OR ts_code LIKE ?
                ORDER BY signal_strength DESC, confidence DESC, latest_signal_date DESC
                LIMIT 8
                """,
                (f"%{normalized}%", f"%{normalized}%"),
            ).fetchall()
            for row in matched:
                item = dict(row)
                item["source_table"] = table_name
                rows.append(item)
        deduped: list[dict[str, Any]] = []
        seen = set()
        for item in rows:
            key = str(item.get("signal_key") or item.get("subject_name") or item.get("ts_code") or "")
            if key in seen:
                continue
            seen.add(key)
            deduped.append(
                {
                    "signal_key": item.get("signal_key"),
                    "subject_name": item.get("subject_name"),
                    "ts_code": item.get("ts_code"),
                    "direction": item.get("direction"),
                    "signal_strength": round(_as_float(item.get("signal_strength")), 2),
                    "confidence": round(_as_float(item.get("confidence")), 2),
                    "signal_status": item.get("signal_status"),
                    "latest_signal_date": item.get("latest_signal_date"),
                    "source_table": item.get("source_table"),
                }
            )
        return deduped
    finally:
        pass


def _recent_stock_news_for_stock(*, conn, ts_code: str, limit: int = 3) -> dict[str, Any]:
    normalized_ts_code = str(ts_code or "").strip().upper()
    if not normalized_ts_code:
        return {"status": "empty", "available": False, "count": 0, "high_importance_count": 0, "latest_pub_time": "", "items": []}
    if not _table_exists(conn, "stock_news_items"):
        return {"status": "missing_source", "available": False, "count": 0, "high_importance_count": 0, "latest_pub_time": "", "items": []}
    cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(stock_news_items)").fetchall()}
    importance_expr = "COALESCE(llm_finance_importance, '') AS finance_importance" if "llm_finance_importance" in cols else "'' AS finance_importance"
    summary_expr = "COALESCE(llm_summary, '') AS llm_summary" if "llm_summary" in cols else "'' AS llm_summary"
    count = int(conn.execute("SELECT COUNT(*) FROM stock_news_items WHERE ts_code = ?", (normalized_ts_code,)).fetchone()[0] or 0)
    if count <= 0:
        return {"status": "empty", "available": True, "count": 0, "high_importance_count": 0, "latest_pub_time": "", "items": []}
    latest_pub_time = str(conn.execute("SELECT MAX(pub_time) FROM stock_news_items WHERE ts_code = ?", (normalized_ts_code,)).fetchone()[0] or "")
    high_importance_count = 0
    if "llm_finance_importance" in cols:
        high_importance_count = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM stock_news_items
                WHERE ts_code = ? AND COALESCE(llm_finance_importance, '') IN ('极高', '高')
                """,
                (normalized_ts_code,),
            ).fetchone()[0]
            or 0
        )
    rows = conn.execute(
        f"""
        SELECT title, pub_time, {importance_expr}, {summary_expr}
        FROM stock_news_items
        WHERE ts_code = ?
        ORDER BY pub_time DESC, id DESC
        LIMIT ?
        """,
        (normalized_ts_code, max(limit, 1)),
    ).fetchall()
    items = [
        {
            "title": str(row["title"] or "").strip(),
            "pub_time": str(row["pub_time"] or ""),
            "finance_importance": str(row["finance_importance"] or ""),
            "summary": str(row["llm_summary"] or "").strip(),
        }
        for row in rows
        if str(row["title"] or "").strip()
    ]
    return {
        "status": "ok",
        "available": True,
        "count": count,
        "high_importance_count": high_importance_count,
        "latest_pub_time": latest_pub_time,
        "items": items,
    }


def _candidate_pool_reason_for_stock(*, conn, ts_code: str, name: str) -> dict[str, Any]:
    normalized_ts_code = str(ts_code or "").strip().upper()
    normalized_name = str(name or "").strip()
    if not _table_exists(conn, "chatroom_stock_candidate_pool"):
        return {"status": "missing_source", "available": False, "matched_count": 0, "items": []}
    where_parts: list[str] = []
    params: list[Any] = []
    if normalized_ts_code:
        where_parts.append("COALESCE(ts_code, '') = ?")
        params.append(normalized_ts_code)
    if normalized_name:
        where_parts.append("candidate_name = ?")
        params.append(normalized_name)
    if not where_parts:
        return {"status": "empty", "available": True, "matched_count": 0, "items": []}
    rows = conn.execute(
        f"""
        SELECT candidate_name, candidate_type, ts_code, dominant_bias, net_score, mention_count, room_count, latest_analysis_date
        FROM chatroom_stock_candidate_pool
        WHERE {' OR '.join(where_parts)}
        ORDER BY ABS(COALESCE(net_score, 0)) DESC, COALESCE(room_count, 0) DESC, COALESCE(mention_count, 0) DESC, candidate_name
        LIMIT 3
        """,
        tuple(params),
    ).fetchall()
    items = [dict(row) for row in rows]
    top = items[0] if items else {}
    return {
        "status": "ok" if items else "empty",
        "available": True,
        "matched_count": len(items),
        "dominant_bias": str(top.get("dominant_bias") or ""),
        "net_score": round(_as_float(top.get("net_score")), 2) if items else 0.0,
        "mention_count": int(_as_float(top.get("mention_count"), 0)) if items else 0,
        "room_count": int(_as_float(top.get("room_count"), 0)) if items else 0,
        "latest_analysis_date": str(top.get("latest_analysis_date") or ""),
        "items": items,
    }


def _score_reason_for_stock(item: dict[str, Any]) -> dict[str, Any]:
    summary = item.get("score_summary") if isinstance(item.get("score_summary"), dict) else {}
    summary_points = [str(summary.get(key) or "").strip() for key in ("trend", "financial", "valuation", "capital_flow", "event", "news", "risk")]
    summary_points = [point for point in summary_points if point]
    return {
        "status": "ok",
        "total_score": round(_as_float(item.get("total_score")), 2),
        "industry_total_score": round(_as_float(item.get("industry_total_score")), 2),
        "score_grade": str(item.get("score_grade") or _score_grade(_as_float(item.get("total_score")))),
        "industry_score_grade": str(item.get("industry_score_grade") or _score_grade(_as_float(item.get("industry_total_score")))),
        "industry_rank": int(_as_float(item.get("industry_rank"), 0)) if item.get("industry_rank") is not None else None,
        "industry_count": int(_as_float(item.get("industry_count"), 0)) if item.get("industry_count") is not None else None,
        "position_label": str(item.get("position_label") or _position_label(_as_float(item.get("total_score")))),
        "decision_reason": str(item.get("decision_reason") or _build_stock_reason(item)),
        "decision_risk": str(item.get("decision_risk") or _build_stock_risk(item)),
        "summary_points": summary_points[:4],
    }


def _signal_reason_for_stock(*, conn, ts_code: str, name: str) -> dict[str, Any]:
    merged: list[dict[str, Any]] = []
    seen = set()
    for term in (str(ts_code or "").strip().upper(), str(name or "").strip()):
        if not term:
            continue
        for item in _related_signals_for_term(conn=conn, term=term):
            key = str(item.get("signal_key") or item.get("subject_name") or item.get("ts_code") or "")
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
    latest_signal_date = str(merged[0].get("latest_signal_date") or "") if merged else ""
    directions = sorted({str(item.get("direction") or "").strip() for item in merged if str(item.get("direction") or "").strip()})
    return {
        "status": "ok" if merged else "empty",
        "available": True,
        "count": len(merged),
        "latest_signal_date": latest_signal_date,
        "directions": directions,
        "items": merged[:3],
    }


def _scoreboard_shortlist(conn, *, target_size: int) -> tuple[str, list[dict[str, Any]], list[str]]:
    latest_score_date, _, rows = _load_stock_score_rows(conn, page=1, page_size=max(target_size * 3, 12))
    shortlist: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    for row in rows:
        ts_code = str(row.get("ts_code") or "").strip().upper()
        if not ts_code or ts_code in seen_codes:
            continue
        seen_codes.add(ts_code)
        shortlist.append(
            {
                "ts_code": ts_code,
                "name": str(row.get("name") or ts_code),
                "industry": str(row.get("industry") or "未知行业"),
                "market": str(row.get("market") or ""),
                "area": str(row.get("area") or ""),
                "total_score": round(_as_float(row.get("total_score")), 2),
                "industry_total_score": round(_as_float(row.get("industry_total_score")), 2),
                "score_grade": str(row.get("score_grade") or _score_grade(_as_float(row.get("total_score")))),
                "industry_score_grade": str(row.get("industry_score_grade") or _score_grade(_as_float(row.get("industry_total_score")))),
                "industry_rank": int(_as_float(row.get("industry_rank"), 0)) if row.get("industry_rank") is not None else None,
                "industry_count": int(_as_float(row.get("industry_count"), 0)) if row.get("industry_count") is not None else None,
                "position_label": str(row.get("position_label") or _position_label(_as_float(row.get("total_score")))),
                "decision_reason": str(row.get("decision_reason") or _build_stock_reason(row)),
                "decision_risk": str(row.get("decision_risk") or _build_stock_risk(row)),
                "source_date": str(row.get("score_date") or latest_score_date or ""),
                "trend_score": round(_as_float(row.get("trend_score")), 2),
                "financial_score": round(_as_float(row.get("financial_score")), 2),
                "valuation_score": round(_as_float(row.get("valuation_score")), 2),
                "capital_flow_score": round(_as_float(row.get("capital_flow_score")), 2),
                "event_score": round(_as_float(row.get("event_score")), 2),
                "news_score": round(_as_float(row.get("news_score")), 2),
                "risk_score": round(_as_float(row.get("risk_score")), 2),
                "score_summary": row.get("score_summary") or {},
            }
        )
        if len(shortlist) >= target_size:
            break
    return latest_score_date, shortlist, [str(item.get("ts_code") or "") for item in shortlist]


def query_decision_scoreboard(*, sqlite3_module, db_path: str, page_size: int = 8) -> dict[str, Any]:
    """
    Layer 2 (data assets) oriented aggregate view:
    - provides scoreboard-oriented evidence packets and source health
    - does not execute user actions
    """
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        page_size = min(max(int(page_size or 8), 3), 20)
        board = _board_payload(conn=conn, sqlite3_module=sqlite3_module, db_path=db_path, page=1, page_size=max(page_size, 8))
        shortlist: list[dict[str, Any]] = list(board.get("shortlist") or [])[:page_size]
        latest_score_date, fallback_items, _ = _scoreboard_shortlist(conn, target_size=page_size)
        seen_codes = {str(item.get("ts_code") or "").strip().upper() for item in shortlist}
        for item in fallback_items:
            ts_code = str(item.get("ts_code") or "").strip().upper()
            if not ts_code or ts_code in seen_codes:
                continue
            shortlist.append(item)
            seen_codes.add(ts_code)
            if len(shortlist) >= page_size:
                break

        source_health = {
            "stock_scores": "ok" if latest_score_date else "missing_source",
            "stock_news": "ok" if _table_exists(conn, "stock_news_items") else "missing_source",
            "signals": "ok" if (_table_exists(conn, "investment_signal_tracker_7d") or _table_exists(conn, "investment_signal_tracker")) else "missing_source",
            "candidate_pool": "ok" if _table_exists(conn, "chatroom_stock_candidate_pool") else "missing_source",
        }
        reason_packets: dict[str, Any] = {}
        for item in shortlist:
            ts_code = str(item.get("ts_code") or "").strip().upper()
            if not ts_code:
                continue
            name = str(item.get("name") or "").strip()
            score_reason = _score_reason_for_stock(item)
            news_reason = _recent_stock_news_for_stock(conn=conn, ts_code=ts_code, limit=3)
            signal_reason = _signal_reason_for_stock(conn=conn, ts_code=ts_code, name=name)
            candidate_reason = _candidate_pool_reason_for_stock(conn=conn, ts_code=ts_code, name=name)
            degraded_sources = [
                source_name
                for source_name, packet in {
                    "news": news_reason,
                    "signals": signal_reason,
                    "candidate_pool": candidate_reason,
                }.items()
                if str(packet.get("status") or "") == "missing_source"
            ]
            reason_packets[ts_code] = {
                "ts_code": ts_code,
                "name": name,
                "industry": str(item.get("industry") or ""),
                "score": score_reason,
                "news": news_reason,
                "signals": signal_reason,
                "candidate_pool": candidate_reason,
                "degraded_sources": degraded_sources,
                "status": "degraded" if degraded_sources else "ok",
            }

        return {
            "generated_at": _utc_now(),
            "snapshot_date": board.get("snapshot_date") or latest_score_date or "",
            "macro_regime": board.get("market_regime") or _aggregate_market_regime(shortlist),
            "industry_scores": list(board.get("industries") or [])[:8],
            "stock_shortlist": shortlist,
            "reason_packets": reason_packets,
            "source_health": source_health,
        }
    finally:
        conn.close()


def _score_strategy_candidate_with_llm(*, board: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    baseline = _as_float(candidate.get("fit_score"))
    mode = str(candidate.get("mode") or "")
    market_mode = str((board.get("market_regime") or {}).get("mode") or "neutral")
    llm_score = baseline
    llm_label = _strategy_feasibility_label(llm_score)
    llm_explanation = str(candidate.get("summary") or candidate.get("rationale") or "暂无说明")
    llm_risk_note = str(candidate.get("risk_control") or "暂无风险控制说明")
    llm_name_hint = str(candidate.get("name") or "")
    llm_source = "heuristic"
    llm_model = ""

    if not _strategy_llm_enabled():
        return {
            "llm_feasibility_score": round(_clamp(llm_score), 2),
            "llm_feasibility_label": llm_label,
            "llm_explanation": llm_explanation,
            "llm_risk_note": llm_risk_note,
            "llm_name_hint": llm_name_hint,
            "llm_model": llm_model,
            "llm_source": llm_source,
        }

    prompt_payload = {
        "market_mode": market_mode,
        "candidate": {
            "name": candidate.get("name"),
            "mode": candidate.get("mode"),
            "fit_score": baseline,
            "entry_rule": candidate.get("entry_rule"),
            "exit_rule": candidate.get("exit_rule"),
            "position_bias": candidate.get("position_bias"),
            "universe": candidate.get("universe"),
            "summary": candidate.get("summary"),
            "rationale": candidate.get("rationale"),
            "risk_control": candidate.get("risk_control"),
            "linked_industries": candidate.get("linked_industries") or [],
            "linked_stocks": candidate.get("linked_stocks") or [],
        },
    }
    messages = [
        {
            "role": "system",
            "content": "你是投研策略可行性评审助手。请只输出严格 JSON，不要输出额外解释。JSON 字段必须包含 feasibility_score(0-100 数字)、feasibility_label(高/中高/中/低)、name_hint、explanation、risk_note。请保持结论辅助性质，不要参与最终排序。",
        },
        {
            "role": "user",
            "content": (
                "请基于以下策略候选给出辅助可行性评分与解释。"
                f"\n{json.dumps(prompt_payload, ensure_ascii=False, default=str)}"
            ),
        },
    ]
    try:
        llm_result = chat_completion_text(
            model=_strategy_llm_model(),
            messages=messages,
            temperature=0.1,
            timeout_s=18,
        )
        parsed = _parse_json(llm_result)
        if parsed:
            llm_score = _clamp(_as_float(parsed.get("feasibility_score"), baseline))
            llm_label = str(parsed.get("feasibility_label") or _strategy_feasibility_label(llm_score))
            llm_explanation = str(parsed.get("explanation") or llm_explanation).strip() or llm_explanation
            llm_risk_note = str(parsed.get("risk_note") or llm_risk_note).strip() or llm_risk_note
            llm_name_hint = str(parsed.get("name_hint") or llm_name_hint).strip() or llm_name_hint
            llm_model = _strategy_llm_model()
            llm_source = "llm"
    except (LLMCallError, Exception):
        llm_score = baseline + 5 if mode in {"aggressive", "single-stock"} and market_mode in {"aggressive", "balanced"} else baseline
        if mode == "defensive":
            llm_score -= 2
        llm_score = _clamp(llm_score)
        llm_label = _strategy_feasibility_label(llm_score)
        llm_explanation = f"基于当前市场模式 {market_mode} 与策略模式 {mode} 的规则推断，建议作为辅助候选观察。"
        llm_risk_note = str(candidate.get("risk_control") or "保持轻仓与人工确认。")
        llm_name_hint = str(candidate.get("name") or "")
        llm_model = ""
        llm_source = "heuristic"

    return {
        "llm_feasibility_score": round(llm_score, 2),
        "llm_feasibility_label": llm_label,
        "llm_explanation": llm_explanation,
        "llm_risk_note": llm_risk_note,
        "llm_name_hint": llm_name_hint,
        "llm_model": llm_model,
        "llm_source": llm_source,
    }


def _load_strategy_candidate_rows(conn, *, run_id: int, page: int, page_size: int) -> list[dict[str, Any]]:
    page = max(page, 1)
    page_size = min(max(page_size, 1), 100)
    offset = (page - 1) * page_size
    rows = conn.execute(
        f"""
        SELECT *
        FROM {DECISION_STRATEGY_CANDIDATE_TABLE}
        WHERE run_id = ?
        ORDER BY rank ASC, id ASC
        LIMIT ? OFFSET ?
        """,
        (run_id, page_size, offset),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["linked_industries"] = _parse_json_list(item.get("linked_industries_json"))
        item["linked_stocks"] = _parse_json_list(item.get("linked_stocks_json"))
        item["candidate_json"] = _parse_json(item.get("candidate_json"))
        items.append(item)
    return items


def _load_strategy_candidate_rows_all(conn, *, run_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT *
        FROM {DECISION_STRATEGY_CANDIDATE_TABLE}
        WHERE run_id = ?
        ORDER BY rank ASC, id ASC
        """,
        (run_id,),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["linked_industries"] = _parse_json_list(item.get("linked_industries_json"))
        item["linked_stocks"] = _parse_json_list(item.get("linked_stocks_json"))
        item["candidate_json"] = _parse_json(item.get("candidate_json"))
        items.append(item)
    return items


def _load_strategy_run_row(conn, *, run_id: int = 0, run_key: str = "", ts_code: str = "", keyword: str = "") -> dict[str, Any] | None:
    where = []
    params: list[Any] = []
    if run_id:
        where.append("id = ?")
        params.append(int(run_id))
    if run_key:
        where.append("run_key = ?")
        params.append(str(run_key))
    if ts_code:
        where.append("ts_code = ?")
        params.append(str(ts_code).strip().upper())
    if keyword:
        where.append("keyword = ?")
        params.append(str(keyword).strip())
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    row = conn.execute(
        f"""
        SELECT *
        FROM {DECISION_STRATEGY_RUN_TABLE}
        {where_sql}
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    return dict(row) if row else None


def _load_latest_strategy_run_row(conn, *, ts_code: str = "", keyword: str = "") -> dict[str, Any] | None:
    return _load_strategy_run_row(conn, ts_code=ts_code, keyword=keyword)


def _load_strategy_run_history(conn, *, page: int, page_size: int, ts_code: str = "", keyword: str = "") -> dict[str, Any]:
    page = max(page, 1)
    page_size = min(max(page_size, 1), 100)
    offset = (page - 1) * page_size
    where = []
    params: list[Any] = []
    normalized_ts_code = str(ts_code or "").strip().upper()
    normalized_keyword = str(keyword or "").strip()
    if normalized_ts_code:
        where.append("ts_code = ?")
        params.append(normalized_ts_code)
    if normalized_keyword:
        where.append("keyword = ?")
        params.append(normalized_keyword)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    total = int(conn.execute(f"SELECT COUNT(*) FROM {DECISION_STRATEGY_RUN_TABLE} {where_sql}", tuple(params)).fetchone()[0] or 0)
    rows = conn.execute(
        f"""
        SELECT *
        FROM {DECISION_STRATEGY_RUN_TABLE}
        {where_sql}
        ORDER BY created_at DESC, id DESC
        LIMIT ? OFFSET ?
        """,
        (*params, page_size, offset),
    ).fetchall()
    items: list[dict[str, Any]] = []
    previous_summary: dict[str, Any] | None = None
    for row in rows:
        item = dict(row)
        summary = _parse_json(item.get("summary_json"))
        item["summary"] = summary
        item["comparison_to_previous"] = _strategy_comparison(summary, previous_summary)
        previous_summary = summary
        items.append(item)
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": items,
    }


def _strategy_candidate_packet_for_store(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": candidate.get("name"),
        "mode": candidate.get("mode"),
        "status": candidate.get("status"),
        "fit_score": candidate.get("fit_score"),
        "llm_feasibility_score": candidate.get("llm_feasibility_score"),
        "llm_feasibility_label": candidate.get("llm_feasibility_label"),
        "llm_explanation": candidate.get("llm_explanation"),
        "llm_risk_note": candidate.get("llm_risk_note"),
        "llm_name_hint": candidate.get("llm_name_hint"),
        "llm_model": candidate.get("llm_model"),
        "llm_source": candidate.get("llm_source"),
        "priority": candidate.get("priority"),
        "rank": candidate.get("rank"),
        "summary": candidate.get("summary"),
        "entry_rule": candidate.get("entry_rule"),
        "exit_rule": candidate.get("exit_rule"),
        "position_bias": candidate.get("position_bias"),
        "universe": candidate.get("universe"),
        "rationale": candidate.get("rationale"),
        "risk_control": candidate.get("risk_control"),
        "linked_industries": list(candidate.get("linked_industries") or []),
        "linked_stocks": list(candidate.get("linked_stocks") or []),
        "approval_hint": candidate.get("approval_hint"),
    }


def _persist_strategy_run(
    *,
    conn,
    board: dict[str, Any],
    packet: dict[str, Any],
    ts_code: str,
    keyword: str,
    source_mode: str,
) -> dict[str, Any]:
    _ensure_tables(conn)
    now = _utc_now()
    prev_version = int(
        conn.execute(
            f"""
            SELECT COALESCE(MAX(run_version), 0)
            FROM {DECISION_STRATEGY_RUN_TABLE}
            WHERE ts_code = ? AND keyword = ?
            """,
            (str(ts_code or "").strip().upper(), str(keyword or "").strip()),
        ).fetchone()[0]
        or 0
    )
    run_version = prev_version + 1
    run_key = _strategy_run_key(run_version)
    summary = dict(packet.get("summary") or {})
    summary["source_mode"] = source_mode
    summary["strategy_count"] = int(summary.get("strategy_count") or len(packet.get("all_strategies") or []))
    summary_json = json.dumps(summary, ensure_ascii=False, default=str)
    board_json = json.dumps(board, ensure_ascii=False, default=str)
    generator_rules_json = json.dumps(list(packet.get("generator_rules") or []), ensure_ascii=False, default=str)
    llm_model = str(packet.get("run", {}).get("llm_model") or "").strip()
    llm_enabled = 1 if any(str(item.get("llm_source") or "") == "llm" for item in (packet.get("all_strategies") or [])) else 0
    conn.execute(
        f"""
        INSERT INTO {DECISION_STRATEGY_RUN_TABLE} (
            run_key, run_version, ts_code, keyword, title, status, source_mode, generator_mode, llm_model, llm_enabled,
            market_mode, approval_state, summary_json, board_snapshot_json, generator_rules_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_key,
            run_version,
            str(ts_code or "").strip().upper(),
            str(keyword or "").strip(),
            str(packet.get("title") or "策略实验台"),
            "completed",
            source_mode,
            "rules+llm",
            llm_model,
            llm_enabled,
            str(summary.get("market_mode") or packet.get("market_regime", {}).get("mode") or "neutral"),
            str(summary.get("approval_state") or "candidate"),
            summary_json,
            board_json,
            generator_rules_json,
            now,
            now,
        ),
    )
    run_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0] or 0)
    stored_candidates = packet.get("all_strategies") or packet.get("strategies") or []
    for candidate in stored_candidates:
        candidate_row = _strategy_candidate_packet_for_store(candidate)
        conn.execute(
            f"""
            INSERT INTO {DECISION_STRATEGY_CANDIDATE_TABLE} (
                run_id, run_key, ts_code, keyword, rank, priority, name, mode, status, fit_score,
                llm_feasibility_score, llm_feasibility_label, llm_explanation, llm_risk_note, llm_name_hint,
                summary, entry_rule, exit_rule, position_bias, universe, rationale, risk_control,
                linked_industries_json, linked_stocks_json, candidate_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                run_key,
                str(ts_code or "").strip().upper(),
                str(keyword or "").strip(),
                int(candidate_row.get("rank") or 0),
                int(candidate_row.get("priority") or 0),
                str(candidate_row.get("name") or ""),
                str(candidate_row.get("mode") or ""),
                str(candidate_row.get("status") or ""),
                _as_float(candidate_row.get("fit_score")),
                _as_float(candidate_row.get("llm_feasibility_score")),
                str(candidate_row.get("llm_feasibility_label") or ""),
                str(candidate_row.get("llm_explanation") or ""),
                str(candidate_row.get("llm_risk_note") or ""),
                str(candidate_row.get("llm_name_hint") or ""),
                str(candidate_row.get("summary") or ""),
                str(candidate_row.get("entry_rule") or ""),
                str(candidate_row.get("exit_rule") or ""),
                str(candidate_row.get("position_bias") or ""),
                str(candidate_row.get("universe") or ""),
                str(candidate_row.get("rationale") or ""),
                str(candidate_row.get("risk_control") or ""),
                json.dumps(candidate_row.get("linked_industries") or [], ensure_ascii=False, default=str),
                json.dumps(candidate_row.get("linked_stocks") or [], ensure_ascii=False, default=str),
                json.dumps(candidate_row, ensure_ascii=False, default=str),
                now,
                now,
            ),
        )
    conn.commit()
    stored_row = _load_strategy_run_row(conn, run_id=run_id)
    return {
        "run_id": run_id,
        "run_key": run_key,
        "run_version": run_version,
        "stored_row": stored_row or {},
    }


def _build_strategy_packet_from_board(
    *,
    board: dict[str, Any],
    conn,
    ts_code: str = "",
    keyword: str = "",
    page: int = 1,
    page_size: int = 12,
    source_mode: str = "preview",
    run_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _build_strategy_candidates(
        board,
        conn=conn,
        ts_code=ts_code,
        keyword=keyword,
        page=page,
        page_size=page_size,
        source_mode=source_mode,
        run_meta=run_meta,
    )


def _build_strategy_detail_response(*, conn, row: dict[str, Any], page: int, page_size: int) -> dict[str, Any]:
    packet = _strategy_run_row_to_packet(row, conn=conn, page=page, page_size=page_size)
    candidates_all = _load_strategy_candidate_rows_all(conn, run_id=int(row.get("id") or 0))
    packet["strategies"] = candidates_all[(max(page, 1) - 1) * min(max(page_size, 1), 100) : (max(page, 1) - 1) * min(max(page_size, 1), 100) + min(max(page_size, 1), 100)]
    packet["all_strategies"] = candidates_all
    packet["summary"]["strategy_count"] = len(candidates_all)
    if candidates_all:
        packet["summary"]["best_strategy"] = candidates_all[0].get("name") or packet["summary"].get("best_strategy", "")
        packet["summary"]["best_fit_score"] = _as_float(candidates_all[0].get("fit_score"), packet["summary"].get("best_fit_score", 0.0))
        packet["summary"]["llm_average_score"] = round(
            sum(_as_float(item.get("llm_feasibility_score")) for item in candidates_all) / len(candidates_all),
            2,
        )
    return packet


def _strategy_run_row_to_packet(row: dict[str, Any], *, conn, page: int, page_size: int) -> dict[str, Any]:
    summary = _parse_json(row.get("summary_json"))
    board_snapshot = _parse_json(row.get("board_snapshot_json"))
    generator_rules = _parse_json_list(row.get("generator_rules_json"))
    run_meta = {
        "run_id": int(row.get("id") or 0),
        "run_key": row.get("run_key"),
        "run_version": int(row.get("run_version") or 0),
        "status": row.get("status"),
        "source_mode": row.get("source_mode"),
        "generator_mode": row.get("generator_mode"),
        "llm_model": row.get("llm_model"),
        "llm_enabled": int(row.get("llm_enabled") or 0),
        "market_mode": row.get("market_mode"),
        "approval_state": row.get("approval_state"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }
    candidates = _load_strategy_candidate_rows_all(conn, run_id=int(row.get("id") or 0))
    packet = {
        "generated_at": row.get("created_at") or _utc_now(),
        "snapshot_date": board_snapshot.get("snapshot_date") or "",
        "title": row.get("title") or "策略实验台",
        "source_mode": "stored",
        "run": run_meta,
        "summary": summary,
        "generator_rules": generator_rules,
        "market_regime": board_snapshot.get("market_regime") or {},
        "validation": board_snapshot.get("validation") or {},
        "kill_switch": board_snapshot.get("kill_switch") or {},
        "focus_name": board_snapshot.get("focus_name") or "",
        "focus_stock": board_snapshot.get("focus_stock") if isinstance(board_snapshot.get("focus_stock"), dict) else {},
        "ts_code": row.get("ts_code") or "",
        "keyword": row.get("keyword") or "",
        "all_strategies": candidates,
    }
    page = max(page, 1)
    page_size = min(max(page_size, 1), 100)
    offset = (page - 1) * page_size
    packet["strategies"] = candidates[offset : offset + page_size]
    packet["strategy_page"] = {
        "page": page,
        "page_size": page_size,
        "total": len(candidates),
    }
    if candidates:
        packet["summary"] = dict(summary)
        packet["summary"]["strategy_count"] = len(candidates)
        packet["summary"]["best_strategy"] = str(summary.get("best_strategy") or candidates[0].get("name") or "")
        packet["summary"]["best_fit_score"] = _as_float(summary.get("best_fit_score"), _as_float(candidates[0].get("fit_score")))
        packet["summary"]["llm_average_score"] = round(
            sum(_as_float(item.get("llm_feasibility_score")) for item in candidates) / len(candidates),
            2,
        )
    else:
        packet["summary"] = dict(summary)
        packet["summary"].setdefault("strategy_count", 0)
        packet["summary"].setdefault("best_strategy", "")
        packet["summary"].setdefault("best_fit_score", 0.0)
        packet["summary"].setdefault("llm_average_score", 0.0)
    packet["comparison"] = _strategy_comparison(packet["summary"], None)
    return packet


def _board_payload(*, conn, sqlite3_module, db_path: str, page: int, page_size: int, ts_code: str = "", keyword: str = "") -> dict[str, Any]:
    latest_score_date, total, rows = _load_stock_score_rows(conn, page=page, page_size=page_size)
    market_regime = _aggregate_market_regime(rows)
    industries = _aggregate_industries(rows)
    shortlist = []
    for row in rows:
        score = _as_float(row.get("total_score"))
        item = {
            "ts_code": str(row.get("ts_code") or ""),
            "name": str(row.get("name") or row.get("ts_code") or ""),
            "industry": str(row.get("industry") or "未知行业"),
            "market": str(row.get("market") or ""),
            "area": str(row.get("area") or ""),
            "total_score": round(score, 2),
            "industry_total_score": round(_as_float(row.get("industry_total_score")), 2),
            "score_grade": str(row.get("score_grade") or _score_grade(score)),
            "industry_score_grade": str(row.get("industry_score_grade") or _score_grade(_as_float(row.get("industry_total_score")))),
            "position_label": _position_label(score),
            "decision_reason": row.get("decision_reason"),
            "decision_risk": row.get("decision_risk"),
            "source_date": row.get("score_date"),
            "trend_score": round(_as_float(row.get("trend_score")), 2),
            "financial_score": round(_as_float(row.get("financial_score")), 2),
            "valuation_score": round(_as_float(row.get("valuation_score")), 2),
            "capital_flow_score": round(_as_float(row.get("capital_flow_score")), 2),
            "event_score": round(_as_float(row.get("event_score")), 2),
            "news_score": round(_as_float(row.get("news_score")), 2),
            "risk_score": round(_as_float(row.get("risk_score")), 2),
            "score_summary": row.get("score_summary") or {},
        }
        item["why_now"] = item["decision_reason"]
        item["entry_note"] = item["position_label"]
        shortlist.append(item)

    kill_switch = _get_control_row(conn)
    trade_plan = _build_trade_plan(market_regime, industries, shortlist, kill_switch)
    validation = _validation_snapshot(conn)
    allow_trading = bool(int(kill_switch.get("allow_trading") or 0))
    for item in shortlist:
        actionable = _build_actionable_fields(
            total_score=_as_float(item.get("total_score")),
            trend_score=_as_float(item.get("trend_score")),
            risk_score=_as_float(item.get("risk_score")),
            position_label=str(item.get("position_label") or ""),
            market_mode=str(market_regime.get("mode") or ""),
            trade_mode=str(trade_plan.get("mode") or ""),
            allow_trading=allow_trading,
        )
        item.update(actionable)
    pipeline_health = _build_pipeline_health(
        conn,
        latest_score_date=str(latest_score_date or ""),
        universe_size=total,
        shortlist_size=len(shortlist),
    )
    payload: dict[str, Any] = {
        "generated_at": _utc_now(),
        "snapshot_date": latest_score_date or "",
        "summary": {
            "universe_size": total,
            "score_date": latest_score_date or "",
            "shortlist_size": len(shortlist),
            "top_score": round(_as_float(shortlist[0]["total_score"]) if shortlist else 0.0, 2),
            "average_score": round(sum(_as_float(item["total_score"]) for item in shortlist) / len(shortlist), 2) if shortlist else 0.0,
        },
        "market_regime": market_regime,
        "industries": industries[:10],
        "shortlist": shortlist,
        "trade_plan": trade_plan,
        "validation": validation,
        "kill_switch": kill_switch,
        "pipeline_health": pipeline_health,
    }
    if ts_code:
        payload["focus_stock"] = query_decision_stock(sqlite3_module=sqlite3_module, db_path=db_path, ts_code=ts_code, keyword=keyword)
    return payload


def query_decision_board(*, sqlite3_module, db_path: str, page: int = 1, page_size: int = 12, ts_code: str = "", keyword: str = "") -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        return _board_payload(conn=conn, sqlite3_module=sqlite3_module, db_path=db_path, page=page, page_size=page_size, ts_code=ts_code, keyword=keyword)
    finally:
        conn.close()


def query_decision_trade_plan(*, sqlite3_module, db_path: str, page: int = 1, page_size: int = 12, ts_code: str = "", keyword: str = "") -> dict[str, Any]:
    board = query_decision_board(
        sqlite3_module=sqlite3_module,
        db_path=db_path,
        page=page,
        page_size=page_size,
        ts_code=ts_code,
        keyword=keyword,
    )
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        return _build_trade_plan_packet(board, conn=conn, ts_code=ts_code, keyword=keyword)
    finally:
        conn.close()


def query_decision_strategy_runs(
    *,
    sqlite3_module,
    db_path: str,
    page: int = 1,
    page_size: int = 20,
    ts_code: str = "",
    keyword: str = "",
) -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        return _load_strategy_run_history(conn, page=page, page_size=page_size, ts_code=ts_code, keyword=keyword)
    finally:
        conn.close()


def query_decision_strategy_lab(
    *,
    sqlite3_module,
    db_path: str,
    page: int = 1,
    page_size: int = 12,
    ts_code: str = "",
    keyword: str = "",
    run_id: int = 0,
) -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        normalized_ts_code = str(ts_code or "").strip().upper()
        normalized_keyword = str(keyword or "").strip()
        if run_id:
            row = _load_strategy_run_row(conn, run_id=run_id, ts_code=normalized_ts_code, keyword=normalized_keyword)
            if row:
                return _build_strategy_detail_response(conn=conn, row=row, page=page, page_size=page_size)
        else:
            row = _load_latest_strategy_run_row(conn, ts_code=normalized_ts_code, keyword=normalized_keyword)
            if row:
                return _build_strategy_detail_response(conn=conn, row=row, page=page, page_size=page_size)
        board = query_decision_board(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            page=page,
            page_size=page_size,
            ts_code=normalized_ts_code,
            keyword=normalized_keyword,
        )
        return _build_strategy_packet_from_board(
            board=board,
            conn=conn,
            ts_code=normalized_ts_code,
            keyword=normalized_keyword,
            page=page,
            page_size=page_size,
            source_mode="preview",
            run_meta={
                "run_id": 0,
                "run_key": "",
                "run_version": 0,
                "status": "preview",
                "source_mode": "preview",
                "llm_enabled": 1 if _strategy_llm_enabled() else 0,
                "llm_model": _strategy_llm_model() if _strategy_llm_enabled() else "",
            },
        )
    finally:
        conn.close()


def run_decision_strategy_generation(
    *,
    sqlite3_module,
    db_path: str,
    page: int = 1,
    page_size: int = 12,
    ts_code: str = "",
    keyword: str = "",
) -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        normalized_ts_code = str(ts_code or "").strip().upper()
        normalized_keyword = str(keyword or "").strip()
        board = query_decision_board(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            page=page,
            page_size=page_size,
            ts_code=normalized_ts_code,
            keyword=normalized_keyword,
        )
        packet = _build_strategy_packet_from_board(
            board=board,
            conn=conn,
            ts_code=normalized_ts_code,
            keyword=normalized_keyword,
            page=page,
            page_size=page_size,
            source_mode="generated",
            run_meta={
                "run_id": 0,
                "run_key": "",
                "run_version": 0,
                "status": "building",
                "source_mode": "generated",
                "llm_enabled": 1 if _strategy_llm_enabled() else 0,
                "llm_model": _strategy_llm_model() if _strategy_llm_enabled() else "",
            },
        )
        run_meta = _persist_strategy_run(
            conn=conn,
            board=board,
            packet=packet,
            ts_code=normalized_ts_code,
            keyword=normalized_keyword,
            source_mode="generated",
        )
        row = _load_strategy_run_row(conn, run_id=run_meta["run_id"])
        if not row:
            raise RuntimeError("策略运行写入失败")
        detail = _build_strategy_detail_response(conn=conn, row=row, page=page, page_size=page_size)
        detail["generated_run"] = {
            "run_id": run_meta["run_id"],
            "run_key": run_meta["run_key"],
            "run_version": run_meta["run_version"],
        }
        detail["source_mode"] = "generated"
        return detail
    finally:
        conn.close()


def query_decision_stock(*, sqlite3_module, db_path: str, ts_code: str, keyword: str = "") -> dict[str, Any]:
    ts_code = str(ts_code or "").strip().upper()
    keyword = str(keyword or "").strip()
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        stock_detail = {}
        try:
            stock_detail = query_stock_detail(sqlite3_module=sqlite3_module, db_path=db_path, ts_code=ts_code, keyword=keyword, lookback=60)
        except Exception as exc:
            stock_detail = {"error": str(exc)}
        score_row = None
        if _table_exists(conn, "stock_scores_daily") and ts_code:
            latest_score_date = conn.execute("SELECT MAX(score_date) FROM stock_scores_daily").fetchone()[0]
            if latest_score_date:
                row = conn.execute(
                    """
                    SELECT *
                    FROM stock_scores_daily
                    WHERE score_date = ? AND ts_code = ?
                    LIMIT 1
                    """,
                    (latest_score_date, ts_code),
                ).fetchone()
                score_row = dict(row) if row else None
        if not score_row and stock_detail.get("score"):
            score_row = {
                "ts_code": ts_code,
                "name": stock_detail.get("profile", {}).get("name") if isinstance(stock_detail.get("profile"), dict) else "",
                "industry": stock_detail.get("profile", {}).get("industry") if isinstance(stock_detail.get("profile"), dict) else "",
                "market": stock_detail.get("profile", {}).get("market") if isinstance(stock_detail.get("profile"), dict) else "",
                "total_score": _as_float((stock_detail.get("score") or {}).get("total_score"), 0.0),
                "industry_total_score": _as_float((stock_detail.get("score") or {}).get("industry_total_score"), 0.0),
                "score_grade": str((stock_detail.get("score") or {}).get("score_grade") or ""),
                "industry_score_grade": str((stock_detail.get("score") or {}).get("industry_score_grade") or ""),
                "score_summary": (stock_detail.get("score") or {}).get("score_summary") or {},
            }
        total_score = _as_float(score_row.get("total_score") if score_row else 0.0)
        industry_score = _as_float(score_row.get("industry_total_score") if score_row else 0.0)
        position_label = _position_label(total_score)
        score_summary = (score_row or {}).get("score_summary") or {}
        reasons = []
        if score_summary:
            reasons.extend([str(score_summary.get(key) or "").strip() for key in ("trend", "financial", "valuation", "capital_flow", "event", "news", "risk") if str(score_summary.get(key) or "").strip()])
        if not reasons:
            reasons.append("复用现有股票综合评分、财务、估值、价格和风险结果")
        risk = []
        if total_score < 65:
            risk.append("综合评分尚未进入强推荐区间")
        if industry_score and industry_score < 60:
            risk.append("行业相对评分偏弱")
        if not risk:
            risk.append("保持仓位纪律，等待右侧确认")
        allow_entry = total_score >= 65
        actionable = _build_actionable_fields(
            total_score=total_score,
            trend_score=_as_float((score_row or {}).get("trend_score")),
            risk_score=_as_float((score_row or {}).get("risk_score"), 60.0),
            position_label=position_label,
            market_mode="single_stock",
            trade_mode="single_stock",
            allow_trading=allow_entry,
        )
        return {
            "generated_at": _utc_now(),
            "ts_code": ts_code,
            "keyword": keyword,
            "score": {
                "total_score": round(total_score, 2),
                "industry_total_score": round(industry_score, 2),
                "score_grade": str(score_row.get("score_grade") if score_row else _score_grade(total_score)),
                "industry_score_grade": str(score_row.get("industry_score_grade") if score_row else _score_grade(industry_score)),
                "position_label": position_label,
            },
            "reason": "；".join(reasons[:4]),
            "risk": "；".join(risk),
            "trade_plan": {
                "suggestion": position_label,
                "allow_entry": allow_entry,
                "watchlist_priority": "high" if total_score >= 75 else "medium" if total_score >= 65 else "low",
                **actionable,
            },
            "detail": stock_detail,
        }
    finally:
        conn.close()


def query_decision_history(*, sqlite3_module, db_path: str, page: int = 1, page_size: int = 20) -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        total = int(conn.execute(f"SELECT COUNT(*) FROM {DECISION_SNAPSHOT_TABLE}").fetchone()[0] or 0)
        rows = conn.execute(
            f"""
            SELECT id, snapshot_date, snapshot_type, payload_json, created_at, updated_at
            FROM {DECISION_SNAPSHOT_TABLE}
            ORDER BY snapshot_date DESC, updated_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (page_size, offset),
        ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["payload"] = _parse_json(item.get("payload_json"))
            item["trace"] = _snapshot_trace_fields(item.get("payload"), snapshot_row_id=item.get("id"), snapshot_date=str(item.get("snapshot_date") or ""))
            item["receipt"] = _decision_receipt(
                item.get("payload"),
                trace=item["trace"],
                source=_receipt_source(item.get("payload"), "decision_snapshot"),
            )
            item["status"] = item["receipt"]["status"]
            item["source"] = item["receipt"]["source"]
            item["context"] = item["receipt"]["context"]
            items.append(item)
        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size if total else 0,
            "items": items,
        }
    finally:
        conn.close()


def query_decision_actions(*, sqlite3_module, db_path: str, page: int = 1, page_size: int = 20, ts_code: str = "") -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        where = []
        params: list[Any] = []
        normalized_ts_code = str(ts_code or "").strip().upper()
        if normalized_ts_code:
            where.append("ts_code = ?")
            params.append(normalized_ts_code)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        total = int(
            conn.execute(f"SELECT COUNT(*) FROM {DECISION_ACTION_TABLE} {where_sql}", tuple(params)).fetchone()[0] or 0
        )
        rows = conn.execute(
            f"""
            SELECT id, action_type, ts_code, stock_name, note, actor, snapshot_date, action_payload_json, created_at
            FROM {DECISION_ACTION_TABLE}
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (*params, page_size, offset),
        ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["payload"] = _parse_json(item.get("action_payload_json"))
            item["trace"] = _action_trace_fields(
                item.get("payload"),
                action_row_id=item.get("id"),
                snapshot_date=str(item.get("snapshot_date") or ""),
            )
            item["receipt"] = _decision_receipt(
                item.get("payload"),
                trace=item["trace"],
                source=_receipt_source(item.get("payload"), "decision_board"),
            )
            item["status"] = item["receipt"]["status"]
            item["source"] = item["receipt"]["source"]
            item["context"] = item["receipt"]["context"]
            item["job_trace"] = _action_job_trace(conn, item)
            items.append(item)
        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size if total else 0,
            "items": items,
        }
    finally:
        conn.close()


def query_decision_calibration(
    *,
    sqlite3_module,
    db_path: str,
    page: int = 1,
    page_size: int = 20,
    ts_code: str = "",
) -> dict[str, Any]:
    """
    Layer 3 (verification/research) endpoint:
    - validates confirm/reject verdict quality with 5/20/60 trading-day returns
    - outputs hit-rate summary for feedback into decision workflows
    """
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size

        where: list[str] = ["action_type IN ('confirm', 'reject')"]
        params: list[Any] = []
        normalized_ts_code = str(ts_code or "").strip().upper()
        if normalized_ts_code:
            where.append("ts_code = ?")
            params.append(normalized_ts_code)
        where_sql = "WHERE " + " AND ".join(where)

        total = int(
            conn.execute(f"SELECT COUNT(*) FROM {DECISION_ACTION_TABLE} {where_sql}", tuple(params)).fetchone()[0] or 0
        )

        _row_sql = f"""
            SELECT id, action_type, ts_code, stock_name, note, actor, snapshot_date, action_payload_json, created_at
            FROM {DECISION_ACTION_TABLE}
            {where_sql}
            ORDER BY created_at DESC, id DESC
            """

        # All rows (no LIMIT) for accurate summary computation
        summary_rows = conn.execute(_row_sql, tuple(params)).fetchall()

        # Current page only for items
        page_rows = conn.execute(
            _row_sql + " LIMIT ? OFFSET ?",
            tuple(params) + (page_size, offset),
        ).fetchall()

        prices_table_exists = _table_exists(conn, "stock_daily_prices")

        # Fix 2b: batch price fetch — one query for all ts_codes in summary_rows
        price_map: dict[str, list[tuple[str, float | None]]] = {}
        if prices_table_exists and summary_rows:
            all_ts_codes = list({dict(r)["ts_code"] for r in summary_rows if dict(r)["ts_code"]})
            if all_ts_codes:
                placeholders = ",".join("?" * len(all_ts_codes))
                price_batch = conn.execute(
                    f"SELECT ts_code, trade_date, close FROM stock_daily_prices"
                    f" WHERE ts_code IN ({placeholders}) ORDER BY ts_code, trade_date ASC",
                    tuple(all_ts_codes),
                ).fetchall()
                for pb in price_batch:
                    ts_val, td_val, cl_val = pb[0], pb[1], pb[2]
                    price_map.setdefault(ts_val, []).append(
                        (td_val, float(cl_val) if cl_val is not None else None)
                    )

        def _closes_from_map(ts: str, verdict_date: str) -> list[float | None]:
            if not prices_table_exists or not ts or len(verdict_date) != 8:
                return []
            return [cl for td, cl in price_map.get(ts, []) if td >= verdict_date][:61]

        def _return_at(closes: list[float | None], idx: int) -> float | None:
            if len(closes) <= idx or closes[0] is None or closes[idx] is None or closes[0] == 0:
                return None
            return round((closes[idx] - closes[0]) / closes[0] * 100, 2)

        def _verdict_date(created_at: str) -> str:
            return str(created_at or "")[:10].replace("-", "")

        def _enrich(d: dict) -> dict:
            vdate = _verdict_date(d["created_at"])
            closes = _closes_from_map(d["ts_code"], vdate)
            r5 = _return_at(closes, 5)
            r20 = _return_at(closes, 20)
            r60 = _return_at(closes, 60)
            hit_5d = None if r5 is None else (r5 > 0 if d["action_type"] == "confirm" else r5 < 0)
            return {
                "id": d["id"],
                "ts_code": d["ts_code"],
                "stock_name": d["stock_name"],
                "action_type": d["action_type"],
                "created_at": d["created_at"],
                "snapshot_date": d["snapshot_date"],
                "actor": d["actor"],
                "note": d["note"],
                "payload": _parse_json(d.get("action_payload_json")),
                "price_at_verdict": closes[0] if closes else None,
                "return_5d": r5,
                "return_20d": r20,
                "return_60d": r60,
                "hit_5d": hit_5d,
            }

        # Summary over ALL rows (no page cap)
        all_enriched = [_enrich(dict(row)) for row in summary_rows]
        confirm_items = [e for e in all_enriched if e["action_type"] == "confirm"]
        reject_items = [e for e in all_enriched if e["action_type"] == "reject"]
        confirm_count = len(confirm_items)
        reject_count = len(reject_items)
        confirm_hit = sum(1 for e in confirm_items if e["hit_5d"] is True)
        reject_hit = sum(1 for e in reject_items if e["hit_5d"] is True)
        total_count = confirm_count + reject_count
        total_hit = confirm_hit + reject_hit
        summary: dict[str, Any] = {
            "confirm_count": confirm_count,
            "confirm_hit_5d": confirm_hit,
            "confirm_hit_rate_5d": round(confirm_hit / confirm_count, 3) if confirm_count else 0.0,
            "reject_count": reject_count,
            "reject_hit_5d": reject_hit,
            "reject_hit_rate_5d": round(reject_hit / reject_count, 3) if reject_count else 0.0,
            "total_count": total_count,
            "total_hit_5d": total_hit,
            "total_hit_rate_5d": round(total_hit / total_count, 3) if total_count else 0.0,
        }

        # Items: enrich only the current page rows
        page_enriched = [_enrich(dict(row)) for row in page_rows]

        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size if total else 0,
            "summary": summary,
            "items": page_enriched,
        }
    finally:
        conn.close()


def record_decision_action(
    *,
    sqlite3_module,
    db_path: str,
    action_type: str,
    ts_code: str = "",
    stock_name: str = "",
    note: str = "",
    actor: str = "",
    snapshot_date: str = "",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        normalized_action_type = str(action_type or "").strip().lower()
        if normalized_action_type not in {"confirm", "reject", "defer", "watch", "review"}:
            raise ValueError("action_type 必须是 confirm/reject/defer/watch/review 之一")
        normalized_ts_code = str(ts_code or "").strip().upper()
        normalized_stock_name = str(stock_name or "").strip()
        normalized_note = str(note or "").strip()
        normalized_actor = str(actor or "").strip() or "anonymous"
        normalized_snapshot_date = str(snapshot_date or "").strip() or _today_cn()
        # Extract idempotency_key from payload for DB-level dedup (NOT stored only in JSON blob)
        idempotency_key = str(payload.get("idempotency_key") or "").strip() if isinstance(payload, dict) else ""
        payload_json = json.dumps(payload or {}, ensure_ascii=False, default=str)
        now = _utc_now()
        # DB-level idempotency check using dedicated column
        if idempotency_key:
            existing_row = conn.execute(
                f"SELECT id FROM {DECISION_ACTION_TABLE} WHERE idempotency_key = ? LIMIT 1",
                (idempotency_key,),
            ).fetchone()
            if existing_row:
                existing_id = existing_row[0]
                return {"ok": True, "id": existing_id, "action_id": f"action-{existing_id}", "deduplicated": True}
        conn.execute(
            f"""
            INSERT INTO {DECISION_ACTION_TABLE} (
                action_type, ts_code, stock_name, note, actor, snapshot_date, action_payload_json, idempotency_key, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_action_type,
                normalized_ts_code,
                normalized_stock_name,
                normalized_note,
                normalized_actor,
                normalized_snapshot_date,
                payload_json,
                idempotency_key if idempotency_key else None,
                now,
            ),
        )
        conn.commit()
        row = conn.execute(
            f"""
            SELECT id, action_type, ts_code, stock_name, note, actor, snapshot_date, action_payload_json, created_at
            FROM {DECISION_ACTION_TABLE}
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        result = dict(row) if row else {}
        result["payload"] = _parse_json(result.get("action_payload_json"))
        result["trace"] = _action_trace_fields(
            result.get("payload"),
            action_row_id=result.get("id"),
            snapshot_date=str(result.get("snapshot_date") or ""),
        )
        result["receipt"] = _decision_receipt(
            result.get("payload"),
            trace=result["trace"],
            source=_receipt_source(result.get("payload"), "decision_board"),
        )
        result["status"] = result["receipt"]["status"]
        result["source"] = result["receipt"]["source"]
        result["context"] = result["receipt"]["context"]
        result.update(result["trace"])
        return result
    finally:
        conn.close()


def run_decision_snapshot(*, sqlite3_module, db_path: str, snapshot_date: str = "") -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        board = _board_payload(conn=conn, sqlite3_module=sqlite3_module, db_path=db_path, page=1, page_size=20)
        snapshot_date = str(snapshot_date or _utc_now()[:10]).strip()
        now = _utc_now()
        payload_json = json.dumps(board, ensure_ascii=False, default=str)
        existing = conn.execute(
            f"""
            SELECT id
            FROM {DECISION_SNAPSHOT_TABLE}
            WHERE snapshot_date = ? AND snapshot_type = 'daily'
            LIMIT 1
            """,
            (snapshot_date,),
        ).fetchone()
        snapshot_row_id = existing[0] if existing else None
        if existing:
            conn.execute(
                f"""
                UPDATE {DECISION_SNAPSHOT_TABLE}
                SET payload_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (payload_json, now, existing[0]),
            )
        else:
            cursor = conn.execute(
                f"""
                INSERT INTO {DECISION_SNAPSHOT_TABLE} (snapshot_date, snapshot_type, payload_json, created_at, updated_at)
                VALUES (?, 'daily', ?, ?, ?)
                """,
                (snapshot_date, payload_json, now, now),
            )
            snapshot_row_id = cursor.lastrowid
        conn.commit()
        trace = _snapshot_trace_fields(board, snapshot_row_id=snapshot_row_id, snapshot_date=snapshot_date)
        receipt = _decision_receipt(
            board,
            trace=trace,
            source=_receipt_source(board, "decision_snapshot"),
        )
        return {
            "ok": True,
            "snapshot_date": snapshot_date,
            "summary": board.get("summary") or {},
            "kill_switch": board.get("kill_switch") or {},
            "trace": trace,
            "receipt": receipt,
            "status": receipt["status"],
            "source": receipt["source"],
            "context": receipt["context"],
            **trace,
        }
    finally:
        conn.close()


def _ensure_funnel_tables_for_sync(conn) -> None:
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


def _sync_shortlist_to_funnel(conn, *, shortlist: list[dict[str, Any]], snapshot_date: str) -> dict[str, Any]:
    _ensure_funnel_tables_for_sync(conn)
    inserted = 0
    updated = 0
    now = _utc_now()
    for item in shortlist:
        ts_code = str(item.get("ts_code") or "").strip().upper()
        if not ts_code:
            continue
        name = str(item.get("name") or ts_code).strip()
        reason = str(item.get("decision_reason") or "").strip()
        evidence_ref = f"decision_snapshot:{snapshot_date}"
        exists = conn.execute(
            f"SELECT id, state_version FROM {FUNNEL_CANDIDATES_TABLE} WHERE ts_code = ? LIMIT 1",
            (ts_code,),
        ).fetchone()
        if exists:
            candidate_id = str(exists[0] or "")
            version = int(exists[1] or 1) + 1
            conn.execute(
                f"""
                UPDATE {FUNNEL_CANDIDATES_TABLE}
                SET name = ?, source = ?, trigger_source = ?, reason = ?, evidence_ref = ?,
                    state = ?, state_version = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    name,
                    "decision_daily_snapshot",
                    "decision_action",
                    reason,
                    evidence_ref,
                    "decision_ready",
                    version,
                    now,
                    candidate_id,
                ),
            )
            updated += 1
        else:
            candidate_id = str(uuid.uuid4())
            conn.execute(
                f"""
                INSERT INTO {FUNNEL_CANDIDATES_TABLE}
                    (id, ts_code, name, source, trigger_source, reason, evidence_ref, state, state_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'decision_ready', 1, ?, ?)
                """,
                (candidate_id, ts_code, name, "decision_daily_snapshot", "decision_action", reason, evidence_ref, now, now),
            )
            inserted += 1
        conn.execute(
            f"""
            INSERT INTO {FUNNEL_TRANSITIONS_TABLE}
                (id, candidate_id, from_state, to_state, reason, evidence_ref, trigger_source, operator, idempotency_key, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), candidate_id, "", "decision_ready", reason, evidence_ref, "decision_action", "scheduler", "", now),
        )
    conn.commit()
    return {
        "inserted": inserted,
        "updated": updated,
        "total_synced": inserted + updated,
    }


def sync_decision_action_to_funnel(
    *,
    sqlite3_module,
    db_path: str,
    action_type: str,
    ts_code: str,
    stock_name: str = "",
    note: str = "",
    actor: str = "decision_board",
    snapshot_date: str = "",
    action_id: str = "",
) -> dict[str, Any]:
    mapped_state = {
        "confirm": "confirmed",
        "reject": "rejected",
        "defer": "deferred",
        "watch": "decision_ready",
    }.get(str(action_type or "").strip().lower(), "")
    if not mapped_state:
        return {"ok": True, "skipped": True, "reason": "action_type not mapped"}

    normalized_ts = str(ts_code or "").strip().upper()
    if not normalized_ts:
        return {"ok": False, "error": "missing ts_code"}

    normalized_name = str(stock_name or normalized_ts).strip()
    normalized_note = str(note or "").strip()
    normalized_actor = str(actor or "decision_board").strip() or "decision_board"
    normalized_snapshot_date = str(snapshot_date or _today_cn()).strip() or _today_cn()
    evidence_ref = f"decision_action:{normalized_snapshot_date}:{action_id or normalized_ts}"
    now = _utc_now()

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_funnel_tables_for_sync(conn)
        row = conn.execute(
            f"""
            SELECT id, state, state_version
            FROM {FUNNEL_CANDIDATES_TABLE}
            WHERE ts_code = ?
            LIMIT 1
            """,
            (normalized_ts,),
        ).fetchone()

        created = False
        transition_count = 0
        if row:
            candidate_id = str(row["id"] or "")
            current_state = str(row["state"] or "").strip() or "decision_ready"
            state_version = int(row["state_version"] or 1)
            # Always refresh metadata from latest decision action.
            conn.execute(
                f"""
                UPDATE {FUNNEL_CANDIDATES_TABLE}
                SET name = ?, source = ?, trigger_source = ?, reason = ?, evidence_ref = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    normalized_name,
                    "decision_action",
                    "decision_action",
                    normalized_note,
                    evidence_ref,
                    now,
                    candidate_id,
                ),
            )
        else:
            candidate_id = str(uuid.uuid4())
            current_state = "decision_ready"
            state_version = 1
            created = True
            conn.execute(
                f"""
                INSERT INTO {FUNNEL_CANDIDATES_TABLE}
                    (id, ts_code, name, source, trigger_source, reason, evidence_ref, state, state_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'decision_ready', 1, ?, ?)
                """,
                (
                    candidate_id,
                    normalized_ts,
                    normalized_name,
                    "decision_action",
                    "decision_action",
                    normalized_note,
                    evidence_ref,
                    now,
                    now,
                ),
            )
            conn.execute(
                f"""
                INSERT INTO {FUNNEL_TRANSITIONS_TABLE}
                    (id, candidate_id, from_state, to_state, reason, evidence_ref, trigger_source, operator, idempotency_key, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    candidate_id,
                    "",
                    "decision_ready",
                    normalized_note,
                    evidence_ref,
                    "decision_action",
                    normalized_actor,
                    str(action_id or "").strip(),
                    now,
                ),
            )
            transition_count += 1

        if mapped_state != current_state:
            next_version = state_version + 1
            conn.execute(
                f"""
                UPDATE {FUNNEL_CANDIDATES_TABLE}
                SET state = ?, state_version = ?, updated_at = ?
                WHERE id = ?
                """,
                (mapped_state, next_version, now, candidate_id),
            )
            conn.execute(
                f"""
                INSERT INTO {FUNNEL_TRANSITIONS_TABLE}
                    (id, candidate_id, from_state, to_state, reason, evidence_ref, trigger_source, operator, idempotency_key, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    candidate_id,
                    current_state,
                    mapped_state,
                    normalized_note,
                    evidence_ref,
                    "decision_action",
                    normalized_actor,
                    str(action_id or "").strip(),
                    now,
                ),
            )
            transition_count += 1
            current_state = mapped_state

        conn.commit()
        return {
            "ok": True,
            "candidate_id": candidate_id,
            "state": current_state,
            "created": created,
            "transition_count": transition_count,
        }
    finally:
        conn.close()


def run_decision_scheduled_job(*, sqlite3_module, db_path: str, job_key: str) -> dict[str, Any]:
    snapshot_date = _today_cn()
    result = run_decision_snapshot(sqlite3_module=sqlite3_module, db_path=db_path, snapshot_date=snapshot_date)
    if result.get("ok"):
        board = query_decision_board(sqlite3_module=sqlite3_module, db_path=db_path, page=1, page_size=20)
        conn = sqlite3_module.connect(db_path)
        conn.row_factory = sqlite3_module.Row
        try:
            sync_summary = _sync_shortlist_to_funnel(
                conn,
                shortlist=list(board.get("shortlist") or []),
                snapshot_date=str(board.get("snapshot_date") or snapshot_date),
            )
        finally:
            conn.close()
        result["pipeline_sync"] = {
            "scores_stage": "ready" if board.get("snapshot_date") else "empty",
            "decision_stage": "ready",
            "funnel_stage": "ready" if int(sync_summary.get("total_synced") or 0) > 0 else "degraded",
            **sync_summary,
        }
    result["job_key"] = job_key
    return result


def build_decision_runtime_deps(*, sqlite3_module, db_path: str) -> dict[str, Any]:
    return {
        "query_decision_board": lambda **kwargs: query_decision_board(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_decision_scoreboard": lambda **kwargs: query_decision_scoreboard(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_decision_stock": lambda **kwargs: query_decision_stock(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_decision_trade_plan": lambda **kwargs: query_decision_trade_plan(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_decision_strategy_lab": lambda **kwargs: query_decision_strategy_lab(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_decision_strategy_runs": lambda **kwargs: query_decision_strategy_runs(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "run_decision_strategy_generation": lambda **kwargs: run_decision_strategy_generation(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_decision_history": lambda **kwargs: query_decision_history(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_decision_actions": lambda **kwargs: query_decision_actions(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_decision_calibration": lambda **kwargs: query_decision_calibration(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "get_decision_kill_switch": lambda: get_decision_kill_switch(sqlite3_module=sqlite3_module, db_path=db_path),
        "set_decision_kill_switch": lambda **kwargs: set_decision_kill_switch(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "record_decision_action": lambda **kwargs: record_decision_action(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "run_decision_snapshot": lambda **kwargs: run_decision_snapshot(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "run_decision_scheduled_job": lambda job_key: run_decision_scheduled_job(sqlite3_module=sqlite3_module, db_path=db_path, job_key=job_key),
        "sync_decision_action_to_funnel": lambda **kwargs: sync_decision_action_to_funnel(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
    }
