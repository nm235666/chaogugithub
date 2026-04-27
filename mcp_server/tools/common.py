from __future__ import annotations

import json
import re
from typing import Any

import db_compat as db
from mcp_server import config


SENSITIVE_TABLE_PREFIXES = ("app_auth_",)
SENSITIVE_COLUMNS = {"password_hash", "token", "reset_code", "session_token", "secret"}

KEY_TABLES = [
    "stock_codes",
    "stock_daily_prices",
    "stock_minline",
    "stock_scores_daily",
    "news_feed_items",
    "stock_news_items",
    "investment_signal_events",
    "theme_hotspot_tracker",
    "wechat_chatlog_clean_items",
    "chatroom_stock_candidate_pool",
    "decision_snapshots",
    "decision_actions",
    "funnel_candidates",
    "funnel_transitions",
    "portfolio_orders",
    "portfolio_positions",
    "portfolio_reviews",
    "agent_runs",
    "agent_steps",
    "agent_messages",
    "agent_memory_items",
    "agent_quality_scores",
    "agent_governance_rules",
    "agent_policy_decisions",
    "research_reports",
    "multi_role_v3_jobs",
    "quantaalpha_runs",
    "job_runs",
    "job_alerts",
]

READONLY_ALLOWED_TABLES = set(KEY_TABLES) | {
    "job_definitions",
    "funnel_review_snapshots",
    "quantaalpha_factor_results",
    "quantaalpha_backtest_results",
    "chief_roundtable_jobs",
}

WRITE_JOB_ALLOWLIST = {
    "decision_daily_snapshot",
    "funnel_ingested_score_align",
    "funnel_review_refresh",
    "chatroom_signal_accuracy_refresh",
    "collect_daily_metrics",
    "agent.funnel_progress.daily",
    "agent.portfolio_reconcile.daily",
    "agent.portfolio_review.daily",
    "agent.decision_orchestrator.daily",
    "agent.memory_refresh.daily",
    "agent.governance_quality_refresh.daily",
    "multi_role_v3_worker_guard",
    "chief_roundtable_worker_guard",
    "quantaalpha_worker_guard",
}


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


def json_safe(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False, default=str)
        return value
    except Exception:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def clamp_limit(value: int, *, default: int = 50, maximum: int = 500) -> int:
    try:
        n = int(value)
    except Exception:
        n = default
    return max(1, min(n, maximum))


def table_safe(name: str) -> str:
    text = str(name or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text):
        raise ValueError(f"invalid_table_name:{text}")
    if text.startswith(SENSITIVE_TABLE_PREFIXES):
        raise ValueError(f"sensitive_table_blocked:{text}")
    return text


def require_write_allowed(payload) -> None:
    if bool(payload.dry_run):
        return
    if not config.MCP_WRITE_ENABLED:
        raise ValueError("mcp_write_disabled")
    if not payload.confirm:
        raise ValueError("confirm_required_for_write")
    if not str(payload.actor or "").strip():
        raise ValueError("actor_required_for_write")
    if not str(payload.reason or "").strip():
        raise ValueError("reason_required_for_write")
    if not str(payload.idempotency_key or "").strip():
        raise ValueError("idempotency_key_required_for_write")


def db_counts(tables: list[str]) -> dict[str, Any]:
    conn = db.connect()
    db.apply_row_factory(conn)
    try:
        out: dict[str, Any] = {}
        for raw in tables:
            table = table_safe(raw)
            if not db.table_exists(conn, table):
                out[table] = {"exists": False, "count": None}
                continue
            row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table}").fetchone()
            out[table] = {"exists": True, "count": int(row_to_dict(row).get("cnt", row[0] if row else 0) or 0)}
        return out
    finally:
        conn.close()
