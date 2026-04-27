from __future__ import annotations

import json
import hashlib
import sqlite3 as _sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

import db_compat as _db

PORTFOLIO_ORDERS_TABLE = "portfolio_orders"
PORTFOLIO_POSITIONS_TABLE = "portfolio_positions"
PORTFOLIO_REVIEWS_TABLE = "portfolio_reviews"
DECISION_STRATEGY_PERFORMANCE_TABLE = "decision_strategy_performance"

VALID_ORDER_STATUSES = {"planned", "executed", "cancelled", "partial"}
VALID_ACTION_TYPES = {"buy", "sell", "add", "reduce", "close", "watch", "defer"}
EXECUTABLE_ACTION_TYPES = {"buy", "sell", "add", "reduce", "close"}
NON_EXECUTABLE_ACTION_TYPES = {"watch", "defer"}

ORDER_TO_EXECUTION_STATUS: dict[str, str] = {
    "planned": "planned",
    "partial": "executing",
    "executed": "done",
    "cancelled": "cancelled",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def _parse_json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    try:
        parsed = json.loads(str(raw or "{}"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _strategy_context_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    strategy = payload.get("strategy") if isinstance(payload.get("strategy"), dict) else {}
    if not strategy:
        today_action = payload.get("today_action") if isinstance(payload.get("today_action"), dict) else {}
        evidence = today_action.get("evidence") if isinstance(today_action.get("evidence"), dict) else {}
        if evidence.get("strategy_key"):
            strategy = {
                "strategy_key": evidence.get("strategy_key"),
                "strategy_run_id": evidence.get("strategy_run_id"),
                "strategy_run_key": evidence.get("strategy_run_key"),
                "strategy_candidate_rank": evidence.get("strategy_candidate_rank"),
                "strategy_fit_score": evidence.get("strategy_fit_score"),
                "strategy_action_bias": evidence.get("strategy_action_bias"),
                "strategy_source": evidence.get("strategy_source") or "strategy_selection",
            }
    strategy_key = str(strategy.get("strategy_key") or "").strip()
    if not strategy_key:
        return {}
    return {
        "strategy_key": strategy_key,
        "strategy_run_id": strategy.get("strategy_run_id") or strategy.get("run_id") or "",
        "strategy_run_key": strategy.get("strategy_run_key") or strategy.get("run_key") or "",
        "strategy_candidate_rank": strategy.get("strategy_candidate_rank") or strategy.get("rank") or "",
        "strategy_fit_score": strategy.get("strategy_fit_score") or strategy.get("fit_score") or 0,
        "strategy_action_bias": strategy.get("strategy_action_bias") or strategy.get("action_bias") or "",
        "strategy_source": strategy.get("strategy_source") or "strategy_selection",
        "summary": strategy.get("summary") or "",
    }


def _load_decision_payloads(conn, decision_action_ids: list[str]) -> dict[str, dict[str, Any]]:
    ids = [str(value or "").strip() for value in decision_action_ids if str(value or "").strip()]
    if not ids or not _db.table_exists(conn, "decision_actions"):
        return {}
    placeholders = ",".join("?" for _ in ids)
    try:
        rows = conn.execute(
            f"""
            SELECT CAST(id AS TEXT) AS id, action_payload_json
            FROM decision_actions
            WHERE CAST(id AS TEXT) IN ({placeholders})
            """,
            tuple(ids),
        ).fetchall()
    except Exception:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = _row_to_dict(row)
        payload = _parse_json_dict(item.get("action_payload_json"))
        out[str(item.get("id") or "")] = payload
    return out


def _load_decision_actions(conn, decision_action_ids: list[str]) -> dict[str, dict[str, Any]]:
    ids = [str(value or "").strip() for value in decision_action_ids if str(value or "").strip()]
    if not ids or not _db.table_exists(conn, "decision_actions"):
        return {}
    placeholders = ",".join("?" for _ in ids)
    try:
        rows = conn.execute(
            f"""
            SELECT CAST(id AS TEXT) AS id, ts_code, action_type, snapshot_date, action_payload_json, created_at
            FROM decision_actions
            WHERE CAST(id AS TEXT) IN ({placeholders})
            """,
            tuple(ids),
        ).fetchall()
    except Exception:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = _row_to_dict(row)
        payload = _parse_json_dict(item.get("action_payload_json"))
        item["decision_payload"] = payload
        item["strategy_context"] = _strategy_context_from_payload(payload)
        out[str(item.get("id") or "")] = item
    return out


def _apply_row_factory(conn) -> None:
    if _db.using_postgres():
        _db.apply_row_factory(conn)
        return
    try:
        conn.row_factory = _sqlite3.Row
    except Exception:
        pass


def _is_executable_action(action_type: str) -> bool:
    return str(action_type or "").strip().lower() in EXECUTABLE_ACTION_TYPES


def _owner_hash(owner_key: str = "") -> str:
    normalized = str(owner_key or "system").strip().lower() or "system"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]


def _display_order_no(value: str = "") -> str:
    raw = str(value or "").strip()
    text = "".join(ch for ch in raw if ch.isdigit())
    if len(text) >= 8:
        return text[-8:]
    if raw:
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        return f"{int(digest[:12], 16) % 100000000:08d}"
    return f"{uuid.uuid4().int % 100000000:08d}"


def _long_order_id(owner_hash: str, order_no: str) -> str:
    return f"{owner_hash}{order_no}{uuid.uuid4().hex[:8]}"


def _commit(conn) -> None:
    try:
        conn.commit()
    except Exception:
        pass


def _table_columns(conn, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(_row_to_dict(row).get("name") or row[1]) for row in rows}
    except Exception:
        return set()


def _ensure_column(conn, table_name: str, column_name: str, definition: str) -> None:
    if column_name in _table_columns(conn, table_name):
        return
    try:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
    except Exception:
        pass


def _ensure_portfolio_tables(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PORTFOLIO_ORDERS_TABLE} (
            id TEXT PRIMARY KEY,
            ts_code TEXT NOT NULL,
            action_type TEXT NOT NULL DEFAULT 'buy',
            planned_price REAL,
            executed_price REAL,
            size INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'planned',
            order_no TEXT NOT NULL DEFAULT '',
            chain_order_id TEXT NOT NULL DEFAULT '',
            owner_hash TEXT NOT NULL DEFAULT '',
            decision_action_id TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            executed_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PORTFOLIO_POSITIONS_TABLE} (
            id TEXT PRIMARY KEY,
            ts_code TEXT NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            quantity INTEGER NOT NULL DEFAULT 0,
            avg_cost REAL NOT NULL DEFAULT 0.0,
            last_price REAL,
            market_value REAL,
            unrealized_pnl REAL,
            order_no TEXT NOT NULL DEFAULT '',
            chain_order_id TEXT NOT NULL DEFAULT '',
            owner_hash TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            UNIQUE(ts_code)
        )
        """
    )
    _ensure_column(conn, PORTFOLIO_ORDERS_TABLE, "order_no", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, PORTFOLIO_ORDERS_TABLE, "chain_order_id", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, PORTFOLIO_ORDERS_TABLE, "owner_hash", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, PORTFOLIO_POSITIONS_TABLE, "order_no", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, PORTFOLIO_POSITIONS_TABLE, "chain_order_id", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, PORTFOLIO_POSITIONS_TABLE, "owner_hash", "TEXT NOT NULL DEFAULT ''")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PORTFOLIO_REVIEWS_TABLE} (
            id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            review_tag TEXT NOT NULL DEFAULT '',
            review_note TEXT NOT NULL DEFAULT '',
            slippage REAL,
            latency_ms INTEGER,
            created_at TEXT NOT NULL
        )
        """
    )


def list_positions() -> dict[str, Any]:
    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            if not _db.table_exists(conn, PORTFOLIO_POSITIONS_TABLE):
                return {"items": [], "total": 0}
            _ensure_portfolio_tables(conn)
            rows = conn.execute(
                f"""
                SELECT id, ts_code, name, quantity, avg_cost, last_price,
                       market_value, unrealized_pnl, order_no, chain_order_id,
                       owner_hash, updated_at
                FROM {PORTFOLIO_POSITIONS_TABLE}
                WHERE quantity > 0
                ORDER BY market_value DESC NULLS LAST
                """
            ).fetchall()
            items = [_row_to_dict(r) for r in rows]
            return {"items": items, "total": len(items)}
        finally:
            conn.close()
    except Exception as exc:
        return {"items": [], "total": 0, "error": str(exc)}


def list_orders(
    *,
    status: str = "",
    decision_action_id: str = "",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            if not _db.table_exists(conn, PORTFOLIO_ORDERS_TABLE):
                return {"items": [], "total": 0, "limit": limit, "offset": offset}
            _ensure_portfolio_tables(conn)
            conditions: list[str] = []
            params: list[Any] = []
            if status and status in VALID_ORDER_STATUSES:
                conditions.append("status = ?")
                params.append(status)
            if decision_action_id:
                conditions.append("decision_action_id = ?")
                params.append(decision_action_id)
            where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
            count_row = conn.execute(
                f"SELECT COUNT(*) FROM {PORTFOLIO_ORDERS_TABLE} {where_clause}", params
            ).fetchone()
            total = int(count_row[0] or 0) if count_row else 0
            rows = conn.execute(
                f"""
                SELECT id, ts_code, action_type, planned_price, executed_price,
                       size, status, order_no, chain_order_id, owner_hash,
                       decision_action_id, note, executed_at,
                       created_at, updated_at
                FROM {PORTFOLIO_ORDERS_TABLE}
                {where_clause}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                params + [limit, offset],
            ).fetchall()
            items = [_row_to_dict(r) for r in rows]
            for item in items:
                if not item.get("order_no"):
                    item["order_no"] = _display_order_no(str(item.get("id") or ""))
            return {"items": items, "total": total, "limit": limit, "offset": offset}
        finally:
            conn.close()
    except Exception as exc:
        return {"items": [], "total": 0, "limit": limit, "offset": offset, "error": str(exc)}


def audit_strategy_attribution(*, apply: bool = False, limit: int = 500) -> dict[str, Any]:
    """Audit and optionally repair strategy attribution links for historical orders.

    Repairs are intentionally conservative: only orders in a chain with exactly one
    known strategy-bearing decision action are updated automatically.
    """
    capped_limit = max(1, min(int(limit or 500), 2000))
    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            if not _db.table_exists(conn, PORTFOLIO_ORDERS_TABLE):
                return {"ok": False, "error": "订单表不存在"}
            _ensure_portfolio_tables(conn)
            rows = conn.execute(
                f"""
                SELECT id, ts_code, action_type, status, order_no, chain_order_id,
                       decision_action_id, note, created_at, updated_at
                FROM {PORTFOLIO_ORDERS_TABLE}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (capped_limit,),
            ).fetchall()
            orders = [_row_to_dict(row) for row in rows]
            decision_actions = _load_decision_actions(
                conn,
                [str(order.get("decision_action_id") or "") for order in orders],
            )

            chains: dict[str, list[dict[str, Any]]] = {}
            for order in orders:
                order_no = str(order.get("order_no") or "").strip()
                if not order_no:
                    order_no = _display_order_no(str(order.get("id") or ""))
                    order["order_no"] = order_no
                action_id = str(order.get("decision_action_id") or "").strip()
                decision_action = decision_actions.get(action_id) if action_id else None
                order["decision_action_exists"] = bool(decision_action)
                order["strategy_context"] = (decision_action or {}).get("strategy_context") or {}
                chains.setdefault(order_no, []).append(order)

            issues: list[dict[str, Any]] = []
            repairs: list[dict[str, Any]] = []
            repaired_order_ids: set[str] = set()
            repaired_decision_action_ids: set[str] = set()
            now = _utc_now()

            for order_no, chain_orders in chains.items():
                strategy_orders = [order for order in chain_orders if order.get("strategy_context")]
                strategy_keys = {
                    str((order.get("strategy_context") or {}).get("strategy_key") or "").strip()
                    for order in strategy_orders
                    if str((order.get("strategy_context") or {}).get("strategy_key") or "").strip()
                }
                canonical = strategy_orders[0] if len(strategy_keys) == 1 and strategy_orders else None
                canonical_action_id = str((canonical or {}).get("decision_action_id") or "").strip()
                canonical_strategy = (canonical or {}).get("strategy_context") or {}
                if len(strategy_keys) > 1:
                    issues.append(
                        {
                            "type": "chain_inconsistent_strategy",
                            "severity": "warning",
                            "repairable": False,
                            "order_no": order_no,
                            "strategy_keys": sorted(strategy_keys),
                            "message": "同一交易链存在多个来源策略，需要人工确认主策略。",
                        }
                    )

                for order in chain_orders:
                    order_id = str(order.get("id") or "").strip()
                    action_id = str(order.get("decision_action_id") or "").strip()
                    decision_action = decision_actions.get(action_id) if action_id else None
                    strategy_context = order.get("strategy_context") or {}
                    base_issue = {
                        "order_id": order_id,
                        "order_no": order_no,
                        "ts_code": order.get("ts_code") or "",
                        "action_type": order.get("action_type") or "",
                        "decision_action_id": action_id,
                    }
                    if not action_id:
                        repairable = bool(canonical_action_id)
                        issues.append(
                            {
                                **base_issue,
                                "type": "missing_decision_action_id",
                                "severity": "error",
                                "repairable": repairable,
                                "suggested_decision_action_id": canonical_action_id,
                                "message": "订单缺少 decision_action_id，无法回溯到今日动作与策略来源。",
                            }
                        )
                        if apply and repairable:
                            conn.execute(
                                f"""
                                UPDATE {PORTFOLIO_ORDERS_TABLE}
                                SET decision_action_id = ?, updated_at = ?
                                WHERE id = ?
                                """,
                                (canonical_action_id, now, order_id),
                            )
                            repaired_order_ids.add(order_id)
                            repairs.append(
                                {
                                    "type": "fill_order_decision_action_id",
                                    "order_id": order_id,
                                    "order_no": order_no,
                                    "decision_action_id": canonical_action_id,
                                    "strategy_context": canonical_strategy,
                                }
                            )
                        continue

                    if not decision_action:
                        repairable = bool(canonical_action_id and canonical_action_id != action_id)
                        issues.append(
                            {
                                **base_issue,
                                "type": "orphan_decision_action_id",
                                "severity": "error",
                                "repairable": repairable,
                                "suggested_decision_action_id": canonical_action_id,
                                "message": "订单指向的 decision_action_id 不存在。",
                            }
                        )
                        if apply and repairable:
                            conn.execute(
                                f"""
                                UPDATE {PORTFOLIO_ORDERS_TABLE}
                                SET decision_action_id = ?, updated_at = ?
                                WHERE id = ?
                                """,
                                (canonical_action_id, now, order_id),
                            )
                            repaired_order_ids.add(order_id)
                            repairs.append(
                                {
                                    "type": "replace_orphan_decision_action_id",
                                    "order_id": order_id,
                                    "order_no": order_no,
                                    "old_decision_action_id": action_id,
                                    "decision_action_id": canonical_action_id,
                                    "strategy_context": canonical_strategy,
                                }
                            )
                        continue

                    if not strategy_context:
                        repairable = bool(canonical_strategy and action_id != canonical_action_id)
                        issues.append(
                            {
                                **base_issue,
                                "type": "missing_strategy_context",
                                "severity": "warning",
                                "repairable": repairable,
                                "suggested_strategy_context": canonical_strategy,
                                "message": "决策动作存在，但 action_payload_json 缺少 strategy 归因。",
                            }
                        )
                        if apply and repairable:
                            payload = _parse_json_dict(decision_action.get("action_payload_json"))
                            payload["strategy"] = canonical_strategy
                            payload["strategy_repair"] = {
                                "source": "portfolio_strategy_attribution_audit",
                                "repaired_from_order_no": order_no,
                                "repaired_from_decision_action_id": canonical_action_id,
                                "repaired_at": now,
                            }
                            conn.execute(
                                "UPDATE decision_actions SET action_payload_json = ? WHERE CAST(id AS TEXT) = ?",
                                (json.dumps(payload, ensure_ascii=False, default=str), action_id),
                            )
                            repaired_decision_action_ids.add(action_id)
                            repairs.append(
                                {
                                    "type": "fill_decision_action_strategy",
                                    "order_id": order_id,
                                    "order_no": order_no,
                                    "decision_action_id": action_id,
                                    "strategy_context": canonical_strategy,
                                }
                            )

            if apply and repairs:
                _commit(conn)

            summary = {
                "checked_orders": len(orders),
                "checked_chains": len(chains),
                "issue_count": len(issues),
                "repairable_count": sum(1 for issue in issues if issue.get("repairable")),
                "repaired_order_count": len(repaired_order_ids),
                "repaired_decision_action_count": len(repaired_decision_action_ids),
                "mode": "apply" if apply else "dry_run",
            }
            return {
                "ok": True,
                "summary": summary,
                "issues": issues,
                "repairs": repairs,
                "limit": capped_limit,
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc), "summary": {"mode": "apply" if apply else "dry_run"}}


def _chain_review_tag(conn, events: list[dict[str, Any]]) -> str:
    if not events or not _db.table_exists(conn, PORTFOLIO_REVIEWS_TABLE):
        return "pending"
    order_ids = [str(event.get("id") or "").strip() for event in events if str(event.get("id") or "").strip()]
    if not order_ids:
        return "pending"
    placeholders = ",".join("?" for _ in order_ids)
    try:
        rows = conn.execute(
            f"""
            SELECT review_tag, created_at
            FROM {PORTFOLIO_REVIEWS_TABLE}
            WHERE order_id IN ({placeholders})
            ORDER BY created_at DESC
            """,
            tuple(order_ids),
        ).fetchall()
    except Exception:
        return "pending"
    tags = [str(_row_to_dict(row).get("review_tag") or "").strip() for row in rows]
    return next((tag for tag in tags if tag and tag != "pending"), "pending")


def refresh_strategy_performance(*, limit: int = 2000) -> dict[str, Any]:
    capped_limit = max(1, min(int(limit or 2000), 5000))
    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            if not _db.table_exists(conn, PORTFOLIO_ORDERS_TABLE):
                return {"ok": False, "error": "订单表不存在"}
            _ensure_portfolio_tables(conn)
            _ensure_strategy_performance_table(conn)
            rows = conn.execute(
                f"""
                SELECT id, ts_code, action_type, planned_price, executed_price,
                       size, status, order_no, chain_order_id, owner_hash,
                       decision_action_id, note, executed_at, created_at, updated_at
                FROM {PORTFOLIO_ORDERS_TABLE}
                WHERE status = 'executed'
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (capped_limit,),
            ).fetchall()
            events = [_row_to_dict(row) for row in rows]
            decision_actions = _load_decision_actions(
                conn,
                [str(event.get("decision_action_id") or "") for event in events],
            )
            chains: dict[str, list[dict[str, Any]]] = {}
            skipped_without_strategy = 0
            for event in events:
                decision_action = decision_actions.get(str(event.get("decision_action_id") or "").strip()) or {}
                event["strategy_context"] = decision_action.get("strategy_context") or {}
                order_no = str(event.get("order_no") or "").strip()
                if not order_no:
                    order_no = _display_order_no(str(event.get("id") or ""))
                    event["order_no"] = order_no
                chains.setdefault(order_no, []).append(event)

            aggregates: dict[str, dict[str, Any]] = {}
            chain_items: list[dict[str, Any]] = []
            for order_no, chain_events in chains.items():
                strategy_context = next((event.get("strategy_context") for event in chain_events if event.get("strategy_context")), {})
                strategy_key = str((strategy_context or {}).get("strategy_key") or "").strip()
                if not strategy_key:
                    skipped_without_strategy += 1
                    continue
                metrics = _trade_chain_metrics(chain_events)
                chain_status = _chain_status(chain_events)
                review_tag = _chain_review_tag(conn, chain_events)
                realized_pnl = float(metrics.get("realized_pnl") or 0.0)
                return_pct = metrics.get("return_pct")
                latest_trade_at = str(chain_events[-1].get("executed_at") or chain_events[-1].get("created_at") or "")
                fit_score = strategy_context.get("strategy_fit_score")
                try:
                    fit_score_value = float(fit_score)
                except (TypeError, ValueError):
                    fit_score_value = None

                aggregate = aggregates.setdefault(
                    strategy_key,
                    {
                        "strategy_key": strategy_key,
                        "strategy_source": strategy_context.get("strategy_source") or "strategy_selection",
                        "trade_count": 0,
                        "closed_trade_count": 0,
                        "win_count": 0,
                        "loss_count": 0,
                        "neutral_count": 0,
                        "pending_count": 0,
                        "total_realized_pnl": 0.0,
                        "return_values": [],
                        "fit_values": [],
                        "latest_trade_at": "",
                        "chains": [],
                    },
                )
                aggregate["trade_count"] += 1
                if chain_status == "closed":
                    aggregate["closed_trade_count"] += 1
                if review_tag == "win":
                    aggregate["win_count"] += 1
                elif review_tag == "loss":
                    aggregate["loss_count"] += 1
                elif review_tag == "neutral":
                    aggregate["neutral_count"] += 1
                else:
                    aggregate["pending_count"] += 1
                aggregate["total_realized_pnl"] += realized_pnl
                if return_pct is not None:
                    aggregate["return_values"].append(float(return_pct))
                if fit_score_value is not None:
                    aggregate["fit_values"].append(fit_score_value)
                if latest_trade_at > str(aggregate.get("latest_trade_at") or ""):
                    aggregate["latest_trade_at"] = latest_trade_at
                chain_snapshot = {
                    "order_no": order_no,
                    "ts_code": chain_events[0].get("ts_code") or "",
                    "action_summary": _chain_summary(chain_events),
                    "chain_status": chain_status,
                    "review_tag": review_tag,
                    "realized_pnl": realized_pnl,
                    "return_pct": return_pct,
                    "latest_trade_at": latest_trade_at,
                }
                aggregate["chains"].append(chain_snapshot)
                chain_items.append({"strategy_key": strategy_key, **chain_snapshot})

            now = _utc_now()
            conn.execute(f"DELETE FROM {DECISION_STRATEGY_PERFORMANCE_TABLE}")
            for aggregate in aggregates.values():
                returns = aggregate.pop("return_values")
                fits = aggregate.pop("fit_values")
                chains_for_json = aggregate.pop("chains")
                avg_return_pct = (sum(returns) / len(returns)) if returns else None
                avg_fit_score = (sum(fits) / len(fits)) if fits else None
                win_rate = (
                    aggregate["win_count"] / max(1, aggregate["win_count"] + aggregate["loss_count"] + aggregate["neutral_count"])
                )
                performance_json = {
                    "win_rate": win_rate,
                    "chains": chains_for_json[:20],
                    "calculation": "portfolio_order_chain_realized_pnl_and_review_tag",
                }
                conn.execute(
                    f"""
                    INSERT INTO {DECISION_STRATEGY_PERFORMANCE_TABLE}
                        (strategy_key, strategy_source, trade_count, closed_trade_count,
                         win_count, loss_count, neutral_count, pending_count,
                         total_realized_pnl, avg_return_pct, avg_fit_score,
                         latest_trade_at, performance_json, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        aggregate["strategy_key"],
                        aggregate["strategy_source"],
                        aggregate["trade_count"],
                        aggregate["closed_trade_count"],
                        aggregate["win_count"],
                        aggregate["loss_count"],
                        aggregate["neutral_count"],
                        aggregate["pending_count"],
                        aggregate["total_realized_pnl"],
                        avg_return_pct,
                        avg_fit_score,
                        aggregate["latest_trade_at"],
                        json.dumps(performance_json, ensure_ascii=False, default=str),
                        now,
                    ),
                )
                aggregate["avg_return_pct"] = avg_return_pct
                aggregate["avg_fit_score"] = avg_fit_score
                aggregate["win_rate"] = win_rate
                aggregate["performance_json"] = performance_json
                aggregate["updated_at"] = now
            _commit(conn)
            items = sorted(
                aggregates.values(),
                key=lambda item: (float(item.get("total_realized_pnl") or 0), int(item.get("trade_count") or 0)),
                reverse=True,
            )
            return {
                "ok": True,
                "summary": {
                    "strategy_count": len(items),
                    "trade_count": sum(int(item.get("trade_count") or 0) for item in items),
                    "chain_count": len(chains),
                    "skipped_without_strategy": skipped_without_strategy,
                    "updated_at": now,
                },
                "items": items,
                "chains": chain_items,
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def query_strategy_performance(*, limit: int = 50, auto_refresh: bool = False) -> dict[str, Any]:
    capped_limit = max(1, min(int(limit or 50), 200))
    if auto_refresh:
        refreshed = refresh_strategy_performance(limit=2000)
        if not refreshed.get("ok"):
            return refreshed
    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            _ensure_strategy_performance_table(conn)
            rows = conn.execute(
                f"""
                SELECT strategy_key, strategy_source, trade_count, closed_trade_count,
                       win_count, loss_count, neutral_count, pending_count,
                       total_realized_pnl, avg_return_pct, avg_fit_score,
                       latest_trade_at, performance_json, updated_at
                FROM {DECISION_STRATEGY_PERFORMANCE_TABLE}
                ORDER BY total_realized_pnl DESC, trade_count DESC, strategy_key ASC
                LIMIT ?
                """,
                (capped_limit,),
            ).fetchall()
            items: list[dict[str, Any]] = []
            for row in rows:
                item = _row_to_dict(row)
                performance_json = _parse_json_dict(item.get("performance_json"))
                item["performance"] = performance_json
                item["win_rate"] = performance_json.get("win_rate")
                items.append(item)
            return {
                "ok": True,
                "summary": {
                    "strategy_count": len(items),
                    "trade_count": sum(int(item.get("trade_count") or 0) for item in items),
                    "updated_at": items[0].get("updated_at") if items else "",
                },
                "items": items,
                "limit": capped_limit,
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc), "items": [], "summary": {"strategy_count": 0}}


def _chain_status(events: list[dict[str, Any]]) -> str:
    net_qty = 0
    for event in events:
        action_type = str(event.get("action_type") or "").strip().lower()
        size = int(event.get("size") or 0)
        if action_type in {"buy", "add"}:
            net_qty += size
        elif action_type in {"sell", "reduce", "close"}:
            net_qty -= size if action_type != "close" else net_qty
    return "closed" if net_qty <= 0 and any(str(e.get("action_type") or "") == "close" for e in events) else "open"


def _chain_summary(events: list[dict[str, Any]]) -> str:
    labels = {
        "buy": "新买",
        "add": "加仓",
        "sell": "卖出",
        "reduce": "减仓",
        "close": "清仓",
        "watch": "观察",
        "defer": "暂缓",
    }
    return " -> ".join(labels.get(str(e.get("action_type") or ""), str(e.get("action_type") or "")) for e in events)


def list_review_chains(*, limit: int = 50, offset: int = 0) -> dict[str, Any]:
    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            if not _db.table_exists(conn, PORTFOLIO_ORDERS_TABLE):
                return {"items": [], "total": 0, "limit": limit, "offset": offset}
            _ensure_portfolio_tables(conn)
            rows = conn.execute(
                f"""
                SELECT id, ts_code, action_type, planned_price, executed_price,
                       size, status, order_no, chain_order_id, owner_hash,
                       decision_action_id, note, executed_at, created_at, updated_at
                FROM {PORTFOLIO_ORDERS_TABLE}
                WHERE status = 'executed'
                ORDER BY created_at ASC
                """
            ).fetchall()
            raw_events = [_row_to_dict(row) for row in rows]
            decision_payloads = _load_decision_payloads(
                conn,
                [str(event.get("decision_action_id") or "") for event in raw_events],
            )
            groups: dict[str, dict[str, Any]] = {}
            for event in raw_events:
                decision_payload = decision_payloads.get(str(event.get("decision_action_id") or "").strip()) or {}
                event["decision_payload"] = decision_payload
                event["strategy_context"] = _strategy_context_from_payload(decision_payload)
                order_no = str(event.get("order_no") or "").strip()
                if not order_no:
                    order_no = _display_order_no(str(event.get("id") or ""))
                    event["order_no"] = order_no
                chain = groups.setdefault(
                    order_no,
                    {
                        "id": order_no,
                        "order_no": order_no,
                        "chain_order_id": event.get("chain_order_id") or "",
                        "owner_hash": event.get("owner_hash") or "",
                        "ts_code": event.get("ts_code") or "",
                        "events": [],
                    },
                )
                if not chain.get("chain_order_id") and event.get("chain_order_id"):
                    chain["chain_order_id"] = event.get("chain_order_id")
                chain["events"].append(event)

            items: list[dict[str, Any]] = []
            for chain in groups.values():
                events = chain["events"]
                first = events[0]
                last = events[-1]
                entry = next((e for e in events if str(e.get("action_type") or "") in {"buy", "add"}), first)
                exit_event = next((e for e in reversed(events) if str(e.get("action_type") or "") in {"sell", "reduce", "close"}), None)
                strategy_context = next((e.get("strategy_context") for e in events if e.get("strategy_context")), {})
                total_buy_size = sum(
                    int(e.get("size") or 0) for e in events if str(e.get("action_type") or "") in {"buy", "add"}
                )
                entry_price = entry.get("executed_price") if entry.get("executed_price") is not None else entry.get("planned_price")
                exit_price = None
                if exit_event:
                    exit_price = (
                        exit_event.get("executed_price")
                        if exit_event.get("executed_price") is not None
                        else exit_event.get("planned_price")
                    )
                items.append(
                    {
                        **chain,
                        "event_count": len(events),
                        "action_summary": _chain_summary(events),
                        "chain_status": _chain_status(events),
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "quantity": total_buy_size or int(first.get("size") or 0),
                        "started_at": first.get("executed_at") or first.get("created_at"),
                        "ended_at": last.get("executed_at") or last.get("created_at"),
                        "latest_order_id": last.get("id"),
                        "latest_action_type": last.get("action_type"),
                        "strategy_context": strategy_context,
                    }
                )
            items.sort(key=lambda item: str(item.get("ended_at") or ""), reverse=True)
            total = len(items)
            return {"items": items[offset : offset + limit], "total": total, "limit": limit, "offset": offset}
        finally:
            conn.close()
    except Exception as exc:
        return {"items": [], "total": 0, "limit": limit, "offset": offset, "error": str(exc)}


def _event_price(event: dict[str, Any]) -> float:
    value = event.get("executed_price")
    if value is None:
        value = event.get("planned_price")
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _trade_chain_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    quantity = 0
    avg_cost = 0.0
    total_buy_amount = 0.0
    total_sell_amount = 0.0
    realized_pnl = 0.0
    timeline: list[dict[str, Any]] = []

    for event in events:
        action_type = str(event.get("action_type") or "").strip().lower()
        size = int(event.get("size") or 0)
        price = _event_price(event)
        if action_type in {"buy", "add"}:
            total_buy_amount += price * size
            quantity_after = quantity + size
            avg_cost = ((quantity * avg_cost) + (size * price)) / quantity_after if quantity_after > 0 else 0.0
            quantity = quantity_after
        elif action_type in {"sell", "reduce"}:
            reduce_size = min(size, quantity)
            total_sell_amount += price * reduce_size
            realized_pnl += (price - avg_cost) * reduce_size
            quantity = max(0, quantity - reduce_size)
            if quantity == 0:
                avg_cost = 0.0
        elif action_type == "close":
            close_size = quantity if quantity > 0 else size
            total_sell_amount += price * close_size
            realized_pnl += (price - avg_cost) * close_size
            quantity = 0
            avg_cost = 0.0

        timeline.append(
            {
                **event,
                "price": price,
                "quantity_after": quantity,
                "avg_cost_after": avg_cost,
                "amount": price * size,
            }
        )

    return_pct = (realized_pnl / total_buy_amount * 100.0) if total_buy_amount > 0 else None
    return {
        "total_buy_amount": total_buy_amount,
        "total_sell_amount": total_sell_amount,
        "realized_pnl": realized_pnl,
        "return_pct": return_pct,
        "remaining_quantity": quantity,
        "avg_cost": avg_cost,
        "timeline": timeline,
    }


def get_trade_chain(order_no: str) -> dict[str, Any]:
    normalized_order_no = str(order_no or "").strip()
    if not normalized_order_no:
        return {"ok": False, "error": "缺少 order_no"}
    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            if not _db.table_exists(conn, PORTFOLIO_ORDERS_TABLE):
                return {"ok": False, "error": "订单表不存在"}
            _ensure_portfolio_tables(conn)
            rows = conn.execute(
                f"""
                SELECT id, ts_code, action_type, planned_price, executed_price,
                       size, status, order_no, chain_order_id, owner_hash,
                       decision_action_id, note, executed_at, created_at, updated_at
                FROM {PORTFOLIO_ORDERS_TABLE}
                WHERE order_no = ? OR chain_order_id = ? OR id = ?
                ORDER BY created_at ASC
                """,
                (normalized_order_no, normalized_order_no, normalized_order_no),
            ).fetchall()
            events = [_row_to_dict(row) for row in rows]
            if not events:
                return {"ok": False, "error": "交易链不存在"}
            decision_payloads = _load_decision_payloads(
                conn,
                [str(event.get("decision_action_id") or "") for event in events],
            )
            for event in events:
                decision_payload = decision_payloads.get(str(event.get("decision_action_id") or "").strip()) or {}
                event["decision_payload"] = decision_payload
                event["strategy_context"] = _strategy_context_from_payload(decision_payload)
            resolved_order_no = str(events[0].get("order_no") or normalized_order_no).strip()
            chain = list_review_chains(limit=1000, offset=0)
            chain_item = next(
                (item for item in chain.get("items") or [] if str(item.get("order_no") or "") == resolved_order_no),
                {},
            )
            metrics = _trade_chain_metrics(events)
            reviews = list_review_groups(order_id=resolved_order_no, limit=100, offset=0)
            return {
                "ok": True,
                "order_no": resolved_order_no,
                "chain_order_id": events[0].get("chain_order_id") or "",
                "owner_hash": events[0].get("owner_hash") or "",
                "ts_code": events[0].get("ts_code") or "",
                "event_count": len(events),
                "action_summary": _chain_summary(events),
                "chain_status": _chain_status(events),
                "started_at": events[0].get("executed_at") or events[0].get("created_at"),
                "ended_at": events[-1].get("executed_at") or events[-1].get("created_at"),
                "entry_price": chain_item.get("entry_price"),
                "exit_price": chain_item.get("exit_price"),
                "events": events,
                "reviews": reviews.get("items") or [],
                "strategy_context": next((event.get("strategy_context") for event in events if event.get("strategy_context")), {})
                or chain_item.get("strategy_context")
                or {},
                **metrics,
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def create_order(
    *,
    ts_code: str,
    action_type: str,
    planned_price: float | None,
    size: int,
    decision_action_id: str,
    note: str,
    owner_key: str = "",
    chain_order_no: str = "",
) -> dict[str, Any]:
    normalized_action = str(action_type or "").strip().lower()
    if normalized_action not in VALID_ACTION_TYPES:
        return {"ok": False, "error": f"无效操作类型: {action_type}", "valid": sorted(VALID_ACTION_TYPES)}
    if _is_executable_action(normalized_action) and int(size or 0) <= 0:
        return {"ok": False, "error": "交易动作必须填写正数 size"}
    owner = _owner_hash(owner_key)
    order_no = _display_order_no(chain_order_no)
    chain_order_id = f"{owner}{order_no}"
    order_id = _long_order_id(owner, order_no)
    now = _utc_now()
    try:
        conn = _db.connect()
        try:
            _ensure_portfolio_tables(conn)
            conn.execute(
                f"""
                INSERT INTO {PORTFOLIO_ORDERS_TABLE}
                    (id, ts_code, action_type, planned_price, size, status,
                     order_no, chain_order_id, owner_hash, decision_action_id,
                     note, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'planned', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    ts_code,
                    normalized_action,
                    planned_price,
                    size,
                    order_no,
                    chain_order_id,
                    owner,
                    decision_action_id,
                    note,
                    now,
                    now,
                ),
            )
            _commit(conn)
        finally:
            conn.close()
        return {
            "ok": True,
            "id": order_id,
            "order_no": order_no,
            "chain_order_id": chain_order_id,
            "status": "planned",
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def create_order_from_decision_action(
    *,
    decision_action_id: str,
    action_type: str,
    planned_price: float,
    size: int,
    note: str = "",
    owner_key: str = "",
) -> dict[str, Any]:
    normalized_decision_id = str(decision_action_id or "").strip()
    if not normalized_decision_id:
        return {"ok": False, "error": "缺少 decision_action_id"}
    normalized_action = str(action_type or "").strip().lower()
    if normalized_action not in EXECUTABLE_ACTION_TYPES:
        return {"ok": False, "error": f"真实交易计划必须使用可执行动作: {sorted(EXECUTABLE_ACTION_TYPES)}"}
    if int(size or 0) <= 0:
        return {"ok": False, "error": "真实交易计划必须填写正数 size"}
    try:
        planned = float(planned_price)
    except (TypeError, ValueError):
        return {"ok": False, "error": "planned_price 必须是数字"}
    if planned <= 0:
        return {"ok": False, "error": "planned_price 必须大于 0"}

    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            if not _db.table_exists(conn, "decision_actions"):
                return {"ok": False, "error": "决策动作表不存在"}
            row = conn.execute(
                """
                SELECT id, action_type, ts_code, stock_name, note, action_payload_json
                FROM decision_actions
                WHERE id = ?
                LIMIT 1
                """,
                (normalized_decision_id,),
            ).fetchone()
            if not row:
                return {"ok": False, "error": "决策动作不存在"}
            action = _row_to_dict(row)
            ts_code = str(action.get("ts_code") or "").strip().upper()
            if not ts_code:
                return {"ok": False, "error": "决策动作缺少 ts_code，无法创建交易计划"}
            source_note = str(action.get("note") or "").strip()
            action_label = str(action.get("action_type") or "").strip()
            merged_note = "；".join(
                part
                for part in [
                    f"来自决策动作 #{normalized_decision_id}({action_label})",
                    str(note or "").strip(),
                    source_note,
                ]
                if part
            )
        finally:
            conn.close()
        result = create_order(
            ts_code=ts_code,
            action_type=normalized_action,
            planned_price=planned,
            size=int(size),
            decision_action_id=normalized_decision_id,
            note=merged_note,
            owner_key=owner_key,
        )
        if result.get("ok"):
            result["decision_action_id"] = normalized_decision_id
            result["ts_code"] = ts_code
            result["action_type"] = normalized_action
        return result
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _writeback_to_decision_action(conn, decision_action_id: str, execution_status: str) -> None:
    """Write portfolio order execution status back to the linked decision action.
    Failure is silently swallowed — must never block the order update itself.
    """
    if not decision_action_id:
        return
    try:
        row = conn.execute(
            "SELECT action_payload_json FROM decision_actions WHERE id = ? LIMIT 1",
            (decision_action_id,),
        ).fetchone()
        if not row:
            return
        d = _row_to_dict(row)
        try:
            payload = json.loads(str(d.get("action_payload_json") or "{}"))
        except Exception:
            payload = {}
        payload["execution_status"] = execution_status
        conn.execute(
            "UPDATE decision_actions SET action_payload_json = ? WHERE id = ?",
            (json.dumps(payload, ensure_ascii=False), decision_action_id),
        )
    except Exception:
        pass  # Writeback failure must not block caller


def _upsert_position(
    conn,
    *,
    ts_code: str,
    quantity: int,
    avg_cost: float,
    last_price: float,
    order_no: str,
    chain_order_id: str,
    owner_hash: str,
    now: str,
) -> None:
    market_value = float(quantity) * float(last_price)
    unrealized_pnl = (float(last_price) - float(avg_cost)) * float(quantity)
    conn.execute(
        f"""
        INSERT INTO {PORTFOLIO_POSITIONS_TABLE}
            (id, ts_code, name, quantity, avg_cost, last_price, market_value,
             unrealized_pnl, order_no, chain_order_id, owner_hash, updated_at)
        VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ts_code) DO UPDATE SET
            quantity = excluded.quantity,
            avg_cost = excluded.avg_cost,
            last_price = excluded.last_price,
            market_value = excluded.market_value,
            unrealized_pnl = excluded.unrealized_pnl,
            order_no = excluded.order_no,
            chain_order_id = excluded.chain_order_id,
            owner_hash = excluded.owner_hash,
            updated_at = excluded.updated_at
        """,
        (
            str(uuid.uuid4()),
            ts_code,
            int(quantity),
            float(avg_cost),
            float(last_price),
            market_value,
            unrealized_pnl,
            order_no,
            chain_order_id,
            owner_hash,
            now,
        ),
    )


def _apply_executed_order_to_position(conn, order: dict[str, Any], *, executed_price: float, now: str) -> dict[str, Any]:
    action_type = str(order.get("action_type") or "").strip().lower()
    if action_type in NON_EXECUTABLE_ACTION_TYPES:
        return {"ok": True, "position_updated": False}
    if action_type not in EXECUTABLE_ACTION_TYPES:
        return {"ok": False, "error": f"无效操作类型: {action_type}"}

    size = int(order.get("size") or 0)
    if size <= 0:
        return {"ok": False, "error": "交易动作必须填写正数 size"}

    ts_code = str(order.get("ts_code") or "").strip().upper()
    row = conn.execute(
        f"""
        SELECT id, ts_code, quantity, avg_cost, order_no, chain_order_id, owner_hash
        FROM {PORTFOLIO_POSITIONS_TABLE}
        WHERE ts_code = ?
        LIMIT 1
        """,
        (ts_code,),
    ).fetchone()
    current = _row_to_dict(row)
    old_qty = int(current.get("quantity") or 0)
    old_avg = float(current.get("avg_cost") or 0.0)
    order_no = str(current.get("order_no") or order.get("order_no") or "").strip()
    if not order_no:
        order_no = _display_order_no(str(order.get("id") or ""))
    chain_order_id = str(current.get("chain_order_id") or order.get("chain_order_id") or "").strip()
    owner_hash = str(current.get("owner_hash") or order.get("owner_hash") or "").strip()
    if not owner_hash:
        owner_hash = _owner_hash("")
    if not chain_order_id:
        chain_order_id = f"{owner_hash}{order_no}"

    if action_type in {"buy", "add"}:
        new_qty = old_qty + size
        new_avg = ((old_qty * old_avg) + (size * float(executed_price))) / new_qty
    elif action_type in {"sell", "reduce"}:
        if old_qty < size:
            return {"ok": False, "error": f"持仓不足，当前 {old_qty}，请求卖出/减仓 {size}"}
        new_qty = old_qty - size
        new_avg = old_avg if new_qty > 0 else 0.0
    else:  # close
        if old_qty <= 0:
            return {"ok": False, "error": "无可清仓持仓"}
        new_qty = 0
        new_avg = 0.0

    _upsert_position(
        conn,
        ts_code=ts_code,
        quantity=new_qty,
        avg_cost=new_avg,
        last_price=executed_price,
        order_no=order_no,
        chain_order_id=chain_order_id,
        owner_hash=owner_hash,
        now=now,
    )
    return {"ok": True, "position_updated": True, "quantity": new_qty}


def _ensure_pending_review(conn, order_id: str, *, now: str) -> None:
    existing = conn.execute(
        f"""
        SELECT id
        FROM {PORTFOLIO_REVIEWS_TABLE}
        WHERE order_id = ? AND review_tag = 'pending'
        LIMIT 1
        """,
        (order_id,),
    ).fetchone()
    if existing:
        return
    conn.execute(
        f"""
        INSERT INTO {PORTFOLIO_REVIEWS_TABLE}
            (id, order_id, review_tag, review_note, slippage, latency_ms, created_at)
        VALUES (?, ?, 'pending', ?, NULL, NULL, ?)
        """,
        (str(uuid.uuid4()), order_id, "系统自动创建：成交后待人工复盘。", now),
    )


def _ensure_strategy_performance_table(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DECISION_STRATEGY_PERFORMANCE_TABLE} (
            strategy_key TEXT PRIMARY KEY,
            strategy_source TEXT NOT NULL DEFAULT '',
            trade_count INTEGER NOT NULL DEFAULT 0,
            closed_trade_count INTEGER NOT NULL DEFAULT 0,
            win_count INTEGER NOT NULL DEFAULT 0,
            loss_count INTEGER NOT NULL DEFAULT 0,
            neutral_count INTEGER NOT NULL DEFAULT 0,
            pending_count INTEGER NOT NULL DEFAULT 0,
            total_realized_pnl REAL NOT NULL DEFAULT 0,
            avg_return_pct REAL,
            avg_fit_score REAL,
            latest_trade_at TEXT NOT NULL DEFAULT '',
            performance_json TEXT NOT NULL DEFAULT '{{}}',
            updated_at TEXT NOT NULL
        )
        """
    )


def _resolve_order_id(conn, order_ref: str) -> str:
    normalized = str(order_ref or "").strip()
    if not normalized or not _db.table_exists(conn, PORTFOLIO_ORDERS_TABLE):
        return normalized
    row = conn.execute(
        f"""
        SELECT id
        FROM {PORTFOLIO_ORDERS_TABLE}
        WHERE id = ? OR order_no = ? OR chain_order_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (normalized, normalized, normalized),
    ).fetchone()
    if not row:
        return normalized
    return str(_row_to_dict(row).get("id") or normalized)


def update_order(
    order_id: str,
    *,
    status: str | None = None,
    executed_price: float | None = None,
    executed_at: str | None = None,
) -> dict[str, Any]:
    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            if not _db.table_exists(conn, PORTFOLIO_ORDERS_TABLE):
                return {"ok": False, "error": "订单表不存在"}
            _ensure_portfolio_tables(conn)
            row = conn.execute(
                f"""
                SELECT id, ts_code, action_type, planned_price, executed_price, size,
                       status, order_no, chain_order_id, owner_hash, decision_action_id
                FROM {PORTFOLIO_ORDERS_TABLE}
                WHERE id = ?
                LIMIT 1
                """,
                (order_id,),
            ).fetchone()
            if not row:
                return {"ok": False, "error": "订单不存在"}
            order_before = _row_to_dict(row)
            previous_status = str(order_before.get("status") or "").strip()
            next_status = str(status or previous_status).strip()
            next_executed_price = executed_price
            if next_executed_price is None:
                existing_executed = order_before.get("executed_price")
                planned_price = order_before.get("planned_price")
                if existing_executed is not None:
                    next_executed_price = float(existing_executed)
                elif planned_price is not None:
                    next_executed_price = float(planned_price)
            should_apply_execution = status == "executed" and previous_status != "executed"
            if should_apply_execution:
                if next_executed_price is None:
                    return {"ok": False, "error": "执行订单必须提供 executed_price 或 planned_price"}
                execution_check = _apply_executed_order_to_position(
                    conn,
                    order_before,
                    executed_price=float(next_executed_price),
                    now=_utc_now(),
                )
                if not execution_check.get("ok"):
                    return execution_check
            now = _utc_now()
            set_parts: list[str] = ["updated_at = ?"]
            params: list[Any] = [now]
            if status is not None:
                if status not in VALID_ORDER_STATUSES:
                    return {"ok": False, "error": f"无效订单状态: {status}"}
                set_parts.append("status = ?")
                params.append(status)
            if executed_price is not None:
                set_parts.append("executed_price = ?")
                params.append(executed_price)
            elif should_apply_execution and next_executed_price is not None:
                set_parts.append("executed_price = ?")
                params.append(next_executed_price)
            if executed_at is not None:
                set_parts.append("executed_at = ?")
                params.append(executed_at)
            params.append(order_id)
            conn.execute(
                f"UPDATE {PORTFOLIO_ORDERS_TABLE} SET {', '.join(set_parts)} WHERE id = ?",
                params,
            )
            # Writeback execution status to linked decision action
            if status is not None and status in ORDER_TO_EXECUTION_STATUS:
                try:
                    order_row = conn.execute(
                        f"SELECT decision_action_id FROM {PORTFOLIO_ORDERS_TABLE} WHERE id = ? LIMIT 1",
                        (order_id,),
                    ).fetchone()
                    da_id = str(_row_to_dict(order_row).get("decision_action_id") or "") if order_row else ""
                    if da_id:
                        _writeback_to_decision_action(conn, da_id, ORDER_TO_EXECUTION_STATUS[status])
                except Exception:
                    pass  # Writeback failure must not block order update
            if should_apply_execution:
                _ensure_pending_review(conn, order_id, now=now)
            _commit(conn)
        finally:
            conn.close()
        return {"ok": True, "id": order_id}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def list_reviews(
    *,
    order_id: str = "",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            if not _db.table_exists(conn, PORTFOLIO_REVIEWS_TABLE):
                return {"items": [], "total": 0, "limit": limit, "offset": offset}
            _ensure_portfolio_tables(conn)
            conditions: list[str] = []
            params: list[Any] = []
            if order_id:
                normalized_order_id = str(order_id).strip()
                conditions.append(
                    f"""
                    (
                        r.order_id = ?
                        OR r.order_id IN (
                            SELECT id FROM {PORTFOLIO_ORDERS_TABLE}
                            WHERE order_no = ? OR chain_order_id = ?
                        )
                    )
                    """
                )
                params.extend([normalized_order_id, normalized_order_id, normalized_order_id])
            where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
            count_row = conn.execute(
                f"SELECT COUNT(*) FROM {PORTFOLIO_REVIEWS_TABLE} r {where_clause}",
                params,
            ).fetchone()
            total = int(count_row[0] or 0) if count_row else 0
            rows = conn.execute(
                f"""
                SELECT r.id, r.order_id, r.review_tag, r.review_note, r.slippage, r.latency_ms, r.created_at,
                       o.ts_code, o.action_type, o.status AS order_status, o.executed_at, o.executed_price,
                       o.order_no, o.chain_order_id, o.owner_hash, o.decision_action_id, o.note AS order_note,
                       da.snapshot_date, da.action_payload_json, da.note AS decision_note
                FROM {PORTFOLIO_REVIEWS_TABLE} r
                LEFT JOIN {PORTFOLIO_ORDERS_TABLE} o ON o.id = r.order_id
                LEFT JOIN decision_actions da ON CAST(da.id AS TEXT) = o.decision_action_id
                {where_clause}
                ORDER BY r.created_at DESC
                LIMIT ? OFFSET ?
                """,
                params + [limit, offset],
            ).fetchall()
            items = []
            for row in rows:
                item = _row_to_dict(row)
                action_payload = _parse_json_dict(item.get("action_payload_json"))
                item["snapshot_id"] = action_payload.get("snapshot_id") or item.get("snapshot_date") or ""
                item["decision_payload"] = action_payload
                item["strategy_context"] = _strategy_context_from_payload(action_payload)
                item["rule_correction_hint"] = item.get("review_note") or action_payload.get("review_conclusion") or ""
                items.append(item)
            return {"items": items, "total": total, "limit": limit, "offset": offset}
        finally:
            conn.close()
    except Exception as exc:
        return {"items": [], "total": 0, "limit": limit, "offset": offset, "error": str(exc)}


def list_review_groups(
    *,
    order_id: str = "",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    result = list_reviews(order_id=order_id, limit=1000, offset=0)
    if result.get("error"):
        return {"items": [], "total": 0, "limit": limit, "offset": offset, "error": result.get("error")}

    groups: dict[str, dict[str, Any]] = {}
    for item in result.get("items") or []:
        review = dict(item)
        order_no = str(review.get("order_no") or "").strip()
        if not order_no:
            order_no = _display_order_no(str(review.get("order_id") or review.get("id") or ""))
            review["order_no"] = order_no
        group = groups.setdefault(
            order_no,
            {
                "id": order_no,
                "order_id": order_no,
                "order_no": order_no,
                "chain_order_id": review.get("chain_order_id") or "",
                "owner_hash": review.get("owner_hash") or "",
                "ts_code": review.get("ts_code") or "",
                "reviews": [],
            },
        )
        if not group.get("chain_order_id") and review.get("chain_order_id"):
            group["chain_order_id"] = review.get("chain_order_id")
        if not group.get("ts_code") and review.get("ts_code"):
            group["ts_code"] = review.get("ts_code")
        group["reviews"].append(review)

    items: list[dict[str, Any]] = []
    for group in groups.values():
        reviews = group["reviews"]
        chronological = sorted(
            reviews,
            key=lambda r: str(r.get("executed_at") or r.get("created_at") or ""),
        )
        primary = next((r for r in reviews if str(r.get("review_tag") or "") != "pending"), reviews[0])
        actions = _chain_summary(chronological)
        pending_count = sum(1 for r in reviews if str(r.get("review_tag") or "") == "pending")
        completed_count = len(reviews) - pending_count
        items.append(
            {
                **group,
                "review_count": len(reviews),
                "pending_count": pending_count,
                "completed_count": completed_count,
                "action_summary": actions,
                "review_tag": primary.get("review_tag"),
                "review_note": primary.get("review_note"),
                "slippage": primary.get("slippage"),
                "latency_ms": primary.get("latency_ms"),
                "created_at": reviews[0].get("created_at"),
                "decision_action_id": primary.get("decision_action_id"),
                "decision_note": primary.get("decision_note"),
                "decision_payload": primary.get("decision_payload"),
                "strategy_context": primary.get("strategy_context") or {},
                "snapshot_id": primary.get("snapshot_id"),
                "order_note": primary.get("order_note"),
                "order_status": "executed" if any(str(r.get("order_status") or "") == "executed" for r in reviews) else primary.get("order_status"),
                "executed_at": reviews[0].get("executed_at"),
                "executed_price": reviews[0].get("executed_price"),
                "rule_correction_hint": primary.get("rule_correction_hint"),
            }
        )

    items.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    total = len(items)
    return {"items": items[offset : offset + limit], "total": total, "limit": limit, "offset": offset}


def add_review(
    *,
    order_id: str,
    review_tag: str,
    review_note: str,
    slippage: float | None,
    latency_ms: int | None,
) -> dict[str, Any]:
    review_id = str(uuid.uuid4())
    now = _utc_now()
    try:
        conn = _db.connect()
        try:
            _apply_row_factory(conn)
            _ensure_portfolio_tables(conn)
            resolved_order_id = _resolve_order_id(conn, order_id)
            conn.execute(
                f"""
                INSERT INTO {PORTFOLIO_REVIEWS_TABLE}
                    (id, order_id, review_tag, review_note, slippage, latency_ms, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (review_id, resolved_order_id, review_tag, review_note, slippage, latency_ms, now),
            )
            _commit(conn)
        finally:
            conn.close()
        return {"ok": True, "id": review_id, "order_id": resolved_order_id}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def delete_review(review_id: str) -> dict[str, Any]:
    normalized_id = str(review_id or "").strip()
    if not normalized_id:
        return {"ok": False, "error": "缺少 review_id"}
    try:
        conn = _db.connect()
        try:
            if not _db.table_exists(conn, PORTFOLIO_REVIEWS_TABLE):
                return {"ok": False, "error": "复盘表不存在"}
            cur = conn.execute(
                f"DELETE FROM {PORTFOLIO_REVIEWS_TABLE} WHERE id = ?",
                (normalized_id,),
            )
            deleted = int(getattr(cur, "rowcount", 0) or 0)
            _commit(conn)
        finally:
            conn.close()
        if deleted <= 0:
            return {"ok": False, "error": "复盘记录不存在"}
        return {"ok": True, "id": normalized_id, "deleted": deleted}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
