from __future__ import annotations

import json
import math
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import db_compat as _db

MACRO_REGIMES_TABLE = "macro_regimes"
MACRO_REGIME_DRAFTS_TABLE = "macro_regime_agent_drafts"
PORTFOLIO_ALLOCATIONS_TABLE = "portfolio_allocations"

VALID_STATES = {"expansion", "slowdown", "risk_rising", "volatile", "contraction", "recovery"}


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


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _row_get(row: Any, key: str, index: int, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        if isinstance(row, dict):
            return row.get(key, default)
        return row[key]
    except Exception:
        try:
            return row[index]
        except Exception:
            return default


def _table_columns(conn, table_name: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    except Exception:
        return set()


def _latest_value(conn, table_name: str, column_name: str) -> str:
    if not _db.table_exists(conn, table_name) or column_name not in _table_columns(conn, table_name):
        return ""
    try:
        row = conn.execute(f"SELECT MAX({column_name}) AS latest_value FROM {table_name}").fetchone()
        return str(_row_get(row, "latest_value", 0, "") or "")
    except Exception:
        return ""


def _recent_values(conn, table_name: str, column_name: str, limit: int) -> list[str]:
    if not _db.table_exists(conn, table_name) or column_name not in _table_columns(conn, table_name):
        return []
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT {column_name} AS value
            FROM {table_name}
            WHERE {column_name} IS NOT NULL AND {column_name} <> ''
            ORDER BY {column_name} DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        return [str(_row_get(row, "value", 0, "") or "") for row in rows if str(_row_get(row, "value", 0, "") or "")]
    except Exception:
        return []


def _confidence_from_score(score: float, coverage: float) -> float:
    return round(max(0.4, min(0.92, 0.48 + abs(float(score)) * 0.32 + max(0.0, min(1.0, coverage)) * 0.16)), 2)


def _state_from_score(score: float) -> str:
    score = float(score)
    if score >= 0.35:
        return "expansion"
    if score >= 0.12:
        return "recovery"
    if score <= -0.65:
        return "contraction"
    if score <= -0.35:
        return "risk_rising"
    if score <= -0.12:
        return "slowdown"
    return "volatile"


def _signal_packet(name: str, score: float, status: str, detail: str, data_points: int = 0) -> dict[str, Any]:
    return {
        "name": name,
        "score": round(_clamp(score), 4),
        "status": status,
        "detail": detail,
        "data_points": max(0, int(data_points or 0)),
    }


def _weighted_cycle_score(signals: list[dict[str, Any]], weights: dict[str, float]) -> tuple[float, float, int]:
    weighted = 0.0
    used_weight = 0.0
    data_points = 0
    for signal in signals:
        name = str(signal.get("name") or "")
        weight = float(weights.get(name, 0.0))
        if weight <= 0 or signal.get("status") != "ok":
            continue
        weighted += float(signal.get("score") or 0.0) * weight
        used_weight += weight
        data_points += _as_int(signal.get("data_points"))
    if used_weight <= 0:
        return 0.0, 0.0, data_points
    return round(_clamp(weighted / used_weight), 4), round(min(1.0, used_weight / max(0.001, sum(weights.values()))), 4), data_points


def _ensure_tables(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MACRO_REGIMES_TABLE} (
            id TEXT PRIMARY KEY,
            short_term_state TEXT NOT NULL DEFAULT 'volatile',
            short_term_confidence REAL NOT NULL DEFAULT 0.7,
            short_term_change_reason TEXT NOT NULL DEFAULT '',
            short_term_changed INTEGER NOT NULL DEFAULT 0,
            medium_term_state TEXT NOT NULL DEFAULT 'volatile',
            medium_term_confidence REAL NOT NULL DEFAULT 0.7,
            medium_term_change_reason TEXT NOT NULL DEFAULT '',
            medium_term_changed INTEGER NOT NULL DEFAULT 0,
            long_term_state TEXT NOT NULL DEFAULT 'volatile',
            long_term_confidence REAL NOT NULL DEFAULT 0.7,
            long_term_change_reason TEXT NOT NULL DEFAULT '',
            long_term_changed INTEGER NOT NULL DEFAULT 0,
            portfolio_action_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MACRO_REGIME_DRAFTS_TABLE} (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'draft',
            source TEXT NOT NULL DEFAULT 'macro_regime_agent',
            short_term_state TEXT NOT NULL DEFAULT 'volatile',
            short_term_confidence REAL NOT NULL DEFAULT 0.7,
            short_term_change_reason TEXT NOT NULL DEFAULT '',
            short_term_changed INTEGER NOT NULL DEFAULT 0,
            medium_term_state TEXT NOT NULL DEFAULT 'volatile',
            medium_term_confidence REAL NOT NULL DEFAULT 0.7,
            medium_term_change_reason TEXT NOT NULL DEFAULT '',
            medium_term_changed INTEGER NOT NULL DEFAULT 0,
            long_term_state TEXT NOT NULL DEFAULT 'volatile',
            long_term_confidence REAL NOT NULL DEFAULT 0.7,
            long_term_change_reason TEXT NOT NULL DEFAULT '',
            long_term_changed INTEGER NOT NULL DEFAULT 0,
            evidence_json TEXT NOT NULL DEFAULT '{{}}',
            agent_summary TEXT NOT NULL DEFAULT '',
            agent_risk_note TEXT NOT NULL DEFAULT '',
            auto_accept_recommended INTEGER NOT NULL DEFAULT 0,
            confirmed_regime_id TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            confirmed_at TEXT NOT NULL DEFAULT '',
            confirmed_by TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PORTFOLIO_ALLOCATIONS_TABLE} (
            id TEXT PRIMARY KEY,
            regime_id TEXT NOT NULL DEFAULT '',
            cash_ratio_pct REAL NOT NULL DEFAULT 10.0,
            max_single_position_pct REAL NOT NULL DEFAULT 8.0,
            max_theme_concentration_pct REAL NOT NULL DEFAULT 20.0,
            stance TEXT NOT NULL DEFAULT 'neutral',
            risk_budget_compression REAL NOT NULL DEFAULT 1.0,
            action_notes TEXT NOT NULL DEFAULT '',
            conflict_ruling TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{MACRO_REGIME_DRAFTS_TABLE}_status_created ON {MACRO_REGIME_DRAFTS_TABLE}(status, created_at DESC)"
    )
    _ensure_outcome_columns(conn)


def _ensure_outcome_columns(conn) -> None:
    conn.execute("""
        ALTER TABLE macro_regimes
        ADD COLUMN IF NOT EXISTS outcome_notes VARCHAR(1000)
    """, [])
    conn.execute("""
        ALTER TABLE macro_regimes
        ADD COLUMN IF NOT EXISTS outcome_rating VARCHAR(20)
    """, [])
    conn.execute("""
        ALTER TABLE macro_regimes
        ADD COLUMN IF NOT EXISTS correction_suggestion VARCHAR(2000)
    """, [])


def _compute_portfolio_suggestion(
    short_state: str,
    medium_state: str,
    long_state: str,
) -> tuple[list[dict], str]:
    """Derive portfolio action suggestions and conflict ruling from regime states."""
    defensive_states = {"contraction", "risk_rising"}
    offensive_states = {"expansion", "recovery"}

    short_def = short_state in defensive_states
    medium_def = medium_state in defensive_states
    long_def = long_state in defensive_states

    short_off = short_state in offensive_states
    medium_off = medium_state in offensive_states
    long_off = long_state in offensive_states

    if short_off and medium_off and long_off:
        return [
            {"type": "cash", "description": "建议现金比例 5%，充分利用进攻机会"},
            {"type": "risk_budget", "description": "风险预算不压缩，允许充分配置"},
            {"type": "theme", "description": "主题集中度上限 25%"},
            {"type": "sector_rotation", "description": "行业权重向高景气进攻方向倾斜"},
            {"type": "strategy_switch", "description": "恢复趋势跟随与高弹性短线策略"},
        ], ""

    if long_def and short_off:
        return [
            {"type": "cash", "description": "建议现金比例 20%，长线防守优先"},
            {"type": "risk_budget", "description": "风险预算压缩至 60%，允许局部高确定性短线参与"},
            {"type": "theme", "description": "单主题集中度上限 15%，禁止高波动策略"},
            {"type": "defence", "description": "同步执行防守动作：提高现金、限制总仓位 ≤ 60%"},
            {"type": "sector_rotation", "description": "行业权重回撤至防守板块，降低高波动赛道暴露"},
            {"type": "strategy_switch", "description": "暂停高波动短线策略，仅保留高确定性参与"},
        ], "短线进攻信号与长线防守状态冲突：允许局部高确定性短线参与，但同步压缩风险预算至60%，限制总仓位≤60%，禁止高波动策略"

    if medium_def and long_def:
        return [
            {"type": "cash", "description": "建议现金比例 30%，中长线均防守"},
            {"type": "risk_budget", "description": "风险预算压缩至 40%"},
            {"type": "theme", "description": "单主题集中度上限 10%"},
            {"type": "defence", "description": "暂停高波动策略，减少净多头敞口"},
            {"type": "sector_rotation", "description": "行业权重切向低波动防守资产"},
            {"type": "strategy_switch", "description": "暂停激进短线策略，保留低波动防守仓位"},
        ], ""

    if short_def or medium_def or long_def:
        return [
            {"type": "cash", "description": "建议现金比例 15%，保持一定防守缓冲"},
            {"type": "risk_budget", "description": "风险预算压缩至 75%"},
            {"type": "theme", "description": "单主题集中度上限 18%"},
            {"type": "sector_rotation", "description": "行业权重适度转向稳健板块"},
            {"type": "strategy_switch", "description": "收缩高波动短线策略，优先低回撤交易"},
        ], ""

    return [
        {"type": "cash", "description": "建议现金比例 10%，中性配置"},
        {"type": "risk_budget", "description": "风险预算不压缩"},
        {"type": "theme", "description": "单主题集中度上限 20%"},
        {"type": "sector_rotation", "description": "行业权重保持均衡配置"},
        {"type": "strategy_switch", "description": "维持当前短线策略节奏"},
    ], ""


def _derive_allocation_params(
    short_state: str,
    long_state: str,
) -> tuple[float, float, float, str, float]:
    """Returns (cash_ratio, max_single, max_theme, stance, risk_compression)."""
    if long_state in {"contraction", "risk_rising"}:
        return 25.0, 5.0, 15.0, "defensive", 0.5
    if short_state in {"expansion", "recovery"} and long_state in {"expansion", "recovery"}:
        return 5.0, 10.0, 25.0, "offensive", 1.0
    if short_state in {"contraction", "risk_rising"}:
        return 18.0, 6.0, 17.0, "defensive", 0.7
    return 10.0, 8.0, 20.0, "neutral", 1.0


def _status_payload(
    *,
    status: str,
    status_reason: str,
    missing_inputs: list[str] | None = None,
    generated_from: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "status_reason": status_reason,
        "missing_inputs": list(missing_inputs or []),
        "generated_from": list(generated_from or []),
    }


def _build_conflict_constraints(allocation: dict[str, Any], macro_actions: list[dict[str, Any]]) -> dict[str, Any]:
    conflict_ruling = str(allocation.get("conflict_ruling") or "").strip()
    compression = float(allocation.get("risk_budget_compression") or 1.0)
    theme_limit = allocation.get("max_theme_concentration_pct")
    defence_action = next((item for item in macro_actions if item.get("type") == "defence"), None)
    strategy_action = next((item for item in macro_actions if item.get("type") == "strategy_switch"), None)

    if not conflict_ruling and compression >= 1 and not defence_action and not strategy_action:
        return {}

    allowed_actions = []
    if "允许局部高确定性短线参与" in conflict_ruling:
        allowed_actions.append("仅允许高确定性短线动作")
    elif compression < 1:
        allowed_actions.append("短线动作需按压缩后风险预算执行")
    else:
        allowed_actions.append("可按当前配置动作执行")

    defence_requirements = []
    if defence_action and defence_action.get("description"):
        defence_requirements.append(str(defence_action.get("description")))
    if theme_limit not in (None, ""):
        defence_requirements.append(f"主题集中度上限收敛至 {float(theme_limit):.0f}%")
    if strategy_action and strategy_action.get("description"):
        defence_requirements.append(str(strategy_action.get("description")))

    trigger_condition = "宏观长线防守与短线进攻信号同时出现时立即生效" if conflict_ruling else "当风险预算进入压缩状态时生效"
    return {
        "allowed_actions": allowed_actions,
        "required_defence_actions": defence_requirements,
        "risk_budget_pct": int(round(compression * 100)),
        "effective_condition": trigger_condition,
    }


def _parse_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _draft_row_to_dict(row) -> dict[str, Any]:
    item = _row_to_dict(row)
    if not item:
        return {}
    item["short_term_changed"] = bool(item.get("short_term_changed"))
    item["medium_term_changed"] = bool(item.get("medium_term_changed"))
    item["long_term_changed"] = bool(item.get("long_term_changed"))
    item["auto_accept_recommended"] = bool(item.get("auto_accept_recommended"))
    item["evidence"] = _parse_json_dict(item.get("evidence_json"))
    return item


def _latest_confirmed_regime(conn) -> dict[str, Any]:
    if not _db.table_exists(conn, MACRO_REGIMES_TABLE):
        return {}
    row = conn.execute(f"SELECT * FROM {MACRO_REGIMES_TABLE} ORDER BY created_at DESC LIMIT 1").fetchone()
    return _row_to_dict(row)


def _build_macro_agent_draft_payload(conn) -> dict[str, Any]:
    suggestion_result = suggest_regime()
    suggestion = dict((suggestion_result or {}).get("suggestion") or {})
    latest = _latest_confirmed_regime(conn)
    short_state = str(suggestion.get("short_term_state") or "volatile")
    medium_state = str(suggestion.get("medium_term_state") or "volatile")
    long_state = str(suggestion.get("long_term_state") or "volatile")
    short_conf = float(suggestion.get("short_term_confidence") or 0.5)
    medium_conf = float(suggestion.get("medium_term_confidence") or 0.4)
    long_conf = float(suggestion.get("long_term_confidence") or 0.4)

    def changed(key: str, next_state: str) -> bool:
        return bool(latest) and str(latest.get(key) or "") != next_state

    short_raw_changed = changed("short_term_state", short_state)
    medium_raw_changed = changed("medium_term_state", medium_state)
    long_raw_changed = changed("long_term_state", long_state)
    basis = str(suggestion.get("basis") or "数据不足，建议人工复核")
    data_points = int(suggestion.get("data_points") or 0)
    avg_conf = (short_conf + medium_conf + long_conf) / 3
    short_changed = short_raw_changed and short_conf >= 0.55 and data_points >= 1
    medium_changed = medium_raw_changed and medium_conf >= 0.65 and data_points >= 3
    long_changed = long_raw_changed and long_conf >= 0.75 and data_points >= 5
    suppressed_changes = []
    if short_raw_changed and not short_changed:
        suppressed_changes.append("短周期变化证据不足，仅记录观察")
    if medium_raw_changed and not medium_changed:
        suppressed_changes.append("中周期变化未达确认阈值")
    if long_raw_changed and not long_changed:
        suppressed_changes.append("长周期变化需要更高置信度和更多证据，暂不建议确认")
    material_changes = short_changed or medium_changed or long_changed
    needs_human_confirmation = bool(material_changes or not latest or avg_conf < 0.65)
    auto_accept_recommended = bool(latest and not material_changes and avg_conf >= 0.65 and data_points >= 3)
    if material_changes:
        change_reason = basis
    elif suppressed_changes:
        change_reason = "；".join(suppressed_changes)
    else:
        change_reason = "状态未较上次发生有效变化，草案仅作盘后记录。"
    agent_summary = (
        f"Agent 草案：短期 {short_state}({short_conf:.0%})，中期 {medium_state}({medium_conf:.0%})，"
        f"长期 {long_state}({long_conf:.0%})。依据：{basis}。"
    )
    if auto_accept_recommended:
        agent_risk_note = "与上次确认状态一致且置信度较高，本草案可作为盘后记录；不强制人工确认。"
    elif data_points <= 0:
        agent_risk_note = "输入证据不足，必须人工复核，不建议直接确认。"
    elif suppressed_changes and not material_changes:
        agent_risk_note = "检测到潜在状态变化，但未达到中长周期确认阈值，建议继续观察而非立即确认。"
    else:
        agent_risk_note = "存在状态变化或置信度不足，需要人工复核变化原因。"
    return {
        "short_term_state": short_state,
        "short_term_confidence": short_conf,
        "short_term_changed": short_changed,
        "short_term_change_reason": change_reason if short_changed else "",
        "medium_term_state": medium_state,
        "medium_term_confidence": medium_conf,
        "medium_term_changed": medium_changed,
        "medium_term_change_reason": change_reason if medium_changed else "",
        "long_term_state": long_state,
        "long_term_confidence": long_conf,
        "long_term_changed": long_changed,
        "long_term_change_reason": change_reason if long_changed else "",
        "evidence": {
            "basis": basis,
            "data_points": data_points,
            "latest_confirmed_regime_id": latest.get("id") or "",
            "latest_confirmed_created_at": latest.get("created_at") or "",
            "evaluation_timing": "post_close",
            "agent_version": "macro_regime_agent_v2",
            "cycle_scores": suggestion.get("cycle_scores") or {},
            "signal_groups": suggestion.get("signal_groups") or {},
            "thresholds": {
                "short_term": "短周期每日评估，变化阈值：confidence >= 0.55 且 data_points >= 1",
                "medium_term": "中周期较低频变化，变化阈值：confidence >= 0.65 且 data_points >= 3",
                "long_term": "长周期高阈值变化，变化阈值：confidence >= 0.75 且 data_points >= 5",
            },
            "suppressed_changes": suppressed_changes,
            "needs_human_confirmation": needs_human_confirmation,
        },
        "agent_summary": agent_summary,
        "agent_risk_note": agent_risk_note,
        "auto_accept_recommended": auto_accept_recommended,
    }


def run_macro_regime_agent() -> dict[str, Any]:
    conn = _db.connect()
    try:
        _db.apply_row_factory(conn)
        _ensure_tables(conn)
        draft = _build_macro_agent_draft_payload(conn)
        now = _utc_now()
        draft_id = f"macro_draft_{uuid.uuid4().hex[:16]}"
        conn.execute(
            f"""
            INSERT INTO {MACRO_REGIME_DRAFTS_TABLE} (
                id, status, source,
                short_term_state, short_term_confidence, short_term_change_reason, short_term_changed,
                medium_term_state, medium_term_confidence, medium_term_change_reason, medium_term_changed,
                long_term_state, long_term_confidence, long_term_change_reason, long_term_changed,
                evidence_json, agent_summary, agent_risk_note, auto_accept_recommended,
                created_at, updated_at
            ) VALUES (?, 'draft', 'macro_regime_agent', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                draft_id,
                draft["short_term_state"],
                draft["short_term_confidence"],
                draft["short_term_change_reason"],
                int(draft["short_term_changed"]),
                draft["medium_term_state"],
                draft["medium_term_confidence"],
                draft["medium_term_change_reason"],
                int(draft["medium_term_changed"]),
                draft["long_term_state"],
                draft["long_term_confidence"],
                draft["long_term_change_reason"],
                int(draft["long_term_changed"]),
                json.dumps(draft["evidence"], ensure_ascii=False, default=str),
                draft["agent_summary"],
                draft["agent_risk_note"],
                int(draft["auto_accept_recommended"]),
                now,
                now,
            ),
        )
        conn.commit()
        latest = get_latest_macro_regime_draft()
        return {"ok": True, "draft": latest.get("draft"), "status": "draft_created"}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "draft": None, "status": "error"}
    finally:
        conn.close()


def get_latest_macro_regime_draft() -> dict[str, Any]:
    conn = _db.connect()
    try:
        _db.apply_row_factory(conn)
        _ensure_tables(conn)
        row = conn.execute(
            f"SELECT * FROM {MACRO_REGIME_DRAFTS_TABLE} ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        draft = _draft_row_to_dict(row)
        return {
            "ok": True,
            "draft": draft or None,
            **_status_payload(
                status="ready" if draft else "empty",
                status_reason="已生成三周期 Agent 草案。" if draft else "当前暂无三周期 Agent 草案。",
                generated_from=["macro_regime_agent_drafts"] if draft else [],
            ),
        }
    except Exception as exc:
        return {"ok": False, "draft": None, "error": str(exc), **_status_payload(status="error", status_reason=f"读取三周期草案失败：{exc}", missing_inputs=["macro_regime_agent_drafts"])}
    finally:
        conn.close()


def confirm_macro_regime_draft(draft_id: str, *, confirmed_by: str = "", overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    draft: dict[str, Any] = {}
    conn = _db.connect()
    try:
        _db.apply_row_factory(conn)
        _ensure_tables(conn)
        row = conn.execute(f"SELECT * FROM {MACRO_REGIME_DRAFTS_TABLE} WHERE id = ? LIMIT 1", (str(draft_id or ""),)).fetchone()
        draft = _draft_row_to_dict(row)
    finally:
        conn.close()
    if not draft:
        return {"ok": False, "error": "draft_not_found"}
    patch = dict(overrides or {})

    def pick(name: str, default: Any) -> Any:
        return patch.get(name, default)

    result = record_regime(
        short_term_state=str(pick("short_term_state", draft["short_term_state"])),
        medium_term_state=str(pick("medium_term_state", draft["medium_term_state"])),
        long_term_state=str(pick("long_term_state", draft["long_term_state"])),
        short_term_confidence=float(pick("short_term_confidence", draft["short_term_confidence"])),
        medium_term_confidence=float(pick("medium_term_confidence", draft["medium_term_confidence"])),
        long_term_confidence=float(pick("long_term_confidence", draft["long_term_confidence"])),
        short_term_change_reason=str(pick("short_term_change_reason", draft["short_term_change_reason"])),
        medium_term_change_reason=str(pick("medium_term_change_reason", draft["medium_term_change_reason"])),
        long_term_change_reason=str(pick("long_term_change_reason", draft["long_term_change_reason"])),
        short_term_changed=bool(pick("short_term_changed", draft["short_term_changed"])),
        medium_term_changed=bool(pick("medium_term_changed", draft["medium_term_changed"])),
        long_term_changed=bool(pick("long_term_changed", draft["long_term_changed"])),
        created_by=confirmed_by,
    )
    if not result.get("ok"):
        return result
    now = _utc_now()
    conn = _db.connect()
    try:
        _db.apply_row_factory(conn)
        _ensure_tables(conn)
        conn.execute(
            f"""
            UPDATE {MACRO_REGIME_DRAFTS_TABLE}
            SET status='confirmed', confirmed_regime_id=?, confirmed_at=?, confirmed_by=?, updated_at=?
            WHERE id=?
            """,
            (str(result.get("id") or ""), now, confirmed_by, now, draft["id"]),
        )
        conn.commit()
        return {"ok": True, "draft_id": draft["id"], "regime_id": result.get("id"), "portfolio_actions": result.get("portfolio_actions") or [], "conflict_ruling": result.get("conflict_ruling") or ""}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    finally:
        conn.close()


def get_latest_regime() -> dict[str, Any]:
    conn = _db.connect()
    try:
        _db.apply_row_factory(conn)
        if not _db.table_exists(conn, MACRO_REGIMES_TABLE):
            return {
                "ok": True,
                "regime": None,
                **_status_payload(
                    status="not_initialized",
                    status_reason="尚未建立三周期状态记录，请先生成系统建议并完成首次人工复核。",
                    missing_inputs=["macro_regimes"],
                ),
            }
        row = conn.execute(
            f"SELECT * FROM {MACRO_REGIMES_TABLE} ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return {
                "ok": True,
                "regime": None,
                **_status_payload(
                    status="not_initialized",
                    status_reason="当前还没有已确认的三周期状态。",
                    missing_inputs=["macro_regime_record"],
                ),
            }
        d = _row_to_dict(row)
        try:
            d["portfolio_action_json"] = json.loads(d.get("portfolio_action_json") or "[]")
        except Exception:
            d["portfolio_action_json"] = []
        alloc_row = conn.execute(
            f"SELECT conflict_ruling FROM {PORTFOLIO_ALLOCATIONS_TABLE} WHERE regime_id = ? ORDER BY created_at DESC LIMIT 1",
            (str(d.get("id") or ""),),
        ).fetchone() if _db.table_exists(conn, PORTFOLIO_ALLOCATIONS_TABLE) else None
        conflict_ruling = str(_row_to_dict(alloc_row).get("conflict_ruling") or "") if alloc_row else ""
        return {
            "ok": True,
            "regime": d,
            "conflict_ruling": conflict_ruling,
            **_status_payload(
                status="ready",
                status_reason="已存在可用于组合动作映射的三周期状态。",
                generated_from=["macro_regimes", "portfolio_allocations" if conflict_ruling else "macro_regimes"],
            ),
        }
    except Exception as exc:
        return {
            "ok": True,
            "regime": None,
            "error": str(exc),
            **_status_payload(
                status="error",
                status_reason=f"读取三周期状态失败：{exc}",
                missing_inputs=["macro_regimes"],
            ),
        }
    finally:
        conn.close()


def record_regime(
    short_term_state: str,
    medium_term_state: str,
    long_term_state: str,
    short_term_confidence: float = 0.7,
    medium_term_confidence: float = 0.7,
    long_term_confidence: float = 0.7,
    short_term_change_reason: str = "",
    medium_term_change_reason: str = "",
    long_term_change_reason: str = "",
    short_term_changed: bool = False,
    medium_term_changed: bool = False,
    long_term_changed: bool = False,
    created_by: str = "",
) -> dict[str, Any]:
    conn = _db.connect()
    try:
        _db.apply_row_factory(conn)
        _ensure_tables(conn)
        actions, conflict_ruling = _compute_portfolio_suggestion(
            short_term_state, medium_term_state, long_term_state
        )
        now = _utc_now()
        regime_id = str(uuid.uuid4())[:16]
        conn.execute(
            f"""
            INSERT INTO {MACRO_REGIMES_TABLE}
            (id, short_term_state, short_term_confidence, short_term_change_reason, short_term_changed,
             medium_term_state, medium_term_confidence, medium_term_change_reason, medium_term_changed,
             long_term_state, long_term_confidence, long_term_change_reason, long_term_changed,
             portfolio_action_json, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                regime_id,
                short_term_state, short_term_confidence, short_term_change_reason, int(short_term_changed),
                medium_term_state, medium_term_confidence, medium_term_change_reason, int(medium_term_changed),
                long_term_state, long_term_confidence, long_term_change_reason, int(long_term_changed),
                json.dumps(actions, ensure_ascii=False), now, created_by,
            ),
        )
        # Auto-create linked allocation record
        cash_ratio, max_single, max_theme, stance, risk_compression = _derive_allocation_params(
            short_term_state, long_term_state
        )
        conn.execute(
            f"""
            INSERT INTO {PORTFOLIO_ALLOCATIONS_TABLE}
            (id, regime_id, cash_ratio_pct, max_single_position_pct, max_theme_concentration_pct,
             stance, risk_budget_compression, action_notes, conflict_ruling, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4())[:16], regime_id,
                cash_ratio, max_single, max_theme,
                stance, risk_compression, "", conflict_ruling, now, created_by,
            ),
        )
        conn.commit()
        return {"ok": True, "id": regime_id, "portfolio_actions": actions, "conflict_ruling": conflict_ruling}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    finally:
        conn.close()


def list_regimes(page: int = 1, page_size: int = 10) -> dict[str, Any]:
    conn = _db.connect()
    try:
        _db.apply_row_factory(conn)
        if not _db.table_exists(conn, MACRO_REGIMES_TABLE):
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 1,
                **_status_payload(
                    status="not_initialized",
                    status_reason="尚无三周期历史记录。",
                    missing_inputs=["macro_regimes"],
                ),
            }
        total_row = conn.execute(f"SELECT COUNT(*) FROM {MACRO_REGIMES_TABLE}").fetchone()
        total = int(total_row[0] or 0) if total_row else 0
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"SELECT * FROM {MACRO_REGIMES_TABLE} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (page_size, offset),
        ).fetchall()
        items = []
        for r in rows:
            d = _row_to_dict(r)
            try:
                d["portfolio_action_json"] = json.loads(d.get("portfolio_action_json") or "[]")
            except Exception:
                d["portfolio_action_json"] = []
            items.append(d)
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)),
            **_status_payload(
                status="ready" if total > 0 else "empty",
                status_reason="已加载三周期历史记录。" if total > 0 else "当前暂无三周期历史记录。",
                generated_from=["macro_regimes"] if total > 0 else [],
            ),
        }
    except Exception as exc:
        return {
            "items": [],
            "total": 0,
            "error": str(exc),
            **_status_payload(
                status="error",
                status_reason=f"三周期历史读取失败：{exc}",
                missing_inputs=["macro_regimes"],
            ),
        }
    finally:
        conn.close()


def get_latest_allocation() -> dict[str, Any]:
    conn = _db.connect()
    try:
        _db.apply_row_factory(conn)
        if not _db.table_exists(conn, PORTFOLIO_ALLOCATIONS_TABLE):
            regime_exists = _db.table_exists(conn, MACRO_REGIMES_TABLE) and bool(
                conn.execute(f"SELECT COUNT(*) FROM {MACRO_REGIMES_TABLE}").fetchone()[0]
            )
            return {
                "ok": True,
                "allocation": None,
                **_status_payload(
                    status="insufficient_evidence" if regime_exists else "not_initialized",
                    status_reason="已有宏观状态，但尚未生成配置动作。" if regime_exists else "尚未建立宏观状态，无法生成配置动作。",
                    missing_inputs=["portfolio_allocations"] if regime_exists else ["macro_regimes", "portfolio_allocations"],
                ),
            }
        row = conn.execute(
            f"SELECT * FROM {PORTFOLIO_ALLOCATIONS_TABLE} ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            regime_exists = _db.table_exists(conn, MACRO_REGIMES_TABLE) and bool(
                conn.execute(f"SELECT COUNT(*) FROM {MACRO_REGIMES_TABLE}").fetchone()[0]
            )
            return {
                "ok": True,
                "allocation": None,
                **_status_payload(
                    status="insufficient_evidence" if regime_exists else "not_initialized",
                    status_reason="三周期状态已存在，但配置动作尚未生成。" if regime_exists else "尚未有可用的三周期状态来推导配置动作。",
                    missing_inputs=["portfolio_allocations"] if regime_exists else ["macro_regimes", "portfolio_allocations"],
                ),
            }
        allocation = _row_to_dict(row)
        regime_id = str(allocation.get("regime_id") or "").strip()
        macro_actions: list[dict[str, Any]] = []
        if regime_id and _db.table_exists(conn, MACRO_REGIMES_TABLE):
            regime_row = conn.execute(
                f"SELECT portfolio_action_json FROM {MACRO_REGIMES_TABLE} WHERE id = ? LIMIT 1",
                (regime_id,),
            ).fetchone()
            regime_payload = _row_to_dict(regime_row)
            try:
                raw_actions = json.loads(regime_payload.get("portfolio_action_json") or "[]")
                if isinstance(raw_actions, list):
                    macro_actions = [item for item in raw_actions if isinstance(item, dict)]
            except Exception:
                macro_actions = []
        allocation["macro_actions"] = macro_actions
        allocation["conflict_constraints"] = _build_conflict_constraints(allocation, macro_actions)
        if regime_id and _db.table_exists(conn, MACRO_REGIMES_TABLE):
            review_row = conn.execute(
                f"SELECT outcome_rating, outcome_notes, correction_suggestion, created_at FROM {MACRO_REGIMES_TABLE} WHERE id = ? LIMIT 1",
                (regime_id,),
            ).fetchone()
            review_payload = _row_to_dict(review_row)
            allocation["long_term_review"] = {
                "regime_id": regime_id,
                "outcome_rating": review_payload.get("outcome_rating") or "",
                "outcome_notes": review_payload.get("outcome_notes") or "",
                "correction_suggestion": review_payload.get("correction_suggestion") or "",
                "regime_created_at": review_payload.get("created_at") or "",
                "action_count": len(macro_actions),
            }
        else:
            allocation["long_term_review"] = {
                "regime_id": regime_id,
                "outcome_rating": "",
                "outcome_notes": "",
                "correction_suggestion": "",
                "regime_created_at": "",
                "action_count": len(macro_actions),
            }
        source = "manual_allocation" if not regime_id else "macro_regime_allocation"
        return {
            "ok": True,
            "allocation": allocation,
            **_status_payload(
                status="ready",
                status_reason="已生成可用于账户级仓位约束的配置动作。",
                generated_from=[source, "portfolio_allocations", "macro_regimes" if macro_actions else source],
            ),
        }
    except Exception as exc:
        return {
            "ok": True,
            "allocation": None,
            "error": str(exc),
            **_status_payload(
                status="error",
                status_reason=f"读取配置动作失败：{exc}",
                missing_inputs=["portfolio_allocations"],
            ),
        }
    finally:
        conn.close()


def record_allocation(
    cash_ratio_pct: float = 10.0,
    max_single_position_pct: float = 8.0,
    max_theme_concentration_pct: float = 20.0,
    stance: str = "neutral",
    risk_budget_compression: float = 1.0,
    action_notes: str = "",
    regime_id: str = "",
    created_by: str = "",
) -> dict[str, Any]:
    conn = _db.connect()
    try:
        _db.apply_row_factory(conn)
        _ensure_tables(conn)
        now = _utc_now()
        alloc_id = str(uuid.uuid4())[:16]
        conn.execute(
            f"""
            INSERT INTO {PORTFOLIO_ALLOCATIONS_TABLE}
            (id, regime_id, cash_ratio_pct, max_single_position_pct, max_theme_concentration_pct,
             stance, risk_budget_compression, action_notes, conflict_ruling, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alloc_id, regime_id,
                cash_ratio_pct, max_single_position_pct, max_theme_concentration_pct,
                stance, risk_budget_compression, action_notes, "", now, created_by,
            ),
        )
        return {"ok": True, "id": alloc_id}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    finally:
        conn.close()


def list_allocations(page: int = 1, page_size: int = 10) -> dict[str, Any]:
    conn = _db.connect()
    try:
        _db.apply_row_factory(conn)
        if not _db.table_exists(conn, PORTFOLIO_ALLOCATIONS_TABLE):
            return {
                "items": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 1,
                **_status_payload(
                    status="not_initialized",
                    status_reason="尚无配置动作历史。",
                    missing_inputs=["portfolio_allocations"],
                ),
            }
        total_row = conn.execute(f"SELECT COUNT(*) FROM {PORTFOLIO_ALLOCATIONS_TABLE}").fetchone()
        total = int(total_row[0] or 0) if total_row else 0
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"SELECT * FROM {PORTFOLIO_ALLOCATIONS_TABLE} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (page_size, offset),
        ).fetchall()
        return {
            "items": [_row_to_dict(r) for r in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)),
            **_status_payload(
                status="ready" if total > 0 else "empty",
                status_reason="已加载配置历史。" if total > 0 else "当前暂无配置历史。",
                generated_from=["portfolio_allocations"] if total > 0 else [],
            ),
        }
    except Exception as exc:
        return {
            "items": [],
            "total": 0,
            "error": str(exc),
            **_status_payload(
                status="error",
                status_reason=f"配置历史读取失败：{exc}",
                missing_inputs=["portfolio_allocations"],
            ),
        }
    finally:
        conn.close()


def update_regime_outcome(regime_id: str, outcome_notes: str, outcome_rating: str, correction_suggestion: str = "") -> dict:
    """Record the actual result and effectiveness rating for a past regime entry."""
    with _db.connect() as conn:
        conn.execute(
            "UPDATE macro_regimes SET outcome_notes=?, outcome_rating=?, correction_suggestion=? WHERE id=?",
            [outcome_notes, outcome_rating, correction_suggestion, regime_id]
        )
        return {"ok": True, "id": regime_id}


def _theme_heat_signal(conn) -> dict[str, Any]:
    table = "theme_hotspot_tracker"
    if not _db.table_exists(conn, table):
        return _signal_packet("theme_heat", 0.0, "missing", "缺少题材热度表")
    columns = _table_columns(conn, table)
    if "direction" not in columns:
        return _signal_packet("theme_heat", 0.0, "missing", "题材热度表缺少 direction 字段")
    time_col = "latest_evidence_time" if "latest_evidence_time" in columns else "created_at" if "created_at" in columns else ""
    threshold = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    where_sql = f"WHERE {time_col} >= ?" if time_col else ""
    params: tuple[Any, ...] = (threshold,) if time_col else ()
    try:
        rows = conn.execute(
            f"""
            SELECT direction, COUNT(*) AS cnt
            FROM {table}
            {where_sql}
            GROUP BY direction
            """,
            params,
        ).fetchall()
    except Exception:
        rows = []
    bullish = bearish = neutral = 0
    for row in rows:
        direction = str(_row_get(row, "direction", 0, "") or "").strip().lower()
        count = _as_int(_row_get(row, "cnt", 1, 0))
        if direction in {"bullish", "看多", "多", "up", "涨"}:
            bullish += count
        elif direction in {"bearish", "看空", "空", "down", "跌"}:
            bearish += count
        else:
            neutral += count
    total = bullish + bearish + neutral
    if total <= 0:
        return _signal_packet("theme_heat", 0.0, "missing", "近7日暂无题材方向证据")
    score = (bullish - bearish) / total
    detail = f"近7日题材多{bullish}/空{bearish}/中性{neutral}"
    return _signal_packet("theme_heat", score, "ok", detail, total)


def _market_breadth_signal(conn) -> dict[str, Any]:
    table = "stock_daily_prices"
    columns = _table_columns(conn, table)
    if not _db.table_exists(conn, table) or not {"trade_date", "pct_chg"}.issubset(columns):
        return _signal_packet("market_breadth", 0.0, "missing", "缺少日线涨跌幅数据")
    latest = _latest_value(conn, table, "trade_date")
    if not latest:
        return _signal_packet("market_breadth", 0.0, "missing", "暂无最新交易日")
    row = conn.execute(
        f"""
        SELECT
          SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS up_count,
          SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) AS down_count,
          COUNT(*) AS total_count,
          AVG(pct_chg) AS avg_pct
        FROM {table}
        WHERE trade_date = ? AND pct_chg IS NOT NULL
        """,
        (latest,),
    ).fetchone()
    total = _as_int(_row_get(row, "total_count", 2, 0))
    if total <= 0:
        return _signal_packet("market_breadth", 0.0, "missing", f"{latest} 无涨跌幅覆盖")
    up = _as_int(_row_get(row, "up_count", 0, 0))
    down = _as_int(_row_get(row, "down_count", 1, 0))
    avg_pct = _as_float(_row_get(row, "avg_pct", 3, 0.0))
    score = _clamp((up - down) / total + avg_pct / 8.0)
    return _signal_packet("market_breadth", score, "ok", f"{latest} 上涨{up}/下跌{down}，平均涨跌幅{avg_pct:.2f}%", total)


def _capital_flow_signal(conn) -> dict[str, Any]:
    table = "capital_flow_stock"
    columns = _table_columns(conn, table)
    if not _db.table_exists(conn, table) or not {"trade_date", "net_inflow"}.issubset(columns):
        return _signal_packet("capital_flow", 0.0, "missing", "缺少个股资金流数据")
    dates = _recent_values(conn, table, "trade_date", 5)
    if not dates:
        return _signal_packet("capital_flow", 0.0, "missing", "暂无最近资金流日期")
    placeholders = ",".join("?" for _ in dates)
    row = conn.execute(
        f"""
        SELECT
          SUM(CASE WHEN net_inflow > 0 THEN 1 ELSE 0 END) AS positive_count,
          SUM(CASE WHEN net_inflow < 0 THEN 1 ELSE 0 END) AS negative_count,
          COUNT(*) AS total_count,
          SUM(net_inflow) AS net_sum
        FROM {table}
        WHERE trade_date IN ({placeholders}) AND net_inflow IS NOT NULL
        """,
        tuple(dates),
    ).fetchone()
    total = _as_int(_row_get(row, "total_count", 2, 0))
    if total <= 0:
        return _signal_packet("capital_flow", 0.0, "missing", "最近5日资金流覆盖为空")
    positive = _as_int(_row_get(row, "positive_count", 0, 0))
    negative = _as_int(_row_get(row, "negative_count", 1, 0))
    net_sum = _as_float(_row_get(row, "net_sum", 3, 0.0))
    score = _clamp((positive - negative) / total + math.tanh(net_sum / 1_000_000_000.0) * 0.25)
    return _signal_packet("capital_flow", score, "ok", f"近5日资金净流入股票{positive}/净流出{negative}", total)


def _index_trend_signal(conn) -> dict[str, Any]:
    table = "stock_scores_daily"
    columns = _table_columns(conn, table)
    if not _db.table_exists(conn, table) or not {"score_date", "trend_score"}.issubset(columns):
        return _signal_packet("index_trend", 0.0, "missing", "缺少趋势评分数据")
    latest = _latest_value(conn, table, "score_date")
    if not latest:
        return _signal_packet("index_trend", 0.0, "missing", "暂无最新评分日期")
    row = conn.execute(
        f"""
        SELECT AVG(trend_score) AS avg_trend, COUNT(*) AS total_count,
               SUM(CASE WHEN trend_score >= 60 THEN 1 ELSE 0 END) AS trend_ok_count
        FROM {table}
        WHERE score_date = ? AND trend_score IS NOT NULL
        """,
        (latest,),
    ).fetchone()
    total = _as_int(_row_get(row, "total_count", 1, 0))
    if total <= 0:
        return _signal_packet("index_trend", 0.0, "missing", f"{latest} 趋势评分覆盖为空")
    avg_trend = _as_float(_row_get(row, "avg_trend", 0, 50.0), 50.0)
    trend_ok = _as_int(_row_get(row, "trend_ok_count", 2, 0))
    score = _clamp((avg_trend - 50.0) / 35.0 + (trend_ok / total - 0.5) * 0.6)
    return _signal_packet("index_trend", score, "ok", f"{latest} 平均趋势分{avg_trend:.1f}，趋势达标{trend_ok}/{total}", total)


def _moving_average_signal(conn) -> dict[str, Any]:
    table = "stock_daily_prices"
    columns = _table_columns(conn, table)
    if not _db.table_exists(conn, table) or not {"trade_date", "close"}.issubset(columns):
        return _signal_packet("moving_average", 0.0, "missing", "缺少日线收盘价数据")
    dates = _recent_values(conn, table, "trade_date", 60)
    if len(dates) < 20:
        return _signal_packet("moving_average", 0.0, "missing", "收盘价日期不足20日")
    latest = dates[0]
    latest_row = conn.execute(f"SELECT AVG(close) AS avg_close FROM {table} WHERE trade_date = ? AND close IS NOT NULL", (latest,)).fetchone()
    latest_avg = _as_float(_row_get(latest_row, "avg_close", 0, 0.0))
    if latest_avg <= 0:
        return _signal_packet("moving_average", 0.0, "missing", f"{latest} 缺少有效收盘价")
    def avg_for(window: int) -> float:
        chosen = dates[:window]
        placeholders = ",".join("?" for _ in chosen)
        row = conn.execute(
            f"SELECT AVG(close) AS avg_close FROM {table} WHERE trade_date IN ({placeholders}) AND close IS NOT NULL",
            tuple(chosen),
        ).fetchone()
        return _as_float(_row_get(row, "avg_close", 0, 0.0))
    ma20 = avg_for(20)
    ma60 = avg_for(min(60, len(dates)))
    if ma20 <= 0 or ma60 <= 0:
        return _signal_packet("moving_average", 0.0, "missing", "均线样本不足")
    score = _clamp((latest_avg / ma20 - 1.0) * 8.0 + (ma20 / ma60 - 1.0) * 6.0)
    return _signal_packet("moving_average", score, "ok", f"{latest} 市场均价/20日/60日={latest_avg:.2f}/{ma20:.2f}/{ma60:.2f}", len(dates))


def _volume_signal(conn) -> dict[str, Any]:
    table = "stock_daily_prices"
    columns = _table_columns(conn, table)
    if not _db.table_exists(conn, table) or not {"trade_date", "amount"}.issubset(columns):
        return _signal_packet("volume", 0.0, "missing", "缺少成交额数据")
    dates = _recent_values(conn, table, "trade_date", 20)
    if len(dates) < 5:
        return _signal_packet("volume", 0.0, "missing", "成交额日期不足")
    daily = []
    for date in dates:
        row = conn.execute(f"SELECT SUM(amount) AS amount_sum FROM {table} WHERE trade_date = ?", (date,)).fetchone()
        daily.append(_as_float(_row_get(row, "amount_sum", 0, 0.0)))
    latest_amount = daily[0]
    avg_amount = sum(daily) / len(daily) if daily else 0.0
    if latest_amount <= 0 or avg_amount <= 0:
        return _signal_packet("volume", 0.0, "missing", "成交额样本为空")
    score = _clamp((latest_amount / avg_amount - 1.0) * 1.2)
    return _signal_packet("volume", score, "ok", f"最新成交额/20日均值={latest_amount:.0f}/{avg_amount:.0f}", len(dates))


def _industry_rotation_signal(conn) -> dict[str, Any]:
    table = "stock_scores_daily"
    columns = _table_columns(conn, table)
    if not _db.table_exists(conn, table) or not {"score_date", "industry", "total_score", "trend_score"}.issubset(columns):
        return _signal_packet("industry_rotation", 0.0, "missing", "缺少行业评分轮动数据")
    latest = _latest_value(conn, table, "score_date")
    rows = conn.execute(
        f"""
        SELECT industry, AVG(total_score) AS avg_total, AVG(trend_score) AS avg_trend, COUNT(*) AS stock_count
        FROM {table}
        WHERE score_date = ? AND industry IS NOT NULL AND industry <> ''
        GROUP BY industry
        HAVING COUNT(*) >= 3
        """,
        (latest,),
    ).fetchall()
    total = len(rows)
    if total <= 0:
        return _signal_packet("industry_rotation", 0.0, "missing", "暂无可用行业轮动样本")
    strong = 0
    weak = 0
    for row in rows:
        avg_total = _as_float(_row_get(row, "avg_total", 1, 50.0), 50.0)
        avg_trend = _as_float(_row_get(row, "avg_trend", 2, 50.0), 50.0)
        if avg_total >= 70 and avg_trend >= 60:
            strong += 1
        if avg_total < 50 or avg_trend < 45:
            weak += 1
    score = _clamp((strong - weak) / total)
    return _signal_packet("industry_rotation", score, "ok", f"强势行业{strong}/弱势行业{weak}/总行业{total}", total)


def _macro_data_signal(conn) -> dict[str, Any]:
    table = "macro_series"
    columns = _table_columns(conn, table)
    if not _db.table_exists(conn, table) or not {"indicator_code", "indicator_name", "value"}.issubset(columns):
        return _signal_packet("macro_data", 0.0, "missing", "缺少宏观序列表")
    pattern_sql = (
        "(indicator_code LIKE ? OR indicator_code LIKE ? OR indicator_name LIKE ? OR indicator_name LIKE ?)"
    )
    pattern_params = ("%近6月涨跌幅%", "%近1年涨跌幅%", "%近6月涨跌幅%", "%近1年涨跌幅%")
    try:
        if "period" in columns:
            row = conn.execute(
                f"""
                WITH latest AS (
                  SELECT indicator_code, MAX(period) AS max_period
                  FROM {table}
                  WHERE {pattern_sql}
                  GROUP BY indicator_code
                )
                SELECT AVG(m.value) AS avg_change, COUNT(*) AS total_count
                FROM {table} m
                JOIN latest l ON l.indicator_code = m.indicator_code AND l.max_period = m.period
                WHERE m.value IS NOT NULL
                  AND {pattern_sql.replace("indicator_code", "m.indicator_code").replace("indicator_name", "m.indicator_name")}
                """,
                pattern_params + pattern_params,
            ).fetchone()
        else:
            row = conn.execute(
                f"""
                SELECT AVG(value) AS avg_change, COUNT(*) AS total_count
                FROM {table}
                WHERE value IS NOT NULL
                  AND {pattern_sql}
                """,
                pattern_params,
            ).fetchone()
    except Exception:
        row = None
    total = _as_int(_row_get(row, "total_count", 1, 0))
    if total <= 0:
        return _signal_packet("macro_data", 0.0, "missing", "缺少6-12个月宏观变化率样本")
    avg_change = _as_float(_row_get(row, "avg_change", 0, 0.0))
    score = _clamp(avg_change / 25.0)
    return _signal_packet("macro_data", score, "ok", f"宏观6-12个月变化率均值{avg_change:.2f}", total)


def _valuation_signal(conn) -> dict[str, Any]:
    table = "stock_scores_daily"
    columns = _table_columns(conn, table)
    if not _db.table_exists(conn, table) or not {"score_date", "valuation_score"}.issubset(columns):
        return _signal_packet("valuation", 0.0, "missing", "缺少估值评分")
    latest = _latest_value(conn, table, "score_date")
    row = conn.execute(
        f"SELECT AVG(valuation_score) AS avg_score, COUNT(*) AS total_count FROM {table} WHERE score_date = ? AND valuation_score IS NOT NULL",
        (latest,),
    ).fetchone()
    total = _as_int(_row_get(row, "total_count", 1, 0))
    if total <= 0:
        return _signal_packet("valuation", 0.0, "missing", "估值评分覆盖为空")
    avg_score = _as_float(_row_get(row, "avg_score", 0, 50.0), 50.0)
    score = _clamp((avg_score - 50.0) / 35.0)
    return _signal_packet("valuation", score, "ok", f"市场平均估值分{avg_score:.1f}", total)


def _earnings_signal(conn) -> dict[str, Any]:
    table = "stock_scores_daily"
    columns = _table_columns(conn, table)
    if _db.table_exists(conn, table) and {"score_date", "financial_score"}.issubset(columns):
        latest = _latest_value(conn, table, "score_date")
        row = conn.execute(
            f"SELECT AVG(financial_score) AS avg_score, COUNT(*) AS total_count FROM {table} WHERE score_date = ? AND financial_score IS NOT NULL",
            (latest,),
        ).fetchone()
        total = _as_int(_row_get(row, "total_count", 1, 0))
        if total > 0:
            avg_score = _as_float(_row_get(row, "avg_score", 0, 50.0), 50.0)
            return _signal_packet("earnings", _clamp((avg_score - 50.0) / 35.0), "ok", f"市场平均财务分{avg_score:.1f}", total)
    fin_table = "stock_financials"
    fin_columns = _table_columns(conn, fin_table)
    if not _db.table_exists(conn, fin_table) or not {"net_profit", "roe"}.issubset(fin_columns):
        return _signal_packet("earnings", 0.0, "missing", "缺少盈利质量数据")
    row = conn.execute(
        f"""
        SELECT AVG(roe) AS avg_roe,
               SUM(CASE WHEN net_profit > 0 THEN 1 ELSE 0 END) AS profit_positive,
               COUNT(*) AS total_count
        FROM {fin_table}
        WHERE net_profit IS NOT NULL
        """
    ).fetchone()
    total = _as_int(_row_get(row, "total_count", 2, 0))
    if total <= 0:
        return _signal_packet("earnings", 0.0, "missing", "盈利样本为空")
    positive = _as_int(_row_get(row, "profit_positive", 1, 0))
    avg_roe = _as_float(_row_get(row, "avg_roe", 0, 0.0))
    score = _clamp((positive / total - 0.5) * 1.2 + avg_roe / 40.0)
    return _signal_packet("earnings", score, "ok", f"盈利为正{positive}/{total}，平均ROE{avg_roe:.1f}", total)


def _credit_rate_signal(conn) -> dict[str, Any]:
    table = "macro_series"
    columns = _table_columns(conn, table)
    if not _db.table_exists(conn, table) or not {"indicator_code", "indicator_name", "value"}.issubset(columns):
        return _signal_packet("credit_rate", 0.0, "missing", "缺少利率/信用宏观序列")
    pattern_sql = (
        "("
        "indicator_code LIKE ? OR indicator_code LIKE ? OR indicator_code LIKE ? OR indicator_code LIKE ? "
        "OR indicator_name LIKE ? OR indicator_name LIKE ? OR indicator_name LIKE ? OR indicator_name LIKE ?"
        ")"
    )
    pattern_params = ("%利率%", "%国债%", "%shibor%", "%社融%", "%利率%", "%国债%", "%shibor%", "%社融%")
    try:
        if "period" in columns:
            row = conn.execute(
                f"""
                WITH latest AS (
                  SELECT indicator_code, MAX(period) AS max_period
                  FROM {table}
                  WHERE {pattern_sql}
                  GROUP BY indicator_code
                )
                SELECT AVG(m.value) AS avg_value, COUNT(*) AS total_count
                FROM {table} m
                JOIN latest l ON l.indicator_code = m.indicator_code AND l.max_period = m.period
                WHERE m.value IS NOT NULL
                  AND {pattern_sql.replace("indicator_code", "m.indicator_code").replace("indicator_name", "m.indicator_name")}
                """,
                pattern_params + pattern_params,
            ).fetchone()
        else:
            row = conn.execute(
                f"""
                SELECT AVG(value) AS avg_value, COUNT(*) AS total_count
                FROM {table}
                WHERE value IS NOT NULL
                  AND {pattern_sql}
                """,
                pattern_params,
            ).fetchone()
    except Exception:
        row = None
    total = _as_int(_row_get(row, "total_count", 1, 0))
    if total <= 0:
        return _signal_packet("credit_rate", 0.0, "missing", "暂无利率/信用样本")
    avg_value = _as_float(_row_get(row, "avg_value", 0, 0.0))
    score = _clamp(-avg_value / 50.0)
    return _signal_packet("credit_rate", score, "ok", f"利率/信用相关序列均值{avg_value:.2f}", total)


def suggest_regime() -> dict:
    """
    Auto-suggest macro regime states based on V2 multi-signal inputs.

    Short cycle: theme heat + market breadth + capital flow.
    Medium cycle: trend score + moving-average structure + volume + industry rotation.
    Long cycle: macro data + valuation + earnings + credit/rate environment.
    """
    try:
        conn = _db.connect()
        try:
            _db.apply_row_factory(conn)
            short_signals = [_theme_heat_signal(conn), _market_breadth_signal(conn), _capital_flow_signal(conn)]
            medium_signals = [
                _index_trend_signal(conn),
                _moving_average_signal(conn),
                _volume_signal(conn),
                _industry_rotation_signal(conn),
            ]
            long_signals = [_macro_data_signal(conn), _valuation_signal(conn), _earnings_signal(conn), _credit_rate_signal(conn)]

            short_score, short_coverage, short_points = _weighted_cycle_score(
                short_signals,
                {"theme_heat": 0.4, "market_breadth": 0.35, "capital_flow": 0.25},
            )
            medium_score, medium_coverage, medium_points = _weighted_cycle_score(
                medium_signals,
                {"index_trend": 0.35, "moving_average": 0.25, "volume": 0.15, "industry_rotation": 0.25},
            )
            long_score, long_coverage, long_points = _weighted_cycle_score(
                long_signals,
                {"macro_data": 0.3, "valuation": 0.25, "earnings": 0.25, "credit_rate": 0.2},
            )

            short_state = _state_from_score(short_score)
            medium_state = _state_from_score(medium_score)
            long_state = _state_from_score(long_score)
            short_conf = _confidence_from_score(short_score, short_coverage)
            medium_conf = _confidence_from_score(medium_score, medium_coverage)
            long_conf = _confidence_from_score(long_score, long_coverage)

            signal_groups = {
                "short_term": {"score": short_score, "coverage": short_coverage, "signals": short_signals},
                "medium_term": {"score": medium_score, "coverage": medium_coverage, "signals": medium_signals},
                "long_term": {"score": long_score, "coverage": long_coverage, "signals": long_signals},
            }
            basis_parts = []
            for label, group in (("短周期", signal_groups["short_term"]), ("中周期", signal_groups["medium_term"]), ("长周期", signal_groups["long_term"])):
                ok_signals = [str(s.get("detail") or s.get("name")) for s in group["signals"] if s.get("status") == "ok"]
                missing = [str(s.get("name") or "") for s in group["signals"] if s.get("status") != "ok"]
                if ok_signals:
                    basis_parts.append(f"{label}：" + "；".join(ok_signals[:2]))
                if missing:
                    basis_parts.append(f"{label}缺失信号：" + "、".join(missing))
            basis = "，".join(basis_parts) if basis_parts else "数据不足，建议人工判断"

            return {
                "ok": True,
                "suggestion": {
                    "short_term_state": short_state,
                    "short_term_confidence": short_conf,
                    "medium_term_state": medium_state,
                    "medium_term_confidence": medium_conf,
                    "long_term_state": long_state,
                    "long_term_confidence": long_conf,
                    "basis": basis,
                    "data_points": short_points + medium_points + long_points,
                    "cycle_scores": {
                        "short_term": short_score,
                        "medium_term": medium_score,
                        "long_term": long_score,
                    },
                    "signal_groups": signal_groups,
                },
            }
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc), "suggestion": None}
