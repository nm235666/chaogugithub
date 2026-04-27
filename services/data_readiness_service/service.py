from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from llm_gateway import DEFAULT_LLM_MODEL, chat_completion_text

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = ROOT_DIR / "stock_codes.db"
DATA_READINESS_RUN_TABLE = "data_readiness_runs"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def _as_bool_env(name: str, default: bool) -> bool:
    value = str(os.getenv(name, "") or "").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _data_readiness_agent_enabled() -> bool:
    return _as_bool_env("DATA_READINESS_AGENT_ENABLED", True)


def _data_readiness_agent_model() -> str:
    return str(os.getenv("DATA_READINESS_AGENT_MODEL", DEFAULT_LLM_MODEL) or DEFAULT_LLM_MODEL).strip() or DEFAULT_LLM_MODEL


def _data_readiness_agent_timeout_seconds() -> int:
    raw = os.getenv("DATA_READINESS_AGENT_TIMEOUT_SECONDS", "8")
    try:
        return max(2, min(30, int(raw)))
    except (TypeError, ValueError):
        return 8


def _data_readiness_path_selector_enabled() -> bool:
    return _as_bool_env("DATA_READINESS_PATH_SELECTOR_ENABLED", True)


def _row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _fetchone(conn, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    return _row_to_dict(conn.execute(sql, params).fetchone())


def _fetchall(conn, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _table_exists(conn, table_name: str) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _ensure_tables(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DATA_READINESS_RUN_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_key TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'check',
            before_json TEXT NOT NULL DEFAULT '{{}}',
            after_json TEXT NOT NULL DEFAULT '{{}}',
            actions_json TEXT NOT NULL DEFAULT '[]',
            summary_json TEXT NOT NULL DEFAULT '{{}}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{DATA_READINESS_RUN_TABLE}_created ON {DATA_READINESS_RUN_TABLE}(created_at DESC)"
    )
    conn.commit()


def _recent_trade_dates(conn, limit: int = 5) -> list[str]:
    if not _table_exists(conn, "stock_daily_prices"):
        return []
    rows = _fetchall(
        conn,
        "SELECT DISTINCT trade_date FROM stock_daily_prices WHERE trade_date IS NOT NULL AND trade_date <> '' ORDER BY trade_date DESC LIMIT ?",
        (max(1, int(limit)),),
    )
    return [str(row.get("trade_date") or "") for row in rows if row.get("trade_date")]


def _placeholders(values: list[Any]) -> str:
    return ",".join("?" for _ in values) or "?"


def _coverage_snapshot(conn) -> dict[str, Any]:
    required = [
        "stock_codes",
        "stock_daily_prices",
        "stock_scores_daily",
        "stock_valuation_daily",
        "capital_flow_stock",
        "risk_scenarios",
        "stock_financials",
        "stock_daily_price_rollups",
    ]
    missing_tables = [name for name in required if not _table_exists(conn, name)]
    if missing_tables:
        return {
            "ok": False,
            "missing_tables": missing_tables,
            "status": "blocked",
            "issues": [{"code": "missing_table", "severity": "blocked", "message": f"缺少关键表：{name}"} for name in missing_tables],
        }

    dates = _recent_trade_dates(conn, 5)
    latest_trade_date = dates[0] if dates else ""
    date_params = tuple(dates or [""])
    date_sql = _placeholders(list(date_params))

    base = _fetchone(
        conn,
        """
        SELECT
          COUNT(*) AS total_codes,
          SUM(CASE WHEN list_status='L' THEN 1 ELSE 0 END) AS active,
          SUM(CASE WHEN list_status='L' AND (name LIKE '%ST%' OR name LIKE '*ST%' OR name LIKE 'ST%') THEN 1 ELSE 0 END) AS st_like,
          SUM(CASE WHEN list_status='L' AND ts_code LIKE '%.BJ' THEN 1 ELSE 0 END) AS bj_active
        FROM stock_codes
        """,
    )
    active = _as_int(base.get("active"))

    score_date = str((_fetchone(conn, "SELECT MAX(score_date) AS d FROM stock_scores_daily").get("d") or ""))
    latest = {
        "price": latest_trade_date,
        "score": score_date,
        "valuation": str((_fetchone(conn, "SELECT MAX(trade_date) AS d FROM stock_valuation_daily").get("d") or "")),
        "flow": str((_fetchone(conn, "SELECT MAX(trade_date) AS d FROM capital_flow_stock").get("d") or "")),
        "risk": str((_fetchone(conn, "SELECT MAX(scenario_date) AS d FROM risk_scenarios").get("d") or "")),
        "financial": str((_fetchone(conn, "SELECT MAX(report_period) AS d FROM stock_financials").get("d") or "")),
    }

    coverage = _fetchone(
        conn,
        f"""
        WITH active AS (SELECT ts_code FROM stock_codes WHERE list_status='L'),
        p AS (SELECT DISTINCT ts_code FROM stock_daily_prices WHERE trade_date = ?),
        s AS (SELECT DISTINCT ts_code FROM stock_scores_daily WHERE score_date = ?),
        v AS (SELECT DISTINCT ts_code FROM stock_valuation_daily WHERE trade_date IN ({date_sql})),
        f AS (SELECT DISTINCT ts_code FROM capital_flow_stock WHERE trade_date IN ({date_sql})),
        r AS (SELECT DISTINCT ts_code FROM risk_scenarios WHERE scenario_date IN ({date_sql})),
        roll20 AS (SELECT DISTINCT ts_code FROM stock_daily_price_rollups WHERE window_days=20),
        roll30 AS (SELECT DISTINCT ts_code FROM stock_daily_price_rollups WHERE window_days=30),
        fin AS (SELECT ts_code, MAX(report_period) AS latest_report FROM stock_financials GROUP BY ts_code)
        SELECT
          COUNT(*) AS active,
          SUM(CASE WHEN p.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS price_latest,
          SUM(CASE WHEN s.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS score_latest,
          SUM(CASE WHEN v.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS valuation_5d,
          SUM(CASE WHEN f.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS flow_5d,
          SUM(CASE WHEN r.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS risk_5d,
          SUM(CASE WHEN roll20.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS rollup20_any,
          SUM(CASE WHEN roll30.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS rollup30_any,
          SUM(CASE WHEN fin.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS financial_any,
          SUM(CASE WHEN fin.latest_report >= '20251231' THEN 1 ELSE 0 END) AS financial_recent
        FROM active a
        LEFT JOIN p ON p.ts_code=a.ts_code
        LEFT JOIN s ON s.ts_code=a.ts_code
        LEFT JOIN v ON v.ts_code=a.ts_code
        LEFT JOIN f ON f.ts_code=a.ts_code
        LEFT JOIN r ON r.ts_code=a.ts_code
        LEFT JOIN roll20 ON roll20.ts_code=a.ts_code
        LEFT JOIN roll30 ON roll30.ts_code=a.ts_code
        LEFT JOIN fin ON fin.ts_code=a.ts_code
        """,
        (latest_trade_date, score_date, *date_params, *date_params, *date_params),
    )

    strict = _fetchone(
        conn,
        f"""
        WITH base AS (
          SELECT ts_code
          FROM stock_codes
          WHERE list_status='L'
            AND name NOT LIKE '%ST%'
            AND name NOT LIKE '*ST%'
            AND name NOT LIKE 'ST%'
            AND ts_code NOT LIKE '%.BJ'
        ),
        p AS (SELECT DISTINCT ts_code FROM stock_daily_prices WHERE trade_date = ?),
        s AS (SELECT ts_code, total_score, trend_score, risk_score FROM stock_scores_daily WHERE score_date = ?),
        v AS (SELECT DISTINCT ts_code FROM stock_valuation_daily WHERE trade_date IN ({date_sql})),
        f AS (SELECT DISTINCT ts_code FROM capital_flow_stock WHERE trade_date IN ({date_sql})),
        r AS (SELECT DISTINCT ts_code FROM risk_scenarios WHERE scenario_date IN ({date_sql})),
        roll AS (SELECT DISTINCT ts_code FROM stock_daily_price_rollups WHERE window_days IN (20, 30))
        SELECT
          COUNT(*) AS base_non_st_main,
          SUM(CASE WHEN p.ts_code IS NOT NULL AND s.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS has_price_score,
          SUM(CASE WHEN p.ts_code IS NOT NULL AND s.ts_code IS NOT NULL AND v.ts_code IS NOT NULL AND f.ts_code IS NOT NULL AND r.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS has_core_inputs_5d,
          SUM(CASE WHEN p.ts_code IS NOT NULL AND s.ts_code IS NOT NULL AND s.risk_score >= 50 AND roll.ts_code IS NOT NULL THEN 1 ELSE 0 END) AS strategy_ready_base,
          SUM(CASE WHEN p.ts_code IS NOT NULL AND s.ts_code IS NOT NULL AND s.total_score >= 65 AND s.trend_score >= 45 AND s.risk_score >= 50 THEN 1 ELSE 0 END) AS rough_candidate_pool
        FROM base a
        LEFT JOIN p ON p.ts_code=a.ts_code
        LEFT JOIN s ON s.ts_code=a.ts_code
        LEFT JOIN v ON v.ts_code=a.ts_code
        LEFT JOIN f ON f.ts_code=a.ts_code
        LEFT JOIN r ON r.ts_code=a.ts_code
        LEFT JOIN roll ON roll.ts_code=a.ts_code
        """,
        (latest_trade_date, score_date, *date_params, *date_params, *date_params),
    )

    issues: list[dict[str, Any]] = []

    def add_issue(code: str, severity: str, message: str, missing: int = 0) -> None:
        issues.append({"code": code, "severity": severity, "message": message, "missing": int(max(0, missing))})

    price_missing = active - _as_int(coverage.get("price_latest"))
    score_missing = active - _as_int(coverage.get("score_latest"))
    valuation_missing = active - _as_int(coverage.get("valuation_5d"))
    flow_missing = active - _as_int(coverage.get("flow_5d"))
    risk_missing = active - _as_int(coverage.get("risk_5d"))
    roll20_missing = active - _as_int(coverage.get("rollup20_any"))
    roll30_missing = active - _as_int(coverage.get("rollup30_any"))
    financial_recent_missing = active - _as_int(coverage.get("financial_recent"))

    if not latest_trade_date:
        add_issue("price_empty", "blocked", "日线价格为空，禁止策略选股", active)
    elif price_missing > 50:
        add_issue("price_latest_missing", "blocked", f"最新交易日价格缺口过大：{price_missing} 只", price_missing)
    elif price_missing > 0:
        add_issue("price_latest_partial", "degraded", f"最新交易日价格缺 {price_missing} 只，缺失股票剔除", price_missing)

    if not score_date or score_missing > 0:
        add_issue("score_latest_missing", "blocked", f"最新评分缺 {score_missing} 只", score_missing)
    if valuation_missing > 100:
        add_issue("valuation_recent_missing", "degraded", f"最近 5 日估值缺 {valuation_missing} 只", valuation_missing)
    if flow_missing > 500:
        add_issue("flow_recent_missing", "degraded", f"最近 5 日资金流缺 {flow_missing} 只，短线策略应降级", flow_missing)
    if risk_missing > 100:
        add_issue("risk_recent_missing", "blocked", f"最近 5 日风险情景缺 {risk_missing} 只，禁止生成买入候选", risk_missing)
    elif risk_missing > 0:
        add_issue("risk_recent_partial", "degraded", f"最近 5 日风险情景缺 {risk_missing} 只，缺失股票剔除", risk_missing)
    if roll20_missing > active * 0.5 and roll30_missing > active * 0.5:
        add_issue("liquidity_rollup_missing", "blocked", "缺少 20/30 日流动性汇总，无法构建策略股票池", max(roll20_missing, roll30_missing))
    elif roll20_missing > active * 0.5:
        add_issue("rollup20_missing", "degraded", "缺少 20 日流动性汇总，临时使用 30 日口径", roll20_missing)
    if financial_recent_missing > active * 0.5:
        add_issue("financial_recent_missing", "degraded", f"较新财务覆盖不足，长线策略应降级：缺 {financial_recent_missing} 只", financial_recent_missing)

    status = "ready"
    if any(item["severity"] == "blocked" for item in issues):
        status = "blocked"
    elif issues:
        status = "degraded"

    return {
        "ok": status == "ready",
        "status": status,
        "latest": latest,
        "recent_trade_dates": dates,
        "base": base,
        "coverage": coverage,
        "strict_tradeable": strict,
        "issues": issues,
    }


def _action_catalog() -> dict[str, dict[str, Any]]:
    return {
        "refresh_prices": {
            "reason": "补最新股票列表和日线价格",
            "command": ["python3", str(ROOT_DIR / "auto_update_stocks_and_prices.py"), "--pause", "0.02"],
            "issue_codes": ["price_empty", "price_latest_missing", "price_latest_partial"],
            "priority": 10,
            "risk": "medium",
        },
        "refresh_valuation": {
            "reason": "补最近估值数据",
            "command": ["python3", str(ROOT_DIR / "backfill_stock_valuation_daily.py"), "--lookback-days", "10", "--pause", "0.02"],
            "issue_codes": ["valuation_recent_missing"],
            "priority": 30,
            "risk": "low",
        },
        "refresh_capital_flow": {
            "reason": "补最近资金流数据",
            "command": ["python3", str(ROOT_DIR / "backfill_capital_flow_stock.py"), "--lookback-days", "10", "--pause", "0.02"],
            "issue_codes": ["flow_recent_missing"],
            "priority": 30,
            "risk": "medium",
        },
        "refresh_risk_scenarios": {
            "reason": "补风险情景数据",
            "command": ["python3", str(ROOT_DIR / "backfill_risk_scenarios.py"), "--lookback-bars", "120"],
            "issue_codes": ["risk_recent_missing", "risk_recent_partial"],
            "priority": 20,
            "risk": "low",
        },
        "build_price_rollups": {
            "reason": "补 20/30/90/365 日流动性汇总",
            "command": ["python3", str(ROOT_DIR / "build_stock_daily_price_rollups.py"), "--window-days", "20,30,90,365"],
            "issue_codes": ["liquidity_rollup_missing", "rollup20_missing"],
            "priority": 25,
            "risk": "low",
        },
        "refresh_financials": {
            "reason": "补最近财务数据",
            "command": ["python3", str(ROOT_DIR / "fast_backfill_stock_financials.py"), "--recent-periods", "8", "--pause", "0.02"],
            "issue_codes": ["financial_recent_missing"],
            "priority": 40,
            "risk": "medium",
        },
        "refresh_scores": {
            "reason": "补数后重算股票评分",
            "command": ["python3", str(ROOT_DIR / "backfill_stock_scores_daily.py"), "--truncate-date"],
            "issue_codes": ["score_latest_missing"],
            "priority": 90,
            "risk": "low",
        },
    }


def _materialize_action(action_key: str, spec: dict[str, Any], *, selected_by: str = "rule", selected_reason: str = "") -> dict[str, Any]:
    return {
        "action_key": action_key,
        "reason": spec.get("reason"),
        "command": list(spec.get("command") or []),
        "status": "planned",
        "selected_by": selected_by,
        "selected_reason": selected_reason,
        "risk": spec.get("risk"),
    }


def _candidate_actions(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    issue_codes = {str(item.get("code") or "") for item in snapshot.get("issues") or []}
    catalog = _action_catalog()
    selected_keys: list[str] = []
    for action_key, spec in sorted(catalog.items(), key=lambda item: int(item[1].get("priority") or 999)):
        codes = {str(code) for code in spec.get("issue_codes") or []}
        if codes.intersection(issue_codes):
            selected_keys.append(action_key)
    if "score_latest_missing" in issue_codes or any(
        key in selected_keys for key in {"refresh_prices", "refresh_valuation", "refresh_capital_flow", "refresh_risk_scenarios", "refresh_financials"}
    ):
        if "refresh_scores" not in selected_keys:
            selected_keys.append("refresh_scores")
    return [_materialize_action(key, catalog[key]) for key in selected_keys if key in catalog]


def _action_selection_prompt(snapshot: dict[str, Any], candidates: list[dict[str, Any]]) -> list[dict[str, str]]:
    payload = {
        "status": snapshot.get("status"),
        "latest": snapshot.get("latest"),
        "coverage": snapshot.get("coverage"),
        "strict_tradeable": snapshot.get("strict_tradeable"),
        "issues": snapshot.get("issues"),
        "candidate_actions": [
            {
                "action_key": item.get("action_key"),
                "reason": item.get("reason"),
                "risk": item.get("risk"),
                "command_summary": " ".join(str(part) for part in (item.get("command") or [])[:2]),
            }
            for item in candidates
        ],
    }
    return [
        {
            "role": "system",
            "content": (
                "你是 Data Readiness Agent 的补数路径选择器。你只能从 candidate_actions 的 action_key 中选择，"
                "不能编造命令，不能新增脚本，不能删除必要的评分重算。输出严格 JSON。"
            ),
        },
        {
            "role": "user",
            "content": (
                "请返回 JSON 字段：selected_action_keys, rationale, skipped_action_keys。"
                "selected_action_keys 必须是数组，按推荐执行顺序排列。\n"
                f"输入：{json.dumps(payload, ensure_ascii=False, default=str)}"
            ),
        },
    ]


def _rule_action_selection(candidates: list[dict[str, Any]], *, source: str = "rule") -> tuple[list[dict[str, Any]], dict[str, Any]]:
    actions = [{**item, "selected_by": source} for item in candidates]
    return actions, {
        "source": source,
        "selected_action_keys": [item.get("action_key") for item in actions],
        "rationale": "按规则优先级选择补数路径",
        "skipped_action_keys": [],
        "error": "",
    }


def _select_actions_with_ai(
    snapshot: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    ai_enabled: bool,
    path_selection_enabled: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not candidates:
        return [], {"source": "none", "selected_action_keys": [], "rationale": "无数据缺口，无需补数", "skipped_action_keys": [], "error": ""}
    if not ai_enabled or not path_selection_enabled or not _data_readiness_agent_enabled() or not _data_readiness_path_selector_enabled():
        return _rule_action_selection(candidates, source="disabled")
    candidate_by_key = {str(item.get("action_key") or ""): item for item in candidates}
    model = _data_readiness_agent_model()
    try:
        text = chat_completion_text(
            model=model,
            messages=_action_selection_prompt(snapshot, candidates),
            temperature=0.1,
            timeout_s=_data_readiness_agent_timeout_seconds(),
            max_retries=1,
        )
        parsed = _parse_json_object(text)
        raw_keys = parsed.get("selected_action_keys") if isinstance(parsed, dict) else []
        selected_keys: list[str] = []
        if isinstance(raw_keys, list):
            for key in raw_keys:
                normalized = str(key or "").strip()
                if normalized in candidate_by_key and normalized not in selected_keys:
                    selected_keys.append(normalized)
        if not selected_keys:
            raise ValueError("empty_or_invalid_selected_action_keys")
        if "refresh_scores" in candidate_by_key and any(
            key in selected_keys for key in {"refresh_prices", "refresh_valuation", "refresh_capital_flow", "refresh_risk_scenarios", "refresh_financials"}
        ):
            selected_keys = [key for key in selected_keys if key != "refresh_scores"] + ["refresh_scores"]
        actions = [{**candidate_by_key[key], "selected_by": "llm", "selected_reason": str(parsed.get("rationale") or "").strip()} for key in selected_keys]
        skipped = [key for key in candidate_by_key if key not in selected_keys]
        return actions, {
            "source": "llm",
            "model": model,
            "selected_action_keys": selected_keys,
            "rationale": str(parsed.get("rationale") or "AI 选择补数路径").strip(),
            "skipped_action_keys": parsed.get("skipped_action_keys") if isinstance(parsed.get("skipped_action_keys"), list) else skipped,
            "candidate_action_keys": list(candidate_by_key),
            "error": "",
        }
    except Exception as exc:
        actions, selection = _rule_action_selection(candidates, source="rule_fallback")
        selection["error"] = str(exc)
        return actions, selection


def _planned_actions(snapshot: dict[str, Any], *, ai_enabled: bool = True, path_selection_enabled: bool = True) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidates = _candidate_actions(snapshot)
    return _select_actions_with_ai(snapshot, candidates, ai_enabled=ai_enabled, path_selection_enabled=path_selection_enabled)


def _run_action(command: list[str], *, timeout_seconds: int) -> dict[str, Any]:
    started = _utc_now()
    try:
        proc = subprocess.run(
            command,
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            timeout=max(30, int(timeout_seconds)),
            check=False,
        )
        return {
            "status": "done" if proc.returncode == 0 else "failed",
            "returncode": proc.returncode,
            "started_at": started,
            "finished_at": _utc_now(),
            "stdout_tail": (proc.stdout or "")[-2000:],
            "stderr_tail": (proc.stderr or "")[-2000:],
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "status": "timeout",
            "returncode": None,
            "started_at": started,
            "finished_at": _utc_now(),
            "stdout_tail": str(exc.stdout or "")[-2000:],
            "stderr_tail": str(exc.stderr or "")[-2000:],
        }


def _parse_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            try:
                parsed = json.loads(raw[start : end + 1])
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
    return {}


def _compact_action_for_ai(action: dict[str, Any]) -> dict[str, Any]:
    return {
        "action_key": action.get("action_key"),
        "reason": action.get("reason"),
        "status": action.get("status"),
        "returncode": action.get("returncode"),
        "stderr_tail": str(action.get("stderr_tail") or "")[-500:],
    }


def _heuristic_ai_diagnosis(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    actions: list[dict[str, Any]],
    final_status: str,
    source: str = "heuristic",
    error: str = "",
) -> dict[str, Any]:
    issues = list(after.get("issues") or before.get("issues") or [])
    failed_actions = [item for item in actions if str(item.get("status") or "") in {"failed", "timeout"}]
    blocked_codes = [str(item.get("code") or "") for item in issues if str(item.get("severity") or "") == "blocked"]
    degraded_codes = [str(item.get("code") or "") for item in issues if str(item.get("severity") or "") == "degraded"]
    if final_status == "ready":
        root_cause = "关键数据检查通过，当前未发现会阻断策略选股的数据缺口。"
        business_impact = "可以继续运行后续策略选股和交易建议 Agent。"
        degrade_strategy = "无需降级。"
    elif final_status == "blocked":
        root_cause = "存在硬阻断数据缺口：" + "、".join(blocked_codes or ["unknown"])
        business_impact = "应暂停策略选股和新增买入建议，避免使用不完整数据生成错误结论。"
        degrade_strategy = "只允许查看历史持仓和已有计划，不生成新的候选股票池。"
    else:
        root_cause = "存在非致命数据缺口：" + "、".join(degraded_codes or ["unknown"])
        business_impact = "可以有限运行后续流程，但相关策略需要剔除缺失股票或降低依赖该数据的权重。"
        degrade_strategy = "短线策略优先剔除缺资金流、风险、行情的股票；长线策略降低财务/估值权重。"
    next_actions = [str(item.get("reason") or item.get("action_key") or "") for item in actions[:5] if item.get("reason") or item.get("action_key")]
    if failed_actions:
        next_actions.insert(0, "优先排查失败或超时的补数脚本")
    if not next_actions and final_status == "ready":
        next_actions = ["保持当前定时检查节奏"]
    return {
        "source": source,
        "model": "",
        "root_cause_summary": root_cause,
        "business_impact": business_impact,
        "degrade_strategy": degrade_strategy,
        "repair_priority": "high" if final_status == "blocked" else ("medium" if final_status == "degraded" else "low"),
        "next_actions": next_actions,
        "manual_check_required": bool(failed_actions or final_status == "blocked"),
        "can_run_strategy_agent": final_status == "ready",
        "error": error,
    }


def _data_readiness_ai_prompt(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    actions: list[dict[str, Any]],
    final_status: str,
) -> list[dict[str, str]]:
    payload = {
        "rule_status": final_status,
        "before": {
            "status": before.get("status"),
            "latest": before.get("latest"),
            "coverage": before.get("coverage"),
            "strict_tradeable": before.get("strict_tradeable"),
            "issues": before.get("issues"),
        },
        "after": {
            "status": after.get("status"),
            "latest": after.get("latest"),
            "coverage": after.get("coverage"),
            "strict_tradeable": after.get("strict_tradeable"),
            "issues": after.get("issues"),
        },
        "actions": [_compact_action_for_ai(action) for action in actions],
    }
    return [
        {
            "role": "system",
            "content": (
                "你是 Data Readiness Agent，只能做数据诊断和修复建议，不能下单，不能覆盖规则硬阻断。"
                "请基于输入的数据覆盖率、缺口、补数动作结果，输出严格 JSON。"
                "如果 rule_status 是 blocked，不能建议直接运行策略选股；如果是 degraded，只能建议降级运行。"
            ),
        },
        {
            "role": "user",
            "content": (
                "返回 JSON 字段：root_cause_summary, business_impact, degrade_strategy, "
                "repair_priority, next_actions, manual_check_required, can_run_strategy_agent。\n"
                f"输入：{json.dumps(payload, ensure_ascii=False, default=str)}"
            ),
        },
    ]


def _build_ai_diagnosis(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    actions: list[dict[str, Any]],
    final_status: str,
    ai_enabled: bool,
) -> dict[str, Any]:
    if not ai_enabled or not _data_readiness_agent_enabled():
        return _heuristic_ai_diagnosis(before=before, after=after, actions=actions, final_status=final_status, source="disabled")
    model = _data_readiness_agent_model()
    try:
        text = chat_completion_text(
            model=model,
            messages=_data_readiness_ai_prompt(before=before, after=after, actions=actions, final_status=final_status),
            temperature=0.2,
            timeout_s=_data_readiness_agent_timeout_seconds(),
            max_retries=1,
        )
        parsed = _parse_json_object(text)
        if not parsed:
            raise ValueError("empty_or_invalid_json")
        fallback = _heuristic_ai_diagnosis(before=before, after=after, actions=actions, final_status=final_status)
        can_run = bool(parsed.get("can_run_strategy_agent", fallback["can_run_strategy_agent"]))
        if final_status == "blocked":
            can_run = False
        return {
            **fallback,
            "source": "llm",
            "model": model,
            "root_cause_summary": str(parsed.get("root_cause_summary") or fallback["root_cause_summary"]).strip(),
            "business_impact": str(parsed.get("business_impact") or fallback["business_impact"]).strip(),
            "degrade_strategy": str(parsed.get("degrade_strategy") or fallback["degrade_strategy"]).strip(),
            "repair_priority": str(parsed.get("repair_priority") or fallback["repair_priority"]).strip(),
            "next_actions": parsed.get("next_actions") if isinstance(parsed.get("next_actions"), list) else fallback["next_actions"],
            "manual_check_required": bool(parsed.get("manual_check_required", fallback["manual_check_required"])),
            "can_run_strategy_agent": can_run,
            "error": "",
        }
    except Exception as exc:
        return _heuristic_ai_diagnosis(
            before=before,
            after=after,
            actions=actions,
            final_status=final_status,
            source="heuristic_fallback",
            error=str(exc),
        )


def run_data_readiness_agent(
    *,
    sqlite3_module,
    db_path: str,
    auto_fix: bool = True,
    dry_run: bool = False,
    ai_enabled: bool = True,
    path_selection_enabled: bool = True,
    command_timeout_seconds: int = 1800,
) -> dict[str, Any]:
    run_key = f"data_readiness:{_utc_now()}"
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        before = _coverage_snapshot(conn)
        actions, action_selection = _planned_actions(
            before,
            ai_enabled=ai_enabled,
            path_selection_enabled=path_selection_enabled,
        )
        mode = "dry_run" if dry_run or not auto_fix else "auto_fix"
        if auto_fix and not dry_run:
            for action in actions:
                result = _run_action(list(action.get("command") or []), timeout_seconds=command_timeout_seconds)
                action.update(result)
        after = _coverage_snapshot(conn) if auto_fix and not dry_run and actions else before
        failed_actions = [a for a in actions if str(a.get("status") or "") in {"failed", "timeout"}]
        final_status = str(after.get("status") or before.get("status") or "blocked")
        if failed_actions and final_status == "ready":
            final_status = "degraded"
        ai_diagnosis = _build_ai_diagnosis(
            before=before,
            after=after,
            actions=actions,
            final_status=final_status,
            ai_enabled=ai_enabled,
        )
        summary = {
            "status": final_status,
            "before_status": before.get("status"),
            "after_status": after.get("status"),
            "actions_total": len(actions),
            "actions_failed": len(failed_actions),
            "mode": mode,
            "action_selection": action_selection,
            "ai_diagnosis": ai_diagnosis,
        }
        now = _utc_now()
        conn.execute(
            f"""
            INSERT INTO {DATA_READINESS_RUN_TABLE} (
                run_key, status, mode, before_json, after_json, actions_json, summary_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_key,
                final_status,
                mode,
                _safe_json(before),
                _safe_json(after),
                _safe_json(actions),
                _safe_json(summary),
                now,
                now,
            ),
        )
        conn.commit()
        return {
            "ok": final_status == "ready",
            "run_key": run_key,
            "status": final_status,
            "mode": mode,
            "before": before,
            "after": after,
            "actions": actions,
            "summary": summary,
        }
    finally:
        conn.close()


def query_latest_data_readiness_report(*, sqlite3_module, db_path: str) -> dict[str, Any]:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn)
        row = conn.execute(
            f"""
            SELECT run_key, status, mode, before_json, after_json, actions_json, summary_json, created_at, updated_at
            FROM {DATA_READINESS_RUN_TABLE}
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            snapshot = _coverage_snapshot(conn)
            return {"ok": snapshot.get("status") == "ready", "status": snapshot.get("status"), "snapshot": snapshot, "latest_run": None}
        item = dict(row)
        return {
            "ok": str(item.get("status") or "") == "ready",
            "latest_run": {
                "run_key": item.get("run_key"),
                "status": item.get("status"),
                "mode": item.get("mode"),
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "before": json.loads(str(item.get("before_json") or "{}")),
                "after": json.loads(str(item.get("after_json") or "{}")),
                "actions": json.loads(str(item.get("actions_json") or "[]")),
                "summary": json.loads(str(item.get("summary_json") or "{}")),
            },
        }
    finally:
        conn.close()


def build_data_readiness_runtime_deps(*, sqlite3_module, db_path: str) -> dict[str, Any]:
    return {
        "run_data_readiness_agent": lambda **kwargs: run_data_readiness_agent(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_latest_data_readiness_report": lambda: query_latest_data_readiness_report(sqlite3_module=sqlite3_module, db_path=db_path),
    }
