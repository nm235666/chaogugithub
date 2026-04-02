from __future__ import annotations

from services.agent_service.graph.company_research_graph import run_company_research_graph
from services.agent_service.graph.trend_analysis_graph import run_trend_analysis_graph


def _extract_max_drawdown_from_context(payload: dict) -> float:
    context = payload.get("context") or {}
    risk_summary = context.get("risk_summary") or {}
    items = list(risk_summary.get("items") or [])
    if not items:
        return 0.0
    first = items[0] or {}
    try:
        return float(first.get("max_drawdown") or 0.0)
    except Exception:
        return 0.0


def _build_pretrade_signal_payload(payload: dict, *, kind: str) -> dict:
    confidence = payload.get("decision_confidence") or {}
    score = confidence.get("score")
    try:
        score_num = float(score) if score is not None else 55.0
    except Exception:
        score_num = 55.0
    target_weight = max(0.05, min(0.3, score_num / 400.0))
    volatility = 0.25
    if kind == "trend":
        features = payload.get("features") or {}
        metrics = features.get("trend_metrics") or {}
        try:
            volatility = max(0.0, float(metrics.get("annualized_volatility_pct") or 25.0) / 100.0)
        except Exception:
            volatility = 0.25
    max_drawdown = _extract_max_drawdown_from_context(payload) if kind == "multi_role" else 0.0
    return {
        "target_position_weight": target_weight,
        "max_drawdown": max_drawdown,
        "volatility": volatility,
        "liquidity_score": 0.5,
    }


def _maybe_attach_risk_check(deps: dict, payload: dict, *, kind: str) -> None:
    if not deps.get("enable_risk_precheck"):
        return
    risk_fn = deps.get("pre_trade_check_fn")
    if not callable(risk_fn):
        return
    signal_payload = _build_pretrade_signal_payload(payload, kind=kind)
    payload["pre_trade_check"] = risk_fn(signal_payload)


def _maybe_notify(deps: dict, payload: dict, *, title: str, subject_key: str) -> None:
    if not deps.get("enable_notifications"):
        return
    notify_fn = deps.get("notify_result_fn")
    if not callable(notify_fn):
        return
    try:
        payload["notification"] = notify_fn(
            title=title,
            summary=f"{subject_key} 分析已完成，模型：{payload.get('used_model') or ''}",
            markdown=str(payload.get("analysis_markdown") or ""),
            subject_key=subject_key,
            link="",
        )
    except Exception as exc:
        payload["notification"] = {"ok": False, "error": str(exc)}


def run_trend_analysis(deps: dict, *, ts_code: str, lookback: int, model: str, temperature: float = 0.2) -> dict:
    payload = run_trend_analysis_graph(
        deps,
        ts_code=ts_code,
        lookback=lookback,
        model=model,
        temperature=temperature,
    ).to_payload()
    _maybe_attach_risk_check(deps, payload, kind="trend")
    _maybe_notify(deps, payload, title="趋势分析完成", subject_key=ts_code)
    return payload


def run_multi_role_analysis(
    deps: dict,
    *,
    ts_code: str,
    lookback: int,
    roles: list[str],
    model: str,
    temperature: float = 0.2,
    context: dict | None = None,
) -> dict:
    payload = run_company_research_graph(
        deps,
        ts_code=ts_code,
        lookback=lookback,
        roles=roles,
        model=model,
        temperature=temperature,
        context=context,
    ).to_payload()
    _maybe_attach_risk_check(deps, payload, kind="multi_role")
    _maybe_notify(deps, payload, title="多角色研究完成", subject_key=ts_code)
    return payload
