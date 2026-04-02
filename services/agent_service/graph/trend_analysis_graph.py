from __future__ import annotations

from services.agent_service.contracts import AgentAnalysisResult
from services.agent_service.features.trend_features import build_trend_features, summarize_feature_dimensions
from services.agent_service.outputs.markdown_report import build_portfolio_view, build_risk_review, infer_decision_confidence


def run_trend_analysis_graph(deps: dict, ts_code: str, lookback: int, model: str, temperature: float = 0.2) -> AgentAnalysisResult:
    features = build_trend_features(deps, ts_code, lookback)
    llm_result = deps["call_llm_trend"](ts_code, features, model=model, temperature=temperature)
    analysis_markdown = str(llm_result.get("analysis") or "")
    conn = deps["sqlite3"].connect(deps["DB_PATH"])
    try:
        logic_view = deps["get_or_build_cached_logic_view"](
            conn,
            entity_type="llm_trend",
            entity_key=f"{ts_code}|{lookback}|{llm_result.get('used_model') or model}",
            source_payload=analysis_markdown,
            builder=lambda text=analysis_markdown: deps["extract_logic_view_from_markdown"](text),
        )
    finally:
        conn.close()
    summary = logic_view.get("summary") or {}
    return AgentAnalysisResult(
        analysis_markdown=analysis_markdown,
        used_model=str(llm_result.get("used_model") or model),
        requested_model=str(llm_result.get("requested_model") or model),
        attempts=list(llm_result.get("attempts") or []),
        used_context_dims=summarize_feature_dimensions(features),
        decision_confidence=infer_decision_confidence(analysis_markdown),
        risk_review=build_risk_review(analysis_markdown, fallback_text=str(summary.get("risk") or "")),
        portfolio_view=build_portfolio_view(analysis_markdown, fallback_text=str(summary.get("focus") or "")),
        logic_view=logic_view,
        legacy_payload={
            "ts_code": ts_code,
            "name": features.get("name", ""),
            "lookback": lookback,
            "model": str(llm_result.get("used_model") or model),
            "features": features,
            "analysis": analysis_markdown,
        },
    )
