from __future__ import annotations

from services.agent_service.context.company_context_builder import build_company_context, summarize_context_dimensions
from services.agent_service.contracts import AgentAnalysisResult, RoleOutput
from services.agent_service.outputs.markdown_report import build_portfolio_view, build_risk_review, infer_decision_confidence


def run_company_research_graph(
    deps: dict,
    ts_code: str,
    lookback: int,
    roles: list[str],
    model: str,
    temperature: float = 0.2,
    context: dict | None = None,
) -> AgentAnalysisResult:
    context = context or build_company_context(deps, ts_code, lookback)
    llm_result = deps["call_llm_multi_role"](context, roles, model=model, temperature=temperature)
    analysis_markdown = str(llm_result.get("analysis") or "")
    split_payload = deps["split_multi_role_analysis"](analysis_markdown, roles)
    conn = deps["sqlite3"].connect(deps["DB_PATH"])
    try:
        logic_view = deps["get_or_build_cached_logic_view"](
            conn,
            entity_type="llm_multi_role",
            entity_key=f"{ts_code}|{lookback}|{llm_result.get('used_model') or model}|{','.join(roles)}",
            source_payload=analysis_markdown,
            builder=lambda text=analysis_markdown: split_payload.get("logic_view", deps["extract_logic_view_from_markdown"](text)),
        )
    finally:
        conn.close()
    role_outputs = [
        RoleOutput(
            role=str(item.get("role") or ""),
            content=str(item.get("content") or ""),
            matched=bool(item.get("matched", True)),
            logic_view=item.get("logic_view") or {},
        )
        for item in list(split_payload.get("role_sections") or [])
    ]
    summary = logic_view.get("summary") or {}
    return AgentAnalysisResult(
        analysis_markdown=analysis_markdown,
        used_model=str(llm_result.get("used_model") or model),
        requested_model=str(llm_result.get("requested_model") or model),
        attempts=list(llm_result.get("attempts") or []),
        used_context_dims=summarize_context_dimensions(context),
        decision_confidence=infer_decision_confidence(analysis_markdown),
        risk_review=build_risk_review(analysis_markdown, fallback_text=str(summary.get("risk") or "")),
        portfolio_view=build_portfolio_view(analysis_markdown, fallback_text=str(summary.get("focus") or "")),
        logic_view=logic_view,
        role_outputs=role_outputs,
        legacy_payload={
            "ts_code": ts_code,
            "name": context.get("company_profile", {}).get("name", ""),
            "lookback": lookback,
            "model": str(llm_result.get("used_model") or model),
            "roles": roles,
            "context": context,
            "analysis": analysis_markdown,
            "role_sections": [item.to_dict() for item in role_outputs],
            "common_sections_markdown": split_payload.get("common_sections_markdown", ""),
        },
    )
