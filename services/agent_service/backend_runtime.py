from __future__ import annotations

from services.agent_service.service import run_multi_role_analysis, run_trend_analysis


def build_backend_runtime_deps(
    *,
    sqlite3_module,
    db_path,
    build_trend_features,
    call_llm_trend,
    extract_logic_view_from_markdown,
    get_or_build_cached_logic_view,
    build_multi_role_context,
    call_llm_multi_role,
    split_multi_role_analysis,
    enable_risk_precheck: bool = False,
    pre_trade_check_fn=None,
    enable_notifications: bool = False,
    notify_result_fn=None,
) -> dict:
    return {
        "sqlite3": sqlite3_module,
        "DB_PATH": db_path,
        "build_trend_features": build_trend_features,
        "call_llm_trend": call_llm_trend,
        "get_or_build_cached_logic_view": get_or_build_cached_logic_view,
        "extract_logic_view_from_markdown": extract_logic_view_from_markdown,
        "build_multi_role_context": build_multi_role_context,
        "call_llm_multi_role": call_llm_multi_role,
        "split_multi_role_analysis": split_multi_role_analysis,
        "enable_risk_precheck": bool(enable_risk_precheck),
        "pre_trade_check_fn": pre_trade_check_fn,
        "enable_notifications": bool(enable_notifications),
        "notify_result_fn": notify_result_fn,
        "run_trend_analysis": run_trend_analysis,
        "run_multi_role_analysis": run_multi_role_analysis,
    }
