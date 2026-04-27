from __future__ import annotations

from typing import Any, Callable


def _build_layer1_user_decision_deps(
    *,
    build_decision_runtime_deps: Callable[[], dict[str, Any]],
    roundtable_create: Callable[..., Any],
    roundtable_get: Callable[..., Any],
    roundtable_list: Callable[..., Any],
) -> dict[str, Any]:
    return {
        **build_decision_runtime_deps(),
        "roundtable_create": roundtable_create,
        "roundtable_get": roundtable_get,
        "roundtable_list": roundtable_list,
    }


def _build_layer2_data_assets_deps(
    *,
    build_stock_news_service_runtime_deps: Callable[[], dict[str, Any]],
    query_news_sources: Callable[..., Any],
    query_news: Callable[..., Any],
    build_reporting_service_deps: Callable[[], dict[str, Any]],
    build_chatrooms_service_runtime_deps: Callable[[], dict[str, Any]],
    build_signals_service_runtime_deps: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    return {
        **build_stock_news_service_runtime_deps(),
        "query_news_sources": query_news_sources,
        "query_news": query_news,
        **build_reporting_service_deps(),
        **build_chatrooms_service_runtime_deps(),
        **build_signals_service_runtime_deps(),
    }


def _build_layer3_verification_research_deps(
    *,
    build_quantaalpha_runtime_deps: Callable[[], dict[str, Any]],
    enable_quant_factors: bool,
) -> dict[str, Any]:
    return {
        **build_quantaalpha_runtime_deps(),
        "quant_factors_enabled": enable_quant_factors,
    }


def _build_layer4_governance_deps(
    *,
    build_info: Callable[[], dict[str, Any]],
    build_data_readiness_runtime_deps: Callable[[], dict[str, Any]],
    permission_matrix: Callable[[], dict[str, Any]],
    effective_permissions_for_user: Callable[[dict[str, Any] | None], list[str]],
    get_navigation_groups: Callable[[], dict[str, Any]],
    get_dynamic_rbac_payload: Callable[[], dict[str, Any]],
    rbac_dynamic_enforced: bool,
    llm_provider_admin_deps: dict[str, Any],
    ai_retrieval_deps: dict[str, Any],
    frontend_dist_exists: bool,
    frontend_url: str,
) -> dict[str, Any]:
    return {
        "build_info": build_info,
        **build_data_readiness_runtime_deps(),
        "permission_matrix": permission_matrix,
        "effective_permissions_for_user": effective_permissions_for_user,
        "get_navigation_groups": get_navigation_groups,
        "get_dynamic_rbac_payload": get_dynamic_rbac_payload,
        "rbac_dynamic_enforced": rbac_dynamic_enforced,
        **llm_provider_admin_deps,
        **ai_retrieval_deps,
        "frontend_dist_exists": frontend_dist_exists,
        "frontend_url": frontend_url,
    }


def build_layered_route_deps(
    *,
    build_stock_news_service_runtime_deps: Callable[[], dict[str, Any]],
    query_news_sources: Callable[..., Any],
    query_news: Callable[..., Any],
    build_reporting_service_deps: Callable[[], dict[str, Any]],
    build_chatrooms_service_runtime_deps: Callable[[], dict[str, Any]],
    build_signals_service_runtime_deps: Callable[[], dict[str, Any]],
    build_quantaalpha_runtime_deps: Callable[[], dict[str, Any]],
    build_decision_runtime_deps: Callable[[], dict[str, Any]],
    build_data_readiness_runtime_deps: Callable[[], dict[str, Any]],
    roundtable_create: Callable[..., Any],
    roundtable_get: Callable[..., Any],
    roundtable_list: Callable[..., Any],
    enable_quant_factors: bool,
    build_info: Callable[[], dict[str, Any]],
    permission_matrix: Callable[[], dict[str, Any]],
    effective_permissions_for_user: Callable[[dict[str, Any] | None], list[str]],
    get_navigation_groups: Callable[[], dict[str, Any]],
    get_dynamic_rbac_payload: Callable[[], dict[str, Any]],
    rbac_dynamic_enforced: bool,
    llm_provider_admin_deps: dict[str, Any],
    ai_retrieval_deps: dict[str, Any],
    frontend_dist_exists: bool,
    frontend_url: str,
) -> dict[str, Any]:
    deps: dict[str, Any] = {}
    deps.update(
        _build_layer1_user_decision_deps(
            build_decision_runtime_deps=build_decision_runtime_deps,
            roundtable_create=roundtable_create,
            roundtable_get=roundtable_get,
            roundtable_list=roundtable_list,
        )
    )
    deps.update(
        _build_layer2_data_assets_deps(
            build_stock_news_service_runtime_deps=build_stock_news_service_runtime_deps,
            query_news_sources=query_news_sources,
            query_news=query_news,
            build_reporting_service_deps=build_reporting_service_deps,
            build_chatrooms_service_runtime_deps=build_chatrooms_service_runtime_deps,
            build_signals_service_runtime_deps=build_signals_service_runtime_deps,
        )
    )
    deps.update(
        _build_layer3_verification_research_deps(
            build_quantaalpha_runtime_deps=build_quantaalpha_runtime_deps,
            enable_quant_factors=enable_quant_factors,
        )
    )
    deps.update(
        _build_layer4_governance_deps(
            build_info=build_info,
            build_data_readiness_runtime_deps=build_data_readiness_runtime_deps,
            permission_matrix=permission_matrix,
            effective_permissions_for_user=effective_permissions_for_user,
            get_navigation_groups=get_navigation_groups,
            get_dynamic_rbac_payload=get_dynamic_rbac_payload,
            rbac_dynamic_enforced=rbac_dynamic_enforced,
            llm_provider_admin_deps=llm_provider_admin_deps,
            ai_retrieval_deps=ai_retrieval_deps,
            frontend_dist_exists=frontend_dist_exists,
            frontend_url=frontend_url,
        )
    )
    return deps
