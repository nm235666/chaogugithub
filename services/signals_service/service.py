from __future__ import annotations

from .admin import query_signal_audit, query_signal_quality_config, save_signal_mapping_blocklist, save_signal_quality_rules
from .queries import (
    query_investment_signal_timeline,
    query_investment_signals,
    query_signal_state_timeline,
    query_theme_hotspots,
)

def build_signals_service_deps(
    *,
    query_investment_signals_fn,
    query_theme_hotspots_fn,
    query_signal_state_timeline_fn,
    query_research_reports_fn,
    query_investment_signal_timeline_fn,
    query_macro_indicators_fn,
    query_macro_series_fn,
) -> dict:
    return {
        "query_investment_signals": query_investment_signals_fn,
        "query_theme_hotspots": query_theme_hotspots_fn,
        "query_signal_state_timeline": query_signal_state_timeline_fn,
        "query_research_reports": query_research_reports_fn,
        "query_investment_signal_timeline": query_investment_signal_timeline_fn,
        "query_macro_indicators": query_macro_indicators_fn,
        "query_macro_series": query_macro_series_fn,
    }


def build_signals_runtime_deps(
    *,
    sqlite3_module,
    db_path,
    resolve_signal_table_fn,
    cache_get_json_fn,
    cache_set_json_fn,
    redis_cache_ttl_signals: int,
    redis_cache_ttl_themes: int,
    get_or_build_cached_logic_view_fn,
    build_signal_logic_view_fn,
    build_signal_event_logic_view_fn,
    query_research_reports_fn,
    query_macro_indicators_fn,
    query_macro_series_fn,
) -> dict:
    return {
        "query_investment_signals": lambda **kwargs: query_investment_signals(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            resolve_signal_table_fn=resolve_signal_table_fn,
            cache_get_json_fn=cache_get_json_fn,
            cache_set_json_fn=cache_set_json_fn,
            redis_cache_ttl_signals=redis_cache_ttl_signals,
            **kwargs,
        ),
        "query_investment_signal_timeline": lambda **kwargs: query_investment_signal_timeline(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            get_or_build_cached_logic_view_fn=get_or_build_cached_logic_view_fn,
            build_signal_logic_view_fn=build_signal_logic_view_fn,
            build_signal_event_logic_view_fn=build_signal_event_logic_view_fn,
            **kwargs,
        ),
        "query_theme_hotspots": lambda **kwargs: query_theme_hotspots(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            cache_get_json_fn=cache_get_json_fn,
            cache_set_json_fn=cache_set_json_fn,
            redis_cache_ttl_themes=redis_cache_ttl_themes,
            **kwargs,
        ),
        "query_signal_state_timeline": lambda **kwargs: query_signal_state_timeline(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            **kwargs,
        ),
        "query_research_reports": query_research_reports_fn,
        "query_macro_indicators": query_macro_indicators_fn,
        "query_macro_series": query_macro_series_fn,
        "query_signal_audit": lambda **kwargs: query_signal_audit(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            resolve_signal_table_fn=resolve_signal_table_fn,
            **kwargs,
        ),
        "query_signal_quality_config": lambda **kwargs: query_signal_quality_config(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            **kwargs,
        ),
        "save_signal_quality_rules": lambda **kwargs: save_signal_quality_rules(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            **kwargs,
        ),
        "save_signal_mapping_blocklist": lambda **kwargs: save_signal_mapping_blocklist(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            **kwargs,
        ),
    }
