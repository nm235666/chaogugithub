"""Unified agent-driven research services."""

from .backend_runtime import build_backend_runtime_deps
from .runtime_ops import (
    cleanup_async_jobs,
    create_async_multi_role_job,
    build_multi_role_context,
    build_multi_role_prompt,
    build_trend_features,
    call_llm_multi_role,
    call_llm_trend,
    get_async_multi_role_job,
    run_async_multi_role_job,
    serialize_async_job,
    start_async_multi_role_job,
    split_multi_role_analysis,
)
from .service import run_multi_role_analysis, run_trend_analysis

__all__ = [
    "build_backend_runtime_deps",
    "run_trend_analysis",
    "run_multi_role_analysis",
    "build_trend_features",
    "call_llm_trend",
    "build_multi_role_context",
    "build_multi_role_prompt",
    "call_llm_multi_role",
    "split_multi_role_analysis",
    "cleanup_async_jobs",
    "serialize_async_job",
    "create_async_multi_role_job",
    "run_async_multi_role_job",
    "start_async_multi_role_job",
    "get_async_multi_role_job",
]
