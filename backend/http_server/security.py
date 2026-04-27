#!/usr/bin/env python3
from __future__ import annotations

import re
from urllib.parse import urlparse

import db_compat as sqlite3
from db_compat import get_redis_client
from runtime_secrets import BACKEND_ALLOWED_ORIGINS
from services.ai_retrieval_service import (
    AI_RETRIEVAL_ENABLED,
    AI_RETRIEVAL_SHADOW_MODE,
    build_context_packet as ai_retrieval_build_context_packet,
    query_retrieval_metrics as ai_retrieval_query_metrics,
    search as ai_retrieval_search_service,
    sync_scene_index as ai_retrieval_sync_scene_index,
)

from backend.http_server.config import (
    DB_PATH,
    DEFAULT_ALLOWED_ADMIN_ORIGINS,
    LIMITED_ALLOWED_EXACT_PATHS,
    LIMITED_ALLOWED_PATH_PREFIXES,
    PROTECTED_GET_PATHS,
    PROTECTED_POST_PATHS,
    TRUSTED_FRONTEND_PORTS,
)

def _normalize_origin(origin: str) -> str:
    return (origin or "").strip().rstrip("/")


def _request_is_protected(path: str, method: str, params: dict[str, list[str]] | None = None) -> bool:
    normalized_method = (method or "").upper()
    if path.startswith("/api/llm/multi-role/v3/"):
        return True
    if normalized_method == "POST":
        return path in PROTECTED_POST_PATHS
    if normalized_method in {"GET", "OPTIONS"} and path in PROTECTED_GET_PATHS:
        return True
    if normalized_method == "OPTIONS" and path in PROTECTED_POST_PATHS:
        return True
    if path == "/api/database-audit":
        refresh_raw = ((params or {}).get("refresh", ["0"])[0] or "").strip().lower()
        return refresh_raw in {"1", "true", "yes", "y", "on"}
    return False


def ai_retrieval_search(*, query: str, scene: str, top_k: int = 8, requested_model: str = "") -> dict:
    return ai_retrieval_search_service(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        query=query,
        scene=scene,
        top_k=top_k,
        requested_model=requested_model,
    )


def ai_retrieval_context(*, query: str, scene: str, top_k: int = 6, max_chars: int = 2400, requested_model: str = "") -> dict:
    return ai_retrieval_build_context_packet(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        query=query,
        scene=scene,
        top_k=top_k,
        max_chars=max_chars,
        requested_model=requested_model,
    )


def ai_retrieval_sync(*, scene: str, limit: int = 300) -> dict:
    return ai_retrieval_sync_scene_index(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        scene=scene,
        limit=limit,
    )


def ai_retrieval_metrics(*, days: int = 1) -> dict:
    return ai_retrieval_query_metrics(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        days=days,
    )


def _origin_matches_current_host(origin: str, host_header: str) -> bool:
    parsed_origin = urlparse(origin)
    if parsed_origin.scheme not in {"http", "https"}:
        return False
    origin_host = (parsed_origin.hostname or "").strip().lower()
    request_host = (host_header or "").split(":", 1)[0].strip().lower()
    if not origin_host or not request_host or origin_host != request_host:
        return False
    if parsed_origin.port is None:
        return parsed_origin.scheme == "https"
    return str(parsed_origin.port) in TRUSTED_FRONTEND_PORTS


def _origin_allowed(origin: str, host_header: str) -> bool:
    normalized = _normalize_origin(origin)
    if not normalized:
        return False
    if normalized in DEFAULT_ALLOWED_ADMIN_ORIGINS:
        return True
    if normalized in {_normalize_origin(item) for item in BACKEND_ALLOWED_ORIGINS}:
        return True
    return _origin_matches_current_host(normalized, host_header)


def _permission_denied_payload(path: str) -> dict:
    code = "AUTH_PERMISSION_DENIED"
    hint = "当前账号无此接口权限，请联系管理员升级或切换账号。"
    if path in {"/api/stocks", "/api/stocks/filters"}:
        code = "AUTH_PERMISSION_DENIED_STOCK_SEARCH"
        hint = "该账号不可访问股票检索接口。可改用 ts_code 直接分析，或联系管理员开通检索权限。"
    elif path in {
        "/api/llm/multi-role",
        "/api/llm/multi-role/start",
        "/api/llm/multi-role/task",
        "/api/llm/multi-role/v2/start",
        "/api/llm/multi-role/v2/task",
        "/api/llm/multi-role/v2/stream",
        "/api/llm/multi-role/v2/decision",
        "/api/llm/multi-role/v2/retry-aggregate",
        "/api/llm/multi-role/v2/history",
    } or path.startswith("/api/llm/multi-role/v3/"):
        code = "AUTH_PERMISSION_DENIED_MULTI_ROLE"
        hint = "该账号不可访问多角色分析接口，请联系管理员开通。"
    elif path == "/api/llm/trend":
        code = "AUTH_PERMISSION_DENIED_TREND"
        hint = "该账号不可访问走势分析接口，请联系管理员开通。"
    elif path.startswith("/api/signals"):
        code = "AUTH_PERMISSION_DENIED_SIGNALS"
        hint = "该账号不可访问投资信号模块。"
    elif path.startswith("/api/quant-factors"):
        code = "AUTH_PERMISSION_DENIED_QUANT_FACTORS"
        hint = "该账号不可访问因子挖掘与回测模块。"
    elif path.startswith("/api/agents") or path.startswith("/api/agent-governance"):
        code = "AUTH_PERMISSION_DENIED_AGENTS"
        hint = "该账号不可访问 Agent 写接口；只读查询需要 research_advanced，审批与创建需要 admin_system。"
    elif path.startswith("/api/system") or path.startswith("/api/jobs"):
        code = "AUTH_PERMISSION_DENIED_SYSTEM"
        hint = "该账号不可访问系统运维接口。"
    return {
        "error": "当前账号权限不足，请升级后使用该功能",
        "code": code,
        "path": path,
        "hint": hint,
    }


_AGENTS_RUN_DETAIL_GET = re.compile(r"^/api/agents/runs/([^/]+)$")


def _agents_read_get_allowed(path: str) -> bool:
    if path in ("/api/agents/runs", "/api/agents/health"):
        return True
    if path.startswith("/api/agents/mcp-audit"):
        return True
    return bool(_AGENTS_RUN_DETAIL_GET.match(path))

__all__ = ['_normalize_origin', '_request_is_protected', 'ai_retrieval_search', 'ai_retrieval_context', 'ai_retrieval_sync', 'ai_retrieval_metrics', '_origin_matches_current_host', '_origin_allowed', '_permission_denied_payload', '_AGENTS_RUN_DETAIL_GET', '_agents_read_get_allowed']
