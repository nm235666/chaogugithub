#!/usr/bin/env python3
from __future__ import annotations

import ipaddress
import json
import mimetypes
import os
import secrets
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import db_compat as sqlite3
from db_compat import assert_database_ready, db_label
from runtime_secrets import BACKEND_ADMIN_TOKEN
from llm_gateway import DEFAULT_LLM_MODEL, normalize_model_name
from job_orchestrator import dry_run_job, query_job_alerts, query_job_definitions, query_job_runs, run_job
from services.ai_retrieval_service import (
    AI_RETRIEVAL_ENABLED,
    AI_RETRIEVAL_SHADOW_MODE,
    ensure_retrieval_tables as ai_retrieval_ensure_tables,
)
from services.agent_service.multi_role_v3 import ensure_multi_role_v3_tables
from services.agent_service.chief_roundtable_v1 import ensure_roundtable_tables as _ensure_roundtable_tables
from services.data_readiness_service import build_data_readiness_runtime_deps

from backend.layers import (
assert_layer_write_allowed,
build_layered_route_deps,
is_api_method_allowed,
list_api_layer_contracts,
)
from backend.routes import agent_governance as agent_governance_routes
from backend.routes import agents as agents_routes
from backend.routes import ai_retrieval as ai_retrieval_routes
from backend.routes import analytics as analytics_routes
from backend.routes import chatrooms as chatroom_routes
from backend.routes import decision as decision_routes
from backend.routes import funnel as funnel_routes
from backend.routes import llm_quick_insight as llm_quick_insight_routes
from backend.routes import macro_regime as macro_regime_routes
from backend.routes import market as market_routes
from backend.routes import news as news_routes
from backend.routes import portfolio as portfolio_routes
from backend.routes import portfolio_allocation as portfolio_allocation_routes
from backend.routes import quant_factors as quant_factor_routes
from backend.routes import roundtable as roundtable_routes
from backend.routes import signals as signal_routes
from backend.routes import stocks as stock_routes
from backend.routes import system as system_routes
from services.system.llm_providers_admin import (
create_llm_provider,
delete_llm_provider,
list_llm_providers,
test_model_llm_providers,
test_one_llm_provider,
update_default_rate_limit,
update_llm_provider,
get_multi_role_v2_policies,
update_multi_role_v2_policies,
)

from backend.http_server import api_catalog
from backend.http_server import auth_users
from backend.http_server import config
from backend.http_server import legacy_queries
from backend.http_server import llm_workbench
from backend.http_server import multi_role_runtime
from backend.http_server import rbac
from backend.http_server import roundtable_api
from backend.http_server import security

# Populate handler module globals for ApiHandler / _route_deps (same as monolithic server.py).
from backend.http_server.api_catalog import *  # noqa: F403
from backend.http_server.security import *  # noqa: F403
from backend.http_server.rbac import *  # noqa: F403
from backend.http_server.auth_users import *  # noqa: F403
from backend.http_server.legacy_queries import *  # noqa: F403
from backend.http_server.llm_workbench import *  # noqa: F403
from backend.http_server.multi_role_runtime import *  # noqa: F403
from backend.http_server.roundtable_api import *  # noqa: F403

from backend.http_server.config import (  # noqa: E402
    AUTH_PUBLIC_API_PATHS,
    DB_PATH,
    ENABLE_QUANT_FACTORS,
    HOST,
    PORT,
    RBAC_DYNAMIC_ENFORCED,
    WEB_DIST_DIR,
)

class ApiHandler(BaseHTTPRequestHandler):
    def _request_params(self) -> dict[str, list[str]]:
        return parse_qs(urlparse(self.path).query)

    def _request_is_protected(self) -> bool:
        parsed = urlparse(self.path)
        return _request_is_protected(parsed.path, self.command, self._request_params())

    def _cors_origin_for_request(self) -> str:
        origin = _normalize_origin(self.headers.get("Origin", ""))
        if not origin:
            return "" if self._request_is_protected() else "*"
        if self._request_is_protected():
            host = self.headers.get("Host", "")
            return origin if _origin_allowed(origin, host) else ""
        return "*"

    def _send_cors_headers(self):
        cors_origin = self._cors_origin_for_request()
        if not cors_origin:
            return
        self.send_header("Access-Control-Allow-Origin", cors_origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Admin-Token")
        if cors_origin != "*":
            self.send_header("Vary", "Origin")

    def _extract_admin_token(self) -> str:
        auth_header = (self.headers.get("Authorization", "") or "").strip()
        if auth_header.lower().startswith("bearer "):
            return auth_header[7:].strip()
        return (self.headers.get("X-Admin-Token", "") or "").strip()

    def _configured_admin_token(self) -> str:
        return (BACKEND_ADMIN_TOKEN or "").strip()

    def _resolve_auth_context(self) -> dict:
        token = self._extract_admin_token()
        configured = self._configured_admin_token()
        if configured and token and secrets.compare_digest(token, configured):
            return {
                "authenticated": True,
                "auth_mode": "admin_token",
                "is_admin": True,
                "user": {
                    "id": 0,
                    "username": "system_admin",
                    "display_name": "System Admin",
                    "role": "admin",
                    "tier": "admin",
                    "email_verified": True,
                },
            }
        user = _validate_auth_session(token) if token else None
        return {
            "authenticated": bool(user),
            "auth_mode": "account_session" if user else "anonymous",
            "is_admin": bool(user and str(user.get("role") or "") == "admin"),
            "user": user,
        }

    def _admin_token_required(self) -> bool:
        return bool(self._configured_admin_token()) or _active_auth_users_count() > 0

    def _token_valid(self, token: str) -> bool:
        configured_token = self._configured_admin_token()
        normalized = (token or "").strip()
        if configured_token and normalized and secrets.compare_digest(normalized, configured_token):
            return True
        return _validate_auth_session(normalized) is not None

    def _requires_auth_for_path(self, path: str) -> bool:
        if not path.startswith("/api/"):
            return False
        if path in AUTH_PUBLIC_API_PATHS:
            return False
        return self._admin_token_required()

    def _has_permission(self, auth_ctx: dict, path: str, method: str = "GET") -> bool:
        if not path.startswith("/api/"):
            return True
        if path in AUTH_PUBLIC_API_PATHS:
            return True
        if path == "/api/navigation-groups":
            return True
        if auth_ctx.get("is_admin"):
            return True
        user = auth_ctx.get("user") or {}
        perms = set(_effective_permissions_for_user(user))
        if "*" in perms:
            return True

        def _has(perm: str) -> bool:
            return perm in perms

        method_upper = str(method or "GET").upper()
        if path.startswith("/api/agents"):
            if method_upper == "GET" and _agents_read_get_allowed(path):
                return _has("research_advanced")
            return _has("admin_system")
        if path.startswith("/api/agent-governance"):
            return _has("admin_system")

        if path.startswith("/api/system/") or path.startswith("/api/jobs") or path.startswith("/api/job-"):
            return _has("admin_system")
        if path.startswith("/api/auth/users") or path.startswith("/api/auth/user/") or path.startswith("/api/auth/sessions") or path.startswith("/api/auth/audit-logs") or path.startswith("/api/auth/invite"):
            return _has("admin_users")
        if path == "/api/llm/trend":
            return _has("trend_analyze")
        if path in {
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
            return _has("multi_role_analyze")
        if path.startswith("/api/llm/chief-roundtable"):
            return _has("multi_role_analyze")
        if path.startswith("/api/stocks") or path in {"/api/stock-detail", "/api/prices", "/api/minline", "/api/stock-scores", "/api/stock-scores/filters"}:
            return _has("stocks_advanced")
        if path.startswith("/api/macro/regime"):
            return _has("macro_advanced") or _has("research_advanced")
        if path.startswith("/api/macro"):
            return _has("macro_advanced")
        if path.startswith("/api/portfolio/allocation"):
            return _has("research_advanced")
        if path.startswith("/api/portfolio"):
            return _has("research_advanced") or _has("stocks_advanced")
        if path.startswith("/api/funnel"):
            return _has("research_advanced")
        if path.startswith("/api/market/conclusion"):
            return _has("research_advanced")
        if path == "/api/news/daily-summaries":
            return _has("daily_summary_read")
        if path in {"/api/news", "/api/news/sources"}:
            return _has("news_read")
        if path in {"/api/stock-news", "/api/stock-news/sources"}:
            return _has("stock_news_read")
        if path in {
            "/api/investment-signals",
            "/api/investment-signals/timeline",
            "/api/theme-hotspots",
            "/api/signal-state/timeline",
            "/api/signal-audit",
            "/api/signal-quality/config",
            "/api/signal-quality/rules/save",
            "/api/signal-quality/blocklist/save",
        }:
            return _has("signals_advanced")
        if path.startswith("/api/decision"):
            return _has("research_advanced") or _has("signals_advanced") or _has("stocks_advanced")
        if path.startswith("/api/chatrooms") or path.startswith("/api/wechat-chatlog"):
            return _has("chatrooms_advanced")
        if path in {"/api/reports", "/api/research-reports"} or path.startswith("/api/research/"):
            return _has("research_advanced")
        if path.startswith("/api/ai-retrieval"):
            return _has("research_advanced") or _has("multi_role_analyze") or _has("daily_summary_read")
        if path == "/api/source-monitor" or path == "/api/database-audit":
            return _has("admin_system")
        if path.startswith("/api/quant-factors"):
            return _has("research_advanced")
        return False

    def _client_is_private(self) -> bool:
        raw_ip = (self.client_address[0] if self.client_address else "") or ""
        try:
            addr = ipaddress.ip_address(raw_ip)
        except ValueError:
            return False
        return bool(addr.is_private or addr.is_loopback)

    def _reject_protected_request(self) -> bool:
        if not self._request_is_protected():
            return False

        origin = _normalize_origin(self.headers.get("Origin", ""))
        host = self.headers.get("Host", "")
        if origin and not _origin_allowed(origin, host):
            self._send_json({"error": f"当前来源不在允许列表中: {origin}"}, status=403)
            return True

        if not self._admin_token_required():
            # Dev/LAN default: if admin token is not configured, protected routes are open.
            return False

        token = self._extract_admin_token()
        if not token or not self._token_valid(token):
            self._send_json({"error": "缺少或无效的管理令牌"}, status=401)
            return True

        if not origin and not self._client_is_private():
            self._send_json({"error": "受保护接口仅允许可信来源访问"}, status=403)
            return True

        return False

    def _send_json(self, payload: dict, status: int = 200):
        clean_payload = _sanitize_json_value(payload)
        body = json.dumps(clean_payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._send_cors_headers()
        _req_start = getattr(self, "_request_started_at", None)
        if _req_start is not None:
            elapsed_ms = int((time.time() - _req_start) * 1000)
            self.send_header("X-Response-Time-Ms", str(elapsed_ms))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        if self._request_is_protected():
            origin = _normalize_origin(self.headers.get("Origin", ""))
            if not origin or not _origin_allowed(origin, self.headers.get("Host", "")):
                self._send_json({"error": "当前来源不被允许"}, status=403)
                return
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()

    def _agent_service_deps(self) -> dict:
        return build_agent_service_deps()

    def _safe_agent_service_deps(self) -> dict:
        try:
            return self._agent_service_deps()
        except Exception:
            return {}

    def _safe_layered_route_deps(self) -> dict:
        try:
            return build_layered_route_deps(
                build_stock_news_service_runtime_deps=build_stock_news_service_runtime_deps,
                query_news_sources=query_news_sources,
                query_news=query_news,
                build_reporting_service_deps=build_reporting_service_deps,
                build_chatrooms_service_runtime_deps=build_chatrooms_service_runtime_deps,
                build_signals_service_runtime_deps=build_signals_service_runtime_deps,
                build_quantaalpha_runtime_deps=build_quantaalpha_runtime_deps,
                build_decision_runtime_deps=build_decision_runtime_deps,
                build_data_readiness_runtime_deps=lambda: build_data_readiness_runtime_deps(
                    sqlite3_module=sqlite3,
                    db_path=str(DB_PATH),
                ),
                roundtable_create=_roundtable_create,
                roundtable_get=_roundtable_get,
                roundtable_list=_roundtable_list,
                enable_quant_factors=ENABLE_QUANT_FACTORS,
                build_info=_build_info_payload,
                permission_matrix=_role_permission_matrix,
                effective_permissions_for_user=_effective_permissions_for_user,
                get_navigation_groups=_get_navigation_groups,
                get_dynamic_rbac_payload=_get_dynamic_rbac_payload,
                rbac_dynamic_enforced=RBAC_DYNAMIC_ENFORCED,
                llm_provider_admin_deps={
                    "list_llm_providers": list_llm_providers,
                    "create_llm_provider": create_llm_provider,
                    "update_llm_provider": update_llm_provider,
                    "delete_llm_provider": delete_llm_provider,
                    "test_one_llm_provider": test_one_llm_provider,
                    "test_model_llm_providers": test_model_llm_providers,
                    "update_default_rate_limit": update_default_rate_limit,
                    "get_multi_role_v2_policies": get_multi_role_v2_policies,
                    "update_multi_role_v2_policies": update_multi_role_v2_policies,
                },
                ai_retrieval_deps={
                    "ai_retrieval_enabled": AI_RETRIEVAL_ENABLED,
                    "ai_retrieval_shadow_mode": AI_RETRIEVAL_SHADOW_MODE,
                    "ai_retrieval_search": ai_retrieval_search,
                    "ai_retrieval_context": ai_retrieval_context,
                    "ai_retrieval_sync": ai_retrieval_sync,
                    "ai_retrieval_metrics": ai_retrieval_metrics,
                },
                frontend_dist_exists=bool((WEB_DIST_DIR / "index.html").exists()),
                frontend_url=f"http://{self.headers.get('Host', f'127.0.0.1:{PORT}')}/",
            )
        except Exception:
            return {}

    def _safe_decision_route_deps(self) -> dict:
        try:
            return build_decision_runtime_deps()
        except Exception:
            return {}

    def _route_deps(self) -> dict:
        return {
            "api_endpoints_catalog": API_ENDPOINTS_CATALOG,
            "api_layer_contracts": list_api_layer_contracts,
            "assert_write_allowed": assert_layer_write_allowed,
            "build_info": _build_info_payload,
            "permission_matrix": _role_permission_matrix,
            "effective_permissions_for_user": _effective_permissions_for_user,
            "get_navigation_groups": _get_navigation_groups,
            "get_dynamic_rbac_payload": _get_dynamic_rbac_payload,
            "rbac_dynamic_enforced": RBAC_DYNAMIC_ENFORCED,
            "db_label": db_label,
            "admin_token_required": self._admin_token_required,
            "token_valid": self._token_valid,
            "extract_admin_token": self._extract_admin_token,
            "validate_auth_session": _validate_auth_session,
            "register_auth_user": _register_auth_user,
            "login_auth_user": _login_auth_user,
            "verify_email_code": _verify_email_code,
            "resend_email_verification": _resend_email_verification,
            "forgot_password": _forgot_password,
            "reset_password_with_code": _reset_password_with_code,
            "create_invite_code": _create_invite_code,
            "revoke_auth_session": _revoke_auth_session,
            "active_auth_users_count": _active_auth_users_count,
            "consume_trend_daily_quota": _consume_trend_daily_quota,
            "get_trend_daily_quota_status": _get_trend_daily_quota_status,
            "consume_multi_role_daily_quota": _consume_multi_role_daily_quota,
            "get_multi_role_daily_quota_status": _get_multi_role_daily_quota_status,
            "ensure_auth_tables": _ensure_auth_tables,
            "record_auth_audit": _record_auth_audit,
            "query_auth_users": _query_auth_users,
            "update_auth_user": _update_auth_user,
            "admin_reset_user_password": _admin_reset_user_password,
            "admin_reset_user_trend_quota": _admin_reset_user_trend_quota,
            "admin_reset_user_multi_role_quota": _admin_reset_user_multi_role_quota,
            "admin_reset_quota_batch": _admin_reset_quota_batch,
            "get_auth_role_policies": _get_auth_role_policies,
            "update_auth_role_policy": _update_auth_role_policy,
            "reset_auth_role_policies_to_default": _reset_auth_role_policies_to_default,
            "query_auth_sessions": _query_auth_sessions,
            "revoke_auth_session_by_id": _revoke_auth_session_by_id,
            "revoke_auth_sessions_by_user": _revoke_auth_sessions_by_user,
            "query_auth_audit_logs": _query_auth_audit_logs,
            "DEFAULT_LLM_MODEL": DEFAULT_LLM_MODEL,
            "DB_PATH": DB_PATH,
            "sqlite3": sqlite3,
            "normalize_model_name": normalize_model_name,
            "_resolve_roles": _resolve_roles,
            "query_job_definitions": query_job_definitions,
            "query_job_runs": query_job_runs,
            "query_job_alerts": query_job_alerts,
            "dry_run_job": dry_run_job,
            "run_job": run_job,
            "query_dashboard": query_dashboard,
            "query_source_monitor": query_source_monitor,
            "query_database_audit": query_database_audit,
            "query_database_health": query_database_health,
            "query_stock_detail": query_stock_detail,
            "query_stocks": query_stocks,
            "query_stock_filters": query_stock_filters,
            "query_stock_score_filters": query_stock_score_filters,
            "query_stock_scores": query_stock_scores,
            "query_prices": query_prices,
            "query_minline": query_minline,
            "query_multi_role_analysis_history": query_multi_role_analysis_history,
            **self._safe_decision_route_deps(),
            **self._safe_agent_service_deps(),
            "start_async_multi_role_job": start_async_multi_role_job,
            "get_async_multi_role_job": get_async_multi_role_job,
            "start_async_multi_role_v2_job": start_async_multi_role_v2_job,
            "get_async_multi_role_v2_job": get_async_multi_role_v2_job,
            "decide_async_multi_role_v2_job": decide_async_multi_role_v2_job,
            "retry_async_multi_role_v2_aggregate": retry_async_multi_role_v2_aggregate,
            "find_today_reusable_multi_role_v2_job": find_today_reusable_multi_role_v2_job,
            "start_multi_role_v3_job": start_multi_role_v3_job,
            "get_multi_role_v3_job_by_id": get_multi_role_v3_job_by_id,
            "decide_multi_role_v3_job": decide_multi_role_v3_job,
            "action_multi_role_v3_job": action_multi_role_v3_job,
            **self._safe_layered_route_deps(),
        }

    def _enforce_api_contract_method(self, parsed, method: str) -> bool:
        path = str(getattr(parsed, "path", "") or "")
        if not path.startswith("/api/"):
            return False
        if is_api_method_allowed(path=path, method=method):
            return False
        self._send_json(
            {
                "error": "Method Not Allowed",
                "path": path,
                "method": str(method or "").upper(),
            },
            status=405,
        )
        return True

    def _serve_frontend_static(self, parsed) -> bool:
        path = str(parsed.path or "").strip()
        if not path or path.startswith("/api/") or path.startswith("/ws/"):
            return False
        index_path = WEB_DIST_DIR / "index.html"
        if not index_path.exists():
            return False

        raw_path = path.split("?", 1)[0].split("#", 1)[0]
        norm = os.path.normpath(unquote(raw_path)).lstrip("/")
        root = WEB_DIST_DIR.resolve()
        candidate = (root / norm).resolve()
        if not str(candidate).startswith(str(root)):
            candidate = index_path.resolve()

        static_suffixes = {
            ".js",
            ".css",
            ".map",
            ".svg",
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".ico",
            ".webp",
            ".woff",
            ".woff2",
            ".ttf",
            ".json",
            ".txt",
        }
        suffix = Path(raw_path).suffix.lower()
        if candidate.exists() and candidate.is_file():
            self._send_static_file(candidate)
            return True
        if raw_path.startswith("/assets/") or suffix in static_suffixes:
            self._send_json({"error": "Not Found"}, status=404)
            return True
        self._send_static_file(index_path)
        return True

    def _send_static_file(self, file_path: Path) -> None:
        try:
            data = file_path.read_bytes()
        except Exception as exc:
            self._send_json({"error": f"静态资源读取失败: {exc}"}, status=500)
            return
        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        if file_path.name == "index.html":
            self.send_header("Cache-Control", "no-cache")
        else:
            self.send_header("Cache-Control", "public, max-age=3600")
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        self._request_started_at = time.time()
        parsed = urlparse(self.path)
        if self._enforce_api_contract_method(parsed, "POST"):
            return
        if self._reject_protected_request():
            return
        auth_ctx = self._resolve_auth_context()
        if self._requires_auth_for_path(parsed.path) and not auth_ctx.get("authenticated"):
            self._send_json(
                {
                    "error": "请先登录后再访问该接口",
                    "code": "AUTH_REQUIRED",
                    "path": parsed.path,
                    "hint": "请先完成账号登录，若已登录请重新登录刷新会话。",
                },
                status=401,
            )
            return
        if self._requires_auth_for_path(parsed.path) and not self._has_permission(auth_ctx, parsed.path, "POST"):
            self._send_json(_permission_denied_payload(parsed.path), status=403)
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8", errors="ignore") or "{}")
        except Exception:
            self._send_json({"error": "请求体必须是 JSON"}, status=400)
            return

        deps = self._route_deps()
        deps["auth_context"] = auth_ctx
        if system_routes.dispatch_post(self, parsed, payload, deps):
            return
        if stock_routes.dispatch_post(self, parsed, payload, deps):
            return
        if quant_factor_routes.dispatch_post(self, parsed, payload, deps):
            return
        if decision_routes.dispatch_post(self, parsed, payload, deps):
            return
        if roundtable_routes.dispatch_post(self, parsed, payload, deps):
            return
        if ai_retrieval_routes.dispatch_post(self, parsed, payload, deps):
            return
        if funnel_routes.dispatch_post(self, parsed, payload, deps):
            return
        if portfolio_routes.dispatch_post(self, parsed, payload, deps):
            return
        if macro_regime_routes.dispatch_post(self, parsed, payload, deps):
            return
        if portfolio_allocation_routes.dispatch_post(self, parsed, payload, deps):
            return
        if llm_quick_insight_routes.dispatch_post(self, parsed, payload, deps):
            return
        if analytics_routes.dispatch_post(self, parsed, payload, deps):
            return
        if agents_routes.dispatch_post(self, parsed, payload, deps):
            return
        if agent_governance_routes.dispatch_post(self, parsed, payload, deps):
            return

        self._send_json({"error": "Not Found"}, status=404)

    def do_PATCH(self):
        self._request_started_at = time.time()
        parsed = urlparse(self.path)
        if self._enforce_api_contract_method(parsed, "PATCH"):
            return
        if self._reject_protected_request():
            return
        auth_ctx = self._resolve_auth_context()
        if self._requires_auth_for_path(parsed.path) and not auth_ctx.get("authenticated"):
            self._send_json(
                {
                    "error": "请先登录后再访问该接口",
                    "code": "AUTH_REQUIRED",
                    "path": parsed.path,
                    "hint": "请先完成账号登录，若已登录请重新登录刷新会话。",
                },
                status=401,
            )
            return
        if self._requires_auth_for_path(parsed.path) and not self._has_permission(auth_ctx, parsed.path, "PATCH"):
            self._send_json(_permission_denied_payload(parsed.path), status=403)
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8", errors="ignore") or "{}")
        except Exception:
            self._send_json({"error": "请求体必须是 JSON"}, status=400)
            return
        deps = self._route_deps()
        deps["auth_context"] = auth_ctx
        if portfolio_routes.dispatch_patch(self, parsed, payload, deps):
            return
        if macro_regime_routes.dispatch_patch(self, parsed, payload, deps):
            return
        self._send_json({"error": "Not Found"}, status=404)

    def do_DELETE(self):
        self._request_started_at = time.time()
        parsed = urlparse(self.path)
        if self._enforce_api_contract_method(parsed, "DELETE"):
            return
        if self._reject_protected_request():
            return
        auth_ctx = self._resolve_auth_context()
        if self._requires_auth_for_path(parsed.path) and not auth_ctx.get("authenticated"):
            self._send_json(
                {
                    "error": "请先登录后再访问该接口",
                    "code": "AUTH_REQUIRED",
                    "path": parsed.path,
                    "hint": "请先完成账号登录，若已登录请重新登录刷新会话。",
                },
                status=401,
            )
            return
        if self._requires_auth_for_path(parsed.path) and not self._has_permission(auth_ctx, parsed.path, "DELETE"):
            self._send_json(_permission_denied_payload(parsed.path), status=403)
            return
        deps = self._route_deps()
        deps["auth_context"] = auth_ctx
        if portfolio_routes.dispatch_delete(self, parsed, deps):
            return
        self._send_json({"error": "Not Found"}, status=404)

    def do_GET(self):
        self._request_started_at = time.time()
        parsed = urlparse(self.path)
        if self._enforce_api_contract_method(parsed, "GET"):
            return
        host = self.headers.get("Host", f"127.0.0.1:{PORT}").split(":")[0]
        if self._reject_protected_request():
            return
        auth_ctx = self._resolve_auth_context()
        if self._requires_auth_for_path(parsed.path) and not auth_ctx.get("authenticated"):
            self._send_json(
                {
                    "error": "请先登录后再访问该接口",
                    "code": "AUTH_REQUIRED",
                    "path": parsed.path,
                    "hint": "请先完成账号登录，若已登录请重新登录刷新会话。",
                },
                status=401,
            )
            return
        if self._requires_auth_for_path(parsed.path) and not self._has_permission(auth_ctx, parsed.path, "GET"):
            self._send_json(_permission_denied_payload(parsed.path), status=403)
            return

        deps = self._route_deps()
        deps["auth_context"] = auth_ctx
        if system_routes.dispatch_get(self, parsed, host, deps):
            return
        if stock_routes.dispatch_get(self, parsed, deps):
            return
        if news_routes.dispatch_get(self, parsed, deps):
            return
        if quant_factor_routes.dispatch_get(self, parsed, deps):
            return
        if decision_routes.dispatch_get(self, parsed, deps):
            return
        if roundtable_routes.dispatch_get(self, parsed, deps):
            return
        if chatroom_routes.dispatch_get(self, parsed, deps):
            return
        if signal_routes.dispatch_get(self, parsed, deps):
            return
        if ai_retrieval_routes.dispatch_get(self, parsed, deps):
            return
        if market_routes.dispatch_get(self, parsed, deps):
            return
        if funnel_routes.dispatch_get(self, parsed, deps):
            return
        if portfolio_routes.dispatch_get(self, parsed, deps):
            return
        if macro_regime_routes.dispatch_get(self, parsed, deps):
            return
        if portfolio_allocation_routes.dispatch_get(self, parsed, deps):
            return
        if agents_routes.dispatch_get(self, parsed, deps):
            return
        if agent_governance_routes.dispatch_get(self, parsed, deps):
            return

        if self._serve_frontend_static(parsed):
            return

        self._send_json({"error": "Not Found"}, status=404)

