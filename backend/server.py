#!/usr/bin/env python3
from __future__ import annotations

import bisect
import hashlib
import ipaddress
import json
import math
import os
import re
import secrets
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db_compat as sqlite3
from db_compat import assert_database_ready, cache_get_json, cache_set_json, db_label, get_redis_client
from job_orchestrator import dry_run_job, query_job_alerts, query_job_definitions, query_job_runs, run_job
from llm_gateway import (
    DEFAULT_LLM_MODEL,
    chat_completion_with_fallback,
    normalize_model_name,
    normalize_temperature_for_model,
)
from realtime_streams import publish_app_event
from runtime_secrets import BACKEND_ADMIN_TOKEN, BACKEND_ALLOWED_ORIGINS
from services.execution import pre_trade_check
from services.agent_service import (
    build_backend_runtime_deps,
    build_multi_role_context as agent_build_multi_role_context,
    build_multi_role_prompt as agent_build_multi_role_prompt,
    build_trend_features as agent_build_trend_features,
    call_llm_multi_role as agent_call_llm_multi_role,
    call_llm_trend as agent_call_llm_trend,
    cleanup_async_jobs as agent_cleanup_async_jobs,
    create_async_multi_role_job as agent_create_async_multi_role_job,
    get_async_multi_role_job as agent_get_async_multi_role_job,
    run_async_multi_role_job as agent_run_async_multi_role_job,
    serialize_async_job as agent_serialize_async_job,
    start_async_multi_role_job as agent_start_async_multi_role_job,
    split_multi_role_analysis as agent_split_multi_role_analysis,
)
from services.reporting import build_reporting_runtime_deps
from services.reporting import (
    cleanup_async_jobs as reporting_cleanup_async_jobs,
    create_async_daily_summary_job as reporting_create_async_daily_summary_job,
    generate_daily_summary as reporting_generate_daily_summary,
    get_async_daily_summary_job as reporting_get_async_daily_summary_job,
    get_daily_summary_by_date as reporting_get_daily_summary_by_date,
    query_research_reports as reporting_query_research_reports,
    query_news_daily_summaries as reporting_query_news_daily_summaries,
    run_async_daily_summary_job as reporting_run_async_daily_summary_job,
    serialize_async_daily_summary_job as reporting_serialize_async_daily_summary_job,
    start_async_daily_summary_job as reporting_start_async_daily_summary_job,
)
from services.chatrooms_service import (
    build_chatrooms_service_deps,
    fetch_single_chatroom_now as chatrooms_fetch_single_now,
    query_chatroom_candidate_pool as chatrooms_query_candidate_pool,
    query_chatroom_investment_analysis as chatrooms_query_investment_analysis,
    query_chatroom_overview as chatrooms_query_overview,
    query_wechat_chatlog as chatrooms_query_wechat_chatlog,
)
from services.signals_service import build_signals_runtime_deps
from services.notifications import build_notification_payload, notify_with_wecom
from services.quantaalpha_service import build_quantaalpha_service_runtime_deps
from services.stock_detail_service import (
    build_capital_flow_summary as stock_detail_build_capital_flow_summary,
    build_financial_summary as stock_detail_build_financial_summary,
    build_fx_context as stock_detail_build_fx_context,
    build_governance_summary as stock_detail_build_governance_summary,
    build_macro_context as stock_detail_build_macro_context,
    build_rate_spread_context as stock_detail_build_rate_spread_context,
    build_risk_summary as stock_detail_build_risk_summary,
    build_stock_detail_runtime_deps,
    build_stock_news_summary as stock_detail_build_stock_news_summary,
    build_valuation_summary as stock_detail_build_valuation_summary,
)
from services.stock_news_service import (
    build_stock_news_service_deps,
    fetch_stock_news_now as stock_news_fetch_now,
    query_stock_news as stock_news_query,
    query_stock_news_sources as stock_news_query_sources,
    score_stock_news_now as stock_news_score_now,
)
from services.system.llm_providers_admin import (
    create_llm_provider,
    delete_llm_provider,
    list_llm_providers,
    test_model_llm_providers,
    test_one_llm_provider,
    update_default_rate_limit,
    update_llm_provider,
)
from skills.strategies import load_strategy_template_text
from backend.routes import chatrooms as chatroom_routes
from backend.routes import news as news_routes
from backend.routes import quant_factors as quant_factor_routes
from backend.routes import signals as signal_routes
from backend.routes import stocks as stock_routes
from backend.routes import system as system_routes

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8000"))
DB_PATH = ROOT_DIR / "stock_codes.db"
SERVER_STARTED_AT_UTC = datetime.now(timezone.utc).isoformat()


def _resolve_build_id() -> str:
    env_id = str(os.getenv("BACKEND_BUILD_ID", "") or "").strip()
    if env_id:
        return env_id
    try:
        rev = (
            subprocess.check_output(
                ["git", "-C", str(ROOT_DIR), "rev-parse", "--short", "HEAD"],
                stderr=subprocess.DEVNULL,
            )
            .decode("utf-8", errors="ignore")
            .strip()
        )
        if rev:
            return f"git-{rev}"
    except Exception:
        pass
    return f"dev-{int(time.time())}"


BUILD_ID = _resolve_build_id()
DEFAULT_MULTI_ROLES = [
    "宏观经济分析师",
    "股票分析师",
    "国际资本分析师",
    "汇率分析师",
]
ROLE_PROFILES = {
    "宏观经济分析师": {
        "focus": "经济周期、增长与通胀、政策方向、利率与信用环境",
        "framework": "总量-政策-传导链条（宏观变量 -> 行业/资产定价）",
        "indicators": ["GDP/PMI趋势", "通胀与实际利率", "信用扩张/社融", "政策预期变化"],
        "risk_bias": "偏重宏观拐点与政策误判风险",
    },
    "股票分析师": {
        "focus": "价格趋势、量价结构、估值与交易拥挤度",
        "framework": "趋势-动量-波动-成交量联合判断",
        "indicators": ["MA结构", "涨跌幅/回撤", "成交量变化", "波动率", "关键支撑阻力"],
        "risk_bias": "偏重交易层面的失真与假突破风险",
    },
    "国际资本分析师": {
        "focus": "跨境资金流、风险偏好、全球资产配置偏移",
        "framework": "全球流动性-风险偏好-资金流向三段式",
        "indicators": ["北向/南向资金行为", "美债收益率变化", "全球风险资产相关性", "地缘风险溢价"],
        "risk_bias": "偏重外部冲击与资金风格切换风险",
    },
    "汇率分析师": {
        "focus": "汇率方向、利差变化、汇率对盈利与估值的影响",
        "framework": "利差-汇率-资产定价传导",
        "indicators": ["美元指数趋势", "中美利差", "人民币汇率波动", "汇率敏感行业影响"],
        "risk_bias": "偏重汇率波动放大财务与估值波动的风险",
    },
    "行业分析师": {
        "focus": "行业景气、供需结构、竞争格局与政策监管",
        "framework": "景气周期-竞争格局-盈利能力",
        "indicators": ["行业增速", "价格/库存", "龙头份额变化", "监管与政策红利"],
        "risk_bias": "偏重行业β变化与景气反转风险",
    },
    "风险控制官": {
        "focus": "组合回撤、尾部风险、情景压力测试",
        "framework": "仓位-波动-回撤约束",
        "indicators": ["最大回撤", "波动率阈值", "流动性风险", "事件冲击情景"],
        "risk_bias": "偏重先活下来再优化收益",
    },
}
ENABLE_AGENT_RISK_PRECHECK = str(os.getenv("ENABLE_AGENT_RISK_PRECHECK", "1")).strip().lower() in {"1", "true", "yes", "on"}
ENABLE_AGENT_NOTIFICATIONS = str(os.getenv("ENABLE_AGENT_NOTIFICATIONS", "0")).strip().lower() in {"1", "true", "yes", "on"}
ENABLE_REPORTING_NOTIFICATIONS = str(os.getenv("ENABLE_REPORTING_NOTIFICATIONS", "0")).strip().lower() in {"1", "true", "yes", "on"}
ENABLE_SKILLS_TEMPLATE_PROMPTS = str(os.getenv("ENABLE_SKILLS_TEMPLATE_PROMPTS", "1")).strip().lower() in {"1", "true", "yes", "on"}
ENABLE_QUANT_FACTORS = str(os.getenv("ENABLE_QUANT_FACTORS", "1")).strip().lower() in {"1", "true", "yes", "on"}
WECOM_WEBHOOK_URL = str(os.getenv("WECOM_BOT_WEBHOOK", "")).strip()
ASYNC_JOB_TTL_SECONDS = 3600
ASYNC_MULTI_ROLE_JOBS: dict[str, dict] = {}
ASYNC_MULTI_ROLE_LOCK = threading.Lock()
ASYNC_DAILY_SUMMARY_JOBS: dict[str, dict] = {}
ASYNC_DAILY_SUMMARY_LOCK = threading.Lock()
TMP_DIR = Path("/tmp")
AUTH_SESSION_DAYS = int(os.getenv("AUTH_SESSION_DAYS", "30") or "30")
AUTH_LOCK_THRESHOLD = int(os.getenv("AUTH_LOCK_THRESHOLD", "5") or "5")
AUTH_LOCK_MINUTES = int(os.getenv("AUTH_LOCK_MINUTES", "15") or "15")
AUTH_USERS_COUNT_CACHE_SECONDS = 15
AUTH_USERS_COUNT_CACHE: dict[str, float | int] = {"value": -1, "expires_at": 0.0}
AUTH_USERS_COUNT_LOCK = threading.Lock()
STOCK_SCORE_CACHE_TTL_SECONDS = 300
STOCK_SCORE_CACHE: dict[str, object] = {
    "generated_at": 0.0,
    "items": [],
    "summary": {},
}
REDIS_CACHE_TTL_SOURCE_MONITOR = 30
REDIS_CACHE_TTL_DASHBOARD = 30
REDIS_CACHE_TTL_PRICES = 180
REDIS_CACHE_TTL_STOCKS = 30
REDIS_CACHE_TTL_SIGNALS = 30
REDIS_CACHE_TTL_THEMES = 30
PRICES_DEFAULT_LOOKBACK_DAYS = 30
PRICES_MAX_PAGE_SIZE = 100
AUDIT_REPORT_PATH = ROOT_DIR / "docs" / "database_audit_report.md"
PROTECTED_POST_PATHS = {
    "/api/signal-quality/rules/save",
    "/api/signal-quality/blocklist/save",
    "/api/auth/quota/reset-batch",
}
PROTECTED_GET_PATHS = {
    "/api/jobs",
    "/api/job-runs",
    "/api/job-alerts",
    "/api/jobs/trigger",
    "/api/jobs/dry-run",
    "/api/chatrooms/fetch",
    "/api/stock-news/fetch",
    "/api/stock-news/score",
    "/api/news/daily-summaries/generate",
    "/api/llm/multi-role/start",
}
DEFAULT_ALLOWED_ADMIN_ORIGINS = {
    "http://127.0.0.1:8077",
    "http://localhost:8077",
    "http://127.0.0.1:8080",
    "http://localhost:8080",
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:4173",
    "http://localhost:4173",
    "http://tianbo.asia:6273",
    "https://tianbo.asia:6273",
}
TRUSTED_FRONTEND_PORTS = {"8077", "8080", "5173", "4173"}
AUTH_PUBLIC_API_PATHS = {
    "/api/health",
    "/api/auth/status",
    "/api/auth/login",
    "/api/auth/register",
    "/api/auth/verify-email",
    "/api/auth/send-verify-code",
    "/api/auth/forgot-password",
    "/api/auth/reset-password",
    "/api/auth/logout",
}
LIMITED_ALLOWED_PATH_PREFIXES = (
    "/api/news",
    "/api/stock-news",
)
LIMITED_ALLOWED_EXACT_PATHS = {
    "/api/news",
    "/api/news/sources",
    "/api/news/daily-summaries",
    "/api/stock-news",
    "/api/stock-news/sources",
    "/api/llm/trend",
}
ROLE_PERMISSIONS = {
    "admin": {"*"},
    "pro": {"*"},
    "limited": {"news_read", "stock_news_read", "daily_summary_read", "trend_analyze", "multi_role_analyze"},
}
TREND_DAILY_LIMIT_BY_ROLE = {
    "limited": 30,
}
MULTI_ROLE_DAILY_LIMIT_BY_ROLE = {
    "limited": 10,
}


def _role_permission_matrix() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for role, perms in ROLE_PERMISSIONS.items():
        out[str(role)] = sorted(str(x) for x in perms)
    return out


def _effective_permissions_for_user(user: dict | None) -> list[str]:
    if not user:
        return []
    role = str((user or {}).get("role") or (user or {}).get("tier") or "limited").lower()
    perms = ROLE_PERMISSIONS.get(role, set())
    return sorted(str(x) for x in perms)


def _build_info_payload() -> dict:
    return {
        "build_id": BUILD_ID,
        "port": PORT,
        "pid": os.getpid(),
        "started_at": SERVER_STARTED_AT_UTC,
    }

API_ENDPOINTS_CATALOG = {
    "health": "/api/health",
    "auth_status": "/api/auth/status",
    "auth_permissions": "/api/auth/permissions",
    "auth_register": "/api/auth/register",
    "auth_verify_email": "/api/auth/verify-email",
    "auth_send_verify_code": "/api/auth/send-verify-code",
    "auth_login": "/api/auth/login",
    "auth_forgot_password": "/api/auth/forgot-password",
    "auth_reset_password": "/api/auth/reset-password",
    "auth_logout": "/api/auth/logout",
    "auth_invite_create": "/api/auth/invite/create",
    "auth_invites": "/api/auth/invites?keyword=&active=&page=1&page_size=20",
    "auth_invite_update": "/api/auth/invite/update",
    "auth_invite_delete": "/api/auth/invite/delete",
    "auth_users_summary": "/api/auth/users/summary",
    "auth_users": "/api/auth/users?keyword=&role=&active=&page=1&page_size=20",
    "auth_user_update": "/api/auth/user/update",
    "auth_user_reset_password": "/api/auth/user/reset-password",
    "auth_user_reset_trend_quota": "/api/auth/user/reset-trend-quota",
    "auth_user_reset_multi_role_quota": "/api/auth/user/reset-multi-role-quota",
    "auth_quota_reset_batch": "/api/auth/quota/reset-batch",
    "auth_sessions": "/api/auth/sessions?keyword=&user_id=&page=1&page_size=20",
    "auth_session_revoke": "/api/auth/session/revoke",
    "auth_user_sessions_revoke": "/api/auth/user/revoke-sessions",
    "auth_audit_logs": "/api/auth/audit-logs?keyword=&event_type=&result=&page=1&page_size=20",
    "dashboard": "/api/dashboard",
    "stock_detail": "/api/stock-detail?ts_code=000001.SZ&lookback=60",
    "stocks": "/api/stocks?keyword=&status=&market=&area=&page=1&page_size=20",
    "stocks_filters": "/api/stocks/filters",
    "stock_scores": "/api/stock-scores?keyword=&market=&area=&industry=&min_score=0&page=1&page_size=20&sort_by=total_score&sort_order=desc",
    "stock_scores_filters": "/api/stock-scores/filters",
    "stock_news": "/api/stock-news?ts_code=601100.SH&company_name=&keyword=&source=&finance_levels=极高,高,中&scored=&page=1&page_size=20",
    "stock_news_sources": "/api/stock-news/sources",
    "stock_news_score": "/api/stock-news/score?ts_code=601100.SH&limit=20&force=0",
    "wechat_chatlog": "/api/wechat-chatlog?talker=&sender_name=&keyword=&is_quote=&query_date_start=&query_date_end=&page=1&page_size=20",
    "chatroom_overview": "/api/chatrooms?keyword=&primary_category=&activity_level=&risk_level=&skip_realtime_monitor=&fetch_status=&page=1&page_size=20",
    "chatroom_fetch_now": "/api/chatrooms/fetch?room_id=<room_id>&mode=today|yesterday_and_today",
    "chatroom_investment_analysis": "/api/chatrooms/investment?keyword=&final_bias=&target_keyword=&page=1&page_size=20",
    "chatroom_candidate_pool": "/api/chatrooms/candidate-pool?keyword=&dominant_bias=&candidate_type=&page=1&page_size=20",
    "investment_signals": "/api/investment-signals?keyword=&signal_type=&signal_group=all|stock|non_stock&direction=&signal_status=&page=1&page_size=20",
    "investment_signal_timeline": "/api/investment-signals/timeline?signal_key=<signal_key>&page=1&page_size=20",
    "stock_news_fetch": "/api/stock-news/fetch?ts_code=601100.SH&company_name=&page_size=20&score=1",
    "news": "/api/news?source=&exclude_sources=cn_sina_7x24&exclude_source_prefixes=cn_sina_&keyword=&date_from=&date_to=&finance_levels=高,极高&page=1&page_size=20",
    "news_sources": "/api/news/sources",
    "news_daily_summaries": "/api/news/daily-summaries?summary_date=2026-03-25&source_filter=cn_sina_&model=GPT-5.4&page=1&page_size=20",
    "news_daily_summaries_generate": "/api/news/daily-summaries/generate",
    "news_daily_summaries_task": "/api/news/daily-summaries/task?job_id=<job_id>",
    "prices": "/api/prices?ts_code=000001.SZ&start_date=20260223&end_date=20260325&page=1&page_size=20",
    "minline": "/api/minline?ts_code=600114.SH&trade_date=20260325&page=1&page_size=240",
    "llm_trend": "/api/llm/trend?ts_code=000001.SZ&lookback=120",
    "llm_multi_role": "/api/llm/multi-role?ts_code=000001.SZ&lookback=120&roles=宏观经济分析师,股票分析师,国际资本分析师,汇率分析师",
    "llm_multi_role_start": "/api/llm/multi-role/start?ts_code=000001.SZ&lookback=120&roles=宏观经济分析师,股票分析师",
    "llm_multi_role_task": "/api/llm/multi-role/task?job_id=<job_id>",
    "macro_indicators": "/api/macro/indicators",
    "macro_series": "/api/macro?indicator_code=cn_cpi.nt_yoy&freq=M&period_start=202001&period_end=202512&page=1&page_size=200",
    "source_monitor": "/api/source-monitor",
    "database_audit": "/api/database-audit?refresh=0|1",
    "signal_audit": "/api/signal-audit?scope=7d|1d",
    "signal_quality_config": "/api/signal-quality/config",
    "signal_quality_rules_save": "/api/signal-quality/rules/save",
    "signal_quality_blocklist_save": "/api/signal-quality/blocklist/save",
    "job_alerts": "/api/job-alerts?job_key=&unresolved_only=1&limit=20",
    "job_dry_run": "/api/jobs/dry-run?job_key=<job_key>",
    "system_llm_providers": "/api/system/llm-providers",
    "system_llm_providers_create": "/api/system/llm-providers/create",
    "system_llm_providers_update": "/api/system/llm-providers/update",
    "system_llm_providers_delete": "/api/system/llm-providers/delete",
    "system_llm_providers_test_one": "/api/system/llm-providers/test-one",
    "system_llm_providers_test_model": "/api/system/llm-providers/test-model",
    "quant_factors_mine_start": "/api/quant-factors/mine/start",
    "quant_factors_backtest_start": "/api/quant-factors/backtest/start",
    "quant_factors_task": "/api/quant-factors/task?task_id=<task_id>",
    "quant_factors_results": "/api/quant-factors/results?task_type=&status=&page=1&page_size=20",
}


def _normalize_origin(origin: str) -> str:
    return (origin or "").strip().rstrip("/")


def _request_is_protected(path: str, method: str, params: dict[str, list[str]] | None = None) -> bool:
    normalized_method = (method or "").upper()
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
    elif path in {"/api/llm/multi-role", "/api/llm/multi-role/start", "/api/llm/multi-role/task"}:
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
    elif path.startswith("/api/system") or path.startswith("/api/jobs"):
        code = "AUTH_PERMISSION_DENIED_SYSTEM"
        hint = "该账号不可访问系统运维接口。"
    return {
        "error": "当前账号权限不足，请升级后使用该功能",
        "code": code,
        "path": path,
        "hint": hint,
    }


def resolve_signal_table(conn, scope: str) -> tuple[str, str]:
    scope = (scope or "").strip().lower()
    table_name = "investment_signal_tracker_7d"
    normalized_scope = "7d"
    if scope in {"1d", "1day", "one_day", "recent"}:
        table_name = "investment_signal_tracker_1d"
        normalized_scope = "1d"
    elif scope in {"", "7d", "7day", "seven_day"}:
        table_name = "investment_signal_tracker_7d"
        normalized_scope = "7d"
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()[0]
    if not table_exists:
        table_name = "investment_signal_tracker"
        normalized_scope = "main"
    return table_name, normalized_scope

def query_stocks(keyword: str, status: str, market: str, area: str, page: int, page_size: int):
    keyword = keyword.strip()
    status = status.strip().upper()
    market = market.strip()
    area = area.strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size
    cache_key = f"api:stocks:v1:kw={keyword}:status={status}:market={market}:area={area}:page={page}:size={page_size}"
    cached = cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    where_clauses = []
    params: list[object] = []

    if keyword:
        where_clauses.append("(ts_code LIKE ? OR symbol LIKE ? OR name LIKE ?)")
        kw = f"%{keyword}%"
        params.extend([kw, kw, kw])

    if status in {"L", "D", "P"}:
        where_clauses.append("list_status = ?")
        params.append(status)
    if market:
        where_clauses.append("market = ?")
        params.append(market)
    if area:
        where_clauses.append("area = ?")
        params.append(area)

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    count_sql = f"SELECT COUNT(*) FROM stock_codes{where_sql}"
    data_sql = (
        "SELECT ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status "
        f"FROM stock_codes{where_sql} ORDER BY ts_code LIMIT ? OFFSET ?"
    )

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    payload = {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "items": data,
    }
    cache_set_json(cache_key, payload, REDIS_CACHE_TTL_STOCKS)
    return payload


def query_stock_filters():
    conn = sqlite3.connect(DB_PATH)
    try:
        markets = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT market FROM stock_codes WHERE market IS NOT NULL AND market <> '' ORDER BY market"
            ).fetchall()
        ]
        areas = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT area FROM stock_codes WHERE area IS NOT NULL AND area <> '' ORDER BY area"
            ).fetchall()
        ]
    finally:
        conn.close()
    return {"markets": markets, "areas": areas}


def query_stock_score_filters():
    conn = sqlite3.connect(DB_PATH)
    try:
        markets = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT market FROM stock_codes WHERE list_status = 'L' AND market IS NOT NULL AND market <> '' ORDER BY market"
            ).fetchall()
        ]
        areas = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT area FROM stock_codes WHERE list_status = 'L' AND area IS NOT NULL AND area <> '' ORDER BY area"
            ).fetchall()
        ]
        industries = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT industry FROM stock_codes WHERE list_status = 'L' AND industry IS NOT NULL AND industry <> '' ORDER BY industry"
            ).fetchall()
        ]
    finally:
        conn.close()
    return {"markets": markets, "areas": areas, "industries": industries}


def _stock_score_weights():
    return {
        "trend_score": 0.20,
        "financial_score": 0.20,
        "valuation_score": 0.15,
        "capital_flow_score": 0.15,
        "event_score": 0.10,
        "news_score": 0.10,
        "risk_score": 0.10,
    }


def _load_stock_scores_from_table(
    keyword: str,
    market: str,
    area: str,
    industry: str,
    min_score: float,
    page: int,
    page_size: int,
    sort_by: str,
    sort_order: str,
):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_scores_daily'"
        ).fetchone()[0]
        if not table_exists:
            return None
        latest_score_date = conn.execute("SELECT MAX(score_date) FROM stock_scores_daily").fetchone()[0]
        if not latest_score_date:
            return None

        where_clauses = ["score_date = ?"]
        params: list[object] = [latest_score_date]
        keyword = (keyword or "").strip()
        market = (market or "").strip()
        area = (area or "").strip()
        industry = (industry or "").strip()
        if keyword:
            where_clauses.append("(ts_code LIKE ? OR symbol LIKE ? OR name LIKE ? OR industry LIKE ?)")
            kw = f"%{keyword}%"
            params.extend([kw, kw, kw, kw])
        if market:
            where_clauses.append("market = ?")
            params.append(market)
        if area:
            where_clauses.append("area = ?")
            params.append(area)
        if industry:
            where_clauses.append("industry = ?")
            params.append(industry)
        where_clauses.append("COALESCE(total_score, 0) >= ?")
        params.append(min_score)

        sortable = {
            "total_score",
            "industry_total_score",
            "trend_score",
            "industry_trend_score",
            "financial_score",
            "industry_financial_score",
            "valuation_score",
            "industry_valuation_score",
            "capital_flow_score",
            "industry_capital_flow_score",
            "event_score",
            "industry_event_score",
            "news_score",
            "industry_news_score",
            "risk_score",
            "industry_risk_score",
            "ts_code",
            "name",
            "latest_trade_date",
        }
        sort_by = sort_by if sort_by in sortable else "total_score"
        sort_order = "ASC" if (sort_order or "").lower() == "asc" else "DESC"
        order_sql = f"ORDER BY {sort_by} {sort_order}, ts_code ASC"

        where_sql = " WHERE " + " AND ".join(where_clauses)
        total = conn.execute(
            f"SELECT COUNT(*) FROM stock_scores_daily{where_sql}",
            params,
        ).fetchone()[0]
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT
                score_date, ts_code, name, symbol, market, area, industry, industry_rank, industry_count, score_grade, industry_score_grade,
                total_score, industry_total_score, trend_score, industry_trend_score, financial_score, industry_financial_score, valuation_score, industry_valuation_score, capital_flow_score, industry_capital_flow_score,
                event_score, industry_event_score, news_score, industry_news_score, risk_score, industry_risk_score, latest_trade_date, latest_report_period,
                latest_valuation_date, latest_flow_date, latest_event_date, latest_news_time,
                latest_risk_date, score_payload_json, source, update_time
            FROM stock_scores_daily
            {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()

        summary = {
            "generated_at": latest_score_date,
            "universe_size": conn.execute(
                "SELECT COUNT(*) FROM stock_scores_daily WHERE score_date = ?",
                (latest_score_date,),
            ).fetchone()[0],
            "weights": _stock_score_weights(),
            "notes": [
            "综合评分来自每日落库快照，默认展示最新评分日期。",
            "综合评分为相对评分，主要用于横向比较，不代表未来收益保证。",
            "industry_* 字段为行业内横向百分位评分，适合降低跨行业估值与风格差异带来的失真。",
        ],
            "source_mode": "table",
            "score_date": latest_score_date,
        }

        items = []
        for row in rows:
            item = dict(row)
            payload = {}
            raw_payload = item.get("score_payload_json")
            if raw_payload:
                try:
                    payload = json.loads(raw_payload)
                except Exception:
                    payload = {}
            item["score_summary"] = payload.get("score_summary", {})
            item["raw_metrics"] = payload.get("raw_metrics", {})
            items.append(item)

        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size,
            "sort_by": sort_by,
            "sort_order": sort_order.lower(),
            "min_score": min_score,
            "summary": summary,
            "items": items,
        }
    finally:
        conn.close()


def _parse_any_date(value: str | None):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _days_since(value: str | None):
    dt = _parse_any_date(value)
    if not dt:
        return None
    now = datetime.now()
    return max((now - dt).days, 0)


def _safe_div(a, b):
    a = _safe_float(a)
    b = _safe_float(b)
    if a is None or b in (None, 0):
        return None
    return a / b


def _mean_or_default(values: list[float | None], default: float = 50.0):
    valid = [float(v) for v in values if v is not None and not math.isnan(float(v)) and not math.isinf(float(v))]
    if not valid:
        return default
    return round(sum(valid) / len(valid), 2)


def _score_grade(score: float):
    if score >= 85:
        return "A"
    if score >= 75:
        return "B"
    if score >= 65:
        return "C"
    if score >= 50:
        return "D"
    return "E"


def _percentile_scores(
    items: list[dict],
    raw_key: str,
    score_key: str,
    *,
    reverse: bool = False,
    positive_only: bool = False,
):
    values = []
    for item in items:
        value = _safe_float(item.get(raw_key))
        if value is None:
            continue
        if positive_only and value <= 0:
            continue
        values.append(value)
    values.sort()

    if not values:
        for item in items:
            item[score_key] = None
        return

    last_idx = len(values) - 1
    for item in items:
        value = _safe_float(item.get(raw_key))
        if value is None or (positive_only and value <= 0):
            item[score_key] = None
            continue
        pos = bisect.bisect_right(values, value) - 1
        if last_idx <= 0:
            score = 50.0
        else:
            score = (pos / last_idx) * 100.0
        if reverse:
            score = 100.0 - score
        item[score_key] = round(max(0.0, min(100.0, score)), 2)


def _percentile_scores_by_group(
    items: list[dict],
    group_key: str,
    raw_key: str,
    score_key: str,
    *,
    reverse: bool = False,
    positive_only: bool = False,
):
    grouped: dict[str, list[float]] = {}
    for item in items:
        group = str(item.get(group_key) or "").strip()
        value = _safe_float(item.get(raw_key))
        if not group or value is None:
            continue
        if positive_only and value <= 0:
            continue
        grouped.setdefault(group, []).append(value)
    for values in grouped.values():
        values.sort()

    for item in items:
        group = str(item.get(group_key) or "").strip()
        value = _safe_float(item.get(raw_key))
        values = grouped.get(group, [])
        if not values or value is None or (positive_only and value <= 0):
            item[score_key] = None
            continue
        last_idx = len(values) - 1
        pos = bisect.bisect_right(values, value) - 1
        if last_idx <= 0:
            score = 50.0
        else:
            score = (pos / last_idx) * 100.0
        if reverse:
            score = 100.0 - score
        item[score_key] = round(max(0.0, min(100.0, score)), 2)


def _build_stock_score_universe(force_refresh: bool = False):
    now_ts = time.time()
    if not force_refresh and now_ts - float(STOCK_SCORE_CACHE.get("generated_at", 0.0)) < STOCK_SCORE_CACHE_TTL_SECONDS:
        return {
            "generated_at": STOCK_SCORE_CACHE.get("generated_at"),
            "items": list(STOCK_SCORE_CACHE.get("items", [])),
            "summary": dict(STOCK_SCORE_CACHE.get("summary", {})),
        }

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            WITH listed AS (
                SELECT ts_code, symbol, name, area, industry, market
                FROM stock_codes
                WHERE list_status = 'L'
            ),
            price_rank AS (
                SELECT
                    ts_code,
                    trade_date,
                    close,
                    pct_chg,
                    ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                FROM stock_daily_prices
            ),
            price_feat AS (
                SELECT
                    ts_code,
                    MAX(CASE WHEN rn = 1 THEN trade_date END) AS latest_trade_date,
                    MAX(CASE WHEN rn = 1 THEN close END) AS close_latest,
                    MAX(CASE WHEN rn = 6 THEN close END) AS close_5d_ago,
                    MAX(CASE WHEN rn = 21 THEN close END) AS close_20d_ago,
                    AVG(CASE WHEN rn BETWEEN 1 AND 20 THEN close END) AS ma20,
                    AVG(CASE WHEN rn BETWEEN 1 AND 20 THEN ABS(COALESCE(pct_chg, 0)) END) AS vol20_abs_pct,
                    AVG(CASE WHEN rn BETWEEN 1 AND 5 THEN COALESCE(pct_chg, 0) END) AS avg_pct_5d,
                    COUNT(CASE WHEN rn BETWEEN 1 AND 20 THEN 1 END) AS price_days
                FROM price_rank
                GROUP BY ts_code
            ),
            fin_rank AS (
                SELECT
                    ts_code,
                    report_period,
                    ann_date,
                    revenue,
                    net_profit,
                    roe,
                    gross_margin,
                    debt_to_assets,
                    operating_cf,
                    free_cf,
                    ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY report_period DESC, ann_date DESC) AS rn
                FROM stock_financials
            ),
            fin_feat AS (
                SELECT
                    ts_code,
                    report_period AS latest_report_period,
                    ann_date AS latest_ann_date,
                    revenue,
                    net_profit,
                    roe,
                    gross_margin,
                    debt_to_assets,
                    operating_cf,
                    free_cf
                FROM fin_rank
                WHERE rn = 1
            ),
            val_rank AS (
                SELECT
                    ts_code,
                    trade_date,
                    pe_ttm,
                    pb,
                    ps_ttm,
                    dv_ttm,
                    circ_mv,
                    total_mv,
                    ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                FROM stock_valuation_daily
            ),
            val_feat AS (
                SELECT
                    ts_code,
                    trade_date AS latest_valuation_date,
                    pe_ttm,
                    pb,
                    ps_ttm,
                    dv_ttm,
                    circ_mv,
                    total_mv
                FROM val_rank
                WHERE rn = 1
            ),
            flow_rank AS (
                SELECT
                    ts_code,
                    trade_date,
                    net_inflow,
                    main_inflow,
                    ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                FROM capital_flow_stock
            ),
            flow_feat AS (
                SELECT
                    ts_code,
                    MAX(CASE WHEN rn = 1 THEN trade_date END) AS latest_flow_date,
                    SUM(CASE WHEN rn BETWEEN 1 AND 5 THEN COALESCE(net_inflow, 0) END) AS net_inflow_5d,
                    SUM(CASE WHEN rn BETWEEN 1 AND 20 THEN COALESCE(net_inflow, 0) END) AS net_inflow_20d,
                    SUM(CASE WHEN rn BETWEEN 1 AND 5 THEN COALESCE(main_inflow, 0) END) AS main_inflow_5d,
                    SUM(CASE WHEN rn BETWEEN 1 AND 20 THEN COALESCE(main_inflow, 0) END) AS main_inflow_20d,
                    SUM(CASE WHEN rn BETWEEN 1 AND 5 AND COALESCE(main_inflow, 0) > 0 THEN 1 ELSE 0 END) AS pos_main_days_5
                FROM flow_rank
                GROUP BY ts_code
            ),
            event_feat AS (
                SELECT
                    ts_code,
                    MAX(COALESCE(event_date, ann_date)) AS latest_event_date,
                    SUM(
                        CASE
                            WHEN REPLACE(COALESCE(event_date, ann_date), '-', '') >= strftime('%Y%m%d', 'now', '-30 day')
                            THEN 1 ELSE 0
                        END
                    ) AS event_count_30d,
                    SUM(
                        CASE
                            WHEN REPLACE(COALESCE(event_date, ann_date), '-', '') >= strftime('%Y%m%d', 'now', '-90 day')
                            THEN 1 ELSE 0
                        END
                    ) AS event_count_90d,
                    SUM(
                        CASE
                            WHEN REPLACE(COALESCE(event_date, ann_date), '-', '') >= strftime('%Y%m%d', 'now', '-90 day')
                             AND (
                                title LIKE '%增持%' OR title LIKE '%回购%' OR title LIKE '%预增%' OR title LIKE '%扭亏%'
                                OR title LIKE '%分红%' OR title LIKE '%中标%' OR title LIKE '%签约%' OR title LIKE '%合同%'
                             )
                            THEN 1 ELSE 0
                        END
                    ) AS pos_event_count_90d,
                    SUM(
                        CASE
                            WHEN REPLACE(COALESCE(event_date, ann_date), '-', '') >= strftime('%Y%m%d', 'now', '-90 day')
                             AND (
                                title LIKE '%减持%' OR title LIKE '%质押%' OR title LIKE '%预减%' OR title LIKE '%首亏%'
                                OR title LIKE '%续亏%' OR title LIKE '%问询%' OR title LIKE '%处罚%' OR title LIKE '%立案%'
                                OR title LIKE '%诉讼%' OR title LIKE '%终止%' OR title LIKE '%风险%'
                             )
                            THEN 1 ELSE 0
                        END
                    ) AS neg_event_count_90d
                FROM stock_events
                GROUP BY ts_code
            ),
            news_feat AS (
                SELECT
                    ts_code,
                    MAX(pub_time) AS latest_news_time,
                    AVG(CASE WHEN llm_finance_impact_score IS NOT NULL THEN llm_finance_impact_score END) AS avg_news_impact,
                    AVG(CASE WHEN llm_system_score IS NOT NULL THEN llm_system_score END) AS avg_news_system,
                    MAX(
                        CASE llm_finance_importance
                            WHEN '极高' THEN 100
                            WHEN '高' THEN 85
                            WHEN '中' THEN 65
                            WHEN '低' THEN 35
                            WHEN '极低' THEN 10
                            ELSE NULL
                        END
                    ) AS max_news_importance_score,
                    COUNT(*) AS news_count
                FROM stock_news_items
                WHERE ts_code IS NOT NULL
                  AND ts_code <> ''
                  AND REPLACE(SUBSTR(COALESCE(pub_time, ''), 1, 10), '-', '') >= strftime('%Y%m%d', 'now', '-30 day')
                GROUP BY ts_code
            ),
            risk_latest AS (
                SELECT ts_code, MAX(scenario_date) AS latest_risk_date
                FROM risk_scenarios
                GROUP BY ts_code
            ),
            risk_feat AS (
                SELECT
                    r.ts_code,
                    x.latest_risk_date,
                    AVG(ABS(r.max_drawdown)) AS avg_drawdown,
                    AVG(ABS(r.var_95)) AS avg_var95,
                    AVG(ABS(r.cvar_95)) AS avg_cvar95,
                    AVG(ABS(r.pnl_impact)) AS avg_pnl_abs
                FROM risk_scenarios r
                INNER JOIN risk_latest x
                    ON x.ts_code = r.ts_code
                   AND x.latest_risk_date = r.scenario_date
                GROUP BY r.ts_code, x.latest_risk_date
            )
            SELECT
                l.ts_code,
                l.symbol,
                l.name,
                l.area,
                l.industry,
                l.market,
                p.latest_trade_date,
                p.close_latest,
                p.close_5d_ago,
                p.close_20d_ago,
                p.ma20,
                p.vol20_abs_pct,
                p.avg_pct_5d,
                p.price_days,
                f.latest_report_period,
                f.latest_ann_date,
                f.revenue,
                f.net_profit,
                f.roe,
                f.gross_margin,
                f.debt_to_assets,
                f.operating_cf,
                f.free_cf,
                v.latest_valuation_date,
                v.pe_ttm,
                v.pb,
                v.ps_ttm,
                v.dv_ttm,
                v.circ_mv,
                v.total_mv,
                c.latest_flow_date,
                c.net_inflow_5d,
                c.net_inflow_20d,
                c.main_inflow_5d,
                c.main_inflow_20d,
                c.pos_main_days_5,
                e.latest_event_date,
                e.event_count_30d,
                e.event_count_90d,
                e.pos_event_count_90d,
                e.neg_event_count_90d,
                n.latest_news_time,
                n.avg_news_impact,
                n.avg_news_system,
                n.max_news_importance_score,
                n.news_count,
                r.latest_risk_date,
                r.avg_drawdown,
                r.avg_var95,
                r.avg_cvar95,
                r.avg_pnl_abs
            FROM listed l
            LEFT JOIN price_feat p ON p.ts_code = l.ts_code
            LEFT JOIN fin_feat f ON f.ts_code = l.ts_code
            LEFT JOIN val_feat v ON v.ts_code = l.ts_code
            LEFT JOIN flow_feat c ON c.ts_code = l.ts_code
            LEFT JOIN event_feat e ON e.ts_code = l.ts_code
            LEFT JOIN news_feat n ON n.ts_code = l.ts_code
            LEFT JOIN risk_feat r ON r.ts_code = l.ts_code
            ORDER BY l.ts_code
            """
        ).fetchall()
    finally:
        conn.close()

    items = [dict(row) for row in rows]
    for item in items:
        close_latest = _safe_float(item.get("close_latest"))
        close_5d_ago = _safe_float(item.get("close_5d_ago"))
        close_20d_ago = _safe_float(item.get("close_20d_ago"))
        ma20 = _safe_float(item.get("ma20"))
        revenue = _safe_float(item.get("revenue"))
        net_profit = _safe_float(item.get("net_profit"))
        operating_cf = _safe_float(item.get("operating_cf"))
        free_cf = _safe_float(item.get("free_cf"))
        circ_mv = _safe_float(item.get("circ_mv")) or _safe_float(item.get("total_mv"))
        pos_event = _safe_float(item.get("pos_event_count_90d")) or 0.0
        neg_event = _safe_float(item.get("neg_event_count_90d")) or 0.0

        item["ret_5d_pct"] = ((close_latest / close_5d_ago) - 1.0) * 100.0 if close_latest and close_5d_ago else None
        item["ret_20d_pct"] = ((close_latest / close_20d_ago) - 1.0) * 100.0 if close_latest and close_20d_ago else None
        item["ma20_gap_pct"] = ((close_latest / ma20) - 1.0) * 100.0 if close_latest and ma20 else None
        item["net_margin_pct"] = (net_profit / revenue) * 100.0 if revenue and net_profit is not None else None
        item["operating_cf_margin_pct"] = (operating_cf / revenue) * 100.0 if revenue and operating_cf is not None else None
        item["free_cf_margin_pct"] = (free_cf / revenue) * 100.0 if revenue and free_cf is not None else None
        item["main_flow_ratio_5d_pct"] = (item["main_inflow_5d"] / circ_mv) * 100.0 if circ_mv and item.get("main_inflow_5d") is not None else None
        item["net_flow_ratio_20d_pct"] = (item["net_inflow_20d"] / circ_mv) * 100.0 if circ_mv and item.get("net_inflow_20d") is not None else None
        item["event_balance_90d"] = pos_event - neg_event
        item["event_recency_days"] = _days_since(item.get("latest_event_date"))
        item["news_recency_days"] = _days_since(item.get("latest_news_time"))

    _percentile_scores(items, "ret_5d_pct", "ret_5d_score")
    _percentile_scores(items, "ret_20d_pct", "ret_20d_score")
    _percentile_scores(items, "ma20_gap_pct", "ma20_gap_score")
    _percentile_scores(items, "vol20_abs_pct", "vol20_score", reverse=True)
    _percentile_scores_by_group(items, "industry", "ret_5d_pct", "ret_5d_industry_score")
    _percentile_scores_by_group(items, "industry", "ret_20d_pct", "ret_20d_industry_score")
    _percentile_scores_by_group(items, "industry", "ma20_gap_pct", "ma20_gap_industry_score")
    _percentile_scores_by_group(items, "industry", "vol20_abs_pct", "vol20_industry_score", reverse=True)

    _percentile_scores(items, "roe", "roe_score")
    _percentile_scores(items, "gross_margin", "gross_margin_score")
    _percentile_scores(items, "debt_to_assets", "debt_score", reverse=True)
    _percentile_scores(items, "operating_cf_margin_pct", "cf_margin_score")
    _percentile_scores(items, "free_cf_margin_pct", "fcf_margin_score")
    _percentile_scores(items, "net_margin_pct", "net_margin_score")
    _percentile_scores_by_group(items, "industry", "roe", "roe_industry_score")
    _percentile_scores_by_group(items, "industry", "gross_margin", "gross_margin_industry_score")
    _percentile_scores_by_group(items, "industry", "debt_to_assets", "debt_industry_score", reverse=True)
    _percentile_scores_by_group(items, "industry", "operating_cf_margin_pct", "cf_margin_industry_score")
    _percentile_scores_by_group(items, "industry", "free_cf_margin_pct", "fcf_margin_industry_score")
    _percentile_scores_by_group(items, "industry", "net_margin_pct", "net_margin_industry_score")

    _percentile_scores(items, "pe_ttm", "pe_score", reverse=True, positive_only=True)
    _percentile_scores(items, "pb", "pb_score", reverse=True, positive_only=True)
    _percentile_scores(items, "ps_ttm", "ps_score", reverse=True, positive_only=True)
    _percentile_scores(items, "dv_ttm", "dv_score")
    _percentile_scores_by_group(items, "industry", "pe_ttm", "pe_industry_score", reverse=True, positive_only=True)
    _percentile_scores_by_group(items, "industry", "pb", "pb_industry_score", reverse=True, positive_only=True)
    _percentile_scores_by_group(items, "industry", "ps_ttm", "ps_industry_score", reverse=True, positive_only=True)
    _percentile_scores_by_group(items, "industry", "dv_ttm", "dv_industry_score")

    _percentile_scores(items, "main_flow_ratio_5d_pct", "main_flow_5d_score")
    _percentile_scores(items, "net_flow_ratio_20d_pct", "net_flow_20d_score")
    _percentile_scores(items, "pos_main_days_5", "pos_main_days_score")
    _percentile_scores_by_group(items, "industry", "main_flow_ratio_5d_pct", "main_flow_5d_industry_score")
    _percentile_scores_by_group(items, "industry", "net_flow_ratio_20d_pct", "net_flow_20d_industry_score")
    _percentile_scores_by_group(items, "industry", "pos_main_days_5", "pos_main_days_industry_score")

    _percentile_scores(items, "event_count_30d", "event_count_30d_score")
    _percentile_scores(items, "event_balance_90d", "event_balance_90d_score")
    _percentile_scores(items, "event_recency_days", "event_recency_score", reverse=True)
    _percentile_scores_by_group(items, "industry", "event_count_30d", "event_count_30d_industry_score")
    _percentile_scores_by_group(items, "industry", "event_balance_90d", "event_balance_90d_industry_score")
    _percentile_scores_by_group(items, "industry", "event_recency_days", "event_recency_industry_score", reverse=True)

    _percentile_scores(items, "avg_news_system", "news_system_score")
    _percentile_scores(items, "avg_news_impact", "news_impact_score")
    _percentile_scores(items, "max_news_importance_score", "news_importance_score")
    _percentile_scores(items, "news_recency_days", "news_recency_score", reverse=True)
    _percentile_scores_by_group(items, "industry", "avg_news_system", "news_system_industry_score")
    _percentile_scores_by_group(items, "industry", "avg_news_impact", "news_impact_industry_score")
    _percentile_scores_by_group(items, "industry", "max_news_importance_score", "news_importance_industry_score")
    _percentile_scores_by_group(items, "industry", "news_recency_days", "news_recency_industry_score", reverse=True)

    _percentile_scores(items, "avg_drawdown", "drawdown_score", reverse=True)
    _percentile_scores(items, "avg_var95", "var95_score", reverse=True)
    _percentile_scores(items, "avg_cvar95", "cvar95_score", reverse=True)
    _percentile_scores(items, "avg_pnl_abs", "risk_pnl_score", reverse=True)
    _percentile_scores_by_group(items, "industry", "avg_drawdown", "drawdown_industry_score", reverse=True)
    _percentile_scores_by_group(items, "industry", "avg_var95", "var95_industry_score", reverse=True)
    _percentile_scores_by_group(items, "industry", "avg_cvar95", "cvar95_industry_score", reverse=True)
    _percentile_scores_by_group(items, "industry", "avg_pnl_abs", "risk_pnl_industry_score", reverse=True)

    for item in items:
        item["trend_score"] = _mean_or_default(
            [
                item.get("ret_5d_score"),
                item.get("ret_20d_score"),
                item.get("ma20_gap_score"),
                item.get("vol20_score"),
            ]
        )
        item["industry_trend_score"] = _mean_or_default(
            [
                item.get("ret_5d_industry_score"),
                item.get("ret_20d_industry_score"),
                item.get("ma20_gap_industry_score"),
                item.get("vol20_industry_score"),
            ]
        )
        item["financial_score"] = _mean_or_default(
            [
                item.get("roe_score"),
                item.get("gross_margin_score"),
                item.get("debt_score"),
                item.get("cf_margin_score"),
                item.get("fcf_margin_score"),
                item.get("net_margin_score"),
            ]
        )
        item["industry_financial_score"] = _mean_or_default(
            [
                item.get("roe_industry_score"),
                item.get("gross_margin_industry_score"),
                item.get("debt_industry_score"),
                item.get("cf_margin_industry_score"),
                item.get("fcf_margin_industry_score"),
                item.get("net_margin_industry_score"),
            ]
        )
        item["valuation_score"] = _mean_or_default(
            [
                item.get("pe_score"),
                item.get("pb_score"),
                item.get("ps_score"),
                item.get("dv_score"),
            ]
        )
        item["industry_valuation_score"] = _mean_or_default(
            [
                item.get("pe_industry_score"),
                item.get("pb_industry_score"),
                item.get("ps_industry_score"),
                item.get("dv_industry_score"),
            ]
        )
        item["capital_flow_score"] = _mean_or_default(
            [
                item.get("main_flow_5d_score"),
                item.get("net_flow_20d_score"),
                item.get("pos_main_days_score"),
            ]
        )
        item["industry_capital_flow_score"] = _mean_or_default(
            [
                item.get("main_flow_5d_industry_score"),
                item.get("net_flow_20d_industry_score"),
                item.get("pos_main_days_industry_score"),
            ]
        )
        item["event_score"] = _mean_or_default(
            [
                item.get("event_count_30d_score"),
                item.get("event_balance_90d_score"),
                item.get("event_recency_score"),
            ]
        )
        item["industry_event_score"] = _mean_or_default(
            [
                item.get("event_count_30d_industry_score"),
                item.get("event_balance_90d_industry_score"),
                item.get("event_recency_industry_score"),
            ]
        )
        item["news_score"] = _mean_or_default(
            [
                item.get("news_system_score"),
                item.get("news_impact_score"),
                item.get("news_importance_score"),
                item.get("news_recency_score"),
            ]
        )
        item["industry_news_score"] = _mean_or_default(
            [
                item.get("news_system_industry_score"),
                item.get("news_impact_industry_score"),
                item.get("news_importance_industry_score"),
                item.get("news_recency_industry_score"),
            ]
        )
        item["risk_score"] = _mean_or_default(
            [
                item.get("drawdown_score"),
                item.get("var95_score"),
                item.get("cvar95_score"),
                item.get("risk_pnl_score"),
            ]
        )
        item["industry_risk_score"] = _mean_or_default(
            [
                item.get("drawdown_industry_score"),
                item.get("var95_industry_score"),
                item.get("cvar95_industry_score"),
                item.get("risk_pnl_industry_score"),
            ]
        )
        total_score = (
            item["trend_score"] * 0.20
            + item["financial_score"] * 0.20
            + item["valuation_score"] * 0.15
            + item["capital_flow_score"] * 0.15
            + item["event_score"] * 0.10
            + item["news_score"] * 0.10
            + item["risk_score"] * 0.10
        )
        industry_total_score = (
            item["industry_trend_score"] * 0.20
            + item["industry_financial_score"] * 0.20
            + item["industry_valuation_score"] * 0.15
            + item["industry_capital_flow_score"] * 0.15
            + item["industry_event_score"] * 0.10
            + item["industry_news_score"] * 0.10
            + item["industry_risk_score"] * 0.10
        )
        item["total_score"] = round(total_score, 2)
        item["industry_total_score"] = round(industry_total_score, 2)
        item["score_grade"] = _score_grade(total_score)
        item["industry_score_grade"] = _score_grade(industry_total_score)
        item["score_summary"] = {
            "trend": "近20日动量、5日强弱、均线位置、波动率",
            "financial": "ROE、毛利率、负债率、现金流质量、净利率",
            "valuation": "PE/PB/PS相对水平与股息率",
            "capital_flow": "近5/20日资金净流入与主力连续性",
            "event": "近30/90日事件密度、催化偏正负、事件新鲜度",
            "news": "近30日个股新闻系统分、财经影响分、重要度、时效性",
            "risk": "最新风险情景中的回撤、VaR、CVaR 与损益冲击",
            "industry_neutral": "行业内版本使用同一行业股票做横向百分位比较，降低跨行业估值失真",
        }

    items.sort(key=lambda x: (-float(x.get("total_score") or 0.0), x.get("ts_code") or ""))
    summary = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "universe_size": len(items),
        "weights": _stock_score_weights(),
        "notes": [
            "综合评分为相对评分，主要用于横向比较，不代表未来收益保证。",
            "事件与新闻覆盖不完整时采用中性处理，避免因缺失数据被过度惩罚。",
            "industry_* 字段为行业内横向百分位评分，适合降低跨行业估值与风格差异带来的失真。",
        ],
        "source_mode": "dynamic",
    }
    STOCK_SCORE_CACHE["generated_at"] = now_ts
    STOCK_SCORE_CACHE["items"] = items
    STOCK_SCORE_CACHE["summary"] = summary
    return {"generated_at": now_ts, "items": list(items), "summary": summary}


def query_stock_scores(
    keyword: str,
    market: str,
    area: str,
    industry: str,
    min_score: float,
    page: int,
    page_size: int,
    sort_by: str,
    sort_order: str,
):
    keyword = (keyword or "").strip().lower()
    market = (market or "").strip()
    area = (area or "").strip()
    industry = (industry or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    sort_order = "asc" if (sort_order or "").lower() == "asc" else "desc"
    sortable = {
        "total_score",
        "industry_total_score",
        "trend_score",
        "industry_trend_score",
        "financial_score",
        "industry_financial_score",
        "valuation_score",
        "industry_valuation_score",
        "capital_flow_score",
        "industry_capital_flow_score",
        "event_score",
        "industry_event_score",
        "news_score",
        "industry_news_score",
        "risk_score",
        "industry_risk_score",
        "ts_code",
        "name",
        "latest_trade_date",
    }
    sort_by = sort_by if sort_by in sortable else "total_score"

    table_payload = _load_stock_scores_from_table(
        keyword=keyword,
        market=market,
        area=area,
        industry=industry,
        min_score=min_score,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    if table_payload is not None:
        return table_payload

    universe = _build_stock_score_universe()
    items = universe["items"]

    def keep(item: dict):
        if keyword:
            haystack = " ".join(
                [
                    str(item.get("ts_code") or ""),
                    str(item.get("symbol") or ""),
                    str(item.get("name") or ""),
                    str(item.get("industry") or ""),
                ]
            ).lower()
            if keyword not in haystack:
                return False
        if market and item.get("market") != market:
            return False
        if area and item.get("area") != area:
            return False
        if industry and item.get("industry") != industry:
            return False
        if float(item.get("total_score") or 0.0) < min_score:
            return False
        return True

    filtered = [item for item in items if keep(item)]

    reverse = sort_order != "asc"
    if sort_by in {"ts_code", "name", "latest_trade_date"}:
        filtered.sort(key=lambda x: str(x.get(sort_by) or ""), reverse=reverse)
    else:
        filtered.sort(key=lambda x: (float(x.get(sort_by) or 0.0), str(x.get("ts_code") or "")), reverse=reverse)

    total = len(filtered)
    total_pages = (total + page_size - 1) // page_size if total else 0
    offset = (page - 1) * page_size
    page_items = filtered[offset : offset + page_size]

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "sort_by": sort_by,
        "sort_order": sort_order,
        "min_score": min_score,
        "summary": universe["summary"],
        "items": page_items,
    }


def query_prices(
    ts_code: str, start_date: str, end_date: str, page: int, page_size: int
):
    ts_code = ts_code.strip().upper()
    start_date = start_date.strip()
    end_date = end_date.strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), PRICES_MAX_PAGE_SIZE)
    offset = (page - 1) * page_size

    applied_default_lookback = False

    where_clauses = []
    params: list[object] = []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        if not ts_code and not start_date and not end_date:
            latest_trade_date = conn.execute("SELECT MAX(trade_date) FROM stock_daily_prices").fetchone()[0]
            if latest_trade_date:
                end_date = str(latest_trade_date)
                try:
                    latest_dt = datetime.strptime(end_date, "%Y%m%d")
                    start_date = (latest_dt - timedelta(days=PRICES_DEFAULT_LOOKBACK_DAYS)).strftime("%Y%m%d")
                except Exception:
                    start_date = (datetime.utcnow() - timedelta(days=PRICES_DEFAULT_LOOKBACK_DAYS)).strftime("%Y%m%d")
            else:
                start_date = (datetime.utcnow() - timedelta(days=PRICES_DEFAULT_LOOKBACK_DAYS)).strftime("%Y%m%d")
                end_date = datetime.utcnow().strftime("%Y%m%d")
            applied_default_lookback = True

        if ts_code:
            where_clauses.append("p.ts_code = ?")
            params.append(ts_code)
        if start_date:
            where_clauses.append("p.trade_date >= ?")
            params.append(start_date)
        if end_date:
            where_clauses.append("p.trade_date <= ?")
            params.append(end_date)

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        cache_key = (
            f"api:prices:v2:ts={ts_code or '*'}:start={start_date or '*'}:"
            f"end={end_date or '*'}:page={page}:size={page_size}"
        )
        cached = cache_get_json(cache_key)
        if isinstance(cached, dict):
            cached.setdefault("cache", {})["hit"] = True
            return cached

        count_sql = f"SELECT COUNT(*) FROM stock_daily_prices p{where_sql}"
        data_sql = f"""
        SELECT
            p.ts_code,
            s.name,
            p.trade_date,
            p.open,
            p.high,
            p.low,
            p.close,
            p.pre_close,
            p.change,
            p.pct_chg,
            p.vol,
            p.amount
        FROM stock_daily_prices p
        LEFT JOIN stock_codes s ON s.ts_code = p.ts_code
        {where_sql}
        ORDER BY p.trade_date DESC, p.ts_code ASC
        LIMIT ? OFFSET ?
        """

        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        data = [dict(r) for r in rows]
        payload = {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size,
            "items": data,
            "start_date": start_date,
            "end_date": end_date,
            "default_lookback_applied": applied_default_lookback,
            "message": (
                f"未传查询条件，默认返回最近{PRICES_DEFAULT_LOOKBACK_DAYS}天日线数据。"
                if applied_default_lookback
                else ""
            ),
            "cache": {"hit": False, "ttl_seconds": REDIS_CACHE_TTL_PRICES},
        }
        cache_set_json(cache_key, payload, REDIS_CACHE_TTL_PRICES)
        return payload
    finally:
        conn.close()


def query_minline(
    ts_code: str, trade_date: str, page: int, page_size: int, table_name: str = "stock_minline"
):
    ts_code = ts_code.strip().upper()
    trade_date = trade_date.strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 500)
    offset = (page - 1) * page_size

    where_clauses = []
    params: list[object] = []
    if ts_code:
        where_clauses.append("m.ts_code = ?")
        params.append(ts_code)
    if trade_date:
        where_clauses.append("m.trade_date = ?")
        params.append(trade_date)

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()[0]
        if not table_exists:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        count_sql = f"SELECT COUNT(*) FROM {table_name} m{where_sql}"
        data_sql = f"""
        SELECT
            m.ts_code,
            s.name,
            m.trade_date,
            m.minute_time,
            m.price,
            m.avg_price,
            m.volume,
            m.total_volume,
            m.source
        FROM {table_name} m
        LEFT JOIN stock_codes s ON s.ts_code = m.ts_code
        {where_sql}
        ORDER BY m.trade_date DESC, m.minute_time ASC
        LIMIT ? OFFSET ?
        """
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "items": data,
    }


def query_macro_indicators(limit: int = 500):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='macro_series'"
        ).fetchone()[0]
        if not table_exists:
            return []
        rows = conn.execute(
            """
            SELECT indicator_code, indicator_name, freq, COUNT(*) AS points
            FROM macro_series
            GROUP BY indicator_code, indicator_name, freq
            ORDER BY indicator_code
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def query_macro_series(
    indicator_code: str,
    freq: str,
    period_start: str,
    period_end: str,
    keyword: str,
    page: int,
    page_size: int,
):
    indicator_code = indicator_code.strip()
    freq = freq.strip().upper()
    period_start = period_start.strip()
    period_end = period_end.strip()
    keyword = keyword.strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 500)
    offset = (page - 1) * page_size

    where_clauses = []
    params: list[object] = []

    if indicator_code:
        where_clauses.append("indicator_code = ?")
        params.append(indicator_code)
    if freq in {"D", "W", "M", "Q", "Y"}:
        where_clauses.append("freq = ?")
        params.append(freq)
    if period_start:
        where_clauses.append("period >= ?")
        params.append(period_start)
    if period_end:
        where_clauses.append("period <= ?")
        params.append(period_end)
    if keyword:
        where_clauses.append("(indicator_code LIKE ? OR indicator_name LIKE ?)")
        kw = f"%{keyword}%"
        params.extend([kw, kw])

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='macro_series'"
        ).fetchone()[0]
        if not table_exists:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        count_sql = f"SELECT COUNT(*) FROM macro_series{where_sql}"
        data_sql = f"""
        SELECT indicator_code, indicator_name, freq, period, value, unit, source, publish_date, update_time
        FROM macro_series
        {where_sql}
        ORDER BY indicator_code ASC, period ASC
        LIMIT ? OFFSET ?
        """
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "items": data,
    }


def query_news_sources():
    conn = sqlite3.connect(DB_PATH)
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='news_feed_items'"
        ).fetchone()[0]
        if not table_exists:
            return []
        rows = conn.execute(
            "SELECT DISTINCT source FROM news_feed_items WHERE source IS NOT NULL AND source <> '' ORDER BY source"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def query_news(
    source: str,
    source_prefixes: str,
    keyword: str,
    date_from: str,
    date_to: str,
    finance_levels: str,
    exclude_sources: str,
    exclude_source_prefixes: str,
    page: int,
    page_size: int,
):
    source = source.strip().lower()
    source_prefixes = source_prefixes.strip()
    keyword = keyword.strip()
    date_from = date_from.strip()
    date_to = date_to.strip()
    finance_levels = finance_levels.strip()
    exclude_sources = exclude_sources.strip()
    exclude_source_prefixes = exclude_source_prefixes.strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    where_clauses = []
    params: list[object] = []

    if source:
        where_clauses.append("source = ?")
        params.append(source)
    if source_prefixes:
        prefixes = [x.strip().lower() for x in source_prefixes.split(",") if x.strip()]
        if prefixes:
            prefix_clauses = []
            for p in prefixes:
                prefix_clauses.append("source LIKE ?")
                params.append(f"{p}%")
            where_clauses.append("(" + " OR ".join(prefix_clauses) + ")")
    if exclude_sources:
        ex_list = [x.strip().lower() for x in exclude_sources.split(",") if x.strip()]
        if ex_list:
            placeholders = ",".join(["?"] * len(ex_list))
            where_clauses.append(f"source NOT IN ({placeholders})")
            params.extend(ex_list)
    if exclude_source_prefixes:
        prefixes = [x.strip().lower() for x in exclude_source_prefixes.split(",") if x.strip()]
        for p in prefixes:
            where_clauses.append("source NOT LIKE ?")
            params.append(f"{p}%")
    if keyword:
        where_clauses.append("(title LIKE ? OR summary LIKE ?)")
        kw = f"%{keyword}%"
        params.extend([kw, kw])
    if date_from:
        where_clauses.append("pub_date >= ?")
        params.append(date_from)
    if date_to:
        where_clauses.append("pub_date <= ?")
        params.append(date_to)
    valid_levels = []
    if finance_levels:
        levels = [x.strip() for x in finance_levels.split(",") if x.strip()]
        valid_levels = [x for x in levels if x in {"极高", "高", "中", "低", "极低"}]

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='news_feed_items'"
        ).fetchone()[0]
        if not table_exists:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        cols = {r[1] for r in conn.execute("PRAGMA table_info(news_feed_items)").fetchall()}
        if valid_levels and "llm_finance_importance" in cols:
            placeholders = ",".join(["?"] * len(valid_levels))
            where_clauses.append(f"llm_finance_importance IN ({placeholders})")
            params.extend(valid_levels)
        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        select_scored = ", ".join(
            [
                "llm_system_score" if "llm_system_score" in cols else "NULL AS llm_system_score",
                "llm_finance_impact_score" if "llm_finance_impact_score" in cols else "NULL AS llm_finance_impact_score",
                "llm_finance_importance" if "llm_finance_importance" in cols else "NULL AS llm_finance_importance",
                "llm_impacts_json" if "llm_impacts_json" in cols else "NULL AS llm_impacts_json",
                "related_ts_codes_json" if "related_ts_codes_json" in cols else "NULL AS related_ts_codes_json",
                "related_stock_names_json" if "related_stock_names_json" in cols else "NULL AS related_stock_names_json",
                "llm_direct_related_ts_codes_json" if "llm_direct_related_ts_codes_json" in cols else "NULL AS llm_direct_related_ts_codes_json",
                "llm_direct_related_stock_names_json" if "llm_direct_related_stock_names_json" in cols else "NULL AS llm_direct_related_stock_names_json",
                "llm_model" if "llm_model" in cols else "NULL AS llm_model",
                "llm_scored_at" if "llm_scored_at" in cols else "NULL AS llm_scored_at",
                "llm_sentiment_score" if "llm_sentiment_score" in cols else "NULL AS llm_sentiment_score",
                "llm_sentiment_label" if "llm_sentiment_label" in cols else "NULL AS llm_sentiment_label",
                "llm_sentiment_reason" if "llm_sentiment_reason" in cols else "NULL AS llm_sentiment_reason",
                "llm_sentiment_confidence" if "llm_sentiment_confidence" in cols else "NULL AS llm_sentiment_confidence",
                "llm_sentiment_model" if "llm_sentiment_model" in cols else "NULL AS llm_sentiment_model",
                "llm_sentiment_scored_at" if "llm_sentiment_scored_at" in cols else "NULL AS llm_sentiment_scored_at",
            ]
        )

        count_sql = f"SELECT COUNT(*) FROM news_feed_items{where_sql}"
        data_sql = f"""
        SELECT
            id, source, title, link, guid, summary, category, author, pub_date, fetched_at,
            {select_scored}
        FROM news_feed_items
        {where_sql}
        ORDER BY pub_date DESC, id DESC
        LIMIT ? OFFSET ?
        """
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "items": data,
    }


def query_stock_news(
    ts_code: str,
    company_name: str,
    keyword: str,
    source: str,
    finance_levels: str,
    date_from: str,
    date_to: str,
    scored: str,
    page: int,
    page_size: int,
):
    return stock_news_query(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        ts_code=ts_code,
        company_name=company_name,
        keyword=keyword,
        source=source,
        finance_levels=finance_levels,
        date_from=date_from,
        date_to=date_to,
        scored=scored,
        page=page,
        page_size=page_size,
    )


def query_stock_news_sources():
    return stock_news_query_sources(sqlite3_module=sqlite3, db_path=DB_PATH)


def query_wechat_chatlog(
    talker: str,
    sender_name: str,
    keyword: str,
    is_quote: str,
    query_date_start: str,
    query_date_end: str,
    page: int,
    page_size: int,
):
    return chatrooms_query_wechat_chatlog(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        talker=talker,
        sender_name=sender_name,
        keyword=keyword,
        is_quote=is_quote,
        query_date_start=query_date_start,
        query_date_end=query_date_end,
        page=page,
        page_size=page_size,
    )


def query_chatroom_overview(
    keyword: str,
    primary_category: str,
    activity_level: str,
    risk_level: str,
    skip_realtime_monitor: str,
    fetch_status: str,
    page: int,
    page_size: int,
):
    return chatrooms_query_overview(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        keyword=keyword,
        primary_category=primary_category,
        activity_level=activity_level,
        risk_level=risk_level,
        skip_realtime_monitor=skip_realtime_monitor,
        fetch_status=fetch_status,
        page=page,
        page_size=page_size,
    )


def fetch_single_chatroom_now(room_id: str, fetch_yesterday_and_today: bool):
    return chatrooms_fetch_single_now(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        root_dir=ROOT_DIR,
        publish_app_event=publish_app_event,
        room_id=room_id,
        fetch_yesterday_and_today=fetch_yesterday_and_today,
    )


def query_chatroom_investment_analysis(
    keyword: str,
    final_bias: str,
    target_keyword: str,
    page: int,
    page_size: int,
):
    return chatrooms_query_investment_analysis(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        keyword=keyword,
        final_bias=final_bias,
        target_keyword=target_keyword,
        page=page,
        page_size=page_size,
    )


def query_chatroom_candidate_pool(
    keyword: str,
    dominant_bias: str,
    candidate_type: str,
    page: int,
    page_size: int,
):
    return chatrooms_query_candidate_pool(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        keyword=keyword,
        dominant_bias=dominant_bias,
        candidate_type=candidate_type,
        page=page,
        page_size=page_size,
    )


def query_research_reports(report_type: str, keyword: str, report_date: str, page: int, page_size: int):
    return reporting_query_research_reports(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        report_type=report_type,
        keyword=keyword,
        report_date=report_date,
        page=page,
        page_size=page_size,
    )


def query_news_daily_summaries(
    summary_date: str,
    source_filter: str,
    model: str,
    page: int,
    page_size: int,
):
    return reporting_query_news_daily_summaries(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        get_or_build_cached_logic_view=get_or_build_cached_logic_view,
        extract_logic_view_from_markdown=extract_logic_view_from_markdown,
        summary_date=summary_date,
        source_filter=source_filter,
        model=model,
        page=page,
        page_size=page_size,
    )


def get_daily_summary_by_date(summary_date: str):
    return reporting_get_daily_summary_by_date(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        get_or_build_cached_logic_view=get_or_build_cached_logic_view,
        extract_logic_view_from_markdown=extract_logic_view_from_markdown,
        summary_date=summary_date,
    )


def generate_daily_summary(model: str, summary_date: str):
    return reporting_generate_daily_summary(
        root_dir=ROOT_DIR,
        extract_llm_result_marker=_extract_llm_result_marker,
        model=model,
        summary_date=summary_date,
    )


def fetch_stock_news_now(ts_code: str, company_name: str, page_size: int, timeout_s: int = 180):
    return stock_news_fetch_now(
        root_dir=ROOT_DIR,
        db_path=DB_PATH,
        publish_app_event=publish_app_event,
        ts_code=ts_code,
        company_name=company_name,
        page_size=page_size,
        timeout_s=timeout_s,
    )


def score_stock_news_now(ts_code: str, limit: int, model: str, timeout_s: int = 300, row_id: int = 0, force: bool = False):
    return stock_news_score_now(
        sqlite3_module=sqlite3,
        root_dir=ROOT_DIR,
        db_path=DB_PATH,
        publish_app_event=publish_app_event,
        extract_llm_result_marker=_extract_llm_result_marker,
        ts_code=ts_code,
        limit=limit,
        model=model,
        timeout_s=timeout_s,
        row_id=row_id,
        force=force,
    )


def _parse_iso_datetime(raw: str):
    if not raw:
        return None
    text = str(raw).strip()
    try:
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _parse_yyyymmdd(raw: str):
    if not raw:
        return None
    text = str(raw).strip()
    try:
        dt = datetime.strptime(text, "%Y%m%d")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _age_seconds_from_dt(dt):
    if not dt:
        return None
    return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())


def _status_by_age(age_seconds: float | None, ok_within: float, warn_within: float):
    if age_seconds is None:
        return "error"
    if age_seconds <= ok_within:
        return "ok"
    if age_seconds <= warn_within:
        return "warn"
    return "error"


def _status_text(status: str):
    return {"ok": "正常", "warn": "延迟", "error": "异常"}.get(status, status)


def _combine_process_data_status(
    process_running: bool,
    data_age_seconds: float | None,
    ok_within: float,
    warn_within: float,
):
    data_status = _status_by_age(data_age_seconds, ok_within, warn_within)
    if not process_running:
        return "error"
    if data_status == "error":
        return "error"
    if data_status == "warn":
        return "warn"
    return "ok"


def _iso_from_mtime(path: Path):
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _tail_text(path: Path, lines: int = 8):
    if not path.exists():
        return ""
    try:
        content = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return "\n".join(content[-lines:])
    except Exception:
        return ""


def _pid_running(pid_file: Path):
    if not pid_file.exists():
        return False, ""
    pid = pid_file.read_text(encoding="utf-8", errors="ignore").strip()
    if not pid:
        return False, ""
    try:
        os.kill(int(pid), 0)
        return True, pid
    except Exception:
        return False, pid


def _table_count(conn: sqlite3.Connection, table_name: str):
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    except Exception:
        return 0


def _table_max(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    try:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _fetch_local_json(url: str, timeout_s: int = 2):
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        return json.loads(raw)
    except Exception:
        return None


def query_source_monitor():
    cached = cache_get_json("api:source-monitor:v1")
    if cached:
        return cached

    conn = sqlite3.connect(DB_PATH)
    try:
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        latest_intl_news = _table_max(
            conn,
            "SELECT MAX(pub_date) FROM news_feed_items WHERE source NOT LIKE 'cn_%'",
        )
        latest_marketscreener_news = _table_max(
            conn,
            "SELECT MAX(pub_date) FROM news_feed_items WHERE source = 'marketscreener_byd_news'",
        )
        latest_marketscreener_live_news = _table_max(
            conn,
            "SELECT MAX(pub_date) FROM news_feed_items WHERE source = 'marketscreener_live_news'",
        )
        latest_sina_news = _table_max(
            conn,
            "SELECT MAX(pub_date) FROM news_feed_items WHERE source = 'cn_sina_7x24'",
        )
        latest_eastmoney_news = _table_max(
            conn,
            "SELECT MAX(pub_date) FROM news_feed_items WHERE source = 'cn_eastmoney_fastnews'",
        )
        latest_eastmoney_fetch = _table_max(
            conn,
            "SELECT MAX(fetched_at) FROM news_feed_items WHERE source = 'cn_eastmoney_fastnews'",
        )
        latest_scored_news = _table_max(
            conn,
            "SELECT MAX(llm_scored_at) FROM news_feed_items WHERE llm_scored_at IS NOT NULL AND llm_scored_at <> ''",
        )
        latest_news_summary = _table_max(
            conn,
            "SELECT MAX(created_at) FROM news_daily_summaries",
        )
        latest_stock_price = _table_max(conn, "SELECT MAX(trade_date) FROM stock_daily_prices")
        latest_minline = _table_max(conn, "SELECT MAX(trade_date) FROM stock_minline")
        latest_macro = _table_max(conn, "SELECT MAX(update_time) FROM macro_series")
        latest_financials = _table_max(conn, "SELECT MAX(ann_date) FROM stock_financials")
        latest_valuation = _table_max(conn, "SELECT MAX(trade_date) FROM stock_valuation_daily")
        latest_cf_stock = _table_max(conn, "SELECT MAX(trade_date) FROM capital_flow_stock")
        latest_cf_market = _table_max(conn, "SELECT MAX(trade_date) FROM capital_flow_market")
    finally:
        conn.close()

    eastmoney_running, eastmoney_pid = _pid_running(TMP_DIR / "cn_eastmoney_10s.pid")
    eastmoney_data_age = _age_seconds_from_dt(_parse_iso_datetime(latest_eastmoney_fetch or latest_eastmoney_news))
    eastmoney_status = _combine_process_data_status(
        process_running=eastmoney_running,
        data_age_seconds=eastmoney_data_age,
        ok_within=1800,
        warn_within=3 * 3600,
    )
    ws_health = _fetch_local_json("http://127.0.0.1:8010/health", timeout_s=2)
    redis_client = get_redis_client()
    latest_ws_broadcast = ""
    if redis_client is not None:
        try:
            raw = redis_client.get("realtime:last_status")
            if raw:
                latest_ws_payload = json.loads(raw)
                latest_ws_broadcast = (
                    ((latest_ws_payload.get("payload") or {}).get("created_at"))
                    or latest_ws_payload.get("ts")
                    or ""
                )
        except Exception:
            latest_ws_broadcast = ""

    sources = [
        {
            "key": "intl_news_rss",
            "name": "国际新闻总链路",
            "category": "新闻",
            "status": _status_by_age(_age_seconds_from_dt(_parse_iso_datetime(latest_intl_news)), 1800, 7200),
            "last_update": latest_intl_news or "",
            "detail": "国际新闻 RSS/HTML 抓取与入库",
            "rows": None,
        },
        {
            "key": "marketscreener_byd_news",
            "name": "国际新闻-MarketScreener(BYD)",
            "category": "新闻",
            "status": _status_by_age(_age_seconds_from_dt(_parse_iso_datetime(latest_marketscreener_news)), 86400, 3 * 86400),
            "last_update": latest_marketscreener_news or "",
            "detail": "5分钟定时抓取 BYD Company Limited 新闻页",
            "rows": None,
        },
        {
            "key": "marketscreener_live_news",
            "name": "国际新闻-MarketScreener Live",
            "category": "新闻",
            "status": _status_by_age(_age_seconds_from_dt(_parse_iso_datetime(latest_marketscreener_live_news)), 1800, 7200),
            "last_update": latest_marketscreener_live_news or "",
            "detail": "5分钟定时抓取 Live 区块",
            "rows": None,
        },
        {
            "key": "cn_sina_7x24",
            "name": "国内新闻-新浪7x24",
            "category": "新闻",
            "status": _status_by_age(_age_seconds_from_dt(_parse_iso_datetime(latest_sina_news)), 600, 1800),
            "last_update": latest_sina_news or "",
            "detail": "cron 每2分钟抓取",
            "rows": None,
        },
        {
            "key": "cn_eastmoney_fastnews",
            "name": "国内新闻-东方财富",
            "category": "新闻",
            "status": eastmoney_status,
            "last_update": latest_eastmoney_fetch or latest_eastmoney_news or "",
            "detail": (
                f"10秒循环抓取，进程PID={eastmoney_pid or '-'}，"
                f"进程={'在线' if eastmoney_running else '离线'}，"
                f"最近入库年龄={int(eastmoney_data_age) if eastmoney_data_age is not None else '-'}秒，"
                f"最近新闻发布时间={latest_eastmoney_news or '-'}"
            ),
            "rows": None,
        },
        {
            "key": "news_scoring",
            "name": "新闻自动评分",
            "category": "LLM",
            "status": _status_by_age(_age_seconds_from_dt(_parse_iso_datetime(latest_scored_news)), 3600, 10800),
            "last_update": latest_scored_news or "",
            "detail": "新闻评分任务最近执行时间",
            "rows": None,
        },
        {
            "key": "news_daily_summaries",
            "name": "新闻日报总结",
            "category": "LLM",
            "status": _status_by_age(_age_seconds_from_dt(_parse_iso_datetime(latest_news_summary)), 18 * 3600, 36 * 3600),
            "last_update": latest_news_summary or "",
            "detail": "每日11:30和23:30总结",
            "rows": None,
        },
        {
            "key": "news_realtime_ws",
            "name": "新闻实时广播",
            "category": "实时",
            "status": "ok" if ws_health else "error",
            "last_update": latest_ws_broadcast or "",
            "detail": "Redis Stream -> Worker -> WebSocket",
            "rows": None,
        },
        {
            "key": "stock_daily_prices",
            "name": "股票日线",
            "category": "行情",
            "status": _status_by_age(_age_seconds_from_dt(_parse_yyyymmdd(latest_stock_price)), 3 * 86400, 7 * 86400),
            "last_update": latest_stock_price or "",
            "detail": "日线价格主表",
            "rows": None,
        },
        {
            "key": "stock_minline",
            "name": "股票分钟线",
            "category": "行情",
            "status": _status_by_age(_age_seconds_from_dt(_parse_yyyymmdd(latest_minline)), 2 * 86400, 5 * 86400),
            "last_update": latest_minline or "",
            "detail": "分钟线主表",
            "rows": None,
        },
        {
            "key": "macro_series",
            "name": "宏观指标",
            "category": "宏观",
            "status": _status_by_age(_age_seconds_from_dt(_parse_iso_datetime(latest_macro)), 40 * 86400, 120 * 86400),
            "last_update": latest_macro or "",
            "detail": "宏观指标更新时间",
            "rows": None,
        },
        {
            "key": "stock_financials",
            "name": "财务数据",
            "category": "基本面",
            "status": _status_by_age(_age_seconds_from_dt(_parse_yyyymmdd(latest_financials)), 180 * 86400, 400 * 86400),
            "last_update": latest_financials or "",
            "detail": "财报公告日期",
            "rows": None,
        },
        {
            "key": "stock_valuation_daily",
            "name": "估值数据",
            "category": "基本面",
            "status": _status_by_age(_age_seconds_from_dt(_parse_yyyymmdd(latest_valuation)), 3 * 86400, 7 * 86400),
            "last_update": latest_valuation or "",
            "detail": "估值日频数据",
            "rows": None,
        },
        {
            "key": "capital_flow_stock",
            "name": "个股资金流",
            "category": "资金",
            "status": _status_by_age(_age_seconds_from_dt(_parse_yyyymmdd(latest_cf_stock)), 3 * 86400, 7 * 86400),
            "last_update": latest_cf_stock or "",
            "detail": "个股主力资金",
            "rows": None,
        },
        {
            "key": "capital_flow_market",
            "name": "市场资金流",
            "category": "资金",
            "status": _status_by_age(_age_seconds_from_dt(_parse_yyyymmdd(latest_cf_market)), 3 * 86400, 7 * 86400),
            "last_update": latest_cf_market or "",
            "detail": "北向/南向等市场流向",
            "rows": None,
        },
    ]

    counts_conn = sqlite3.connect(DB_PATH)
    try:
        row_counts = {
            "news_feed_items": _table_count(counts_conn, "news_feed_items"),
            "stock_codes": _table_count(counts_conn, "stock_codes"),
            "stock_daily_prices": _table_count(counts_conn, "stock_daily_prices"),
            "stock_minline": _table_count(counts_conn, "stock_minline"),
            "macro_series": _table_count(counts_conn, "macro_series"),
        }
    finally:
        counts_conn.close()

    processes = [
        {
            "key": "eastmoney_10s_loop",
            "name": "东方财富10秒循环",
            "status": "ok" if eastmoney_running else "error",
            "detail": f"PID={eastmoney_pid or '-'}",
            "last_update": _iso_from_mtime(TMP_DIR / "cn_eastmoney_10s_supervisor.log"),
        },
        {
            "key": "eastmoney_watchdog",
            "name": "东方财富守护脚本",
            "status": "ok" if (TMP_DIR / "cn_eastmoney_10s_watchdog.log").exists() else "warn",
            "detail": "每分钟巡检一次",
            "last_update": _iso_from_mtime(TMP_DIR / "cn_eastmoney_10s_watchdog.log"),
        },
        {
            "key": "main_backend",
            "name": "主后端8002",
            "status": "ok" if (TMP_DIR / "stock_backend.log").exists() else "warn",
            "detail": "股票/新闻主API",
            "last_update": _iso_from_mtime(TMP_DIR / "stock_backend.log"),
        },
        {
            "key": "multi_role_backend",
            "name": "多角色后端8006",
            "status": "ok" if (TMP_DIR / "stock_backend_multi_role.log").exists() else "warn",
            "detail": "LLM多角色分析API",
            "last_update": _iso_from_mtime(TMP_DIR / "stock_backend_multi_role.log"),
        },
        {
            "key": "news_stream_worker",
            "name": "新闻 Stream Worker",
            "status": "ok" if latest_ws_broadcast else "warn",
            "detail": "消费 stream:news_events 并广播",
            "last_update": latest_ws_broadcast or _iso_from_mtime(TMP_DIR / "stream_news_worker.log"),
        },
        {
            "key": "news_ws_service",
            "name": "新闻 WebSocket 服务8010",
            "status": "ok" if ws_health else "error",
            "detail": f"clients={((ws_health or {}).get('clients')) if ws_health else '-'}",
            "last_update": ((ws_health or {}).get("ts")) or _iso_from_mtime(TMP_DIR / "ws_realtime.log"),
        },
    ]

    logs = [
        {
            "name": "东方财富抓取日志",
            "path": str(TMP_DIR / "cn_eastmoney_fetch.log"),
            "last_update": _iso_from_mtime(TMP_DIR / "cn_eastmoney_fetch.log"),
            "tail": _tail_text(TMP_DIR / "cn_eastmoney_fetch.log"),
        },
        {
            "name": "东方财富守护日志",
            "path": str(TMP_DIR / "cn_eastmoney_10s_watchdog.log"),
            "last_update": _iso_from_mtime(TMP_DIR / "cn_eastmoney_10s_watchdog.log"),
            "tail": _tail_text(TMP_DIR / "cn_eastmoney_10s_watchdog.log"),
        },
        {
            "name": "国内新闻cron日志",
            "path": str(TMP_DIR / "cn_news_fetch_cron.log"),
            "last_update": _iso_from_mtime(TMP_DIR / "cn_news_fetch_cron.log"),
            "tail": _tail_text(TMP_DIR / "cn_news_fetch_cron.log"),
        },
        {
            "name": "国际新闻cron日志",
            "path": str(TMP_DIR / "news_fetch_cron.log"),
            "last_update": _iso_from_mtime(TMP_DIR / "news_fetch_cron.log"),
            "tail": _tail_text(TMP_DIR / "news_fetch_cron.log"),
        },
        {
            "name": "新闻 Stream Worker 日志",
            "path": str(TMP_DIR / "stream_news_worker.log"),
            "last_update": _iso_from_mtime(TMP_DIR / "stream_news_worker.log"),
            "tail": _tail_text(TMP_DIR / "stream_news_worker.log"),
        },
        {
            "name": "新闻 WebSocket 日志",
            "path": str(TMP_DIR / "ws_realtime.log"),
            "last_update": _iso_from_mtime(TMP_DIR / "ws_realtime.log"),
            "tail": _tail_text(TMP_DIR / "ws_realtime.log"),
        },
    ]

    summary = {
        "now": now_iso,
        "source_total": len(sources),
        "source_ok": sum(1 for x in sources if x["status"] == "ok"),
        "source_warn": sum(1 for x in sources if x["status"] == "warn"),
        "source_error": sum(1 for x in sources if x["status"] == "error"),
        "process_ok": sum(1 for x in processes if x["status"] == "ok"),
        "process_warn": sum(1 for x in processes if x["status"] == "warn"),
        "process_error": sum(1 for x in processes if x["status"] == "error"),
    }

    for item in sources:
        item["status_text"] = _status_text(item["status"])
    for item in processes:
        item["status_text"] = _status_text(item["status"])

    orchestrator_runs = query_job_runs(limit=10).get("items", [])
    orchestrator_summary = {
        "definitions_total": query_job_definitions().get("total", 0),
        "recent_total": len(orchestrator_runs),
        "success": sum(1 for x in orchestrator_runs if x.get("status") == "success"),
        "running": sum(1 for x in orchestrator_runs if x.get("status") == "running"),
        "failed": sum(1 for x in orchestrator_runs if x.get("status") not in {"success", "running"}),
    }

    payload = {
        "summary": summary,
        "sources": sources,
        "processes": processes,
        "orchestrator": {
            "summary": orchestrator_summary,
            "recent_runs": orchestrator_runs,
        },
        "row_counts": row_counts,
        "logs": logs,
    }
    cache_set_json("api:source-monitor:v1", payload, REDIS_CACHE_TTL_SOURCE_MONITOR)
    return payload


def _parse_audit_summary(markdown_text: str) -> dict:
    summary = {"正常": 0, "警告": 0, "提示": 0}
    lines = (markdown_text or "").splitlines()
    in_overview = False
    for line in lines:
        if line.strip() == "## 总览":
            in_overview = True
            continue
        if in_overview and line.startswith("## "):
            break
        if not in_overview:
            continue
        if "| `" not in line:
            continue
        parts = [p.strip() for p in line.strip().strip("|").split("|")]
        if len(parts) < 5:
            continue
        status = parts[4]
        if status in summary:
            summary[status] += 1
    summary["total"] = sum(summary.values())
    return summary


def query_database_audit(refresh: bool = False):
    if refresh:
        subprocess.run(
            [sys.executable, str(ROOT_DIR / "audit_database_report.py")],
            cwd=str(ROOT_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=300,
        )

    if not AUDIT_REPORT_PATH.exists():
        raise FileNotFoundError(f"审核报告不存在: {AUDIT_REPORT_PATH}")

    markdown = AUDIT_REPORT_PATH.read_text(encoding="utf-8")
    stat = AUDIT_REPORT_PATH.stat()
    generated_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "path": str(AUDIT_REPORT_PATH),
        "generated_at": generated_at,
        "markdown": markdown,
        "summary": _parse_audit_summary(markdown),
    }


def query_database_health():
    conn = sqlite3.connect(DB_PATH)
    try:
        payload = {
            "daily_latest": conn.execute("SELECT MAX(trade_date) FROM stock_daily_prices").fetchone()[0],
            "minline_latest": conn.execute("SELECT MAX(trade_date) FROM stock_minline").fetchone()[0],
            "scores_latest": conn.execute("SELECT MAX(score_date) FROM stock_scores_daily").fetchone()[0],
            "miss_events": conn.execute(
                "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' "
                "AND NOT EXISTS (SELECT 1 FROM stock_events e WHERE e.ts_code=s.ts_code)"
            ).fetchone()[0],
            "miss_governance": conn.execute(
                "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' "
                "AND NOT EXISTS (SELECT 1 FROM company_governance g WHERE g.ts_code=s.ts_code)"
            ).fetchone()[0],
            "miss_flow": conn.execute(
                "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' "
                "AND NOT EXISTS (SELECT 1 FROM capital_flow_stock c WHERE c.ts_code=s.ts_code)"
            ).fetchone()[0],
            "miss_minline": conn.execute(
                "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' "
                "AND NOT EXISTS (SELECT 1 FROM stock_minline m WHERE m.ts_code=s.ts_code)"
            ).fetchone()[0],
            "news_unscored": conn.execute(
                "SELECT COUNT(*) FROM news_feed_items WHERE COALESCE(llm_finance_importance,'')=''"
            ).fetchone()[0],
            "stock_news_unscored": conn.execute(
                "SELECT COUNT(*) FROM stock_news_items WHERE COALESCE(llm_finance_importance,'')=''"
            ).fetchone()[0],
            "news_dup_link": conn.execute(
                "SELECT COUNT(*) FROM (SELECT source, COALESCE(link,''), COUNT(*) c "
                "FROM news_feed_items GROUP BY source, COALESCE(link,'') "
                "HAVING COALESCE(link,'')<>'' AND COUNT(*)>1) t"
            ).fetchone()[0],
            "stock_news_dup_link": conn.execute(
                "SELECT COUNT(*) FROM (SELECT ts_code, COALESCE(link,''), COUNT(*) c "
                "FROM stock_news_items GROUP BY ts_code, COALESCE(link,'') "
                "HAVING COALESCE(link,'')<>'' AND COUNT(*)>1) t"
            ).fetchone()[0],
            "macro_publish_empty": conn.execute(
                "SELECT COUNT(*) FROM macro_series WHERE COALESCE(publish_date,'')=''"
            ).fetchone()[0],
            "chatlog_dup_key": conn.execute(
                "SELECT COUNT(*) FROM (SELECT message_key, COUNT(*) c FROM wechat_chatlog_clean_items "
                "GROUP BY message_key HAVING COUNT(*)>1) t"
            ).fetchone()[0],
        }
    finally:
        conn.close()
    return payload


def _safe_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _probe_backend_port_health(port: int, timeout: float = 1.5) -> dict:
    url = f"http://127.0.0.1:{int(port)}/api/health"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        payload = json.loads(raw) if raw else {}
        return {
            "port": int(port),
            "ok": bool(payload.get("ok")),
            "build_id": str(payload.get("build_id") or ""),
            "pid": payload.get("pid"),
            "started_at": payload.get("started_at"),
            "db": payload.get("db"),
            "error": "",
        }
    except Exception as exc:
        return {
            "port": int(port),
            "ok": False,
            "build_id": "",
            "pid": None,
            "started_at": None,
            "db": None,
            "error": str(exc),
        }


def query_api_stack_consistency() -> dict:
    ports = [8002, 8004, 8005, 8006]
    items = [_probe_backend_port_health(p) for p in ports]
    ok_items = [x for x in items if x.get("ok")]
    build_ids = [str(x.get("build_id") or "") for x in ok_items if str(x.get("build_id") or "")]
    unique_build_ids = sorted(set(build_ids))
    all_ports_online = len(ok_items) == len(ports)
    build_consistent = all_ports_online and len(unique_build_ids) == 1
    return {
        "ports": ports,
        "items": items,
        "all_ports_online": all_ports_online,
        "build_consistent": build_consistent,
        "unique_build_ids": unique_build_ids,
        "expected_build_id": unique_build_ids[0] if len(unique_build_ids) == 1 else "",
    }


def query_dashboard():
    cached = cache_get_json("api:dashboard:v1")
    if cached:
        return cached

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        stock_total = conn.execute("SELECT COUNT(*) FROM stock_codes").fetchone()[0]
        listed_total = conn.execute("SELECT COUNT(*) FROM stock_codes WHERE list_status = 'L'").fetchone()[0]
        delisted_total = conn.execute("SELECT COUNT(*) FROM stock_codes WHERE list_status = 'D'").fetchone()[0]
        paused_total = conn.execute("SELECT COUNT(*) FROM stock_codes WHERE list_status = 'P'").fetchone()[0]

        def _table_exists(name: str) -> bool:
            return (
                conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                    (name,),
                ).fetchone()[0]
                > 0
            )

        news_total = conn.execute("SELECT COUNT(*) FROM news_feed_items").fetchone()[0] if _table_exists("news_feed_items") else 0
        stock_news_total = (
            conn.execute("SELECT COUNT(*) FROM stock_news_items").fetchone()[0] if _table_exists("stock_news_items") else 0
        )
        chatlog_total = (
            conn.execute("SELECT COUNT(*) FROM wechat_chatlog_clean_items").fetchone()[0]
            if _table_exists("wechat_chatlog_clean_items")
            else 0
        )
        chatroom_total = conn.execute("SELECT COUNT(*) FROM chatroom_list_items").fetchone()[0] if _table_exists("chatroom_list_items") else 0
        monitored_chatroom_total = (
            conn.execute(
                "SELECT COUNT(*) FROM chatroom_list_items WHERE COALESCE(skip_realtime_monitor, 0) = 0"
            ).fetchone()[0]
            if _table_exists("chatroom_list_items")
            else 0
        )
        candidate_total = (
            conn.execute("SELECT COUNT(*) FROM chatroom_stock_candidate_pool").fetchone()[0]
            if _table_exists("chatroom_stock_candidate_pool")
            else 0
        )
        daily_summary_total = (
            conn.execute("SELECT COUNT(*) FROM news_daily_summaries").fetchone()[0]
            if _table_exists("news_daily_summaries")
            else 0
        )

        top_scores: list[dict] = []
        if _table_exists("stock_scores_daily"):
            latest_score_date = conn.execute("SELECT MAX(score_date) FROM stock_scores_daily").fetchone()[0]
            if latest_score_date:
                top_scores = [
                    dict(r)
                    for r in conn.execute(
                        """
                        SELECT ts_code, name, industry, market, total_score, industry_total_score, score_date
                        FROM stock_scores_daily
                        WHERE score_date = ?
                        ORDER BY COALESCE(industry_total_score, total_score, 0) DESC, COALESCE(total_score, 0) DESC, ts_code
                        LIMIT 6
                        """,
                        (latest_score_date,),
                    ).fetchall()
                ]

        candidate_pool_top: list[dict] = []
        if _table_exists("chatroom_stock_candidate_pool"):
            candidate_pool_top = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT candidate_name, candidate_type, dominant_bias, net_score, room_count, mention_count, latest_analysis_date
                    FROM chatroom_stock_candidate_pool
                    ORDER BY ABS(COALESCE(net_score, 0)) DESC, COALESCE(room_count, 0) DESC, COALESCE(mention_count, 0) DESC, candidate_name
                    LIMIT 8
                    """
                ).fetchall()
            ]

        recent_daily_summaries: list[dict] = []
        if _table_exists("news_daily_summaries"):
            recent_daily_summaries = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT id, summary_date, model, news_count, created_at
                    FROM news_daily_summaries
                    ORDER BY summary_date DESC, id DESC
                    LIMIT 5
                    """
                ).fetchall()
            ]

        important_news: list[dict] = []
        if _table_exists("news_feed_items"):
            cols = {r[1] for r in conn.execute("PRAGMA table_info(news_feed_items)").fetchall()}
            if "llm_finance_importance" in cols:
                important_news = [
                    dict(r)
                    for r in conn.execute(
                        """
                        SELECT id, source, title, pub_date, llm_finance_importance, llm_finance_impact_score
                        FROM news_feed_items
                        ORDER BY
                            CASE COALESCE(llm_finance_importance, '')
                                WHEN '极高' THEN 5
                                WHEN '高' THEN 4
                                WHEN '中' THEN 3
                                WHEN '低' THEN 2
                                WHEN '极低' THEN 1
                                ELSE 0
                            END DESC,
                            COALESCE(llm_finance_impact_score, 0) DESC,
                            COALESCE(pub_date, '') DESC,
                            id DESC
                        LIMIT 6
                        """
                    ).fetchall()
                ]
            else:
                important_news = [
                    dict(r)
                    for r in conn.execute(
                        "SELECT id, source, title, pub_date FROM news_feed_items ORDER BY COALESCE(pub_date, '') DESC, id DESC LIMIT 6"
                    ).fetchall()
                ]
    finally:
        conn.close()

    source_monitor = query_source_monitor()
    with ASYNC_MULTI_ROLE_LOCK:
        multi_role_jobs = list(ASYNC_MULTI_ROLE_JOBS.values())
    with ASYNC_DAILY_SUMMARY_LOCK:
        daily_summary_jobs = list(ASYNC_DAILY_SUMMARY_JOBS.values())

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "api_stack_consistency": query_api_stack_consistency(),
        "overview": {
            "stock_total": stock_total,
            "listed_total": listed_total,
            "delisted_total": delisted_total,
            "paused_total": paused_total,
            "news_total": news_total,
            "stock_news_total": stock_news_total,
            "chatlog_total": chatlog_total,
            "chatroom_total": chatroom_total,
            "monitored_chatroom_total": monitored_chatroom_total,
            "candidate_total": candidate_total,
            "daily_summary_total": daily_summary_total,
        },
        "source_monitor": {
            "summary": source_monitor.get("summary", {}),
            "sources": source_monitor.get("sources", [])[:6],
            "processes": source_monitor.get("processes", [])[:4],
        },
        "async_jobs": {
            "multi_role_running": sum(1 for x in multi_role_jobs if x.get("status") == "running"),
            "multi_role_error": sum(1 for x in multi_role_jobs if x.get("status") == "error"),
            "daily_summary_running": sum(1 for x in daily_summary_jobs if x.get("status") == "running"),
            "daily_summary_error": sum(1 for x in daily_summary_jobs if x.get("status") == "error"),
        },
        "orchestrator_alerts": query_job_alerts(limit=6, unresolved_only=True).get("items", []),
        "orchestrator_jobs": query_job_runs(limit=8).get("items", []),
        "database_health": query_database_health(),
        "top_scores": top_scores,
        "candidate_pool_top": candidate_pool_top,
        "recent_daily_summaries": recent_daily_summaries,
        "important_news": important_news,
    }
    cache_set_json("api:dashboard:v1", payload, REDIS_CACHE_TTL_DASHBOARD)
    return payload


def query_stock_detail(ts_code: str, keyword: str, lookback: int = 60):
    return build_stock_detail_service_runtime_deps()["query_stock_detail"](
        ts_code=ts_code,
        keyword=keyword,
        lookback=lookback,
    )


def _calc_ma(values: list[float], n: int):
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def _sanitize_json_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, dict):
        return {str(k): _sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize_json_value(v) for v in value]
    return value


def _ensure_auth_tables(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            session_token_hash TEXT NOT NULL UNIQUE,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_sessions_user ON app_auth_sessions(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_sessions_expire ON app_auth_sessions(expires_at)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invite_code TEXT NOT NULL UNIQUE,
            max_uses INTEGER NOT NULL DEFAULT 1,
            used_count INTEGER NOT NULL DEFAULT 0,
            expires_at TIMESTAMP,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_by TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_email_verifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            email TEXT NOT NULL,
            verify_code TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            used_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_usage_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            usage_date TEXT NOT NULL,
            trend_count INTEGER NOT NULL DEFAULT 0,
            multi_role_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_app_auth_usage_user_date ON app_auth_usage_daily(user_id, usage_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_verify_user ON app_auth_email_verifications(user_id, used_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_verify_email ON app_auth_email_verifications(email, used_at)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            username TEXT,
            user_id INTEGER,
            result TEXT NOT NULL DEFAULT 'ok',
            detail TEXT,
            ip TEXT,
            user_agent TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_audit_time ON app_auth_audit_logs(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_audit_user ON app_auth_audit_logs(username, user_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_password_resets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            reset_code TEXT NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            used_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_app_auth_pwd_reset_user ON app_auth_password_resets(user_id, used_at)")

    # migrate columns for existing deployments
    try:
        columns = {
            str(r["name"]) if isinstance(r, dict) else str(r[1])
            for r in conn.execute("PRAGMA table_info(app_auth_users)").fetchall()
        }
    except Exception:
        columns = set()
    if "email" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN email TEXT")
    if "email_verified" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN email_verified INTEGER NOT NULL DEFAULT 0")
    if "role" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN role TEXT NOT NULL DEFAULT 'limited'")
    if "tier" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN tier TEXT NOT NULL DEFAULT 'limited'")
    if "invite_code_used" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN invite_code_used TEXT")
    if "failed_login_count" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN failed_login_count INTEGER NOT NULL DEFAULT 0")
    if "locked_until" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN locked_until TIMESTAMP")
    if "last_login_at" not in columns:
        conn.execute("ALTER TABLE app_auth_users ADD COLUMN last_login_at TIMESTAMP")

    try:
        usage_columns = {
            str(r["name"]) if isinstance(r, dict) else str(r[1])
            for r in conn.execute("PRAGMA table_info(app_auth_usage_daily)").fetchall()
        }
    except Exception:
        usage_columns = set()
    if "multi_role_count" not in usage_columns:
        conn.execute("ALTER TABLE app_auth_usage_daily ADD COLUMN multi_role_count INTEGER NOT NULL DEFAULT 0")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_app_auth_users_email ON app_auth_users(email)")


def _hash_password(password: str, salt_hex: str | None = None) -> str:
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 210_000)
    return f"pbkdf2_sha256$210000${salt.hex()}${digest.hex()}"


def _verify_password(password: str, stored_hash: str) -> bool:
    try:
        algo, rounds_s, salt_hex, digest_hex = (stored_hash or "").split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        rounds = int(rounds_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
        return secrets.compare_digest(actual, expected)
    except Exception:
        return False


def _invalidate_auth_users_count_cache():
    with AUTH_USERS_COUNT_LOCK:
        AUTH_USERS_COUNT_CACHE["value"] = -1
        AUTH_USERS_COUNT_CACHE["expires_at"] = 0.0


def _active_auth_users_count(force: bool = False) -> int:
    now = time.time()
    with AUTH_USERS_COUNT_LOCK:
        if not force and AUTH_USERS_COUNT_CACHE["value"] >= 0 and now < float(AUTH_USERS_COUNT_CACHE["expires_at"]):
            return int(AUTH_USERS_COUNT_CACHE["value"])
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute("SELECT COUNT(*) FROM app_auth_users WHERE is_active = 1").fetchone()
        count = int((row[0] if row else 0) or 0)
    finally:
        conn.close()
    with AUTH_USERS_COUNT_LOCK:
        AUTH_USERS_COUNT_CACHE["value"] = count
        AUTH_USERS_COUNT_CACHE["expires_at"] = now + AUTH_USERS_COUNT_CACHE_SECONDS
    return count


def _new_session_token() -> str:
    return f"u_{secrets.token_urlsafe(32)}"


def _record_auth_audit(
    event_type: str,
    username: str = "",
    user_id: int | None = None,
    result: str = "ok",
    detail: str = "",
    ip: str = "",
    user_agent: str = "",
):
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        conn.execute(
            """
            INSERT INTO app_auth_audit_logs (event_type, username, user_id, result, detail, ip, user_agent, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                (event_type or "").strip(),
                (username or "").strip(),
                int(user_id or 0) if user_id is not None else None,
                (result or "ok").strip(),
                (detail or "").strip()[:500],
                (ip or "").strip()[:100],
                (user_agent or "").strip()[:300],
            ),
        )
    finally:
        conn.close()


def _user_locked(locked_until) -> bool:
    raw = str(locked_until or "").strip()
    if not raw:
        return False
    norm = raw.replace("T", " ").replace("Z", "")
    try:
        when = datetime.strptime(norm[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return False
    return when > datetime.utcnow()


def _register_login_failure(conn, user_id: int):
    row = conn.execute(
        "SELECT failed_login_count FROM app_auth_users WHERE id = ? LIMIT 1",
        (user_id,),
    ).fetchone()
    current = int((row[0] if row else 0) or 0)
    next_count = current + 1
    if next_count >= max(AUTH_LOCK_THRESHOLD, 1):
        lock_until = (datetime.now(timezone.utc) + timedelta(minutes=max(AUTH_LOCK_MINUTES, 1))).replace(tzinfo=None)
        conn.execute(
            "UPDATE app_auth_users SET failed_login_count = ?, locked_until = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (next_count, lock_until, user_id),
        )
    else:
        conn.execute(
            "UPDATE app_auth_users SET failed_login_count = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (next_count, user_id),
        )


def _clear_login_failure(conn, user_id: int):
    conn.execute(
        "UPDATE app_auth_users SET failed_login_count = 0, locked_until = NULL, last_login_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (user_id,),
    )


def _generate_invite_code() -> str:
    return f"INV-{secrets.token_hex(4).upper()}"


def _create_invite_code(
    max_uses: int = 1,
    expires_at: str = "",
    created_by: str = "",
    explicit_code: str = "",
) -> dict:
    code = (explicit_code or _generate_invite_code()).strip().upper()
    max_uses = max(1, int(max_uses or 1))
    expiry = (expires_at or "").strip() or None
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        conn.execute(
            """
            INSERT INTO app_auth_invites (invite_code, max_uses, used_count, expires_at, is_active, created_by, created_at, updated_at)
            VALUES (?, ?, 0, ?, 1, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (code, max_uses, expiry, (created_by or "").strip()),
        )
    finally:
        conn.close()
    return {"invite_code": code, "max_uses": max_uses, "expires_at": expiry}


def _assert_invite_valid(conn, invite_code: str) -> str:
    code = (invite_code or "").strip().upper()
    if not code:
        raise ValueError("邀请码不能为空")
    row = conn.execute(
        """
        SELECT invite_code, max_uses, used_count, is_active, expires_at
        FROM app_auth_invites
        WHERE invite_code = ?
        LIMIT 1
        """,
        (code,),
    ).fetchone()
    if not row:
        raise RuntimeError("邀请码不存在")
    max_uses = int((row[1] or 0) or 0)
    used_count = int((row[2] or 0) or 0)
    is_active = int((row[3] or 0) or 0) == 1
    expires_at = row[4]
    if not is_active:
        raise RuntimeError("邀请码已失效")
    if max_uses > 0 and used_count >= max_uses:
        raise RuntimeError("邀请码已用尽")
    if expires_at:
        expires_s = str(expires_at).strip().replace("T", " ").replace("Z", "")
        if expires_s and datetime.strptime(expires_s[:19], "%Y-%m-%d %H:%M:%S") < datetime.utcnow():
            raise RuntimeError("邀请码已过期")
    return code


def _reserve_invite_use(conn, invite_code: str):
    conn.execute(
        "UPDATE app_auth_invites SET used_count = COALESCE(used_count, 0) + 1, updated_at = CURRENT_TIMESTAMP WHERE invite_code = ?",
        (invite_code,),
    )


def _create_email_verification(conn, user_id: int, email: str) -> dict:
    verify_code = f"{secrets.randbelow(1000000):06d}"
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=15)).replace(tzinfo=None)
    conn.execute(
        "UPDATE app_auth_email_verifications SET used_at = CURRENT_TIMESTAMP WHERE user_id = ? AND used_at IS NULL",
        (user_id,),
    )
    conn.execute(
        """
        INSERT INTO app_auth_email_verifications (user_id, email, verify_code, expires_at, used_at, created_at)
        VALUES (?, ?, ?, ?, NULL, CURRENT_TIMESTAMP)
        """,
        (user_id, email, verify_code, expires_at),
    )
    return {
        "sent": True,
        "expires_minutes": 15,
        "email_masked": re.sub(r"(^.).+(@.+$)", r"\1***\2", email),
        "dev_verify_code": verify_code if os.getenv("AUTH_DEV_EXPOSE_VERIFY_CODE", "1").strip().lower() not in {"0", "false", "no"} else "",
    }


def _register_auth_user(
    username: str,
    password: str,
    display_name: str = "",
    email: str = "",
    invite_code: str = "",
) -> tuple[str, dict, dict]:
    normalized_username = (username or "").strip()
    pwd = (password or "").strip()
    display = (display_name or "").strip()
    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        normalized_email = f"{normalized_username.lower()}@local.invalid"
    if not re.fullmatch(r"[A-Za-z0-9_.\-]{3,32}", normalized_username):
        raise ValueError("用户名仅支持 3-32 位英文、数字、下划线、点和中划线")
    if len(pwd) < 6:
        raise ValueError("密码至少 6 位")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        invite = _assert_invite_valid(conn, invite_code)
        exists = conn.execute("SELECT id FROM app_auth_users WHERE username = ?", (normalized_username,)).fetchone()
        if exists:
            raise RuntimeError("用户名已存在")
        email_exists = conn.execute("SELECT id FROM app_auth_users WHERE email = ? LIMIT 1", (normalized_email,)).fetchone()
        if email_exists:
            raise RuntimeError("邮箱已被注册")
        conn.execute(
            """
            INSERT INTO app_auth_users (
                username, password_hash, display_name, email, email_verified, role, tier, invite_code_used, is_active, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, 1, 'limited', 'limited', ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (normalized_username, _hash_password(pwd), display, normalized_email, invite),
        )
        _reserve_invite_use(conn, invite)
        _invalidate_auth_users_count_cache()
        row = conn.execute(
            "SELECT id, username, display_name, email, role, tier, email_verified FROM app_auth_users WHERE username = ? LIMIT 1",
            (normalized_username,),
        ).fetchone()
        if not row:
            raise RuntimeError("注册后未找到用户")
        user_id = int(row[0])
        token = _new_session_token()
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=max(AUTH_SESSION_DAYS, 1))).replace(tzinfo=None)
        conn.execute("DELETE FROM app_auth_sessions WHERE expires_at <= CURRENT_TIMESTAMP")
        conn.execute(
            "INSERT INTO app_auth_sessions (user_id, session_token_hash, expires_at, created_at, last_seen_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            (user_id, token_hash, expires_at),
        )
        conn.execute(
            """
            DELETE FROM app_auth_sessions
            WHERE user_id = ? AND id NOT IN (
                SELECT id FROM app_auth_sessions WHERE user_id = ? ORDER BY id DESC LIMIT 10
            )
            """,
            (user_id, user_id),
        )
        user = {
            "id": user_id,
            "username": str(row[1] or ""),
            "display_name": str(row[2] or ""),
            "email": str(row[3] or ""),
            "role": str(row[4] or "limited"),
            "tier": str(row[5] or "limited"),
            "email_verified": int((row[6] or 0)) == 1,
        }
        return token, user, {}
    finally:
        conn.close()


def _login_auth_user(username: str, password: str) -> tuple[str, dict]:
    normalized_username = (username or "").strip()
    pwd = (password or "").strip()
    if not normalized_username or not pwd:
        raise ValueError("用户名和密码不能为空")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            "SELECT id, username, display_name, password_hash, is_active, email, role, tier, email_verified, failed_login_count, locked_until FROM app_auth_users WHERE username = ? LIMIT 1",
            (normalized_username,),
        ).fetchone()
        if not row or int((row[4] or 0)) != 1:
            raise RuntimeError("用户名或密码错误")
        if _user_locked(row[10]):
            raise RuntimeError(f"账号已临时锁定，请 {AUTH_LOCK_MINUTES} 分钟后再试")
        if not _verify_password(pwd, str(row[3] or "")):
            _register_login_failure(conn, int(row[0]))
            raise RuntimeError("用户名或密码错误")
        user_id = int(row[0])
        _clear_login_failure(conn, user_id)
        token = _new_session_token()
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        expires_at = (datetime.now(timezone.utc) + timedelta(days=max(AUTH_SESSION_DAYS, 1))).replace(tzinfo=None)
        conn.execute("DELETE FROM app_auth_sessions WHERE expires_at <= CURRENT_TIMESTAMP")
        conn.execute(
            "INSERT INTO app_auth_sessions (user_id, session_token_hash, expires_at, created_at, last_seen_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            (user_id, token_hash, expires_at),
        )
        conn.execute(
            """
            DELETE FROM app_auth_sessions
            WHERE user_id = ? AND id NOT IN (
                SELECT id FROM app_auth_sessions WHERE user_id = ? ORDER BY id DESC LIMIT 10
            )
            """,
            (user_id, user_id),
        )
        return token, {
            "id": user_id,
            "username": str(row[1] or ""),
            "display_name": str(row[2] or ""),
            "email": str(row[5] or ""),
            "role": str(row[6] or "limited"),
            "tier": str(row[7] or "limited"),
            "email_verified": int((row[8] or 0)) == 1,
        }
    finally:
        conn.close()


def _verify_email_code(username: str = "", email: str = "", verify_code: str = "") -> dict:
    normalized_username = (username or "").strip()
    normalized_email = (email or "").strip().lower()
    normalized_code = (verify_code or "").strip()
    if not normalized_code:
        raise ValueError("验证码不能为空")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if normalized_username:
            user = conn.execute("SELECT id, username, email FROM app_auth_users WHERE username = ? LIMIT 1", (normalized_username,)).fetchone()
        elif normalized_email:
            user = conn.execute("SELECT id, username, email FROM app_auth_users WHERE email = ? LIMIT 1", (normalized_email,)).fetchone()
        else:
            raise ValueError("请提供用户名或邮箱")
        if not user:
            raise RuntimeError("用户不存在")
        user_id = int(user[0])
        user_email = str(user[2] or "").strip().lower()
        row = conn.execute(
            """
            SELECT id
            FROM app_auth_email_verifications
            WHERE user_id = ?
              AND email = ?
              AND verify_code = ?
              AND used_at IS NULL
              AND expires_at > CURRENT_TIMESTAMP
            ORDER BY id DESC
            LIMIT 1
            """,
            (user_id, user_email, normalized_code),
        ).fetchone()
        if not row:
            raise RuntimeError("验证码无效或已过期")
        conn.execute("UPDATE app_auth_email_verifications SET used_at = CURRENT_TIMESTAMP WHERE id = ?", (int(row[0]),))
        conn.execute(
            "UPDATE app_auth_users SET email_verified = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (user_id,),
        )
        return {"user_id": user_id, "username": str(user[1] or ""), "email": user_email, "verified": True}
    finally:
        conn.close()


def _resend_email_verification(username: str = "", email: str = "") -> dict:
    normalized_username = (username or "").strip()
    normalized_email = (email or "").strip().lower()
    if not normalized_username and not normalized_email:
        raise ValueError("请提供用户名或邮箱")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if normalized_username:
            user = conn.execute(
                "SELECT id, username, email, email_verified FROM app_auth_users WHERE username = ? LIMIT 1",
                (normalized_username,),
            ).fetchone()
        else:
            user = conn.execute(
                "SELECT id, username, email, email_verified FROM app_auth_users WHERE email = ? LIMIT 1",
                (normalized_email,),
            ).fetchone()
        if not user:
            raise RuntimeError("用户不存在，请先注册")
        if int((user[3] or 0)) == 1:
            raise RuntimeError("该账号邮箱已验证，无需重复发送")
        payload = _create_email_verification(conn, int(user[0]), str(user[2] or "").strip().lower())
        payload["username"] = str(user[1] or "")
        return payload
    finally:
        conn.close()


def _forgot_password(username_or_email: str = "") -> dict:
    key = (username_or_email or "").strip()
    if not key:
        raise ValueError("请输入账号或邮箱")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            """
            SELECT id, username, email, is_active
            FROM app_auth_users
            WHERE username = ? OR email = ?
            LIMIT 1
            """,
            (key, key.lower()),
        ).fetchone()
        if not row or int((row[3] or 0)) != 1:
            return {"sent": True}
        user_id = int(row[0])
        username = str(row[1] or "")
        code = f"{secrets.randbelow(1000000):06d}"
        expires_at = (datetime.now(timezone.utc) + timedelta(minutes=15)).replace(tzinfo=None)
        conn.execute(
            "UPDATE app_auth_password_resets SET used_at = CURRENT_TIMESTAMP WHERE user_id = ? AND used_at IS NULL",
            (user_id,),
        )
        conn.execute(
            """
            INSERT INTO app_auth_password_resets (user_id, username, reset_code, expires_at, used_at, created_at)
            VALUES (?, ?, ?, ?, NULL, CURRENT_TIMESTAMP)
            """,
            (user_id, username, code, expires_at),
        )
        return {
            "sent": True,
            "username": username,
            "expires_minutes": 15,
            "dev_reset_code": code if os.getenv("AUTH_DEV_EXPOSE_VERIFY_CODE", "1").strip().lower() not in {"0", "false", "no"} else "",
        }
    finally:
        conn.close()


def _reset_password_with_code(username: str = "", reset_code: str = "", new_password: str = "") -> dict:
    uname = (username or "").strip()
    code = (reset_code or "").strip()
    pwd = (new_password or "").strip()
    if not uname or not code or not pwd:
        raise ValueError("缺少用户名、验证码或新密码")
    if len(pwd) < 6:
        raise ValueError("新密码至少6位")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        user = conn.execute("SELECT id, username FROM app_auth_users WHERE username = ? LIMIT 1", (uname,)).fetchone()
        if not user:
            raise RuntimeError("用户不存在")
        user_id = int(user[0])
        row = conn.execute(
            """
            SELECT id
            FROM app_auth_password_resets
            WHERE user_id = ?
              AND username = ?
              AND reset_code = ?
              AND used_at IS NULL
              AND expires_at > CURRENT_TIMESTAMP
            ORDER BY id DESC
            LIMIT 1
            """,
            (user_id, uname, code),
        ).fetchone()
        if not row:
            raise RuntimeError("重置码无效或已过期")
        conn.execute("UPDATE app_auth_password_resets SET used_at = CURRENT_TIMESTAMP WHERE id = ?", (int(row[0]),))
        conn.execute(
            "UPDATE app_auth_users SET password_hash = ?, failed_login_count = 0, locked_until = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (_hash_password(pwd), user_id),
        )
        conn.execute("DELETE FROM app_auth_sessions WHERE user_id = ?", (user_id,))
        return {"ok": True, "username": uname}
    finally:
        conn.close()


def _consume_trend_daily_quota(user: dict | None) -> dict:
    if not user:
        return {"allowed": True, "limit": None, "used": 0, "remaining": None}
    role = str(user.get("role") or user.get("tier") or "limited")
    limit = TREND_DAILY_LIMIT_BY_ROLE.get(role)
    if not limit:
        return {"allowed": True, "limit": None, "used": 0, "remaining": None}
    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        return {"allowed": False, "limit": limit, "used": limit, "remaining": 0}
    usage_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            "SELECT trend_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (user_id, usage_date),
        ).fetchone()
        used = int((row[0] if row else 0) or 0)
        if used >= limit:
            return {"allowed": False, "limit": limit, "used": used, "remaining": 0}
        if row:
            conn.execute(
                "UPDATE app_auth_usage_daily SET trend_count = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND usage_date = ?",
                (used + 1, user_id, usage_date),
            )
        else:
            conn.execute(
                "INSERT INTO app_auth_usage_daily (user_id, usage_date, trend_count, created_at, updated_at) VALUES (?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (user_id, usage_date),
            )
        return {"allowed": True, "limit": limit, "used": used + 1, "remaining": max(limit - (used + 1), 0)}
    finally:
        conn.close()


def _get_trend_daily_quota_status(user: dict | None) -> dict:
    if not user:
        return {"limit": None, "used": 0, "remaining": None}
    role = str(user.get("role") or user.get("tier") or "limited")
    limit = TREND_DAILY_LIMIT_BY_ROLE.get(role)
    if not limit:
        return {"limit": None, "used": 0, "remaining": None}
    user_id = int(user.get("id") or 0)
    usage_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            "SELECT trend_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (user_id, usage_date),
        ).fetchone()
        used = int((row[0] if row else 0) or 0)
        return {"limit": limit, "used": used, "remaining": max(limit - used, 0)}
    finally:
        conn.close()


def _consume_multi_role_daily_quota(user: dict | None) -> dict:
    if not user:
        return {"allowed": True, "limit": None, "used": 0, "remaining": None}
    role = str(user.get("role") or user.get("tier") or "limited")
    limit = MULTI_ROLE_DAILY_LIMIT_BY_ROLE.get(role)
    if not limit:
        return {"allowed": True, "limit": None, "used": 0, "remaining": None}
    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        return {"allowed": False, "limit": limit, "used": limit, "remaining": 0}
    usage_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            "SELECT multi_role_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (user_id, usage_date),
        ).fetchone()
        used = int((row[0] if row else 0) or 0)
        if used >= limit:
            return {"allowed": False, "limit": limit, "used": used, "remaining": 0}
        if row:
            conn.execute(
                "UPDATE app_auth_usage_daily SET multi_role_count = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND usage_date = ?",
                (used + 1, user_id, usage_date),
            )
        else:
            conn.execute(
                "INSERT INTO app_auth_usage_daily (user_id, usage_date, multi_role_count, created_at, updated_at) VALUES (?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (user_id, usage_date),
            )
        return {"allowed": True, "limit": limit, "used": used + 1, "remaining": max(limit - (used + 1), 0)}
    finally:
        conn.close()


def _get_multi_role_daily_quota_status(user: dict | None) -> dict:
    if not user:
        return {"limit": None, "used": 0, "remaining": None}
    role = str(user.get("role") or user.get("tier") or "limited")
    limit = MULTI_ROLE_DAILY_LIMIT_BY_ROLE.get(role)
    if not limit:
        return {"limit": None, "used": 0, "remaining": None}
    user_id = int(user.get("id") or 0)
    usage_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            "SELECT multi_role_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (user_id, usage_date),
        ).fetchone()
        used = int((row[0] if row else 0) or 0)
        return {"limit": limit, "used": used, "remaining": max(limit - used, 0)}
    finally:
        conn.close()


def _validate_auth_session(token: str) -> dict | None:
    normalized = (token or "").strip()
    if not normalized:
        return None
    token_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        row = conn.execute(
            """
            SELECT u.id, u.username, u.display_name, u.email, u.role, u.tier, u.email_verified
            FROM app_auth_sessions s
            JOIN app_auth_users u ON u.id = s.user_id
            WHERE s.session_token_hash = ?
              AND s.expires_at > CURRENT_TIMESTAMP
              AND u.is_active = 1
            ORDER BY s.id DESC
            LIMIT 1
            """,
            (token_hash,),
        ).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE app_auth_sessions SET last_seen_at = CURRENT_TIMESTAMP WHERE session_token_hash = ?",
            (token_hash,),
        )
        return {
            "id": int(row[0]),
            "username": str(row[1] or ""),
            "display_name": str(row[2] or ""),
            "email": str(row[3] or ""),
            "role": str(row[4] or "limited"),
            "tier": str(row[5] or "limited"),
            "email_verified": int((row[6] or 0)) == 1,
        }
    finally:
        conn.close()


def _revoke_auth_session(token: str) -> int:
    normalized = (token or "").strip()
    if not normalized:
        return 0
    token_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        cur = conn.execute("DELETE FROM app_auth_sessions WHERE session_token_hash = ?", (token_hash,))
        return int(getattr(cur, "rowcount", 0) or 0)
    finally:
        conn.close()


def _query_auth_users(keyword: str = "", role: str = "", active: str = "", page: int = 1, page_size: int = 20) -> dict:
    kw = (keyword or "").strip()
    role_s = (role or "").strip().lower()
    active_s = (active or "").strip().lower()
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 20), 200), 1)
    where = []
    vals: list[object] = []
    if kw:
        where.append("(username LIKE ? OR display_name LIKE ? OR email LIKE ?)")
        like = f"%{kw}%"
        vals.extend([like, like, like])
    if role_s:
        where.append("LOWER(role) = ?")
        vals.append(role_s)
    if active_s in {"1", "true", "yes", "on"}:
        where.append("is_active = 1")
    elif active_s in {"0", "false", "no", "off"}:
        where.append("is_active = 0")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        total = int((conn.execute(f"SELECT COUNT(*) FROM app_auth_users {where_sql}", tuple(vals)).fetchone()[0]) or 0)
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT id, username, display_name, email, role, tier, is_active, email_verified, failed_login_count, locked_until, created_at, updated_at, last_login_at
            FROM app_auth_users
            {where_sql}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*vals, page_size, offset]),
        ).fetchall()
    finally:
        conn.close()
    items = [
        {
            "id": int(r[0]),
            "username": str(r[1] or ""),
            "display_name": str(r[2] or ""),
            "email": str(r[3] or ""),
            "role": str(r[4] or "limited"),
            "tier": str(r[5] or "limited"),
            "is_active": int((r[6] or 0)) == 1,
            "email_verified": int((r[7] or 0)) == 1,
            "failed_login_count": int((r[8] or 0) or 0),
            "locked_until": r[9],
            "created_at": r[10],
            "updated_at": r[11],
            "last_login_at": r[12],
        }
        for r in rows
    ]
    usage_date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    usage_map: dict[int, dict[str, int]] = {}
    if items:
        user_ids = [int(it["id"]) for it in items]
        conn = sqlite3.connect(DB_PATH)
        try:
            _ensure_auth_tables(conn)
            placeholders = ",".join(["?"] * len(user_ids))
            usage_rows = conn.execute(
                f"""
                SELECT user_id, trend_count, multi_role_count
                FROM app_auth_usage_daily
                WHERE usage_date = ? AND user_id IN ({placeholders})
                """,
                tuple([usage_date_utc, *user_ids]),
            ).fetchall()
            usage_map = {
                int(r[0]): {
                    "trend": int((r[1] or 0) or 0),
                    "multi_role": int((r[2] or 0) or 0),
                }
                for r in usage_rows
            }
        finally:
            conn.close()
    for item in items:
        role_key = str(item.get("role") or item.get("tier") or "limited")
        usage = usage_map.get(int(item["id"]), {"trend": 0, "multi_role": 0})
        trend_limit = TREND_DAILY_LIMIT_BY_ROLE.get(role_key)
        trend_used = int(usage.get("trend", 0))
        multi_role_limit = MULTI_ROLE_DAILY_LIMIT_BY_ROLE.get(role_key)
        multi_role_used = int(usage.get("multi_role", 0))
        item["trend_usage_date_utc"] = usage_date_utc
        item["trend_used_today"] = trend_used
        item["trend_limit"] = trend_limit
        item["trend_remaining_today"] = (None if trend_limit is None else max(int(trend_limit) - trend_used, 0))
        item["multi_role_used_today"] = multi_role_used
        item["multi_role_limit"] = multi_role_limit
        item["multi_role_remaining_today"] = (
            None if multi_role_limit is None else max(int(multi_role_limit) - multi_role_used, 0)
        )
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size else 0,
    }


def _update_auth_user(user_id: int | None = None, username: str = "", role: str | None = None, is_active: int | None = None, display_name: str | None = None) -> dict:
    uid = int(user_id or 0)
    uname = (username or "").strip()
    if uid <= 0 and not uname:
        raise ValueError("缺少 user_id 或 username")
    updates = []
    vals: list[object] = []
    if role is not None:
        role_s = str(role or "").strip().lower()
        if role_s not in {"limited", "pro", "admin"}:
            raise ValueError("role 必须是 limited/pro/admin")
        updates.extend(["role = ?", "tier = ?"])
        vals.extend([role_s, role_s])
    if is_active is not None:
        updates.append("is_active = ?")
        vals.append(1 if int(is_active) else 0)
    if display_name is not None:
        updates.append("display_name = ?")
        vals.append(str(display_name or "").strip())
    if not updates:
        raise ValueError("未提供可更新字段")
    updates.append("updated_at = CURRENT_TIMESTAMP")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if uid > 0:
            vals.append(uid)
            conn.execute(f"UPDATE app_auth_users SET {', '.join(updates)} WHERE id = ?", tuple(vals))
            row = conn.execute("SELECT id, username, role, tier, is_active, display_name FROM app_auth_users WHERE id = ? LIMIT 1", (uid,)).fetchone()
        else:
            vals.append(uname)
            conn.execute(f"UPDATE app_auth_users SET {', '.join(updates)} WHERE username = ?", tuple(vals))
            row = conn.execute("SELECT id, username, role, tier, is_active, display_name FROM app_auth_users WHERE username = ? LIMIT 1", (uname,)).fetchone()
        if not row:
            raise RuntimeError("用户不存在")
        _invalidate_auth_users_count_cache()
        return {
            "id": int(row[0]),
            "username": str(row[1] or ""),
            "role": str(row[2] or "limited"),
            "tier": str(row[3] or "limited"),
            "is_active": int((row[4] or 0)) == 1,
            "display_name": str(row[5] or ""),
        }
    finally:
        conn.close()


def _admin_reset_user_password(user_id: int | None = None, username: str = "", new_password: str = "") -> dict:
    uid = int(user_id or 0)
    uname = (username or "").strip()
    pwd = (new_password or "").strip()
    if uid <= 0 and not uname:
        raise ValueError("缺少 user_id 或 username")
    if len(pwd) < 6:
        raise ValueError("新密码至少6位")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if uid > 0:
            conn.execute(
                "UPDATE app_auth_users SET password_hash = ?, failed_login_count = 0, locked_until = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (_hash_password(pwd), uid),
            )
            row = conn.execute("SELECT id, username FROM app_auth_users WHERE id = ? LIMIT 1", (uid,)).fetchone()
        else:
            conn.execute(
                "UPDATE app_auth_users SET password_hash = ?, failed_login_count = 0, locked_until = NULL, updated_at = CURRENT_TIMESTAMP WHERE username = ?",
                (_hash_password(pwd), uname),
            )
            row = conn.execute("SELECT id, username FROM app_auth_users WHERE username = ? LIMIT 1", (uname,)).fetchone()
        if not row:
            raise RuntimeError("用户不存在")
        conn.execute("DELETE FROM app_auth_sessions WHERE user_id = ?", (int(row[0]),))
        return {"id": int(row[0]), "username": str(row[1] or ""), "session_revoked": True}
    finally:
        conn.close()


def _admin_reset_user_trend_quota(user_id: int | None = None, username: str = "", usage_date: str = "") -> dict:
    uid = int(user_id or 0)
    uname = (username or "").strip()
    usage_date_s = (usage_date or "").strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if uid <= 0 and not uname:
        raise ValueError("缺少 user_id 或 username")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if uid > 0:
            row = conn.execute(
                "SELECT id, username FROM app_auth_users WHERE id = ? LIMIT 1",
                (uid,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT id, username FROM app_auth_users WHERE username = ? LIMIT 1",
                (uname,),
            ).fetchone()
        if not row:
            raise RuntimeError("用户不存在")
        target_user_id = int(row[0])
        row_usage = conn.execute(
            "SELECT trend_count, multi_role_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (target_user_id, usage_date_s),
        ).fetchone()
        if row_usage:
            conn.execute(
                "UPDATE app_auth_usage_daily SET trend_count = 0, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND usage_date = ?",
                (target_user_id, usage_date_s),
            )
            old_trend = int((row_usage[0] or 0) or 0)
        else:
            conn.execute(
                "INSERT INTO app_auth_usage_daily (user_id, usage_date, trend_count, multi_role_count, created_at, updated_at) VALUES (?, ?, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (target_user_id, usage_date_s),
            )
            old_trend = 0
        return {
            "id": target_user_id,
            "username": str(row[1] or ""),
            "usage_date": usage_date_s,
            "previous_trend_count": old_trend,
            "trend_count": 0,
        }
    finally:
        conn.close()


def _admin_reset_user_multi_role_quota(user_id: int | None = None, username: str = "", usage_date: str = "") -> dict:
    uid = int(user_id or 0)
    uname = (username or "").strip()
    usage_date_s = (usage_date or "").strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if uid <= 0 and not uname:
        raise ValueError("缺少 user_id 或 username")
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        if uid > 0:
            row = conn.execute(
                "SELECT id, username FROM app_auth_users WHERE id = ? LIMIT 1",
                (uid,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT id, username FROM app_auth_users WHERE username = ? LIMIT 1",
                (uname,),
            ).fetchone()
        if not row:
            raise RuntimeError("用户不存在")
        target_user_id = int(row[0])
        row_usage = conn.execute(
            "SELECT trend_count, multi_role_count FROM app_auth_usage_daily WHERE user_id = ? AND usage_date = ? LIMIT 1",
            (target_user_id, usage_date_s),
        ).fetchone()
        if row_usage:
            conn.execute(
                "UPDATE app_auth_usage_daily SET multi_role_count = 0, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND usage_date = ?",
                (target_user_id, usage_date_s),
            )
            old_multi = int((row_usage[1] or 0) or 0)
        else:
            conn.execute(
                "INSERT INTO app_auth_usage_daily (user_id, usage_date, trend_count, multi_role_count, created_at, updated_at) VALUES (?, ?, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (target_user_id, usage_date_s),
            )
            old_multi = 0
        return {
            "id": target_user_id,
            "username": str(row[1] or ""),
            "usage_date": usage_date_s,
            "previous_multi_role_count": old_multi,
            "multi_role_count": 0,
        }
    finally:
        conn.close()


def _admin_reset_quota_batch(usage_date: str = "", role: str = "", usernames: list[str] | None = None) -> dict:
    usage_date_s = (usage_date or "").strip() or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    role_s = (role or "").strip().lower()
    name_list = [str(x or "").strip() for x in (usernames or []) if str(x or "").strip()]
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        where = ["is_active = 1"]
        vals: list[object] = []
        if role_s:
            where.append("LOWER(role) = ?")
            vals.append(role_s)
        if name_list:
            placeholders = ",".join(["?"] * len(name_list))
            where.append(f"username IN ({placeholders})")
            vals.extend(name_list)
        rows = conn.execute(
            f"SELECT id, username, role FROM app_auth_users WHERE {' AND '.join(where)} ORDER BY id ASC",
            tuple(vals),
        ).fetchall()
        if not rows:
            return {
                "usage_date": usage_date_s,
                "matched_users": 0,
                "affected_rows": 0,
                "items": [],
            }
        user_ids = [int(r[0]) for r in rows]
        placeholders = ",".join(["?"] * len(user_ids))
        args = tuple([usage_date_s, *user_ids])
        cur = conn.execute(
            f"""
            UPDATE app_auth_usage_daily
            SET trend_count = 0,
                multi_role_count = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE usage_date = ? AND user_id IN ({placeholders})
            """,
            args,
        )
        affected_rows = int(getattr(cur, "rowcount", 0) or 0)
        items = [
            {"id": int(r[0]), "username": str(r[1] or ""), "role": str(r[2] or "")}
            for r in rows
        ]
        return {
            "usage_date": usage_date_s,
            "matched_users": len(items),
            "affected_rows": affected_rows,
            "items": items,
        }
    finally:
        conn.close()


def _query_auth_sessions(keyword: str = "", user_id: int | None = None, page: int = 1, page_size: int = 20) -> dict:
    kw = (keyword or "").strip()
    uid = int(user_id or 0)
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 20), 200), 1)
    where = ["s.expires_at > CURRENT_TIMESTAMP"]
    vals: list[object] = []
    if uid > 0:
        where.append("s.user_id = ?")
        vals.append(uid)
    if kw:
        where.append("(u.username LIKE ? OR u.display_name LIKE ? OR u.email LIKE ?)")
        like = f"%{kw}%"
        vals.extend([like, like, like])
    where_sql = f"WHERE {' AND '.join(where)}"
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        total = int(
            (
                conn.execute(
                    f"SELECT COUNT(*) FROM app_auth_sessions s JOIN app_auth_users u ON u.id = s.user_id {where_sql}",
                    tuple(vals),
                ).fetchone()[0]
            )
            or 0
        )
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT s.id, s.user_id, u.username, u.display_name, s.expires_at, s.created_at, s.last_seen_at, s.session_token_hash
            FROM app_auth_sessions s
            JOIN app_auth_users u ON u.id = s.user_id
            {where_sql}
            ORDER BY s.last_seen_at DESC, s.id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*vals, page_size, offset]),
        ).fetchall()
    finally:
        conn.close()
    items = [
        {
            "session_id": int(r[0]),
            "user_id": int(r[1]),
            "username": str(r[2] or ""),
            "display_name": str(r[3] or ""),
            "expires_at": r[4],
            "created_at": r[5],
            "last_seen_at": r[6],
            "token_hash_preview": f"{str(r[7] or '')[:12]}...",
        }
        for r in rows
    ]
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size else 0,
    }


def _revoke_auth_session_by_id(session_id: int) -> int:
    sid = int(session_id or 0)
    if sid <= 0:
        return 0
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        cur = conn.execute("DELETE FROM app_auth_sessions WHERE id = ?", (sid,))
        return int(getattr(cur, "rowcount", 0) or 0)
    finally:
        conn.close()


def _revoke_auth_sessions_by_user(user_id: int) -> int:
    uid = int(user_id or 0)
    if uid <= 0:
        return 0
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        cur = conn.execute("DELETE FROM app_auth_sessions WHERE user_id = ?", (uid,))
        return int(getattr(cur, "rowcount", 0) or 0)
    finally:
        conn.close()


def _query_auth_audit_logs(keyword: str = "", event_type: str = "", result: str = "", page: int = 1, page_size: int = 20) -> dict:
    kw = (keyword or "").strip()
    evt = (event_type or "").strip()
    res = (result or "").strip()
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 20), 200), 1)
    where = []
    vals: list[object] = []
    if kw:
        where.append("(username LIKE ? OR detail LIKE ? OR ip LIKE ?)")
        like = f"%{kw}%"
        vals.extend([like, like, like])
    if evt:
        where.append("event_type = ?")
        vals.append(evt)
    if res:
        where.append("result = ?")
        vals.append(res)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        total = int((conn.execute(f"SELECT COUNT(*) FROM app_auth_audit_logs {where_sql}", tuple(vals)).fetchone()[0]) or 0)
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT id, event_type, username, user_id, result, detail, ip, user_agent, created_at
            FROM app_auth_audit_logs
            {where_sql}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*vals, page_size, offset]),
        ).fetchall()
    finally:
        conn.close()
    items = [
        {
            "id": int(r[0]),
            "event_type": str(r[1] or ""),
            "username": str(r[2] or ""),
            "user_id": int((r[3] or 0) or 0),
            "result": str(r[4] or ""),
            "detail": str(r[5] or ""),
            "ip": str(r[6] or ""),
            "user_agent": str(r[7] or ""),
            "created_at": r[8],
        }
        for r in rows
    ]
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size else 0,
    }


def _extract_llm_result_marker(text: str) -> dict:
    raw = str(text or "")
    for line in reversed(raw.splitlines()):
        if not line.startswith("__LLM_RESULT__="):
            continue
        try:
            obj = json.loads(line.split("=", 1)[1])
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def build_trend_features(ts_code: str, lookback: int):
    return agent_build_trend_features(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        safe_float=_safe_float,
        calc_ma=_calc_ma,
        ts_code=ts_code,
        lookback=lookback,
    )


def _parse_json_text(raw: str):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _round_or_none(value, digits: int = 4):
    num = _safe_float(value)
    if num is None:
        return None
    return round(num, digits)


def _build_price_rollups_summary(conn: sqlite3.Connection, ts_code: str):
    try:
        rows = conn.execute(
            """
            SELECT
                ts_code, window_days, start_date, end_date, rows_count,
                close_first, close_last, close_change_pct, high_max, low_min, vol_avg, amount_avg, update_time
            FROM stock_daily_price_rollups
            WHERE ts_code = ?
            ORDER BY window_days ASC, end_date DESC
            """,
            (ts_code,),
        ).fetchall()
    except Exception:
        return {"items": [], "by_window": {}}

    latest_by_window: dict[str, dict] = {}
    for row in rows:
        d = dict(row)
        key = str(d.get("window_days") or "")
        if not key or key in latest_by_window:
            continue
        latest_by_window[key] = {
            "window_days": d.get("window_days"),
            "start_date": d.get("start_date"),
            "end_date": d.get("end_date"),
            "rows_count": d.get("rows_count"),
            "close_first": _round_or_none(d.get("close_first"), 3),
            "close_last": _round_or_none(d.get("close_last"), 3),
            "close_change_pct": _round_or_none(d.get("close_change_pct"), 2),
            "high_max": _round_or_none(d.get("high_max"), 3),
            "low_min": _round_or_none(d.get("low_min"), 3),
            "vol_avg": _round_or_none(d.get("vol_avg"), 2),
            "amount_avg": _round_or_none(d.get("amount_avg"), 2),
            "update_time": d.get("update_time"),
        }
    ordered_items = [latest_by_window[k] for k in sorted(latest_by_window.keys(), key=lambda x: int(x))]
    return {"items": ordered_items, "by_window": latest_by_window}


def _build_event_summary(conn: sqlite3.Connection, ts_code: str):
    rows = conn.execute(
        """
        SELECT event_type, event_date, ann_date, title, detail_json, source
        FROM stock_events
        WHERE ts_code = ?
        ORDER BY COALESCE(event_date, ann_date) DESC, ann_date DESC, id DESC
        LIMIT 8
        """,
        (ts_code,),
    ).fetchall()
    if not rows:
        return {}
    items = []
    type_count: dict[str, int] = {}
    for row in rows:
        d = dict(row)
        type_count[d["event_type"]] = type_count.get(d["event_type"], 0) + 1
        items.append(
            {
                "event_type": d["event_type"],
                "event_date": d["event_date"],
                "ann_date": d["ann_date"],
                "title": d["title"],
                "detail": _parse_json_text(d["detail_json"]),
            }
        )
    return {"recent_events": items, "event_type_count": type_count}


def _stock_news_latest_pub(conn: sqlite3.Connection, ts_code: str):
    row = conn.execute(
        "SELECT MAX(pub_time) FROM stock_news_items WHERE ts_code = ?",
        (ts_code,),
    ).fetchone()
    return row[0] if row and row[0] else ""


def _stock_news_is_fresh(conn: sqlite3.Connection, ts_code: str):
    latest_pub = _stock_news_latest_pub(conn, ts_code)
    if not latest_pub:
        return False, ""
    latest_date = str(latest_pub).strip()[:10]
    today_cn = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    return latest_date == today_cn, latest_pub


def _ensure_stock_news_fresh(
    ts_code: str,
    company_name: str,
    page_size: int = 20,
    score_model: str = DEFAULT_LLM_MODEL,
    score_limit: int = 3,
    score_timeout_s: int = 90,
):
    conn = sqlite3.connect(DB_PATH)
    try:
        fresh, latest_pub = _stock_news_is_fresh(conn, ts_code)
    finally:
        conn.close()
    if fresh:
        return {"fetched": False, "scored": False, "latest_pub": latest_pub}
    out = {"fetched": False, "scored": False, "latest_pub": latest_pub, "fetch_error": "", "score_error": ""}
    fetch_info = {"stdout": "", "stderr": ""}
    score_info = {"stdout": "", "stderr": ""}
    try:
        fetch_info = fetch_stock_news_now(ts_code=ts_code, company_name=company_name, page_size=page_size)
        out["fetched"] = True
    except Exception as exc:
        out["fetch_error"] = str(exc)
    try:
        safe_limit = max(1, min(int(score_limit or 1), min(page_size, 10)))
        score_info = score_stock_news_now(
            ts_code=ts_code,
            limit=safe_limit,
            model=score_model,
            timeout_s=max(30, int(score_timeout_s or 90)),
        )
        out["scored"] = True
    except Exception as exc:
        out["score_error"] = str(exc)
    conn = sqlite3.connect(DB_PATH)
    try:
        latest_pub = _stock_news_latest_pub(conn, ts_code)
    finally:
        conn.close()
    out["latest_pub"] = latest_pub
    out["fetch_stdout"] = fetch_info.get("stdout", "")
    out["score_stdout"] = score_info.get("stdout", "")
    return out


def _load_strategy_template_for(name: str) -> str:
    if not ENABLE_SKILLS_TEMPLATE_PROMPTS:
        return ""
    return load_strategy_template_text(name)


def _notify_result(title: str, summary: str, markdown: str, subject_key: str = "", link: str = "") -> dict:
    payload = build_notification_payload(
        title=title,
        summary=summary,
        markdown=markdown,
        subject_key=subject_key,
        link=link,
    )
    if not WECOM_WEBHOOK_URL:
        return {"ok": False, "skipped": True, "reason": "missing_webhook"}
    try:
        result = notify_with_wecom(payload, webhook_url=WECOM_WEBHOOK_URL)
        return {"ok": bool(result.get("ok")), "result": result}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def call_llm_trend(ts_code: str, features: dict, model: str, temperature: float = 0.2):
    return agent_call_llm_trend(
        normalize_model_name=normalize_model_name,
        normalize_temperature_for_model=normalize_temperature_for_model,
        chat_completion_with_fallback=chat_completion_with_fallback,
        default_llm_model=DEFAULT_LLM_MODEL,
        sanitize_json_value=_sanitize_json_value,
        trend_template_text=_load_strategy_template_for("trend_analysis_template.md"),
        ts_code=ts_code,
        features=features,
        model=model,
        temperature=temperature,
    )


def _resolve_roles(raw: str) -> list[str]:
    roles = [x.strip() for x in (raw or "").split(",") if x.strip()]
    return roles or list(DEFAULT_MULTI_ROLES)


def build_multi_role_context(ts_code: str, lookback: int):
    return agent_build_multi_role_context(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        default_llm_model=DEFAULT_LLM_MODEL,
        build_trend_features_fn=build_trend_features,
        ensure_stock_news_fresh=_ensure_stock_news_fresh,
        build_stock_news_summary=stock_detail_build_stock_news_summary,
        build_financial_summary=stock_detail_build_financial_summary,
        build_valuation_summary=stock_detail_build_valuation_summary,
        build_capital_flow_summary=stock_detail_build_capital_flow_summary,
        build_event_summary=_build_event_summary,
        build_macro_context=stock_detail_build_macro_context,
        build_fx_context=stock_detail_build_fx_context,
        build_rate_spread_context=stock_detail_build_rate_spread_context,
        build_governance_summary=stock_detail_build_governance_summary,
        build_risk_summary=stock_detail_build_risk_summary,
        ts_code=ts_code,
        lookback=lookback,
    )


def build_multi_role_prompt(context: dict, roles: list[str]) -> str:
    return agent_build_multi_role_prompt(
        sanitize_json_value=_sanitize_json_value,
        role_profiles=ROLE_PROFILES,
        context=context,
        roles=roles,
        multi_role_template_text=_load_strategy_template_for("multi_role_research_template.md"),
    )


def call_llm_multi_role(context: dict, roles: list[str], model: str, temperature: float = 0.2):
    return agent_call_llm_multi_role(
        normalize_model_name=normalize_model_name,
        normalize_temperature_for_model=normalize_temperature_for_model,
        chat_completion_with_fallback=chat_completion_with_fallback,
        default_llm_model=DEFAULT_LLM_MODEL,
        build_multi_role_prompt_fn=build_multi_role_prompt,
        context=context,
        roles=roles,
        model=model,
        temperature=temperature,
    )


def _normalize_markdown_lines(text: str) -> str:
    return str(text or "").replace("\r\n", "\n")


def _normalize_section_heading(text: str) -> str:
    line = str(text or "").strip()
    line = re.sub(r"^#{1,6}\s*", "", line)
    line = re.sub(r"^\*+\s*", "", line)
    line = re.sub(r"\*+\s*$", "", line)
    line = re.sub(r"^[>\-\*\s]+", "", line)
    line = re.sub(r"^[\(（]?[0-9一二三四五六七八九十]+[\)）\.\、:\-]\s*", "", line)
    line = re.sub(r"^(第[0-9一二三四五六七八九十]+部分[:：]?)\s*", "", line)
    return line.strip()


def _find_section_start(text: str, name: str) -> int:
    normalized_name = _normalize_section_heading(name)
    cursor = 0
    for raw_line in str(text or "").splitlines(True):
        line = raw_line.rstrip("\n")
        heading = _normalize_section_heading(line)
        if heading == normalized_name or normalized_name in heading:
            return cursor
        if heading.endswith("：") or heading.endswith(":"):
            heading2 = heading[:-1].strip()
            if heading2 == normalized_name or normalized_name in heading2:
                return cursor
        cursor += len(raw_line)
    return -1


def _build_common_sections_markdown(text: str) -> str:
    normalized = _normalize_markdown_lines(text)
    common_names = ["综合结论", "行动清单", "关键分歧", "非投资建议免责声明"]
    start = -1
    for name in common_names:
        pos = _find_section_start(normalized, name)
        if pos != -1 and (start == -1 or pos < start):
            start = pos
    if start == -1:
        return ""
    return normalized[start:].strip()


def ensure_logic_view_cache_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS logic_view_cache (
            entity_type TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            logic_view_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            update_time TEXT NOT NULL,
            PRIMARY KEY (entity_type, entity_key, content_hash)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_logic_view_cache_update_time ON logic_view_cache(update_time)"
    )
    conn.commit()


def _logic_view_content_hash(source_payload) -> str:
    if isinstance(source_payload, str):
        raw = source_payload
    else:
        raw = json.dumps(
            _sanitize_json_value(source_payload),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
        )
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def _load_cached_logic_view(conn, entity_type: str, entity_key: str, content_hash: str):
    ensure_logic_view_cache_table(conn)
    row = conn.execute(
        """
        SELECT logic_view_json
        FROM logic_view_cache
        WHERE entity_type = ? AND entity_key = ? AND content_hash = ?
        LIMIT 1
        """,
        (entity_type, entity_key, content_hash),
    ).fetchone()
    if not row:
        return None
    try:
        obj = json.loads(row[0] if not isinstance(row, dict) else row.get("logic_view_json", "{}"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _save_cached_logic_view(conn, entity_type: str, entity_key: str, content_hash: str, logic_view: dict):
    ensure_logic_view_cache_table(conn)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = json.dumps(_sanitize_json_value(logic_view), ensure_ascii=False, allow_nan=False)
    updated = conn.execute(
        """
        UPDATE logic_view_cache
        SET logic_view_json = ?, update_time = ?
        WHERE entity_type = ? AND entity_key = ? AND content_hash = ?
        """,
        (payload, now, entity_type, entity_key, content_hash),
    ).rowcount
    if not updated:
        conn.execute(
            """
            INSERT INTO logic_view_cache (
                entity_type, entity_key, content_hash, logic_view_json, created_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (entity_type, entity_key, content_hash, payload, now, now),
        )
    conn.commit()


def get_or_build_cached_logic_view(conn, entity_type: str, entity_key: str, source_payload, builder):
    content_hash = _logic_view_content_hash(source_payload)
    cached = _load_cached_logic_view(conn, entity_type, entity_key, content_hash)
    if cached is not None:
        return cached
    logic_view = builder()
    _save_cached_logic_view(conn, entity_type, entity_key, content_hash, logic_view)
    return logic_view


def _clean_logic_line(line: str) -> str:
    text = str(line or "")
    text = re.sub(r"^\s*[-*]\s*", "", text)
    text = re.sub(r"^\s*\d+\.\s*", "", text)
    text = re.sub(r"^\s*>\s*", "", text)
    return text.strip()


def _strip_markdown_emphasis(text: str) -> str:
    return str(text or "").replace("**", "").strip()


def _normalize_logic_chain_text(text: str) -> str:
    cleaned = _clean_logic_line(text)
    cleaned = re.sub(r'[“”"]', "", cleaned)
    cleaned = re.sub(r"\s*[-=]*>\s*", " -> ", cleaned)
    cleaned = re.sub(r"\s*→\s*", " -> ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _split_logic_nodes(text: str) -> list[str]:
    return [
        _strip_markdown_emphasis(x).replace("事件发生", "", 1).strip()
        for x in re.split(r"\s*->\s*", _normalize_logic_chain_text(text))
        if _strip_markdown_emphasis(x).strip()
    ]


def _parse_markdown_headline(line: str) -> str:
    m = re.match(r"^\d+\.\s*\*\*(.+?)\*\*", str(line or "").strip())
    return str(m.group(1)).strip() if m else ""


def extract_logic_view_from_markdown(markdown: str) -> dict:
    text = _normalize_markdown_lines(markdown).strip()
    lines = text.splitlines()
    summary = {"conclusion": "", "focus": "", "risk": ""}
    chains: list[dict] = []
    current_title = ""
    capture_next_chain = False

    for raw in lines:
        line = str(raw or "").strip()
        if not line:
            if capture_next_chain:
                capture_next_chain = False
            continue

        maybe_title = _parse_markdown_headline(line)
        if maybe_title:
            current_title = maybe_title

        plain = _strip_markdown_emphasis(_clean_logic_line(line))
        if re.match(r"^(核心结论|结论)[:：]", plain):
            summary["conclusion"] = re.sub(r"^(核心结论|结论)[:：]\s*", "", plain).strip()
        elif re.match(r"^(最值得关注的方向|关注方向)[:：]", plain):
            summary["focus"] = re.sub(r"^(最值得关注的方向|关注方向)[:：]\s*", "", plain).strip()
        elif re.match(r"^(当前最需要警惕的风险|风险提示)[:：]", plain):
            summary["risk"] = re.sub(r"^(当前最需要警惕的风险|风险提示)[:：]\s*", "", plain).strip()

        if re.search(r"(影响传导路径|传导路径|逻辑链条)[:：]", plain):
            inline = re.sub(r"^(影响传导路径|传导路径|逻辑链条)[:：]\s*", "", plain).strip()
            if inline:
                chains.append(
                    {
                        "title": current_title or f"链路 {len(chains) + 1}",
                        "raw": inline,
                        "nodes": _split_logic_nodes(inline),
                    }
                )
                capture_next_chain = False
            else:
                capture_next_chain = True
            continue

        if capture_next_chain:
            candidate = _clean_logic_line(line)
            plain_candidate = _strip_markdown_emphasis(candidate)
            if (
                not candidate
                or re.match(r"^(确定性判断|可能受影响的市场)[:：]", plain_candidate)
            ):
                capture_next_chain = False
                continue
            chains.append(
                {
                    "title": current_title or f"链路 {len(chains) + 1}",
                    "raw": candidate,
                    "nodes": _split_logic_nodes(candidate),
                }
            )
            capture_next_chain = False

    normalized_chains = [
        {
            "title": str(item.get("title") or f"链路 {idx + 1}").strip(),
            "raw": str(item.get("raw") or "").strip(),
            "nodes": [str(x).strip() for x in item.get("nodes", []) if str(x).strip()],
        }
        for idx, item in enumerate(chains)
        if len(item.get("nodes", []) if isinstance(item, dict) else []) >= 2
    ][:8]
    return {
        "summary": summary,
        "chains": normalized_chains,
        "has_logic": bool(
            normalized_chains
            or summary.get("conclusion")
            or summary.get("focus")
            or summary.get("risk")
        ),
    }


def build_signal_logic_view(signal_row: dict | None) -> dict:
    if not signal_row:
        return {"summary": {}, "chains": [], "has_logic": False, "evidence_chain": []}
    source_summary = {}
    try:
        obj = json.loads(signal_row.get("source_summary_json") or "{}")
        source_summary = obj if isinstance(obj, dict) else {}
    except Exception:
        source_summary = {}
    evidence_items = []
    try:
        obj = json.loads(signal_row.get("evidence_json") or "[]")
        evidence_items = obj if isinstance(obj, list) else []
    except Exception:
        evidence_items = []

    source_nodes = []
    label_map = {
        "intl_news": "国际新闻",
        "domestic_news": "国内新闻",
        "stock_news": "个股新闻",
        "chatroom": "群聊",
        "theme_mapping": "主题映射",
    }
    for key, label in label_map.items():
        count = int(source_summary.get(key, 0) or 0)
        if count > 0:
            source_nodes.append(f"{label}({count})")

    evidence_titles = []
    evidence_chain = []
    for item in evidence_items[:5]:
        if not isinstance(item, dict):
            continue
        label = str(
            item.get("title")
            or item.get("theme_name")
            or item.get("group")
            or item.get("source")
            or ""
        ).strip()
        if label:
            evidence_titles.append(label)
            evidence_chain.append(
                {
                    "label": label,
                    "source": str(item.get("source") or item.get("driver_type") or "").strip(),
                    "direction": str(item.get("direction") or "").strip(),
                    "date": str(item.get("date") or item.get("pub_date") or "").strip(),
                }
            )

    summary = {
        "conclusion": f"{signal_row.get('subject_name') or ''} 当前方向为 {signal_row.get('direction') or '-'}，状态 {signal_row.get('signal_status') or '-'}",
        "focus": f"强度 {signal_row.get('signal_strength') or 0} / 置信度 {signal_row.get('confidence') or 0}%",
        "risk": "若证据源减少、方向反转或置信度显著回落，需要重新评估信号有效性",
    }
    chain_nodes = [
        str(signal_row.get("subject_name") or signal_row.get("signal_key") or "").strip(),
        *source_nodes[:3],
        *(evidence_titles[:2] if evidence_titles else []),
        f"{signal_row.get('direction') or '-'} / {signal_row.get('signal_status') or '-'}",
    ]
    chain_nodes = [x for x in chain_nodes if x]
    chains = []
    if len(chain_nodes) >= 2:
        chains.append(
            {
                "title": "当前信号形成链路",
                "raw": " -> ".join(chain_nodes),
                "nodes": chain_nodes,
            }
        )
    return {
        "summary": summary,
        "chains": chains,
        "has_logic": bool(chains),
        "evidence_chain": evidence_chain,
    }


def build_signal_event_logic_view(event_row: dict) -> dict:
    event = dict(event_row or {})
    try:
        evidence_items = json.loads(event.get("evidence_json") or "[]")
        if not isinstance(evidence_items, list):
            evidence_items = []
    except Exception:
        evidence_items = []
    driver_type_map = {
        "intl_news": "国际新闻",
        "domestic_news": "国内新闻",
        "news": "新闻",
        "stock_news": "个股新闻",
        "chatroom": "群聊",
        "price": "价格",
        "mixed": "混合驱动",
    }
    driver_label = driver_type_map.get(str(event.get("driver_type") or "").strip(), str(event.get("driver_type") or "").strip() or "未知驱动")
    evidence_chain = []
    evidence_titles = []
    for item in evidence_items[:5]:
        if not isinstance(item, dict):
            continue
        label = str(
            item.get("title")
            or item.get("theme_name")
            or item.get("group")
            or item.get("source")
            or ""
        ).strip()
        if label:
            evidence_titles.append(label)
            evidence_chain.append(
                {
                    "label": label,
                    "source": str(item.get("source") or "").strip(),
                    "direction": str(item.get("direction") or "").strip(),
                    "date": str(item.get("date") or "").strip(),
                }
            )
    summary = {
        "conclusion": str(event.get("event_summary") or display_name_from_event_type(event.get("event_type"))).strip(),
        "focus": f"{event.get('old_direction') or '-'} -> {event.get('new_direction') or '-'} | 强度 Δ {event.get('delta_strength') or 0}",
        "risk": f"事件后状态 {event.get('status_after_event') or '-'}，需持续跟踪后续证据是否延续",
    }
    chain_nodes = [
        driver_label,
        str(event.get("driver_source") or "").strip(),
        *(evidence_titles[:2] if evidence_titles else []),
        str(event.get("event_summary") or "").strip(),
        f"{event.get('new_direction') or '-'} / {event.get('status_after_event') or '-'}",
    ]
    chain_nodes = [x for x in chain_nodes if x]
    chains = []
    if len(chain_nodes) >= 2:
        chains.append(
            {
                "title": "事件驱动链路",
                "raw": " -> ".join(chain_nodes),
                "nodes": chain_nodes,
            }
        )
    return {
        "summary": summary,
        "chains": chains,
        "has_logic": bool(chains),
        "evidence_chain": evidence_chain,
    }


def display_name_from_event_type(event_type: str) -> str:
    mapping = {
        "new_signal": "新信号",
        "strengthen": "信号增强",
        "weaken": "信号减弱",
        "flip": "方向反转",
        "falsify": "原判断被证伪",
        "revive": "信号恢复",
        "expire": "信号失效",
        "status_change": "状态变化",
    }
    return mapping.get(str(event_type or "").strip(), str(event_type or "").strip())


def split_multi_role_analysis(markdown: str, roles: list[str]) -> dict:
    return agent_split_multi_role_analysis(
        extract_logic_view_from_markdown=extract_logic_view_from_markdown,
        normalize_markdown_lines=_normalize_markdown_lines,
        build_common_sections_markdown=_build_common_sections_markdown,
        find_section_start=_find_section_start,
        markdown=markdown,
        roles=roles,
    )


def _cleanup_async_multi_role_jobs():
    agent_cleanup_async_jobs(
        jobs=ASYNC_MULTI_ROLE_JOBS,
        lock=ASYNC_MULTI_ROLE_LOCK,
        ttl_seconds=ASYNC_JOB_TTL_SECONDS,
    )


def _serialize_async_job(job: dict):
    return agent_serialize_async_job(job)


def _create_async_multi_role_job(ts_code: str, lookback: int, model: str, roles: list[str], context: dict | None = None):
    _cleanup_async_multi_role_jobs()
    return agent_create_async_multi_role_job(
        jobs=ASYNC_MULTI_ROLE_JOBS,
        lock=ASYNC_MULTI_ROLE_LOCK,
        publish_app_event=publish_app_event,
        ts_code=ts_code,
        lookback=lookback,
        model=model,
        roles=roles,
        context=context,
    )


def _run_async_multi_role_job(job_id: str):
    agent_run_async_multi_role_job(
        jobs=ASYNC_MULTI_ROLE_JOBS,
        lock=ASYNC_MULTI_ROLE_LOCK,
        publish_app_event=publish_app_event,
        build_multi_role_context_fn=build_multi_role_context,
        agent_deps_builder=build_agent_service_deps,
        job_id=job_id,
    )


def start_async_multi_role_job(ts_code: str, lookback: int, model: str, roles: list[str]):
    return agent_start_async_multi_role_job(
        cleanup_async_jobs_fn=_cleanup_async_multi_role_jobs,
        create_async_multi_role_job_fn=_create_async_multi_role_job,
        serialize_async_job_fn=_serialize_async_job,
        run_async_multi_role_job_fn=_run_async_multi_role_job,
        ts_code=ts_code,
        lookback=lookback,
        model=model,
        roles=roles,
    )


def get_async_multi_role_job(job_id: str):
    return agent_get_async_multi_role_job(
        jobs=ASYNC_MULTI_ROLE_JOBS,
        lock=ASYNC_MULTI_ROLE_LOCK,
        cleanup_async_jobs_fn=_cleanup_async_multi_role_jobs,
        serialize_async_job_fn=_serialize_async_job,
        job_id=job_id,
    )


def build_agent_service_deps() -> dict:
    return build_backend_runtime_deps(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        build_trend_features=build_trend_features,
        call_llm_trend=call_llm_trend,
        extract_logic_view_from_markdown=extract_logic_view_from_markdown,
        get_or_build_cached_logic_view=get_or_build_cached_logic_view,
        build_multi_role_context=build_multi_role_context,
        call_llm_multi_role=call_llm_multi_role,
        split_multi_role_analysis=split_multi_role_analysis,
        enable_risk_precheck=ENABLE_AGENT_RISK_PRECHECK,
        pre_trade_check_fn=pre_trade_check,
        enable_notifications=ENABLE_AGENT_NOTIFICATIONS,
        notify_result_fn=_notify_result,
    )


def build_reporting_service_deps() -> dict:
    return build_reporting_runtime_deps(
        query_news_daily_summaries=query_news_daily_summaries,
        start_async_daily_summary_job=start_async_daily_summary_job,
        get_async_daily_summary_job=get_async_daily_summary_job,
    )


def build_stock_news_service_runtime_deps() -> dict:
    return build_stock_news_service_deps(
        root_dir=ROOT_DIR,
        db_path=DB_PATH,
        sqlite3_module=sqlite3,
        publish_app_event=publish_app_event,
        extract_llm_result_marker=_extract_llm_result_marker,
    )


def build_chatrooms_service_runtime_deps() -> dict:
    return build_chatrooms_service_deps(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        root_dir=ROOT_DIR,
        publish_app_event=publish_app_event,
    )


def build_stock_detail_service_runtime_deps() -> dict:
    return build_stock_detail_runtime_deps(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
    )


def build_signals_service_runtime_deps() -> dict:
    return build_signals_runtime_deps(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        resolve_signal_table_fn=resolve_signal_table,
        cache_get_json_fn=cache_get_json,
        cache_set_json_fn=cache_set_json,
        redis_cache_ttl_signals=REDIS_CACHE_TTL_SIGNALS,
        redis_cache_ttl_themes=REDIS_CACHE_TTL_THEMES,
        get_or_build_cached_logic_view_fn=get_or_build_cached_logic_view,
        build_signal_logic_view_fn=build_signal_logic_view,
        build_signal_event_logic_view_fn=build_signal_event_logic_view,
        query_research_reports_fn=query_research_reports,
        query_macro_indicators_fn=query_macro_indicators,
        query_macro_series_fn=query_macro_series,
    )


def build_quantaalpha_runtime_deps() -> dict:
    return build_quantaalpha_service_runtime_deps(
        sqlite3_module=sqlite3,
        db_path=str(DB_PATH),
    )


def _cleanup_async_daily_summary_jobs():
    reporting_cleanup_async_jobs(
        jobs=ASYNC_DAILY_SUMMARY_JOBS,
        lock=ASYNC_DAILY_SUMMARY_LOCK,
        ttl_seconds=ASYNC_JOB_TTL_SECONDS,
    )


def _serialize_async_daily_summary_job(job: dict):
    return reporting_serialize_async_daily_summary_job(job)


def _create_async_daily_summary_job(model: str, summary_date: str):
    _cleanup_async_daily_summary_jobs()
    return reporting_create_async_daily_summary_job(
        jobs=ASYNC_DAILY_SUMMARY_JOBS,
        lock=ASYNC_DAILY_SUMMARY_LOCK,
        publish_app_event=publish_app_event,
        model=model,
        summary_date=summary_date,
    )


def _run_async_daily_summary_job(job_id: str):
    reporting_run_async_daily_summary_job(
        jobs=ASYNC_DAILY_SUMMARY_JOBS,
        lock=ASYNC_DAILY_SUMMARY_LOCK,
        publish_app_event=publish_app_event,
        generate_daily_summary_fn=generate_daily_summary,
        get_daily_summary_by_date_fn=get_daily_summary_by_date,
        notify_fn=_notify_result if ENABLE_REPORTING_NOTIFICATIONS else None,
        job_id=job_id,
    )


def start_async_daily_summary_job(model: str, summary_date: str):
    return reporting_start_async_daily_summary_job(
        jobs=ASYNC_DAILY_SUMMARY_JOBS,
        lock=ASYNC_DAILY_SUMMARY_LOCK,
        cleanup_async_jobs_fn=_cleanup_async_daily_summary_jobs,
        create_async_daily_summary_job_fn=_create_async_daily_summary_job,
        serialize_async_daily_summary_job_fn=_serialize_async_daily_summary_job,
        run_async_daily_summary_job_fn=_run_async_daily_summary_job,
        model=model,
        summary_date=summary_date,
    )


def get_async_daily_summary_job(job_id: str):
    return reporting_get_async_daily_summary_job(
        jobs=ASYNC_DAILY_SUMMARY_JOBS,
        lock=ASYNC_DAILY_SUMMARY_LOCK,
        cleanup_async_jobs_fn=_cleanup_async_daily_summary_jobs,
        serialize_async_daily_summary_job_fn=_serialize_async_daily_summary_job,
        job_id=job_id,
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
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
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

    def _has_permission(self, auth_ctx: dict, path: str) -> bool:
        if not path.startswith("/api/"):
            return True
        if path in AUTH_PUBLIC_API_PATHS:
            return True
        if auth_ctx.get("is_admin"):
            return True
        user = auth_ctx.get("user") or {}
        role = str(user.get("role") or user.get("tier") or "limited")
        perms = ROLE_PERMISSIONS.get(role, set())
        if "*" in perms:
            return True
        if path == "/api/llm/trend":
            return "trend_analyze" in perms
        if path in {"/api/llm/multi-role", "/api/llm/multi-role/start", "/api/llm/multi-role/task"}:
            return "multi_role_analyze" in perms
        if path in {"/api/stocks", "/api/stocks/filters"}:
            return "multi_role_analyze" in perms
        if path == "/api/news/daily-summaries":
            return "daily_summary_read" in perms
        if path in {"/api/news", "/api/news/sources"}:
            return "news_read" in perms
        if path in {"/api/stock-news", "/api/stock-news/sources"}:
            return "stock_news_read" in perms
        if path.startswith("/api/quant-factors"):
            return "research_advanced" in perms
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

    def _route_deps(self) -> dict:
        return {
            "api_endpoints_catalog": API_ENDPOINTS_CATALOG,
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
            **self._agent_service_deps(),
            "start_async_multi_role_job": start_async_multi_role_job,
            "get_async_multi_role_job": get_async_multi_role_job,
            **build_stock_news_service_runtime_deps(),
            "query_news_sources": query_news_sources,
            "query_news": query_news,
            **build_reporting_service_deps(),
            **build_chatrooms_service_runtime_deps(),
            **build_signals_service_runtime_deps(),
            **build_quantaalpha_runtime_deps(),
            "quant_factors_enabled": ENABLE_QUANT_FACTORS,
            "build_info": _build_info_payload,
            "permission_matrix": _role_permission_matrix,
            "effective_permissions_for_user": _effective_permissions_for_user,
            "list_llm_providers": list_llm_providers,
            "create_llm_provider": create_llm_provider,
            "update_llm_provider": update_llm_provider,
            "delete_llm_provider": delete_llm_provider,
            "test_one_llm_provider": test_one_llm_provider,
            "test_model_llm_providers": test_model_llm_providers,
            "update_default_rate_limit": update_default_rate_limit,
        }

    def do_POST(self):
        parsed = urlparse(self.path)
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
        if self._requires_auth_for_path(parsed.path) and not self._has_permission(auth_ctx, parsed.path):
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
        if quant_factor_routes.dispatch_post(self, parsed, payload, deps):
            return

        self._send_json({"error": "Not Found"}, status=404)

    def do_GET(self):
        parsed = urlparse(self.path)
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
        if self._requires_auth_for_path(parsed.path) and not self._has_permission(auth_ctx, parsed.path):
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
        if chatroom_routes.dispatch_get(self, parsed, deps):
            return
        if signal_routes.dispatch_get(self, parsed, deps):
            return

        self._send_json({"error": "Not Found"}, status=404)


if __name__ == "__main__":
    assert_database_ready()

    server = ThreadingHTTPServer((HOST, PORT), ApiHandler)
    print(f"Backend API running on http://{HOST}:{PORT}")
    server.serve_forever()
