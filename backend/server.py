#!/usr/bin/env python3
from __future__ import annotations

import bisect
import concurrent.futures
import mimetypes
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
from collections import deque
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

WEB_DIST_DIR = ROOT_DIR / "apps" / "web" / "dist"

import db_compat as sqlite3
from db_compat import assert_database_ready, cache_get_json, cache_set_json, db_label, get_redis_client
from job_orchestrator import dry_run_job, query_job_alerts, query_job_definitions, query_job_runs, run_job
from llm_gateway import (
    DEFAULT_LLM_MODEL,
    chat_completion_with_fallback,
    normalize_model_name,
    normalize_temperature_for_model,
)
from llm_provider_config import get_provider_candidates
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
from services.agent_service.multi_role_v3 import (
    control_multi_role_v3_job,
    create_multi_role_v3_job,
    ensure_multi_role_v3_tables,
    get_multi_role_v3_job,
    run_multi_role_v3_worker_loop,
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
from services.signals_service import build_signals_runtime_deps, query_signal_chain_graph
from services.notifications import build_notification_payload, notify_with_wecom
from services.agent_service.outputs.markdown_report import build_portfolio_view, build_risk_review, infer_decision_confidence
from services.quantaalpha_service import build_quantaalpha_service_runtime_deps, get_quantaalpha_runtime_health
from services.decision_service import build_decision_runtime_deps as build_decision_service_runtime_deps
from services.agent_service.chief_roundtable_v1 import (
    create_roundtable_job as _create_roundtable_job,
    get_roundtable_job as _get_roundtable_job,
    list_roundtable_jobs as _list_roundtable_jobs,
    ensure_roundtable_tables as _ensure_roundtable_tables,
)
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
from services.ai_retrieval_service import (
    AI_RETRIEVAL_ENABLED,
    AI_RETRIEVAL_SHADOW_MODE,
    build_context_packet as ai_retrieval_build_context_packet,
    ensure_retrieval_tables as ai_retrieval_ensure_tables,
    query_retrieval_metrics as ai_retrieval_query_metrics,
    search as ai_retrieval_search_service,
    sync_scene_index as ai_retrieval_sync_scene_index,
)
from services.system.llm_providers_admin import (
    create_llm_provider,
    delete_llm_provider,
    get_multi_role_v2_policies,
    get_multi_role_v3_policies,
    list_llm_providers,
    test_model_llm_providers,
    test_one_llm_provider,
    update_multi_role_v2_policies,
    update_default_rate_limit,
    update_llm_provider,
)
from skills.strategies import load_strategy_template_text
from backend.routes import chatrooms as chatroom_routes
from backend.routes import ai_retrieval as ai_retrieval_routes
from backend.routes import news as news_routes
from backend.routes import quant_factors as quant_factor_routes
from backend.routes import decision as decision_routes
from backend.routes import signals as signal_routes
from backend.routes import stocks as stock_routes
from backend.routes import system as system_routes
from backend.routes import roundtable as roundtable_routes
from backend.routes import market as market_routes
from backend.routes import funnel as funnel_routes
from backend.routes import portfolio as portfolio_routes
from backend.routes import llm_quick_insight as llm_quick_insight_routes
from backend.routes import macro_regime as macro_regime_routes
from backend.routes import portfolio_allocation as portfolio_allocation_routes
from backend.routes import analytics as analytics_routes
from backend.layers import (
    assert_layer_write_allowed,
    build_layered_route_deps,
    is_api_method_allowed,
    list_api_layer_contracts,
)

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
RBAC_DYNAMIC_ENFORCED = str(os.getenv("RBAC_DYNAMIC_ENFORCED", "1")).strip().lower() in {"1", "true", "yes", "on"}
WECOM_WEBHOOK_URL = str(os.getenv("WECOM_BOT_WEBHOOK", "")).strip()
ASYNC_JOB_TTL_SECONDS = 3600
ASYNC_MULTI_ROLE_JOBS: dict[str, dict] = {}
ASYNC_MULTI_ROLE_LOCK = threading.Lock()
ASYNC_MULTI_ROLE_V2_JOBS: dict[str, dict] = {}
ASYNC_MULTI_ROLE_V2_LOCK = threading.Lock()
ASYNC_MULTI_ROLE_V2_ACTIVE: set[str] = set()
ASYNC_MULTI_ROLE_V2_QUEUE = deque()
MULTI_ROLE_V2_MAX_CONCURRENT_JOBS = max(1, int(os.getenv("MULTI_ROLE_V2_MAX_CONCURRENT_JOBS", "2") or "2"))
MULTI_ROLE_V2_CONTEXT_CACHE: dict[str, dict] = {}
MULTI_ROLE_V2_CONTEXT_CACHE_LOCK = threading.Lock()
LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR = ""
LAST_MULTI_ROLE_V3_POLICY_LOAD_ERROR = ""
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
    "/api/llm/multi-role/v2/start",
    "/api/llm/multi-role/v2/decision",
    "/api/llm/multi-role/v2/retry-aggregate",
    "/api/llm/multi-role/v3/jobs",
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
    "/api/llm/multi-role/v2/stream",
    "/api/llm/multi-role/v3/jobs",
}
DEFAULT_ALLOWED_ADMIN_ORIGINS = {
    "http://127.0.0.1:8002",
    "http://localhost:8002",
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
TRUSTED_FRONTEND_PORTS = {"8002", "8077", "8080", "5173", "4173"}
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
    "pro": {
        "news_read",
        "stock_news_read",
        "daily_summary_read",
        "trend_analyze",
        "multi_role_analyze",
        "research_advanced",
        "chatrooms_advanced",
        "stocks_advanced",
        "macro_advanced",
    },
    "limited": {"news_read", "stock_news_read", "daily_summary_read", "trend_analyze", "multi_role_analyze"},
}
PERMISSION_CATALOG_FALLBACK: list[dict[str, object]] = [
    {"code": "public", "label": "公开访问", "group": "workspace", "system_reserved": False},
    {"code": "news_read", "label": "资讯阅读", "group": "news", "system_reserved": False},
    {"code": "stock_news_read", "label": "个股新闻阅读", "group": "news", "system_reserved": False},
    {"code": "daily_summary_read", "label": "新闻日报总结阅读", "group": "news", "system_reserved": False},
    {"code": "trend_analyze", "label": "走势分析", "group": "research", "system_reserved": False},
    {"code": "multi_role_analyze", "label": "多角色分析", "group": "research", "system_reserved": False},
    {"code": "research_advanced", "label": "深度研究高级功能", "group": "research", "system_reserved": False},
    {"code": "signals_advanced", "label": "信号高级功能", "group": "signals", "system_reserved": False},
    {"code": "chatrooms_advanced", "label": "舆情高级功能", "group": "sentiment", "system_reserved": False},
    {"code": "stocks_advanced", "label": "市场数据高级功能", "group": "market", "system_reserved": False},
    {"code": "macro_advanced", "label": "宏观高级功能", "group": "research", "system_reserved": False},
    {"code": "admin_users", "label": "用户管理", "group": "system", "system_reserved": True},
    {"code": "admin_system", "label": "系统管理", "group": "system", "system_reserved": True},
]
ROUTE_PERMISSIONS_FALLBACK: dict[str, str] = {
    "/login": "public",
    "/upgrade": "public",
    "/dashboard": "admin_system",
    "/stocks/list": "stocks_advanced",
    "/stocks/scores": "stocks_advanced",
    "/stocks/detail/:tsCode?": "stocks_advanced",
    "/stocks/prices": "stocks_advanced",
    "/macro": "macro_advanced",
    "/macro/regime": "research_advanced",
    "/intelligence/global-news": "news_read",
    "/intelligence/cn-news": "news_read",
    "/intelligence/stock-news": "stock_news_read",
    "/intelligence/daily-summaries": "daily_summary_read",
    "/signals/overview": "signals_advanced",
    "/signals/themes": "signals_advanced",
    "/signals/graph": "signals_advanced",
    "/signals/timeline": "signals_advanced",
    "/signals/audit": "signals_advanced",
    "/signals/quality-config": "signals_advanced",
    "/signals/state-timeline": "signals_advanced",
    "/research/reports": "research_advanced",
    "/research/decision": "research_advanced",
    "/research/scoreboard": "research_advanced",
    "/research/quant-factors": "research_advanced",
    "/research/multi-role": "multi_role_analyze",
    "/research/trend": "trend_analyze",
    "/chatrooms/overview": "chatrooms_advanced",
    "/chatrooms/candidates": "chatrooms_advanced",
    "/chatrooms/chatlog": "chatrooms_advanced",
    "/chatrooms/investment": "chatrooms_advanced",
    "/system/source-monitor": "admin_system",
    "/system/jobs-ops": "admin_system",
    "/system/llm-providers": "admin_system",
    "/system/permissions": "admin_system",
    "/system/database-audit": "admin_system",
    "/system/invites": "admin_users",
    "/system/users": "admin_users",
    "/portfolio/allocation": "research_advanced",
}
NAVIGATION_GROUPS_FALLBACK: list[dict[str, object]] = [
    {
        "id": "workspace",
        "title": "工作台",
        "order": 1,
        "items": [
            {"to": "/dashboard", "label": "总控台", "desc": "全局健康度、热点、任务与新鲜度", "permission": "admin_system"},
        ],
    },
    {
        "id": "market",
        "title": "市场数据",
        "order": 2,
        "items": [
            {"to": "/stocks/list", "label": "股票列表", "desc": "代码、简称、市场、地区快速检索", "permission": "stocks_advanced"},
            {"to": "/stocks/scores", "label": "综合评分", "desc": "行业内评分与核心指标排序", "permission": "stocks_advanced"},
            {"to": "/stocks/detail/000001.SZ", "label": "股票详情", "desc": "统一聚合价格、新闻、群聊与分析", "permission": "stocks_advanced"},
            {"to": "/stocks/prices", "label": "价格中心", "desc": "日线 + 分钟线统一查询与图表", "permission": "stocks_advanced"},
        ],
    },
    {
        "id": "news",
        "title": "资讯中心",
        "order": 3,
        "items": [
            {"to": "/intelligence/global-news", "label": "国际资讯", "desc": "全球财经新闻、评分与映射", "permission": "news_read"},
            {"to": "/intelligence/cn-news", "label": "国内资讯", "desc": "新浪 / 东财资讯统一看", "permission": "news_read"},
            {"to": "/intelligence/stock-news", "label": "个股新闻", "desc": "聚焦单股新闻与立即采集", "permission": "stock_news_read"},
            {"to": "/intelligence/daily-summaries", "label": "新闻日报总结", "desc": "日报生成、历史查询与双格式导出", "permission": "daily_summary_read"},
        ],
    },
    {
        "id": "signals",
        "title": "信号研究",
        "order": 4,
        "items": [
            {"to": "/signals/overview", "label": "投资信号", "desc": "股票与主题信号总览", "permission": "signals_advanced"},
            {"to": "/signals/themes", "label": "主题热点", "desc": "主题强度、方向、预期与证据链", "permission": "signals_advanced"},
            {"to": "/signals/graph", "label": "产业链图谱", "desc": "主题、行业、股票关系浏览", "permission": "signals_advanced"},
            {"to": "/signals/audit", "label": "信号质量审计", "desc": "误映射、弱信号与质量问题", "permission": "signals_advanced"},
            {"to": "/signals/quality-config", "label": "信号质量配置", "desc": "规则参数与映射黑名单", "permission": "signals_advanced"},
            {"to": "/signals/state-timeline", "label": "状态时间线", "desc": "状态机迁移与市场预期层", "permission": "signals_advanced"},
        ],
    },
    {
        "id": "research",
        "title": "深度研究",
        "order": 5,
        "items": [
            {"to": "/macro", "label": "宏观看板", "desc": "宏观指标查询与序列趋势", "permission": "macro_advanced"},
            {"to": "/research/trend", "label": "走势分析", "desc": "LLM 股票走势分析工作台", "permission": "trend_analyze"},
            {"to": "/research/reports", "label": "标准报告", "desc": "统一投研报告列表", "permission": "research_advanced"},
            {"to": "/research/scoreboard", "label": "评分总览", "desc": "宏观-行业-个股评分与自动短名单", "permission": "research_advanced"},
            {"to": "/research/decision", "label": "决策看板", "desc": "宏观-行业-个股评分与执行参考", "permission": "research_advanced"},
            {"to": "/research/quant-factors", "label": "因子挖掘", "desc": "双引擎因子挖掘与回测（business/research）", "permission": "research_advanced"},
            {"to": "/research/multi-role", "label": "多角色分析", "desc": "LLM 多角色公司分析工作台", "permission": "multi_role_analyze"},
        ],
    },
    {
        "id": "sentiment",
        "title": "舆情监控",
        "order": 6,
        "items": [
            {"to": "/chatrooms/overview", "label": "群聊总览", "desc": "群聊标签、状态、拉取健康度", "permission": "chatrooms_advanced"},
            {"to": "/chatrooms/chatlog", "label": "聊天记录", "desc": "消息正文、引用和筛选查询", "permission": "chatrooms_advanced"},
            {"to": "/chatrooms/investment", "label": "投资倾向", "desc": "群聊结论、情绪和标的清单", "permission": "chatrooms_advanced"},
            {"to": "/chatrooms/candidates", "label": "股票候选池", "desc": "群聊汇总候选池与偏向", "permission": "chatrooms_advanced"},
        ],
    },
    {
        "id": "system",
        "title": "系统管理",
        "order": 7,
        "items": [
            {"to": "/system/source-monitor", "label": "数据源监控", "desc": "数据源、进程、实时链路统一看板", "permission": "admin_system"},
            {"to": "/system/jobs-ops", "label": "任务调度中心", "desc": "任务列表、dry-run、触发与告警观测", "permission": "admin_system"},
            {"to": "/system/llm-providers", "label": "LLM 节点管理", "desc": "模型节点 CRUD、限速配置与联通测试", "permission": "admin_system"},
            {"to": "/system/permissions", "label": "角色权限策略", "desc": "配置 pro/limited/admin 的权限与日配额", "permission": "admin_system"},
            {"to": "/system/database-audit", "label": "数据库审计", "desc": "缺口、重复、未评分、陈旧数据", "permission": "admin_system"},
            {"to": "/system/invites", "label": "邀请码管理", "desc": "管理员邀请码与账号规模管理", "permission": "admin_users"},
            {"to": "/system/users", "label": "用户与会话", "desc": "用户、会话、审计日志管理", "permission": "admin_users"},
        ],
    },
]
RBAC_DYNAMIC_CONFIG_PATH = ROOT_DIR / "config" / "rbac_dynamic.config.json"
RBAC_DYNAMIC_SCHEMA_VERSION = "2026-04-08.dynamic-rbac.v1"
REQUIRED_PUBLIC_ROUTE_PERMISSIONS: dict[str, str] = {
    "/login": "public",
    "/upgrade": "public",
}


def _normalize_permission_catalog(raw_catalog: object) -> tuple[list[dict[str, object]], int]:
    catalog_raw = raw_catalog if isinstance(raw_catalog, list) else []
    normalized: list[dict[str, object]] = []
    invalid = 0
    seen: set[str] = set()
    for item in catalog_raw:
        if not isinstance(item, dict):
            invalid += 1
            continue
        code = str(item.get("code") or "").strip()
        label = str(item.get("label") or code).strip() or code
        group = str(item.get("group") or "default").strip() or "default"
        if not code or code in seen:
            invalid += 1
            continue
        seen.add(code)
        normalized.append(
            {
                "code": code,
                "label": label,
                "group": group,
                "system_reserved": bool(item.get("system_reserved")),
            }
        )
    return normalized, invalid


def _normalize_route_permissions(raw_map: object, permission_allowlist: set[str]) -> tuple[dict[str, str], int]:
    if not isinstance(raw_map, dict):
        return {}, 0
    normalized: dict[str, str] = {}
    invalid = 0
    for k, v in raw_map.items():
        path = str(k or "").strip()
        permission = str(v or "").strip()
        if not path or not permission or permission not in permission_allowlist:
            invalid += 1
            continue
        normalized[path] = permission
    return normalized, invalid


def _ensure_required_public_routes(route_permissions: dict[str, str]) -> tuple[dict[str, str], list[str]]:
    normalized = dict(route_permissions or {})
    fixed: list[str] = []
    for path, required in REQUIRED_PUBLIC_ROUTE_PERMISSIONS.items():
        current = str(normalized.get(path) or "").strip()
        if current == required:
            continue
        fixed.append(path if not current else f"{path}({current}->{required})")
        normalized[path] = required
    return normalized, fixed


def _normalize_navigation_groups(raw_groups: object, permission_allowlist: set[str]) -> tuple[list[dict[str, object]], int, int]:
    groups_raw = raw_groups if isinstance(raw_groups, list) else []
    normalized: list[dict[str, object]] = []
    invalid_groups = 0
    invalid_items = 0
    for group in groups_raw:
        if not isinstance(group, dict):
            invalid_groups += 1
            continue
        gid = str(group.get("id") or "").strip()
        title = str(group.get("title") or "").strip()
        try:
            order = int(group.get("order") or 0)
        except Exception:
            order = 0
        if not gid or not title:
            invalid_groups += 1
            continue
        items_raw = group.get("items")
        if not isinstance(items_raw, list):
            invalid_groups += 1
            continue
        items: list[dict[str, object]] = []
        for item in items_raw:
            if not isinstance(item, dict):
                invalid_items += 1
                continue
            to = str(item.get("to") or "").strip()
            label = str(item.get("label") or "").strip()
            desc = str(item.get("desc") or "").strip()
            permission = str(item.get("permission") or "").strip()
            if not to or not label or not permission or permission not in permission_allowlist:
                invalid_items += 1
                continue
            items.append({"to": to, "label": label, "desc": desc, "permission": permission})
        if not items:
            invalid_groups += 1
            continue
        normalized.append({"id": gid, "title": title, "order": order, "items": items})
    normalized.sort(key=lambda item: int(item.get("order") or 0))
    return normalized, invalid_groups, invalid_items


def _build_fallback_dynamic_rbac() -> dict[str, object]:
    return {
        "schema_version": RBAC_DYNAMIC_SCHEMA_VERSION,
        "version": "fallback",
        "source": "server_fallback",
        "permission_catalog": PERMISSION_CATALOG_FALLBACK,
        "route_permissions": ROUTE_PERMISSIONS_FALLBACK,
        "navigation_groups": NAVIGATION_GROUPS_FALLBACK,
        "validation": {
            "invalid_catalog_items": 0,
            "invalid_route_items": 0,
            "invalid_nav_groups": 0,
            "invalid_nav_items": 0,
            "fixed_public_route_items": 0,
        },
    }


def _load_dynamic_rbac_config() -> dict[str, object]:
    fallback = _build_fallback_dynamic_rbac()
    try:
        payload = json.loads(RBAC_DYNAMIC_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[rbac-dynamic] load failed from {RBAC_DYNAMIC_CONFIG_PATH}: {exc}")
        return fallback
    if not isinstance(payload, dict):
        print("[rbac-dynamic] config payload is not an object; use fallback")
        return fallback
    catalog, invalid_catalog = _normalize_permission_catalog(payload.get("permission_catalog"))
    if not catalog:
        print("[rbac-dynamic] no valid permission_catalog; use fallback")
        return fallback
    allowlist = {str(item.get("code") or "").strip() for item in catalog if str(item.get("code") or "").strip()}
    route_permissions, invalid_route_items = _normalize_route_permissions(payload.get("route_permissions"), allowlist)
    route_permissions, fixed_public_routes = _ensure_required_public_routes(route_permissions)
    nav_groups, invalid_nav_groups, invalid_nav_items = _normalize_navigation_groups(payload.get("navigation_groups"), allowlist)
    if fixed_public_routes:
        print(f"[rbac-dynamic] patched required public route mappings: {', '.join(fixed_public_routes)}")
    if not route_permissions or not nav_groups:
        print("[rbac-dynamic] route_permissions/navigation_groups invalid; use fallback")
        return fallback
    return {
        "schema_version": str(payload.get("schema_version") or RBAC_DYNAMIC_SCHEMA_VERSION),
        "version": str(payload.get("version") or "unknown"),
        "source": str(payload.get("source") or "repo_dynamic_config"),
        "permission_catalog": catalog,
        "route_permissions": route_permissions,
        "navigation_groups": nav_groups,
        "validation": {
            "invalid_catalog_items": invalid_catalog,
            "invalid_route_items": invalid_route_items,
            "invalid_nav_groups": invalid_nav_groups,
            "invalid_nav_items": invalid_nav_items,
            "fixed_public_route_items": len(fixed_public_routes),
        },
    }


RBAC_DYNAMIC_CONFIG = _load_dynamic_rbac_config()
TREND_DAILY_LIMIT_BY_ROLE = {
    "pro": 200,
    "limited": 30,
}
MULTI_ROLE_DAILY_LIMIT_BY_ROLE = {
    "pro": 80,
    "limited": 10,
}

DEFAULT_ROLE_POLICIES: dict[str, dict[str, object]] = {
    role: {
        "permissions": sorted(str(x) for x in perms),
        "trend_daily_limit": TREND_DAILY_LIMIT_BY_ROLE.get(role),
        "multi_role_daily_limit": MULTI_ROLE_DAILY_LIMIT_BY_ROLE.get(role),
    }
    for role, perms in ROLE_PERMISSIONS.items()
}
REQUIRED_ROLE_PERMISSIONS: dict[str, set[str]] = {
    "admin": {"*"},
}
ROLE_POLICIES_CACHE_SECONDS = 10.0
ROLE_POLICIES_CACHE: dict[str, object] = {"value": None, "expires_at": 0.0}
ROLE_POLICIES_LOCK = threading.Lock()


def _normalize_role_policy_permissions(value: object) -> set[str]:
    if isinstance(value, str):
        parts = [x.strip() for x in value.split(",")]
        return {x for x in parts if x}
    if isinstance(value, (list, tuple, set)):
        return {str(x or "").strip() for x in value if str(x or "").strip()}
    return set()


def _normalize_role_policy_limit(value: object) -> int | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw or raw in {"none", "null", "unlimited", "inf"}:
        return None
    try:
        num = int(raw)
    except Exception:
        return None
    return max(num, 0)


def _invalidate_role_policies_cache() -> None:
    with ROLE_POLICIES_LOCK:
        ROLE_POLICIES_CACHE["value"] = None
        ROLE_POLICIES_CACHE["expires_at"] = 0.0


def _effective_role_policies(force: bool = False) -> dict[str, dict[str, object]]:
    now = time.time()
    with ROLE_POLICIES_LOCK:
        cached = ROLE_POLICIES_CACHE.get("value")
        if not force and isinstance(cached, dict) and now < float(ROLE_POLICIES_CACHE.get("expires_at") or 0.0):
            return cached
    policies: dict[str, dict[str, object]] = {
        str(role): {
            "permissions": set(_normalize_role_policy_permissions(payload.get("permissions"))),
            "trend_daily_limit": _normalize_role_policy_limit(payload.get("trend_daily_limit")),
            "multi_role_daily_limit": _normalize_role_policy_limit(payload.get("multi_role_daily_limit")),
        }
        for role, payload in DEFAULT_ROLE_POLICIES.items()
    }
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_auth_tables(conn)
        rows = conn.execute(
            """
            SELECT role, permissions_json, trend_daily_limit, multi_role_daily_limit
            FROM app_auth_role_policies
            """
        ).fetchall()
        for row in rows:
            role = str(row["role"] or "").strip().lower()
            if not role:
                continue
            permissions_raw = str(row["permissions_json"] or "").strip()
            permissions: set[str] = set()
            if permissions_raw:
                try:
                    parsed = json.loads(permissions_raw)
                except Exception:
                    parsed = None
                if parsed is not None:
                    permissions = _normalize_role_policy_permissions(parsed)
            # Only fallback when DB field is truly empty.
            # If the row explicitly stores [], keep it as an empty permission set.
            if not permissions_raw and role in DEFAULT_ROLE_POLICIES:
                permissions = set(_normalize_role_policy_permissions(DEFAULT_ROLE_POLICIES[role].get("permissions")))
            policies[role] = {
                "permissions": permissions,
                "trend_daily_limit": _normalize_role_policy_limit(row["trend_daily_limit"]),
                "multi_role_daily_limit": _normalize_role_policy_limit(row["multi_role_daily_limit"]),
            }
    finally:
        conn.close()
    with ROLE_POLICIES_LOCK:
        ROLE_POLICIES_CACHE["value"] = policies
        ROLE_POLICIES_CACHE["expires_at"] = now + ROLE_POLICIES_CACHE_SECONDS
    for role, required in REQUIRED_ROLE_PERMISSIONS.items():
        if not required:
            continue
        payload = policies.get(role) or {}
        current = set(_normalize_role_policy_permissions(payload.get("permissions")))
        payload["permissions"] = current | set(required)
        policies[role] = payload
    return policies


def _role_permission_matrix() -> dict[str, list[str]]:
    policies = _effective_role_policies()
    out: dict[str, list[str]] = {}
    for role, payload in policies.items():
        out[str(role)] = sorted(str(x) for x in _normalize_role_policy_permissions(payload.get("permissions")))
    return out


def _effective_permissions_for_user(user: dict | None) -> list[str]:
    if not user:
        return []
    role = str((user or {}).get("role") or (user or {}).get("tier") or "limited").strip().lower()
    policies = _effective_role_policies()
    policy = policies.get(role) or {}
    perms = _normalize_role_policy_permissions(policy.get("permissions"))
    return sorted(str(x) for x in perms)


def _build_info_payload() -> dict:
    return {
        "build_id": BUILD_ID,
        "port": PORT,
        "pid": os.getpid(),
        "started_at": SERVER_STARTED_AT_UTC,
    }


def _get_navigation_groups() -> dict:
    groups = RBAC_DYNAMIC_CONFIG.get("navigation_groups") if isinstance(RBAC_DYNAMIC_CONFIG, dict) else []
    if not isinstance(groups, list):
        groups = []
    return {
        "ok": True,
        "groups": groups,
        "version": str(RBAC_DYNAMIC_CONFIG.get("version") or "unknown"),
        "source": str(RBAC_DYNAMIC_CONFIG.get("source") or "unknown"),
        "schema_version": str(RBAC_DYNAMIC_CONFIG.get("schema_version") or RBAC_DYNAMIC_SCHEMA_VERSION),
        "validation": dict(RBAC_DYNAMIC_CONFIG.get("validation") or {}),
    }


def _get_dynamic_rbac_payload() -> dict:
    return {
        "schema_version": str(RBAC_DYNAMIC_CONFIG.get("schema_version") or RBAC_DYNAMIC_SCHEMA_VERSION),
        "version": str(RBAC_DYNAMIC_CONFIG.get("version") or "unknown"),
        "source": str(RBAC_DYNAMIC_CONFIG.get("source") or "unknown"),
        "permission_catalog": list(RBAC_DYNAMIC_CONFIG.get("permission_catalog") or []),
        "route_permissions": dict(RBAC_DYNAMIC_CONFIG.get("route_permissions") or {}),
        "navigation_groups": list(RBAC_DYNAMIC_CONFIG.get("navigation_groups") or []),
        "validation": dict(RBAC_DYNAMIC_CONFIG.get("validation") or {}),
    }

API_ENDPOINTS_CATALOG = {
    "health": "/api/health",
    "auth_status": "/api/auth/status",
    "auth_permissions": "/api/auth/permissions",
    "navigation_groups": "/api/navigation-groups",
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
    "auth_role_policies": "/api/auth/role-policies",
    "auth_role_policies_update": "/api/auth/role-policies/update",
    "auth_role_policies_reset_default": "/api/auth/role-policies/reset-default",
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
    "ai_retrieval_search": "/api/ai-retrieval/search",
    "ai_retrieval_context": "/api/ai-retrieval/context",
    "ai_retrieval_sync": "/api/ai-retrieval/sync",
    "ai_retrieval_metrics": "/api/ai-retrieval/metrics?days=1",
    "prices": "/api/prices?ts_code=000001.SZ&start_date=20260223&end_date=20260325&page=1&page_size=20",
    "minline": "/api/minline?ts_code=600114.SH&trade_date=20260325&page=1&page_size=240",
    "llm_trend": "/api/llm/trend?ts_code=000001.SZ&lookback=120",
    "llm_multi_role": "/api/llm/multi-role?ts_code=000001.SZ&lookback=120&roles=宏观经济分析师,股票分析师,国际资本分析师,汇率分析师",
    "llm_multi_role_start": "/api/llm/multi-role/start?ts_code=000001.SZ&lookback=120&roles=宏观经济分析师,股票分析师",
    "llm_multi_role_task": "/api/llm/multi-role/task?job_id=<job_id>",
    "llm_multi_role_v2_start": "/api/llm/multi-role/v2/start",
    "llm_multi_role_v2_task": "/api/llm/multi-role/v2/task?job_id=<job_id>",
    "llm_multi_role_v2_stream": "/api/llm/multi-role/v2/stream?job_id=<job_id>",
    "llm_multi_role_v2_decision": "/api/llm/multi-role/v2/decision",
    "llm_multi_role_v2_retry_aggregate": "/api/llm/multi-role/v2/retry-aggregate",
    "llm_multi_role_v2_history": "/api/llm/multi-role/v2/history?version=v2&ts_code=000001.SZ&status=done&page=1&page_size=20",
    "llm_multi_role_v3_jobs_create": "/api/llm/multi-role/v3/jobs",
    "llm_multi_role_v3_job_get": "/api/llm/multi-role/v3/jobs/<job_id>",
    "llm_multi_role_v3_job_stream": "/api/llm/multi-role/v3/jobs/<job_id>/stream?interval_ms=1000&timeout_seconds=180",
    "llm_multi_role_v3_job_decisions": "/api/llm/multi-role/v3/jobs/<job_id>/decisions",
    "llm_multi_role_v3_job_actions": "/api/llm/multi-role/v3/jobs/<job_id>/actions",
    "macro_indicators": "/api/macro/indicators",
    "macro_series": "/api/macro?indicator_code=cn_cpi.nt_yoy&freq=M&period_start=202001&period_end=202512&page=1&page_size=200",
    "source_monitor": "/api/source-monitor",
    "metrics_summary": "/api/metrics/summary",
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
    "system_llm_multi_role_v2_policies": "/api/system/llm-providers/multi-role-v2-policies",
    "quant_factors_mine_start": "/api/quant-factors/mine/start",
    "quant_factors_backtest_start": "/api/quant-factors/backtest/start",
    "quant_factors_task": "/api/quant-factors/task?task_id=<task_id>",
    "quant_factors_results": "/api/quant-factors/results?task_type=&status=&page=1&page_size=20",
    "quant_factors_health": "/api/quant-factors/health",
}


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


def query_multi_role_analysis_history(
    *,
    version: str,
    ts_code: str,
    status: str,
    page: int,
    page_size: int,
):
    version = str(version or "").strip().lower() or "v2"
    ts_code = str(ts_code or "").strip().upper()
    status = str(status or "").strip().lower()
    page = max(int(page or 1), 1)
    page_size = min(max(int(page_size or 20), 1), 200)
    offset = (page - 1) * page_size
    where = []
    values: list[object] = []
    if version:
        where.append("version = ?")
        values.append(version)
    if ts_code:
        where.append("ts_code = ?")
        values.append(ts_code)
    if status:
        where.append("status = ?")
        values.append(status)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        ensure_multi_role_analysis_history_table(conn)
        total = int(
            (
                conn.execute(
                    f"SELECT COUNT(*) FROM multi_role_analysis_history {where_sql}",
                    tuple(values),
                ).fetchone()[0]
            )
            or 0
        )
        rows = conn.execute(
            f"""
            SELECT
              id, job_id, version, status, ts_code, name, lookback,
              roles_json, accept_auto_degrade, used_model, requested_model,
              warnings_json, error, created_at, updated_at, finished_at
            FROM multi_role_analysis_history
            {where_sql}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*values, page_size, offset]),
        ).fetchall()
    finally:
        conn.close()

    items = []
    for row in rows:
        item = dict(row)
        try:
            item["roles"] = json.loads(item.get("roles_json") or "[]")
            if not isinstance(item["roles"], list):
                item["roles"] = []
        except Exception:
            item["roles"] = []
        try:
            item["warnings"] = json.loads(item.get("warnings_json") or "[]")
            if not isinstance(item["warnings"], list):
                item["warnings"] = []
        except Exception:
            item["warnings"] = []
        item["accept_auto_degrade"] = bool(item.get("accept_auto_degrade"))
        items.append(item)

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "items": items,
    }


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


def _status_by_lag(lag: int | None, ok_within: int, warn_within: int):
    if lag is None:
        return "error"
    if lag <= ok_within:
        return "ok"
    if lag <= warn_within:
        return "warn"
    return "error"


def _trading_day_lag(conn: sqlite3.Connection, table_name: str, base_trade_date: str, latest_trade_date: str):
    base = str(base_trade_date or "").strip()
    latest = str(latest_trade_date or "").strip()
    if not base or not latest:
        return None
    if len(base) != 8 or len(latest) != 8:
        return None
    if base >= latest:
        return 0
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT trade_date)
            FROM {table_name}
            WHERE trade_date > ? AND trade_date <= ?
            """,
            (base, latest),
        ).fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return None


def _status_text(status: str):
    return {"ok": "正常", "warn": "延迟", "error": "异常"}.get(status, status)


def _max_iso_datetime(*values: str):
    parsed = [_parse_iso_datetime(v) for v in values if str(v or "").strip()]
    parsed = [x for x in parsed if x is not None]
    if not parsed:
        return ""
    latest = max(parsed)
    return latest.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        latest_marketscreener_family_news = _max_iso_datetime(
            latest_marketscreener_news or "",
            latest_marketscreener_live_news or "",
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
        minline_trade_lag = _trading_day_lag(conn, "stock_daily_prices", latest_minline or "", latest_stock_price or "")
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
    try:
        quant_health = get_quantaalpha_runtime_health(sqlite3_module=sqlite3, db_path=DB_PATH)
    except Exception:
        quant_health = {}
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
            "status": _status_by_age(_age_seconds_from_dt(_parse_iso_datetime(latest_marketscreener_family_news)), 86400, 3 * 86400),
            "last_update": latest_marketscreener_news or "",
            "detail": (
                "5分钟定时抓取 BYD Company Limited 新闻页"
                f"（家族最新={latest_marketscreener_family_news or '-'}，BYD与Live跨源去重）"
            ),
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
            "status": (
                _status_by_lag(minline_trade_lag, 0, 1)
                if minline_trade_lag is not None
                else _status_by_age(_age_seconds_from_dt(_parse_yyyymmdd(latest_minline)), 3 * 86400, 7 * 86400)
            ),
            "last_update": latest_minline or "",
            "detail": (
                f"分钟线主表 · 交易日落后 {minline_trade_lag} 天"
                if minline_trade_lag is not None
                else "分钟线主表"
            ),
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
        {
            "key": "quant_research_stack",
            "name": "因子研究栈(Qlib)",
            "category": "量化",
            "status": ("ok" if ((quant_health or {}).get("research_stack") or {}).get("status") == "ok" else "warn"),
            "last_update": (((quant_health or {}).get("worker") or {}).get("heartbeat") or {}).get("ts") or "",
            "detail": f"reason={(((quant_health or {}).get('research_stack') or {}).get('reason') or '-')}",
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
        {
            "key": "quant_research_worker",
            "name": "因子研究 Worker",
            "status": "ok" if (((quant_health or {}).get("worker") or {}).get("alive")) else "warn",
            "detail": (
                f"mode={(((quant_health or {}).get('worker') or {}).get('execution_mode') or '-')}, "
                f"pending={(((quant_health or {}).get('queue') or {}).get('pending') or 0)}"
            ),
            "last_update": ((((quant_health or {}).get("worker") or {}).get("heartbeat") or {}).get("ts") or ""),
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
    cached = cache_get_json("api:dashboard:v2")
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

    try:
        database_health = query_database_health()
    except Exception as exc:
        database_health = {"error": f"database health unavailable: {exc}"}

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
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
        "database_health": database_health,
        "top_scores": top_scores,
        "candidate_pool_top": candidate_pool_top,
        "recent_daily_summaries": recent_daily_summaries,
        "important_news": important_news,
    }
    cache_set_json("api:dashboard:v2", payload, REDIS_CACHE_TTL_DASHBOARD)
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_auth_role_policies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            role TEXT NOT NULL UNIQUE,
            permissions_json TEXT NOT NULL DEFAULT '[]',
            trend_daily_limit INTEGER,
            multi_role_daily_limit INTEGER,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
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
    for role, payload in DEFAULT_ROLE_POLICIES.items():
        perms_json = json.dumps(sorted(_normalize_role_policy_permissions(payload.get("permissions"))), ensure_ascii=False)
        trend_limit = _normalize_role_policy_limit(payload.get("trend_daily_limit"))
        multi_role_limit = _normalize_role_policy_limit(payload.get("multi_role_daily_limit"))
        conn.execute(
            """
            INSERT OR IGNORE INTO app_auth_role_policies
            (role, permissions_json, trend_daily_limit, multi_role_daily_limit, created_at, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (str(role), perms_json, trend_limit, multi_role_limit),
        )


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
            "role": str(row[4] or "limited").strip().lower(),
            "tier": str(row[5] or "limited").strip().lower(),
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
            "role": str(row[6] or "limited").strip().lower(),
            "tier": str(row[7] or "limited").strip().lower(),
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
    role = str(user.get("role") or user.get("tier") or "limited").strip().lower()
    policies = _effective_role_policies()
    limit = _normalize_role_policy_limit((policies.get(role) or {}).get("trend_daily_limit"))
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
    role = str(user.get("role") or user.get("tier") or "limited").strip().lower()
    policies = _effective_role_policies()
    limit = _normalize_role_policy_limit((policies.get(role) or {}).get("trend_daily_limit"))
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
    role = str(user.get("role") or user.get("tier") or "limited").strip().lower()
    policies = _effective_role_policies()
    limit = _normalize_role_policy_limit((policies.get(role) or {}).get("multi_role_daily_limit"))
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
    role = str(user.get("role") or user.get("tier") or "limited").strip().lower()
    policies = _effective_role_policies()
    limit = _normalize_role_policy_limit((policies.get(role) or {}).get("multi_role_daily_limit"))
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
            "role": str(row[4] or "limited").strip().lower(),
            "tier": str(row[5] or "limited").strip().lower(),
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
    policies = _effective_role_policies()
    for item in items:
        role_key = str(item.get("role") or item.get("tier") or "limited").strip().lower()
        usage = usage_map.get(int(item["id"]), {"trend": 0, "multi_role": 0})
        policy = policies.get(role_key) or {}
        trend_limit = _normalize_role_policy_limit(policy.get("trend_daily_limit"))
        trend_used = int(usage.get("trend", 0))
        multi_role_limit = _normalize_role_policy_limit(policy.get("multi_role_daily_limit"))
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


def _get_auth_role_policies() -> dict:
    policies = _effective_role_policies(force=True)
    roles = []
    for role in sorted(policies.keys()):
        payload = policies.get(role) or {}
        roles.append(
            {
                "role": role,
                "permissions": sorted(_normalize_role_policy_permissions(payload.get("permissions"))),
                "trend_daily_limit": _normalize_role_policy_limit(payload.get("trend_daily_limit")),
                "multi_role_daily_limit": _normalize_role_policy_limit(payload.get("multi_role_daily_limit")),
            }
        )
    return {"ok": True, "roles": roles, "effective_source": "db"}


def _update_auth_role_policy(role: str, permissions: list[str], trend_daily_limit: object, multi_role_daily_limit: object) -> dict:
    role_key = str(role or "").strip().lower()
    if role_key not in {"admin", "pro", "limited"}:
        raise ValueError("role 必须是 admin/pro/limited")
    normalized_perms = sorted(_normalize_role_policy_permissions(permissions))
    if role_key == "admin":
        normalized_perms = ["*"]
    elif "*" in normalized_perms:
        raise ValueError("仅 admin 角色允许使用 * 权限")
    trend_limit = _normalize_role_policy_limit(trend_daily_limit)
    multi_role_limit = _normalize_role_policy_limit(multi_role_daily_limit)
    perms_json = json.dumps(normalized_perms, ensure_ascii=False)
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        conn.execute(
            """
            INSERT INTO app_auth_role_policies (role, permissions_json, trend_daily_limit, multi_role_daily_limit, created_at, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(role) DO UPDATE SET
              permissions_json = excluded.permissions_json,
              trend_daily_limit = excluded.trend_daily_limit,
              multi_role_daily_limit = excluded.multi_role_daily_limit,
              updated_at = CURRENT_TIMESTAMP
            """,
            (role_key, perms_json, trend_limit, multi_role_limit),
        )
    finally:
        conn.close()
    _invalidate_role_policies_cache()
    return {"ok": True, "role": role_key}


def _reset_auth_role_policies_to_default() -> dict:
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        for role, payload in DEFAULT_ROLE_POLICIES.items():
            perms_json = json.dumps(sorted(_normalize_role_policy_permissions(payload.get("permissions"))), ensure_ascii=False)
            trend_limit = _normalize_role_policy_limit(payload.get("trend_daily_limit"))
            multi_role_limit = _normalize_role_policy_limit(payload.get("multi_role_daily_limit"))
            conn.execute(
                """
                INSERT INTO app_auth_role_policies (role, permissions_json, trend_daily_limit, multi_role_daily_limit, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(role) DO UPDATE SET
                  permissions_json = excluded.permissions_json,
                  trend_daily_limit = excluded.trend_daily_limit,
                  multi_role_daily_limit = excluded.multi_role_daily_limit,
                  updated_at = CURRENT_TIMESTAMP
                """,
                (str(role), perms_json, trend_limit, multi_role_limit),
            )
    finally:
        conn.close()
    _invalidate_role_policies_cache()
    return _get_auth_role_policies()


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
            "role": str(row[2] or "limited").strip().lower(),
            "tier": str(row[3] or "limited").strip().lower(),
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
    non_blocking: bool = False,
):
    conn = sqlite3.connect(DB_PATH)
    try:
        fresh, latest_pub = _stock_news_is_fresh(conn, ts_code)
    finally:
        conn.close()
    if fresh:
        return {"fetched": False, "scored": False, "latest_pub": latest_pub}
    if non_blocking:
        # 页面主链路优先：非阻塞模式下不在请求链路同步做采集/评分，避免多角色分析长时间卡住。
        return {
            "fetched": False,
            "scored": False,
            "latest_pub": latest_pub,
            "skipped": True,
            "reason": "non_blocking_mode",
        }
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
    context = agent_build_multi_role_context(
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
    if AI_RETRIEVAL_ENABLED and not AI_RETRIEVAL_SHADOW_MODE:
        try:
            company_name = str((context.get("company_profile") or {}).get("name") or "").strip()
            retrieval_query = f"{ts_code} {company_name}".strip()
            if retrieval_query:
                report_ctx = ai_retrieval_context(query=retrieval_query, scene="report", top_k=4, max_chars=1200)
                news_ctx = ai_retrieval_context(query=retrieval_query, scene="news", top_k=4, max_chars=1200)
                context["retrieval_context"] = {
                    "query": retrieval_query,
                    "report_context": report_ctx.get("context") or {},
                    "news_context": news_ctx.get("context") or {},
                    "trace": {
                        "report": report_ctx.get("trace") or {},
                        "news": news_ctx.get("trace") or {},
                    },
                }
        except Exception as exc:
            context["retrieval_context"] = {"error": str(exc), "query": ts_code}
    return context


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


def ensure_multi_role_analysis_history_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS multi_role_analysis_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            version TEXT NOT NULL,
            status TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            name TEXT,
            lookback INTEGER NOT NULL DEFAULT 120,
            roles_json TEXT NOT NULL DEFAULT '[]',
            accept_auto_degrade INTEGER NOT NULL DEFAULT 1,
            requested_model TEXT,
            used_model TEXT,
            attempts_json TEXT NOT NULL DEFAULT '[]',
            role_runs_json TEXT NOT NULL DEFAULT '[]',
            aggregator_run_json TEXT NOT NULL DEFAULT '{}',
            decision_state_json TEXT NOT NULL DEFAULT '{}',
            warnings_json TEXT NOT NULL DEFAULT '[]',
            error TEXT,
            analysis_markdown TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_multi_role_analysis_history_job_id ON multi_role_analysis_history(job_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_multi_role_analysis_history_ts_code ON multi_role_analysis_history(ts_code, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_multi_role_analysis_history_status ON multi_role_analysis_history(status, created_at)"
    )
    conn.commit()


def _persist_multi_role_analysis_v2_job(job: dict):
    if not isinstance(job, dict):
        return
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_multi_role_analysis_history_table(conn)
        conn.execute(
            """
            INSERT INTO multi_role_analysis_history (
                job_id, version, status, ts_code, name, lookback, roles_json, accept_auto_degrade,
                requested_model, used_model, attempts_json, role_runs_json, aggregator_run_json,
                decision_state_json, warnings_json, error, analysis_markdown, created_at, updated_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                status = excluded.status,
                name = excluded.name,
                lookback = excluded.lookback,
                roles_json = excluded.roles_json,
                accept_auto_degrade = excluded.accept_auto_degrade,
                requested_model = excluded.requested_model,
                used_model = excluded.used_model,
                attempts_json = excluded.attempts_json,
                role_runs_json = excluded.role_runs_json,
                aggregator_run_json = excluded.aggregator_run_json,
                decision_state_json = excluded.decision_state_json,
                warnings_json = excluded.warnings_json,
                error = excluded.error,
                analysis_markdown = excluded.analysis_markdown,
                updated_at = excluded.updated_at,
                finished_at = excluded.finished_at
            """,
            (
                str(job.get("job_id") or ""),
                "v2",
                str(job.get("status") or ""),
                str(job.get("ts_code") or ""),
                str(job.get("name") or ""),
                int(job.get("lookback") or 120),
                json.dumps(_sanitize_json_value(job.get("roles") or []), ensure_ascii=False, allow_nan=False),
                1 if bool(job.get("accept_auto_degrade", True)) else 0,
                str(job.get("requested_model") or ""),
                str(job.get("used_model") or ""),
                json.dumps(_sanitize_json_value(job.get("attempts") or []), ensure_ascii=False, allow_nan=False),
                json.dumps(_sanitize_json_value(job.get("role_runs") or []), ensure_ascii=False, allow_nan=False),
                json.dumps(_sanitize_json_value(job.get("aggregator_run") or {}), ensure_ascii=False, allow_nan=False),
                json.dumps(_sanitize_json_value(job.get("decision_state") or {}), ensure_ascii=False, allow_nan=False),
                json.dumps(_sanitize_json_value(job.get("warnings") or []), ensure_ascii=False, allow_nan=False),
                str(job.get("error") or ""),
                str(job.get("analysis_markdown") or ""),
                str(job.get("created_at") or datetime.now(timezone.utc).isoformat()),
                str(job.get("updated_at") or datetime.now(timezone.utc).isoformat()),
                str(job.get("finished_at") or ""),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _parse_iso_dt(value: str):
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _cn_day_utc_range(now_utc: datetime | None = None) -> tuple[datetime, datetime]:
    now_utc = now_utc or datetime.now(timezone.utc)
    cn_tz = timezone(timedelta(hours=8))
    cn_now = now_utc.astimezone(cn_tz)
    start_cn = cn_now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_cn = start_cn + timedelta(days=1)
    return start_cn.astimezone(timezone.utc), end_cn.astimezone(timezone.utc)


def _hydrate_persisted_multi_role_v2_row(row: dict) -> dict:
    def _loads(value, default):
        try:
            parsed = json.loads(value or "")
            return parsed if isinstance(parsed, type(default)) else default
        except Exception:
            return default

    return {
        "job_id": str(row.get("job_id") or ""),
        "status": str(row.get("status") or ""),
        "progress": 100 if str(row.get("status") or "") in {"done", "done_with_warnings", "error"} else 0,
        "stage": "done" if str(row.get("status") or "") in {"done", "done_with_warnings"} else str(row.get("status") or ""),
        "message": "复用当日已完成分析结果" if str(row.get("status") or "") in {"done", "done_with_warnings"} else str(row.get("status") or ""),
        "created_at": str(row.get("created_at") or ""),
        "updated_at": str(row.get("updated_at") or ""),
        "finished_at": str(row.get("finished_at") or ""),
        "ts_code": str(row.get("ts_code") or ""),
        "name": str(row.get("name") or ""),
        "lookback": int(row.get("lookback") or 120),
        "roles": _loads(row.get("roles_json"), []),
        "accept_auto_degrade": bool(row.get("accept_auto_degrade")),
        "requested_model": str(row.get("requested_model") or ""),
        "used_model": str(row.get("used_model") or ""),
        "attempts": _loads(row.get("attempts_json"), []),
        "role_runs": _loads(row.get("role_runs_json"), []),
        "aggregator_run": _loads(row.get("aggregator_run_json"), {}),
        "decision_state": _loads(row.get("decision_state_json"), {}),
        "warnings": _loads(row.get("warnings_json"), []),
        "error": str(row.get("error") or ""),
        "analysis": str(row.get("analysis_markdown") or ""),
        "analysis_markdown": str(row.get("analysis_markdown") or ""),
        "role_outputs": [],
        "role_sections": [],
        "common_sections_markdown": "",
        "decision_confidence": infer_decision_confidence(str(row.get("analysis_markdown") or "")).to_dict(),
        "risk_review": build_risk_review(str(row.get("analysis_markdown") or "")).to_dict(),
        "portfolio_view": build_portfolio_view(str(row.get("analysis_markdown") or "")).to_dict(),
        "used_context_dims": [],
        "context_build_ms": 0,
        "role_parallel_ms": 0,
        "total_ms": 0,
        "queue_position": 0,
        "queue_total": 0,
        "max_concurrent_jobs": MULTI_ROLE_V2_MAX_CONCURRENT_JOBS,
        "current_concurrent_jobs": 0,
        "queue_length": 0,
        "context": {},
    }


def _load_persisted_multi_role_v2_job(job_id: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        ensure_multi_role_analysis_history_table(conn)
        row = conn.execute(
            """
            SELECT
              job_id, status, ts_code, name, lookback, roles_json, accept_auto_degrade,
              requested_model, used_model, attempts_json, role_runs_json, aggregator_run_json,
              decision_state_json, warnings_json, error, analysis_markdown, created_at, updated_at, finished_at
            FROM multi_role_analysis_history
            WHERE job_id = ? AND version = 'v2'
            LIMIT 1
            """,
            (str(job_id or "").strip(),),
        ).fetchone()
        if not row:
            return None
        return _hydrate_persisted_multi_role_v2_row(dict(row))
    finally:
        conn.close()


def find_today_reusable_multi_role_v2_job(*, ts_code: str, lookback: int, roles: list[str]):
    ts_code = str(ts_code or "").strip().upper()
    target_roles = [str(x).strip() for x in list(roles or []) if str(x).strip()]
    target_roles_sorted = sorted(target_roles)
    start_utc, end_utc = _cn_day_utc_range()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        ensure_multi_role_analysis_history_table(conn)
        rows = conn.execute(
            """
            SELECT
              job_id, status, ts_code, name, lookback, roles_json, accept_auto_degrade,
              requested_model, used_model, attempts_json, role_runs_json, aggregator_run_json,
              decision_state_json, warnings_json, error, analysis_markdown, created_at, updated_at, finished_at
            FROM multi_role_analysis_history
            WHERE version = 'v2'
              AND ts_code = ?
              AND lookback = ?
              AND status IN ('done', 'done_with_warnings')
            ORDER BY id DESC
            LIMIT 100
            """,
            (ts_code, int(lookback or 120)),
        ).fetchall()
        for row_obj in rows:
            row = dict(row_obj)
            # 复用口径优先使用完成时间，避免“前一日创建、当日完成”的任务漏命中。
            anchor_dt = (
                _parse_iso_dt(str(row.get("finished_at") or ""))
                or _parse_iso_dt(str(row.get("updated_at") or ""))
                or _parse_iso_dt(str(row.get("created_at") or ""))
            )
            if not anchor_dt:
                continue
            if anchor_dt.tzinfo is None:
                anchor_dt = anchor_dt.replace(tzinfo=timezone.utc)
            anchor_utc = anchor_dt.astimezone(timezone.utc)
            if anchor_utc < start_utc or anchor_utc >= end_utc:
                continue
            try:
                row_roles = json.loads(row.get("roles_json") or "[]")
                if not isinstance(row_roles, list):
                    row_roles = []
            except Exception:
                row_roles = []
            row_roles_sorted = sorted([str(x).strip() for x in row_roles if str(x).strip()])
            if row_roles_sorted != target_roles_sorted:
                continue
            analysis_markdown = str(row.get("analysis_markdown") or "").strip()
            if not analysis_markdown:
                continue
            return _hydrate_persisted_multi_role_v2_row(row)
        return None
    finally:
        conn.close()


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


def _cleanup_async_multi_role_v2_jobs():
    agent_cleanup_async_jobs(
        jobs=ASYNC_MULTI_ROLE_V2_JOBS,
        lock=ASYNC_MULTI_ROLE_V2_LOCK,
        ttl_seconds=ASYNC_JOB_TTL_SECONDS,
    )
    with ASYNC_MULTI_ROLE_V2_LOCK:
        live_ids = set(ASYNC_MULTI_ROLE_V2_JOBS.keys())
        ASYNC_MULTI_ROLE_V2_ACTIVE.intersection_update(live_ids)
        old_queue = list(ASYNC_MULTI_ROLE_V2_QUEUE)
        rebuilt_queue = [jid for jid in old_queue if jid in live_ids]
        if rebuilt_queue != old_queue:
            ASYNC_MULTI_ROLE_V2_QUEUE.clear()
            for jid in rebuilt_queue:
                ASYNC_MULTI_ROLE_V2_QUEUE.append(jid)
        _refresh_multi_role_v2_runtime_meta_locked()


def _queue_position_locked(job_id: str) -> int:
    for idx, queued_job_id in enumerate(list(ASYNC_MULTI_ROLE_V2_QUEUE), start=1):
        if queued_job_id == job_id:
            return idx
    return 0


def _refresh_multi_role_v2_runtime_meta_locked() -> None:
    active_count = len(ASYNC_MULTI_ROLE_V2_ACTIVE)
    queue_length = len(ASYNC_MULTI_ROLE_V2_QUEUE)
    queue_pos_map = {jid: idx for idx, jid in enumerate(list(ASYNC_MULTI_ROLE_V2_QUEUE), start=1)}
    for jid, job in ASYNC_MULTI_ROLE_V2_JOBS.items():
        status = str((job or {}).get("status") or "")
        job["current_concurrent_jobs"] = active_count
        job["queue_length"] = queue_length
        job["queue_total"] = queue_length
        job["max_concurrent_jobs"] = MULTI_ROLE_V2_MAX_CONCURRENT_JOBS
        job["queue_position"] = int(queue_pos_map.get(jid, 0)) if status == "queued" else 0


def _dispatch_multi_role_v2_queue():
    launch_ids: list[str] = []
    with ASYNC_MULTI_ROLE_V2_LOCK:
        while ASYNC_MULTI_ROLE_V2_QUEUE and len(ASYNC_MULTI_ROLE_V2_ACTIVE) < MULTI_ROLE_V2_MAX_CONCURRENT_JOBS:
            job_id = str(ASYNC_MULTI_ROLE_V2_QUEUE.popleft() or "")
            if not job_id:
                continue
            job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
            if not job:
                continue
            if str(job.get("status") or "") != "queued":
                continue
            if job_id in ASYNC_MULTI_ROLE_V2_ACTIVE:
                continue
            ASYNC_MULTI_ROLE_V2_ACTIVE.add(job_id)
            now = datetime.now(timezone.utc).isoformat()
            job["message"] = "排队结束，任务开始执行"
            job["queue_position"] = 0
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
            launch_ids.append(job_id)
            try:
                _persist_multi_role_analysis_v2_job(job)
            except Exception:
                pass
        _refresh_multi_role_v2_runtime_meta_locked()
    for job_id in launch_ids:
        worker = threading.Thread(
            target=_run_async_multi_role_v2_job,
            args=(job_id,),
            daemon=True,
            name=f"multi_role_v2_{job_id[:8]}",
        )
        worker.start()


def _release_multi_role_v2_slot(job_id: str):
    with ASYNC_MULTI_ROLE_V2_LOCK:
        ASYNC_MULTI_ROLE_V2_ACTIVE.discard(str(job_id or ""))
        _refresh_multi_role_v2_runtime_meta_locked()
    _dispatch_multi_role_v2_queue()


def _context_cache_cn_date() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")


def _context_cache_key(ts_code: str, lookback: int, cn_date: str) -> str:
    return f"{str(ts_code or '').strip().upper()}|{int(lookback or 120)}|{str(cn_date or '')}"


def _get_cached_multi_role_v2_context(ts_code: str, lookback: int):
    key = _context_cache_key(ts_code, lookback, _context_cache_cn_date())
    with MULTI_ROLE_V2_CONTEXT_CACHE_LOCK:
        cached = MULTI_ROLE_V2_CONTEXT_CACHE.get(key)
        if not isinstance(cached, dict):
            return None
        context = cached.get("context")
        if not isinstance(context, dict):
            return None
        # Context is treated as read-only in the v2 pipeline; return directly to
        # avoid deep-copy overhead on hot path.
        return context


def _set_cached_multi_role_v2_context(ts_code: str, lookback: int, context: dict):
    if not isinstance(context, dict):
        return
    today = _context_cache_cn_date()
    key = _context_cache_key(ts_code, lookback, today)
    now = datetime.now(timezone.utc).isoformat()
    with MULTI_ROLE_V2_CONTEXT_CACHE_LOCK:
        # 跨天自动清理旧 key，避免常驻进程内存膨胀。
        stale = [k for k in MULTI_ROLE_V2_CONTEXT_CACHE.keys() if str(k).split("|")[-1] != today]
        for stale_key in stale:
            MULTI_ROLE_V2_CONTEXT_CACHE.pop(stale_key, None)
        MULTI_ROLE_V2_CONTEXT_CACHE[key] = {"context": context, "updated_at": now}


def _build_role_specific_context(role: str, full_context: dict) -> dict:
    source = dict(full_context or {})
    price = source.get("price_summary") or {}
    stock_news = source.get("stock_news_summary") or {}
    cap_flow = source.get("capital_flow_summary") or {}

    scoped = {
        "company_profile": source.get("company_profile") or {},
        "price_summary": {
            "latest": (price.get("latest") or {}),
            "metrics": (price.get("metrics") or {}),
        },
        "stock_news_summary": {
            "latest_pub_time": stock_news.get("latest_pub_time"),
            "high_importance_count_recent_8": stock_news.get("high_importance_count_recent_8"),
        },
    }

    role_name = str(role or "").strip()
    if role_name == "宏观经济分析师":
        scoped["macro_context"] = source.get("macro_context") or {}
        scoped["rate_spread_context"] = source.get("rate_spread_context") or {}
    elif role_name == "股票分析师":
        scoped["price_summary"]["recent_20_bars"] = price.get("recent_20_bars") or []
        scoped["valuation_summary"] = source.get("valuation_summary") or {}
        scoped["capital_flow_summary"] = {"stock_flow": (cap_flow.get("stock_flow") or {})}
    elif role_name == "国际资本分析师":
        scoped["capital_flow_summary"] = {"market_flow": (cap_flow.get("market_flow") or {})}
        scoped["macro_context"] = source.get("macro_context") or {}
        scoped["fx_context"] = source.get("fx_context") or {}
    elif role_name == "汇率分析师":
        scoped["fx_context"] = source.get("fx_context") or {}
        scoped["rate_spread_context"] = source.get("rate_spread_context") or {}
    elif role_name == "风险控制官":
        scoped["risk_summary"] = source.get("risk_summary") or {}
    elif role_name == "行业分析师":
        scoped["event_summary"] = source.get("event_summary") or {}
        news_items = list((stock_news.get("recent_items") or []))[:3]
        scoped["stock_news_summary"]["recent_items"] = news_items
    else:
        # 默认给一个轻量可用集合，避免未知角色直接空数据。
        scoped["macro_context"] = source.get("macro_context") or {}
        scoped["valuation_summary"] = source.get("valuation_summary") or {}

    return _sanitize_json_value(scoped)


def _default_multi_role_v2_policies() -> dict:
    out = {"__aggregator__": {"primary_model": "gpt-5.4-multi-role", "fallback_models": ["kimi-k2.5", "deepseek-chat"]}}
    for role in ROLE_PROFILES.keys():
        out[str(role)] = {"primary_model": "gpt-5.4-multi-role", "fallback_models": ["kimi-k2.5", "deepseek-chat"]}
    return out


def _load_multi_role_v2_policies() -> dict:
    global LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR
    defaults = _default_multi_role_v2_policies()
    try:
        payload = get_multi_role_v2_policies()
        raw = payload.get("multi_role_v2_policies") or {}
        if isinstance(raw, dict):
            for key, cfg in raw.items():
                role = str(key or "").strip()
                if not role or not isinstance(cfg, dict):
                    continue
                primary = str(cfg.get("primary_model") or "").strip() or defaults.get(role, {}).get("primary_model", DEFAULT_LLM_MODEL)
                fallback_raw = cfg.get("fallback_models") or []
                if isinstance(fallback_raw, str):
                    fallback = [x.strip() for x in fallback_raw.split(",") if x.strip()]
                elif isinstance(fallback_raw, list):
                    fallback = [str(x).strip() for x in fallback_raw if str(x).strip()]
                else:
                    fallback = []
                defaults[role] = {"primary_model": primary, "fallback_models": fallback}
        LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR = ""
    except Exception as exc:
        LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR = f"{type(exc).__name__}: {exc}"
        print(
            f"[multi-role-v2] policy load failed; fallback to defaults. error={LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR}",
            flush=True,
        )
    return defaults


def _default_multi_role_v3_policies() -> dict:
    return {
        "quick_think_llm": "deepseek-chat",
        "deep_think_llm": "gpt-5.4-multi-role",
        "fallback_models": ["deepseek-chat"],
        "stage_models": {
            "analyst": {"mode": "quick"},
            "research_debate": {"mode": "deep"},
            "research_manager": {"mode": "deep"},
            "trader": {"mode": "deep"},
            "risk_debate": {"mode": "deep"},
            "portfolio_manager": {"mode": "deep"},
        },
        "role_models": {
            "analyst:news": {"primary_model": "zhipu-news", "fallback_models": ["deepseek-chat"]},
            "analyst:sentiment": {"primary_model": "zhipu-news", "fallback_models": ["deepseek-chat"]},
        },
    }


def _normalize_multi_role_v3_policy_entry(
    raw: dict,
    *,
    fallback_profile: str,
    quick_model: str,
    deep_model: str,
    global_fallback: list[str],
) -> dict:
    mode = str(raw.get("mode") or "").strip().lower()
    if mode not in {"quick", "deep"}:
        mode = fallback_profile
    primary = str(raw.get("primary_model") or "").strip()
    if not primary:
        primary = quick_model if mode == "quick" else deep_model
    fallback_raw = raw.get("fallback_models") or []
    if isinstance(fallback_raw, str):
        fallback = [x.strip() for x in fallback_raw.split(",") if x.strip()]
    elif isinstance(fallback_raw, list):
        fallback = [str(x).strip() for x in fallback_raw if str(x).strip()]
    else:
        fallback = []
    if not fallback:
        fallback = list(global_fallback)
    return {
        "mode": mode,
        "primary_model": primary,
        "fallback_models": fallback,
    }


def _load_multi_role_v3_policies() -> dict:
    global LAST_MULTI_ROLE_V3_POLICY_LOAD_ERROR
    defaults = _default_multi_role_v3_policies()
    try:
        payload = get_multi_role_v3_policies()
        raw = payload.get("multi_role_v3_policies") or {}
        if isinstance(raw, dict):
            quick = str(raw.get("quick_think_llm") or defaults.get("quick_think_llm") or "").strip() or defaults["quick_think_llm"]
            deep = str(raw.get("deep_think_llm") or defaults.get("deep_think_llm") or "").strip() or defaults["deep_think_llm"]
            fallback_raw = raw.get("fallback_models") or defaults.get("fallback_models") or []
            if isinstance(fallback_raw, str):
                fallback = [x.strip() for x in fallback_raw.split(",") if x.strip()]
            elif isinstance(fallback_raw, list):
                fallback = [str(x).strip() for x in fallback_raw if str(x).strip()]
            else:
                fallback = list(defaults.get("fallback_models") or [])
            defaults["quick_think_llm"] = quick
            defaults["deep_think_llm"] = deep
            defaults["fallback_models"] = fallback

            stage_models = dict(defaults.get("stage_models") or {})
            role_models = dict(defaults.get("role_models") or {})
            raw_stages = raw.get("stage_models") or {}
            if isinstance(raw_stages, dict):
                for stage_key, cfg in raw_stages.items():
                    key = str(stage_key or "").strip()
                    if not key or not isinstance(cfg, dict):
                        continue
                    stage_models[key] = _normalize_multi_role_v3_policy_entry(
                        cfg,
                        fallback_profile="deep",
                        quick_model=quick,
                        deep_model=deep,
                        global_fallback=fallback,
                    )
            raw_roles = raw.get("role_models") or {}
            if isinstance(raw_roles, dict):
                for role_key, cfg in raw_roles.items():
                    key = str(role_key or "").strip().lower()
                    if not key or not isinstance(cfg, dict):
                        continue
                    role_models[key] = _normalize_multi_role_v3_policy_entry(
                        cfg,
                        fallback_profile="quick",
                        quick_model=quick,
                        deep_model=deep,
                        global_fallback=fallback,
                    )
            defaults["stage_models"] = stage_models
            defaults["role_models"] = role_models
        LAST_MULTI_ROLE_V3_POLICY_LOAD_ERROR = ""
    except Exception as exc:
        LAST_MULTI_ROLE_V3_POLICY_LOAD_ERROR = f"{type(exc).__name__}: {exc}"
        print(
            f"[multi-role-v3] policy load failed; fallback to defaults. error={LAST_MULTI_ROLE_V3_POLICY_LOAD_ERROR}",
            flush=True,
        )
    return defaults


def _policy_model_chain(policy_map: dict, role: str) -> list[str]:
    cfg = policy_map.get(role) or {}
    primary = normalize_model_name(str(cfg.get("primary_model") or DEFAULT_LLM_MODEL))
    fallback = [normalize_model_name(str(x or "")) for x in list(cfg.get("fallback_models") or []) if str(x or "").strip()]
    chain: list[str] = []
    for item in [primary, *fallback]:
        if item and item not in chain:
            chain.append(item)
    return chain or [normalize_model_name(DEFAULT_LLM_MODEL)]


def _is_kimi_model(model: str) -> bool:
    return "kimi-k2.5" in normalize_model_name(str(model or "")).lower()


def _is_gpt54_model(model: str) -> bool:
    return normalize_model_name(str(model or "")).lower() == "gpt-5.4"


def _is_gpt54_family_model(model: str) -> bool:
    normalized = normalize_model_name(str(model or "")).lower()
    return normalized == "gpt-5.4" or "gpt-5.4" in normalized


def _candidate_route_chain(model_chain: list[str], *, per_model_limit: int) -> list[dict]:
    routes: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for model in model_chain:
        normalized = normalize_model_name(str(model or ""))
        if not normalized:
            continue
        candidates = list(get_provider_candidates(normalized))
        if not candidates:
            key = (normalized, "")
            if key in seen:
                continue
            seen.add(key)
            routes.append({"model": normalized, "base_url": "", "api_key": ""})
            continue
        for item in candidates[: max(1, int(per_model_limit or 1))]:
            route_model = normalize_model_name(str(item.model or normalized))
            route_base = str(item.base_url or "").strip()
            route_key = str(item.api_key or "").strip()
            key = (route_model, route_base.rstrip("/"))
            if key in seen:
                continue
            seen.add(key)
            routes.append({"model": route_model, "base_url": route_base, "api_key": route_key})
    return routes


def _build_multi_role_v2_single_prompt(context: dict, role: str) -> str:
    profile = ROLE_PROFILES.get(role, {})
    role_spec = {
        "role": role,
        "focus": profile.get("focus", "围绕该角色职责进行分析"),
        "framework": profile.get("framework", "使用该角色常用框架"),
        "indicators": profile.get("indicators", []),
        "risk_bias": profile.get("risk_bias", "识别该角色关注的核心风险"),
    }
    return (
        "你将以单一角色完成独立研究任务。\n"
        f"你的角色是：{role}\n"
        "请使用该角色视角给出结构化结论，避免泛泛而谈。\n"
        "输出要求（必须严格使用 Markdown 二级标题）：\n"
        f"## {role}\n"
        "内容必须包含：\n"
        "1) 核心观点（结论先行）\n"
        "2) 关键依据（3-5条，尽量引用给定数据中的最近日期/数值）\n"
        "3) 主要风险（2-4条）\n"
        "4) 后续跟踪指标（3-5条，尽量量化）\n\n"
        f"角色设定(JSON)：\n{json.dumps(_sanitize_json_value(role_spec), ensure_ascii=False, allow_nan=False)}\n\n"
        f"输入数据(JSON)：\n{json.dumps(_sanitize_json_value(context), ensure_ascii=False, allow_nan=False)}"
    )


def _run_multi_role_v2_single_role(*, role: str, context: dict, policy_map: dict, attempt_budget: int) -> dict:
    model_chain = _policy_model_chain(policy_map, role)
    route_chain = _candidate_route_chain(model_chain, per_model_limit=2)
    attempts: list[dict] = []
    started = time.time()
    prompt = _build_multi_role_v2_single_prompt(context, role)
    system_prompt = f"你是{role}，请保持角色口径稳定、可执行、可审计。"
    for idx in range(max(1, int(attempt_budget))):
        route = route_chain[idx % len(route_chain)] if route_chain else {"model": model_chain[idx % len(model_chain)], "base_url": "", "api_key": ""}
        request_model = str(route.get("model") or model_chain[idx % len(model_chain)])
        request_base_url = str(route.get("base_url") or "")
        request_api_key = str(route.get("api_key") or "")
        role_timeout_s = 90 if _is_gpt54_family_model(request_model) else 60
        try:
            result = chat_completion_with_fallback(
                model=request_model,
                base_url=request_base_url,
                api_key=request_api_key,
                temperature=0.2,
                timeout_s=role_timeout_s,
                max_retries=0,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
            )
            for item in result.attempts:
                attempts.append({"model": item.model, "base_url": item.base_url, "error": item.error})
            return {
                "ok": True,
                "role": role,
                "output": str(result.text or ""),
                "used_model": str(result.used_model or request_model),
                "requested_model": str(result.requested_model or request_model),
                "attempts": attempts,
                "error": "",
                "duration_ms": int((time.time() - started) * 1000),
            }
        except Exception as exc:
            attempts.append({"model": request_model, "base_url": "", "error": str(exc)})
    return {
        "ok": False,
        "role": role,
        "output": "",
        "used_model": "",
        "requested_model": "",
        "attempts": attempts,
        "error": str(attempts[-1].get("error") if attempts else "unknown error"),
        "duration_ms": int((time.time() - started) * 1000),
    }


def _trim_role_output_for_aggregate(role: str, content: str, per_role_limit: int = 1800) -> str:
    text = str(content or "").strip()
    if not text:
        return ""
    lines = [ln.rstrip() for ln in text.splitlines()]
    while lines and not lines[0].strip():
        lines.pop(0)
    if lines and lines[0].lstrip().startswith("##"):
        heading = lines[0].replace(" ", "")
        if role and role.replace(" ", "") in heading:
            lines = lines[1:]
    text = "\n".join(lines).strip()
    if len(text) <= per_role_limit:
        return text
    keep = max(500, per_role_limit)
    return text[:keep]


def _trim_aggregator_inputs(inputs: list[str], max_total_chars: int = 12000) -> list[str]:
    if not inputs:
        return []
    total = sum(len(x) for x in inputs)
    if total <= max_total_chars:
        return inputs
    scale = float(max_total_chars) / float(max(total, 1))
    trimmed: list[str] = []
    for item in inputs:
        limit = max(400, int(len(item) * scale))
        trimmed.append(item[:limit])
    return trimmed


def _run_multi_role_v2_aggregator(*, role_runs: list[dict], ts_code: str, lookback: int, policy_map: dict) -> dict:
    started = time.time()
    inputs = []
    role_order = []
    for item in role_runs:
        role = str(item.get("role") or "").strip()
        content = str(item.get("output") or "").strip()
        if not role or not content:
            continue
        compact_content = _trim_role_output_for_aggregate(role, content)
        if not compact_content:
            continue
        role_order.append(role)
        inputs.append(f"## {role}\n{compact_content}")
    if not inputs:
        raise RuntimeError("没有可用的角色输出，无法汇总")
    inputs = _trim_aggregator_inputs(inputs)

    chain = _policy_model_chain(policy_map, "__aggregator__")
    route_chain = _candidate_route_chain(chain, per_model_limit=2)
    prompt = (
        "你是投研委员会秘书，请将多个角色的独立结论串行汇总，输出最终会议纪要。\n"
        "输出要求：\n"
        "1) 保留所有角色的独立观点，不得混合改写为单一口径。\n"
        "2) 必须包含公共段落：综合结论、行动清单、关键分歧、非投资建议免责声明。\n"
        "3) 结构必须使用 Markdown 二级标题，角色标题必须与输入角色名一致。\n"
        "4) 若角色间冲突，务必在“关键分歧”明确记录。\n"
        "5) 行动清单优先给出可执行和可验证项。\n\n"
        "请严格按如下标题骨架输出：\n"
        + "".join([f"## {role}\n" for role in role_order])
        + "## 综合结论\n## 行动清单\n## 关键分歧\n## 非投资建议免责声明\n\n"
        f"股票：{ts_code}，观察窗口：{lookback}日\n\n"
        "角色输入如下：\n\n"
        + "\n\n".join(inputs)
    )
    attempts: list[dict] = []
    agg_attempt_budget = max(2, len(route_chain) if route_chain else len(chain))
    for idx in range(agg_attempt_budget):
        route = route_chain[idx % len(route_chain)] if route_chain else {"model": chain[idx % len(chain)], "base_url": "", "api_key": ""}
        request_model = str(route.get("model") or chain[idx % len(chain)])
        request_base_url = str(route.get("base_url") or "")
        request_api_key = str(route.get("api_key") or "")
        aggregate_timeout_s = 120 if _is_gpt54_family_model(request_model) else 75
        try:
            result = chat_completion_with_fallback(
                model=request_model,
                base_url=request_base_url,
                api_key=request_api_key,
                temperature=0.2,
                timeout_s=aggregate_timeout_s,
                max_retries=0,
                messages=[
                    {"role": "system", "content": "你是严谨的投研纪要整合器，只输出结构化 Markdown。"},
                    {"role": "user", "content": prompt},
                ],
            )
            for item in result.attempts:
                attempts.append({"model": item.model, "base_url": item.base_url, "error": item.error})
            return {
                "ok": True,
                "analysis_markdown": str(result.text or ""),
                "used_model": str(result.used_model or request_model),
                "requested_model": str(result.requested_model or request_model),
                "attempts": attempts,
                "error": "",
                "duration_ms": int((time.time() - started) * 1000),
            }
        except Exception as exc:
            attempts.append({"model": request_model, "base_url": "", "error": str(exc)})

    fallback = "\n\n".join(inputs)
    fallback += (
        "\n\n## 综合结论\n聚合模型暂不可用，本次先保留角色原文，请结合各角色观点自行判读。\n"
        "\n## 行动清单\n1. 等待聚合模型恢复后重试汇总。\n2. 对冲突观点优先做数据复核。\n"
        "\n## 关键分歧\n角色间存在潜在冲突，需人工复核。\n"
        "\n## 非投资建议免责声明\n以上内容仅供研究参考，不构成任何投资建议。\n"
    )
    return {
        "ok": False,
        "analysis_markdown": fallback,
        "used_model": "",
        "requested_model": "",
        "attempts": attempts,
        "error": str(attempts[-1].get("error") if attempts else "aggregator failed"),
        "duration_ms": int((time.time() - started) * 1000),
    }


def _serialize_async_multi_role_v2_job(job: dict) -> dict:
    aggregate_ms = int(((job.get("aggregator_run") or {}).get("duration_ms") or 0))
    debug = {}
    if str(LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR or "").strip():
        debug["policy_warning"] = LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR
    return {
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "progress": job.get("progress"),
        "stage": job.get("stage"),
        "message": job.get("message"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "finished_at": job.get("finished_at"),
        "ts_code": job.get("ts_code"),
        "name": job.get("name"),
        "lookback": job.get("lookback"),
        "roles": job.get("roles"),
        "accept_auto_degrade": bool(job.get("accept_auto_degrade", True)),
        "decision_timeout_seconds": int(job.get("decision_timeout_seconds") or 0),
        "decision_state": job.get("decision_state") or {},
        "role_runs": list(job.get("role_runs") or []),
        "aggregator_run": job.get("aggregator_run") or {},
        "analysis": job.get("analysis") or "",
        "analysis_markdown": job.get("analysis_markdown") or "",
        "role_outputs": job.get("role_outputs") or [],
        "role_sections": job.get("role_sections") or [],
        "common_sections_markdown": job.get("common_sections_markdown") or "",
        "used_model": job.get("used_model") or "",
        "requested_model": job.get("requested_model") or "",
        "attempts": job.get("attempts") or [],
        "decision_confidence": job.get("decision_confidence") or {},
        "risk_review": job.get("risk_review") or {},
        "portfolio_view": job.get("portfolio_view") or {},
        "used_context_dims": job.get("used_context_dims") or [],
        "context": job.get("context") or {},
        "context_build_ms": int(job.get("context_build_ms") or 0),
        "role_parallel_ms": int(job.get("role_parallel_ms") or 0),
        "aggregate_ms": aggregate_ms,
        "total_ms": int(job.get("total_ms") or 0),
        "warnings": list(job.get("warnings") or []),
        "error": job.get("error") or "",
        "queue_position": int(job.get("queue_position") or 0),
        "queue_total": int(job.get("queue_total") or 0),
        "max_concurrent_jobs": int(job.get("max_concurrent_jobs") or MULTI_ROLE_V2_MAX_CONCURRENT_JOBS),
        "current_concurrent_jobs": int(job.get("current_concurrent_jobs") or 0),
        "queue_length": int(job.get("queue_length") or 0),
        "debug": debug,
    }


def _create_async_multi_role_v2_job(
    *,
    ts_code: str,
    lookback: int,
    roles: list[str],
    accept_auto_degrade: bool,
    decision_timeout_seconds: int,
) -> dict:
    _cleanup_async_multi_role_v2_jobs()
    now = datetime.now(timezone.utc).isoformat()
    job_id = uuid.uuid4().hex
    policy_map = _load_multi_role_v2_policies()
    role_runs = []
    for role in roles:
        role_policy = policy_map.get(role, {})
        role_runs.append(
            {
                "role": role,
                "status": "queued",
                "requested_model": str(role_policy.get("primary_model") or DEFAULT_LLM_MODEL),
                "used_model": "",
                "attempts": [],
                "retry_count": 0,
                "duration_ms": 0,
                "error": "",
                "output": "",
            }
        )
    job = {
        "job_id": job_id,
        "status": "queued",
        "progress": 5,
        "stage": "queued",
        "message": "V2 任务已创建，等待后台执行",
        "created_at": now,
        "updated_at": now,
        "finished_at": "",
        "updated_at_ts": time.time(),
        "ts_code": ts_code,
        "name": "",
        "lookback": lookback,
        "roles": roles,
        "requested_model": DEFAULT_LLM_MODEL,
        "used_model": "",
        "accept_auto_degrade": bool(accept_auto_degrade),
        "decision_timeout_seconds": max(60, int(decision_timeout_seconds or 600)),
        "decision_state": {"pending_user_decision": False, "round": 0, "last_action": "", "pending_roles": []},
        "role_runs": role_runs,
        "aggregator_run": {"status": "queued", "used_model": "", "attempts": [], "error": "", "duration_ms": 0},
        "context": {},
        "analysis": "",
        "analysis_markdown": "",
        "role_outputs": [],
        "role_sections": [],
        "common_sections_markdown": "",
        "decision_confidence": {},
        "risk_review": {},
        "portfolio_view": {},
        "used_context_dims": [],
        "context_build_ms": 0,
        "role_parallel_ms": 0,
        "total_ms": 0,
        "attempts": [],
        "warnings": [],
        "error": "",
        "queue_position": 0,
        "queue_total": 0,
        "max_concurrent_jobs": MULTI_ROLE_V2_MAX_CONCURRENT_JOBS,
        "current_concurrent_jobs": 0,
        "queue_length": 0,
    }
    with ASYNC_MULTI_ROLE_V2_LOCK:
        ASYNC_MULTI_ROLE_V2_JOBS[job_id] = job
    try:
        _persist_multi_role_analysis_v2_job(job)
    except Exception:
        pass
    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": job_id, "status": "queued", "progress": 5, "stage": "queued", "ts_code": ts_code, "mode": "v2"},
        producer="backend.server",
    )
    return job


def _run_multi_role_v2_role_batch(job_id: str, role_names: list[str], attempt_budget: int, stage: str):
    with ASYNC_MULTI_ROLE_V2_LOCK:
        job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return []
        context = dict(job.get("context") or {})
        ts_code = str(job.get("ts_code") or "")
        for item in job.get("role_runs", []):
            if item.get("role") in role_names:
                item["status"] = "retrying" if stage == "role_retry" else "running"
                item["error"] = ""
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()
        job["stage"] = stage
        job["message"] = "角色任务并行执行中"
    policy_map = _load_multi_role_v2_policies()
    results: list[dict] = []
    workers = max(1, min(len(role_names), 6))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="multi_role_v2_role") as pool:
        futures = [
            pool.submit(
                _run_multi_role_v2_single_role,
                role=role,
                context=_build_role_specific_context(role, context),
                policy_map=policy_map,
                attempt_budget=attempt_budget,
            )
            for role in role_names
        ]
        for fut in concurrent.futures.as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as exc:
                results.append({"ok": False, "role": "", "attempts": [], "output": "", "used_model": "", "error": str(exc), "duration_ms": 0})

    by_role = {str(x.get("role") or ""): x for x in results}
    with ASYNC_MULTI_ROLE_V2_LOCK:
        job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return results
        for item in job.get("role_runs", []):
            role = str(item.get("role") or "")
            if role not in by_role:
                continue
            result = by_role[role]
            item_attempts = list(item.get("attempts") or [])
            item_attempts.extend(list(result.get("attempts") or []))
            item["attempts"] = item_attempts
            item["retry_count"] = max(0, len(item_attempts) - 1)
            item["duration_ms"] = int((item.get("duration_ms") or 0) + int(result.get("duration_ms") or 0))
            item["used_model"] = str(result.get("used_model") or item.get("used_model") or "")
            item["requested_model"] = str(result.get("requested_model") or item.get("requested_model") or "")
            item["output"] = str(result.get("output") or item.get("output") or "")
            item["error"] = str(result.get("error") or "")
            item["status"] = "done" if result.get("ok") else "error"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()
        current_role_runs = [
            item
            for item in list(job.get("role_runs") or [])
            if str(item.get("role") or "") in set(role_names)
        ]
    role_done = [x for x in current_role_runs if str(x.get("status") or "") == "done"]
    kimi_hits = sum(1 for x in role_done if _is_kimi_model(str(x.get("used_model") or "")))
    gpt_fallback_hits = sum(
        1
        for x in role_done
        if _is_gpt54_model(str(x.get("used_model") or "")) and _is_kimi_model(str(x.get("requested_model") or ""))
    )
    print(
        f"[multi-role-v2] role_model_usage stage={stage} ts_code={ts_code} "
        f"done={len(role_done)} kimi_hits={kimi_hits} gpt_fallback_hits={gpt_fallback_hits}",
        flush=True,
    )
    return results


def _finalize_multi_role_v2_job(job_id: str, *, final_status: str):
    with ASYNC_MULTI_ROLE_V2_LOCK:
        job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return
        done_roles = [x for x in list(job.get("role_runs") or []) if x.get("status") == "done" and str(x.get("output") or "").strip()]
        failed_roles = [x for x in list(job.get("role_runs") or []) if x.get("status") != "done"]
        if failed_roles:
            job["warnings"] = [f"{x.get('role')}: {x.get('error') or 'failed'}" for x in failed_roles]
        ts_code = str(job.get("ts_code") or "")
        lookback = int(job.get("lookback") or 120)
        roles = [str(x.get("role") or "") for x in list(job.get("role_runs") or []) if str(x.get("role") or "").strip()]
        job["stage"] = "aggregating"
        job["progress"] = 85
        job["message"] = "角色阶段完成，正在汇总"
        job["aggregator_run"] = {"status": "running", "used_model": "", "attempts": [], "error": "", "duration_ms": 0}
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()

    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": job_id, "status": "running", "progress": 85, "stage": "aggregating", "ts_code": ts_code, "mode": "v2"},
        producer="backend.server",
    )

    aggregator = _run_multi_role_v2_aggregator(
        role_runs=done_roles,
        ts_code=ts_code,
        lookback=lookback,
        policy_map=_load_multi_role_v2_policies(),
    )
    agg_attempts = list(aggregator.get("attempts") or [])
    agg_kimi_hits = sum(1 for x in agg_attempts if _is_kimi_model(str(x.get("model") or "")) and not str(x.get("error") or "").strip())
    agg_gpt_fallback_hits = sum(1 for x in agg_attempts if _is_gpt54_model(str(x.get("model") or "")) and not str(x.get("error") or "").strip())
    agg_used_model = str(aggregator.get("used_model") or "")

    analysis_markdown = str(aggregator.get("analysis_markdown") or "")
    split_payload = split_multi_role_analysis(analysis_markdown, roles)
    role_outputs = split_payload.get("role_sections") or [
        {"role": x.get("role"), "content": x.get("output"), "matched": True, "logic_view": {}}
        for x in done_roles
    ]
    confidence = infer_decision_confidence(analysis_markdown).to_dict()
    risk_review = build_risk_review(analysis_markdown).to_dict()
    portfolio = build_portfolio_view(analysis_markdown).to_dict()

    now = datetime.now(timezone.utc).isoformat()
    with ASYNC_MULTI_ROLE_V2_LOCK:
        job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return
        context_build_ms = int(job.get("context_build_ms") or 0)
        role_parallel_ms = int(job.get("role_parallel_ms") or 0)
        aggregate_ms = int(aggregator.get("duration_ms") or 0)
        total_ms = context_build_ms + role_parallel_ms + aggregate_ms
        if total_ms <= 0:
            created_dt = _parse_iso_dt(str(job.get("created_at") or ""))
            if created_dt is not None:
                if created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=timezone.utc)
                total_ms = int((datetime.now(timezone.utc) - created_dt.astimezone(timezone.utc)).total_seconds() * 1000)
        total_ms = max(total_ms, 0)
        job["aggregator_run"] = {
            "status": "done" if aggregator.get("ok") else "error",
            "used_model": aggregator.get("used_model") or "",
            "requested_model": aggregator.get("requested_model") or "",
            "attempts": aggregator.get("attempts") or [],
            "error": aggregator.get("error") or "",
            "duration_ms": aggregate_ms,
        }
        job["analysis_markdown"] = analysis_markdown
        job["analysis"] = analysis_markdown
        job["used_model"] = str(aggregator.get("used_model") or "")
        job["attempts"] = aggregator.get("attempts") or []
        job["role_outputs"] = role_outputs
        job["role_sections"] = role_outputs
        job["common_sections_markdown"] = split_payload.get("common_sections_markdown") or ""
        job["decision_confidence"] = confidence
        job["risk_review"] = risk_review
        job["portfolio_view"] = portfolio
        context = job.get("context") or {}
        job["used_context_dims"] = [k for k, v in context.items() if v not in (None, "", [], {})]
        job["status"] = final_status
        job["stage"] = "done"
        job["progress"] = 100
        base_msg = "分析完成（含降级告警）" if final_status == "done_with_warnings" else "分析完成"
        job["total_ms"] = total_ms
        job["message"] = (
            f"{base_msg} · total {total_ms}ms "
            f"(context {context_build_ms}ms / roles {role_parallel_ms}ms / aggregate {aggregate_ms}ms)"
        )
        job["finished_at"] = now
        job["updated_at"] = now
        job["updated_at_ts"] = time.time()
        job["decision_state"] = {
            **(job.get("decision_state") or {}),
            "pending_user_decision": False,
            "pending_roles": [],
            "updated_at": now,
        }
        ts_code = str(job.get("ts_code") or "")
        try:
            _persist_multi_role_analysis_v2_job(job)
        except Exception:
            pass
    print(
        f"[multi-role-v2] total_ms={total_ms} context_build_ms={context_build_ms} "
        f"role_parallel_ms={role_parallel_ms} aggregate_ms={aggregate_ms} ts_code={ts_code} status={final_status} "
        f"aggregator_used_model={agg_used_model or '-'} aggregator_kimi_hits={agg_kimi_hits} "
        f"aggregator_gpt_fallback_hits={agg_gpt_fallback_hits}",
        flush=True,
    )
    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": job_id, "status": final_status, "progress": 100, "stage": "done", "ts_code": ts_code, "mode": "v2"},
        producer="backend.server",
    )


def _run_async_multi_role_v2_job(job_id: str):
    try:
        with ASYNC_MULTI_ROLE_V2_LOCK:
            job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
            if not job:
                return
            ts_code = str(job.get("ts_code") or "")
            lookback = int(job.get("lookback") or 120)
            roles = [str(x) for x in list(job.get("roles") or []) if str(x).strip()]
            job["status"] = "running"
            job["progress"] = 12
            job["stage"] = "context"
            job["message"] = "正在构建分析上下文"
            job["updated_at"] = datetime.now(timezone.utc).isoformat()
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="multi_role_job_update",
            payload={"job_id": job_id, "status": "running", "progress": 12, "stage": "context", "ts_code": ts_code, "mode": "v2"},
            producer="backend.server",
        )
        context_started = time.time()
        cached_context = _get_cached_multi_role_v2_context(ts_code, lookback)
        context_cache_hit = isinstance(cached_context, dict)
        if context_cache_hit:
            context = cached_context
        else:
            context = build_multi_role_context(ts_code, lookback)
            _set_cached_multi_role_v2_context(ts_code, lookback, context)
        context_build_ms = int((time.time() - context_started) * 1000)
        print(
            f"[multi-role-v2] context_build_ms={context_build_ms} ts_code={ts_code} lookback={lookback} cache_hit={context_cache_hit}",
            flush=True,
        )
        with ASYNC_MULTI_ROLE_V2_LOCK:
            job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
            if not job:
                return
            job["context"] = context
            job["context_build_ms"] = context_build_ms
            job["name"] = context.get("company_profile", {}).get("name", "")
            job["progress"] = 30
            job["stage"] = "role_parallel"
            job["message"] = "角色任务并行执行中"
            job["updated_at"] = datetime.now(timezone.utc).isoformat()
            job["updated_at_ts"] = time.time()
            roles = [str(x) for x in list(job.get("roles") or []) if str(x).strip()]
        publish_app_event(
            event="multi_role_job_update",
            payload={"job_id": job_id, "status": "running", "progress": 30, "stage": "role_parallel", "ts_code": ts_code, "mode": "v2"},
            producer="backend.server",
        )
        role_started = time.time()
        _run_multi_role_v2_role_batch(job_id, roles, 2, "role_parallel")
        role_parallel_ms = int((time.time() - role_started) * 1000)
        print(
            f"[multi-role-v2] role_parallel_ms={role_parallel_ms} ts_code={ts_code} roles={len(roles)}",
            flush=True,
        )
        with ASYNC_MULTI_ROLE_V2_LOCK:
            job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
            if not job:
                return
            job["role_parallel_ms"] = int(job.get("role_parallel_ms") or 0) + role_parallel_ms
            failed_roles = [x.get("role") for x in list(job.get("role_runs") or []) if x.get("status") != "done"]
            accept_auto_degrade = bool(job.get("accept_auto_degrade", True))
            if failed_roles and not accept_auto_degrade:
                now = datetime.now(timezone.utc).isoformat()
                job["status"] = "pending_user_decision"
                job["progress"] = 70
                job["stage"] = "pending_user_decision"
                job["message"] = (
                    f"部分角色失败，等待用户决策（context {int(job.get('context_build_ms') or 0)}ms / "
                    f"roles {int(job.get('role_parallel_ms') or 0)}ms）"
                )
                job["decision_state"] = {
                    "pending_user_decision": True,
                    "pending_roles": failed_roles,
                    "round": int((job.get("decision_state") or {}).get("round") or 0) + 1,
                    "last_action": "awaiting",
                    "updated_at": now,
                }
                job["updated_at"] = now
                job["updated_at_ts"] = time.time()
                try:
                    _persist_multi_role_analysis_v2_job(job)
                except Exception:
                    pass
                publish_app_event(
                    event="multi_role_job_update",
                    payload={
                        "job_id": job_id,
                        "status": "pending_user_decision",
                        "progress": 70,
                        "stage": "pending_user_decision",
                        "ts_code": ts_code,
                        "mode": "v2",
                    },
                    producer="backend.server",
                )
                return
            final_status = "done_with_warnings" if failed_roles else "done"
        _finalize_multi_role_v2_job(job_id, final_status=final_status)
    except Exception as exc:
        now = datetime.now(timezone.utc).isoformat()
        with ASYNC_MULTI_ROLE_V2_LOCK:
            job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
            if not job:
                return
            job["status"] = "error"
            job["progress"] = 100
            job["stage"] = "error"
            job["message"] = "分析失败"
            job["error"] = str(exc)
            job["finished_at"] = now
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
            ts_code = str(job.get("ts_code") or "")
            try:
                _persist_multi_role_analysis_v2_job(job)
            except Exception:
                pass
        publish_app_event(
            event="multi_role_job_update",
            payload={"job_id": job_id, "status": "error", "progress": 100, "stage": "error", "ts_code": ts_code, "mode": "v2", "error": str(exc)},
            producer="backend.server",
        )
    finally:
        _release_multi_role_v2_slot(job_id)


def _retry_async_multi_role_v2_job(job_id: str):
    with ASYNC_MULTI_ROLE_V2_LOCK:
        job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return
        if str(job.get("status")) != "running":
            return
        ts_code = str(job.get("ts_code") or "")
        pending = list((job.get("decision_state") or {}).get("pending_roles") or [])
        if not pending:
            return
        job["progress"] = 72
        job["stage"] = "role_retry"
        job["message"] = "按用户指令重试失败角色中"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()
    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": job_id, "status": "running", "progress": 72, "stage": "role_retry", "ts_code": ts_code, "mode": "v2"},
        producer="backend.server",
    )
    role_started = time.time()
    _run_multi_role_v2_role_batch(job_id, pending, 2, "role_retry")
    role_retry_ms = int((time.time() - role_started) * 1000)
    print(
        f"[multi-role-v2] role_retry_ms={role_retry_ms} ts_code={ts_code} retry_roles={len(pending)}",
        flush=True,
    )
    with ASYNC_MULTI_ROLE_V2_LOCK:
        job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return
        job["role_parallel_ms"] = int(job.get("role_parallel_ms") or 0) + role_retry_ms
        failed_roles = [x.get("role") for x in list(job.get("role_runs") or []) if x.get("status") != "done"]
        if failed_roles:
            now = datetime.now(timezone.utc).isoformat()
            job["status"] = "pending_user_decision"
            job["progress"] = 78
            job["stage"] = "pending_user_decision"
            job["message"] = (
                f"重试后仍有失败角色，等待用户决策（roles {int(job.get('role_parallel_ms') or 0)}ms）"
            )
            job["decision_state"] = {
                "pending_user_decision": True,
                "pending_roles": failed_roles,
                "round": int((job.get("decision_state") or {}).get("round") or 0) + 1,
                "last_action": "retry_failed",
                "updated_at": now,
            }
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
            try:
                _persist_multi_role_analysis_v2_job(job)
            except Exception:
                pass
            publish_app_event(
                event="multi_role_job_update",
                payload={"job_id": job_id, "status": "pending_user_decision", "progress": 78, "stage": "pending_user_decision", "ts_code": ts_code, "mode": "v2"},
                producer="backend.server",
            )
            return
    _finalize_multi_role_v2_job(job_id, final_status="done")


def start_async_multi_role_v2_job(*, ts_code: str, lookback: int, roles: list[str], accept_auto_degrade: bool, decision_timeout_seconds: int):
    job = _create_async_multi_role_v2_job(
        ts_code=ts_code,
        lookback=lookback,
        roles=roles,
        accept_auto_degrade=accept_auto_degrade,
        decision_timeout_seconds=decision_timeout_seconds,
    )
    should_start = False
    with ASYNC_MULTI_ROLE_V2_LOCK:
        job_id = str(job.get("job_id") or "")
        if len(ASYNC_MULTI_ROLE_V2_ACTIVE) < MULTI_ROLE_V2_MAX_CONCURRENT_JOBS:
            ASYNC_MULTI_ROLE_V2_ACTIVE.add(job_id)
            job["queue_position"] = 0
            should_start = True
        else:
            if job_id not in ASYNC_MULTI_ROLE_V2_QUEUE:
                ASYNC_MULTI_ROLE_V2_QUEUE.append(job_id)
            queue_pos = _queue_position_locked(job_id)
            now = datetime.now(timezone.utc).isoformat()
            job["status"] = "queued"
            job["stage"] = "queued"
            job["progress"] = 5
            job["message"] = (
                f"任务排队中，前方 {max(0, queue_pos - 1)} 个任务，"
                f"并发上限 {MULTI_ROLE_V2_MAX_CONCURRENT_JOBS}"
            )
            job["queue_position"] = int(queue_pos)
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
            try:
                _persist_multi_role_analysis_v2_job(job)
            except Exception:
                pass
        _refresh_multi_role_v2_runtime_meta_locked()
    if should_start:
        worker = threading.Thread(
            target=_run_async_multi_role_v2_job,
            args=(job["job_id"],),
            daemon=True,
            name=f"multi_role_v2_{job['job_id'][:8]}",
        )
        worker.start()
    return _serialize_async_multi_role_v2_job(job)


def get_async_multi_role_v2_job(job_id: str):
    _cleanup_async_multi_role_v2_jobs()
    with ASYNC_MULTI_ROLE_V2_LOCK:
        _refresh_multi_role_v2_runtime_meta_locked()
        job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            persisted = _load_persisted_multi_role_v2_job(job_id)
            if not persisted:
                return None
            status = str(persisted.get("status") or "")
            # 进程内任务字典不包含该 job，但持久化状态仍是 queued/running，
            # 通常意味着服务重启后执行线程已丢失，避免前端长期“假运行”。
            if status in {"queued", "running"}:
                updated_dt = _parse_iso_dt(str(persisted.get("updated_at") or ""))
                stale_seconds = None
                if updated_dt:
                    if updated_dt.tzinfo is None:
                        updated_dt = updated_dt.replace(tzinfo=timezone.utc)
                    stale_seconds = (datetime.now(timezone.utc) - updated_dt.astimezone(timezone.utc)).total_seconds()
                if stale_seconds is None or stale_seconds >= 90:
                    now = datetime.now(timezone.utc).isoformat()
                    persisted["status"] = "error"
                    persisted["stage"] = "error"
                    persisted["progress"] = 100
                    persisted["error"] = "任务执行上下文已丢失（可能服务重启），请重新发起分析"
                    persisted["message"] = "任务未实际执行，已自动转为失败，请重试"
                    persisted["updated_at"] = now
                    persisted["finished_at"] = now
                    persisted["updated_at_ts"] = time.time()
                    try:
                        _persist_multi_role_analysis_v2_job(persisted)
                    except Exception:
                        pass
            persisted["current_concurrent_jobs"] = len(ASYNC_MULTI_ROLE_V2_ACTIVE)
            persisted["queue_length"] = len(ASYNC_MULTI_ROLE_V2_QUEUE)
            persisted["queue_total"] = len(ASYNC_MULTI_ROLE_V2_QUEUE)
            persisted["queue_position"] = 0
            persisted["max_concurrent_jobs"] = MULTI_ROLE_V2_MAX_CONCURRENT_JOBS
            return persisted
        if str(job.get("status") or "") == "queued":
            queue_pos = _queue_position_locked(str(job_id or ""))
            if queue_pos > 0:
                job["message"] = (
                    f"任务排队中，前方 {max(0, queue_pos - 1)} 个任务，"
                    f"并发上限 {MULTI_ROLE_V2_MAX_CONCURRENT_JOBS}"
                )
            job["queue_position"] = int(queue_pos)
        return _serialize_async_multi_role_v2_job(job)


def decide_async_multi_role_v2_job(*, job_id: str, action: str):
    action = str(action or "").strip().lower()
    if action not in {"retry", "degrade", "abort"}:
        raise ValueError("action 必须是 retry|degrade|abort")
    _cleanup_async_multi_role_v2_jobs()
    with ASYNC_MULTI_ROLE_V2_LOCK:
        job = ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            raise RuntimeError(f"任务不存在或已过期: {job_id}")
        if str(job.get("status")) != "pending_user_decision":
            raise RuntimeError(f"当前任务状态不允许决策: {job.get('status')}")
        ts_code = str(job.get("ts_code") or "")
        now = datetime.now(timezone.utc).isoformat()
        state = dict(job.get("decision_state") or {})
        state["last_action"] = action
        state["updated_at"] = now
        state["pending_user_decision"] = False
        job["decision_state"] = state
        job["updated_at"] = now
        job["updated_at_ts"] = time.time()

        if action == "abort":
            job["status"] = "error"
            job["progress"] = 100
            job["stage"] = "error"
            job["message"] = "用户终止任务"
            job["error"] = "用户选择 abort"
            job["finished_at"] = now
            try:
                _persist_multi_role_analysis_v2_job(job)
            except Exception:
                pass
            publish_app_event(
                event="multi_role_job_update",
                payload={"job_id": job_id, "status": "error", "progress": 100, "stage": "error", "ts_code": ts_code, "mode": "v2"},
                producer="backend.server",
            )
            return _serialize_async_multi_role_v2_job(job)

        if action == "degrade":
            job["status"] = "running"
            job["progress"] = 82
            job["stage"] = "aggregating"
            job["message"] = "按用户决策执行降级汇总"
        else:
            job["status"] = "running"
            job["progress"] = 72
            job["stage"] = "role_retry"
            job["message"] = "按用户决策执行补重试"
        try:
            _persist_multi_role_analysis_v2_job(job)
        except Exception:
            pass
    if action == "degrade":
        _finalize_multi_role_v2_job(job_id, final_status="done_with_warnings")
    else:
        worker = threading.Thread(
            target=_retry_async_multi_role_v2_job,
            args=(job_id,),
            daemon=True,
            name=f"multi_role_v2_retry_{job_id[:8]}",
        )
        worker.start()
    return get_async_multi_role_v2_job(job_id) or {"job_id": job_id, "status": "error", "error": "任务不存在"}


def _retry_async_multi_role_v2_aggregate_worker(job_id: str):
    target_job_id = str(job_id or "").strip()
    in_memory = False
    ts_code = ""
    try:
        _cleanup_async_multi_role_v2_jobs()
        with ASYNC_MULTI_ROLE_V2_LOCK:
            live_job = ASYNC_MULTI_ROLE_V2_JOBS.get(target_job_id)
        if live_job:
            job = live_job
            in_memory = True
        else:
            persisted = _load_persisted_multi_role_v2_job(target_job_id)
            if not persisted:
                return
            job = persisted
            in_memory = False

        ts_code = str(job.get("ts_code") or "")
        lookback = int(job.get("lookback") or 120)
        role_runs = list(job.get("role_runs") or [])
        done_roles = [x for x in role_runs if str(x.get("status") or "") == "done" and str(x.get("output") or "").strip()]
        if not done_roles:
            raise RuntimeError("没有可用的角色输出，无法重试汇总")

        aggregator = _run_multi_role_v2_aggregator(
            role_runs=done_roles,
            ts_code=ts_code,
            lookback=lookback,
            policy_map=_load_multi_role_v2_policies(),
        )
        analysis_markdown = str(aggregator.get("analysis_markdown") or "")
        split_payload = split_multi_role_analysis(analysis_markdown, list(job.get("roles") or []))
        role_outputs = split_payload.get("role_sections") or [
            {"role": x.get("role"), "content": x.get("output"), "matched": True, "logic_view": {}}
            for x in done_roles
        ]
        confidence = infer_decision_confidence(analysis_markdown).to_dict()
        risk_review = build_risk_review(analysis_markdown).to_dict()
        portfolio = build_portfolio_view(analysis_markdown).to_dict()

        final_status = "done" if aggregator.get("ok") else "done_with_warnings"
        complete_now = datetime.now(timezone.utc).isoformat()

        if in_memory:
            with ASYNC_MULTI_ROLE_V2_LOCK:
                live = ASYNC_MULTI_ROLE_V2_JOBS.get(target_job_id)
                if not live:
                    return
                live["aggregator_run"] = {
                    "status": "done" if aggregator.get("ok") else "error",
                    "used_model": aggregator.get("used_model") or "",
                    "requested_model": aggregator.get("requested_model") or "",
                    "attempts": aggregator.get("attempts") or [],
                    "error": aggregator.get("error") or "",
                    "duration_ms": int(aggregator.get("duration_ms") or 0),
                }
                live["analysis_markdown"] = analysis_markdown
                live["analysis"] = analysis_markdown
                live["used_model"] = str(aggregator.get("used_model") or "")
                live["attempts"] = aggregator.get("attempts") or []
                live["role_outputs"] = role_outputs
                live["role_sections"] = role_outputs
                live["common_sections_markdown"] = split_payload.get("common_sections_markdown") or ""
                live["decision_confidence"] = confidence
                live["risk_review"] = risk_review
                live["portfolio_view"] = portfolio
                live["status"] = final_status
                live["stage"] = "done"
                live["progress"] = 100
                live["message"] = "汇总重试完成" if aggregator.get("ok") else "汇总重试失败，已保留角色原文"
                live["error"] = ""
                live["finished_at"] = complete_now
                live["updated_at"] = complete_now
                live["updated_at_ts"] = time.time()
                try:
                    _persist_multi_role_analysis_v2_job(live)
                except Exception:
                    pass
            publish_app_event(
                event="multi_role_job_update",
                payload={"job_id": target_job_id, "status": final_status, "progress": 100, "stage": "done", "ts_code": ts_code, "mode": "v2"},
                producer="backend.server",
            )
            return

        job["aggregator_run"] = {
            "status": "done" if aggregator.get("ok") else "error",
            "used_model": aggregator.get("used_model") or "",
            "requested_model": aggregator.get("requested_model") or "",
            "attempts": aggregator.get("attempts") or [],
            "error": aggregator.get("error") or "",
            "duration_ms": int(aggregator.get("duration_ms") or 0),
        }
        job["analysis_markdown"] = analysis_markdown
        job["analysis"] = analysis_markdown
        job["used_model"] = str(aggregator.get("used_model") or "")
        job["attempts"] = aggregator.get("attempts") or []
        job["role_outputs"] = role_outputs
        job["role_sections"] = role_outputs
        job["common_sections_markdown"] = split_payload.get("common_sections_markdown") or ""
        job["decision_confidence"] = confidence
        job["risk_review"] = risk_review
        job["portfolio_view"] = portfolio
        job["status"] = final_status
        job["stage"] = "done"
        job["progress"] = 100
        job["message"] = "汇总重试完成" if aggregator.get("ok") else "汇总重试失败，已保留角色原文"
        job["error"] = ""
        job["finished_at"] = complete_now
        job["updated_at"] = complete_now
        job["updated_at_ts"] = time.time()
        try:
            _persist_multi_role_analysis_v2_job(job)
        except Exception:
            pass
    except Exception as exc:
        fail_now = datetime.now(timezone.utc).isoformat()
        if in_memory:
            with ASYNC_MULTI_ROLE_V2_LOCK:
                live = ASYNC_MULTI_ROLE_V2_JOBS.get(target_job_id)
                if live:
                    live["status"] = "done_with_warnings"
                    live["stage"] = "done"
                    live["progress"] = 100
                    live["message"] = f"汇总重试失败：{exc}"
                    live["error"] = str(exc)
                    live["finished_at"] = fail_now
                    live["updated_at"] = fail_now
                    live["updated_at_ts"] = time.time()
                    try:
                        _persist_multi_role_analysis_v2_job(live)
                    except Exception:
                        pass
            if ts_code:
                publish_app_event(
                    event="multi_role_job_update",
                    payload={"job_id": target_job_id, "status": "done_with_warnings", "progress": 100, "stage": "done", "ts_code": ts_code, "mode": "v2"},
                    producer="backend.server",
                )
            return
        persisted = _load_persisted_multi_role_v2_job(target_job_id)
        if persisted:
            persisted["status"] = "done_with_warnings"
            persisted["stage"] = "done"
            persisted["progress"] = 100
            persisted["message"] = f"汇总重试失败：{exc}"
            persisted["error"] = str(exc)
            persisted["finished_at"] = fail_now
            persisted["updated_at"] = fail_now
            persisted["updated_at_ts"] = time.time()
            try:
                _persist_multi_role_analysis_v2_job(persisted)
            except Exception:
                pass


def retry_async_multi_role_v2_aggregate(*, job_id: str):
    target_job_id = str(job_id or "").strip()
    if not target_job_id:
        raise ValueError("job_id 不能为空")
    _cleanup_async_multi_role_v2_jobs()
    with ASYNC_MULTI_ROLE_V2_LOCK:
        live_job = ASYNC_MULTI_ROLE_V2_JOBS.get(target_job_id)
    if live_job:
        job = live_job
        in_memory = True
    else:
        persisted = _load_persisted_multi_role_v2_job(target_job_id)
        if not persisted:
            raise RuntimeError(f"任务不存在或已过期: {target_job_id}")
        job = persisted
        in_memory = False

    status = str(job.get("status") or "")
    if status in {"queued", "running"}:
        raise RuntimeError(f"任务仍在执行中，当前状态={status}，暂不支持重试汇总")

    role_runs = list(job.get("role_runs") or [])
    done_roles = [x for x in role_runs if str(x.get("status") or "") == "done" and str(x.get("output") or "").strip()]
    if not done_roles:
        raise RuntimeError("没有可用的角色输出，无法重试汇总")

    now = datetime.now(timezone.utc).isoformat()
    ts_code = str(job.get("ts_code") or "")
    if in_memory:
        with ASYNC_MULTI_ROLE_V2_LOCK:
            live = ASYNC_MULTI_ROLE_V2_JOBS.get(target_job_id)
            if not live:
                raise RuntimeError(f"任务不存在或已过期: {target_job_id}")
            live["status"] = "running"
            live["progress"] = 85
            live["stage"] = "aggregating"
            live["message"] = "正在重试汇总器（后台执行）"
            live["updated_at"] = now
            live["updated_at_ts"] = time.time()
            try:
                _persist_multi_role_analysis_v2_job(live)
            except Exception:
                pass
            payload_job = _serialize_async_multi_role_v2_job(live)
    else:
        job["status"] = "running"
        job["progress"] = 85
        job["stage"] = "aggregating"
        job["message"] = "正在重试汇总器（后台执行）"
        job["updated_at"] = now
        job["updated_at_ts"] = time.time()
        try:
            _persist_multi_role_analysis_v2_job(job)
        except Exception:
            pass
        payload_job = _serialize_async_multi_role_v2_job(job)

    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": target_job_id, "status": "running", "progress": 85, "stage": "aggregating", "ts_code": ts_code, "mode": "v2"},
        producer="backend.server",
    )
    worker = threading.Thread(
        target=_retry_async_multi_role_v2_aggregate_worker,
        args=(target_job_id,),
        daemon=True,
        name=f"multi_role_v2_reagg_{target_job_id[:8]}",
    )
    worker.start()
    return payload_job


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
        query_signal_chain_graph_fn=query_signal_chain_graph,
    )


def build_quantaalpha_runtime_deps() -> dict:
    return build_quantaalpha_service_runtime_deps(
        sqlite3_module=sqlite3,
        db_path=str(DB_PATH),
    )


def build_decision_runtime_deps() -> dict:
    return build_decision_service_runtime_deps(
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


def _multi_role_v3_resolve_stage_role(node_key: str, strong: bool) -> tuple[str, str]:
    key = str(node_key or "").strip().lower()
    if key.startswith("analyst:"):
        return "analyst", key
    if key in {"research:bull", "research:bear", "research:manager"}:
        return "research_debate", key
    if key.startswith("risk:"):
        return "risk_debate", key
    if key == "decision:research_manager":
        return "research_manager", key
    if key == "decision:trader":
        return "trader", key
    if key == "decision:portfolio_manager":
        return "portfolio_manager", key
    # 未识别节点回退到阶段强弱模型。
    return ("default_strong" if strong else "default_quick"), key


def _multi_role_v3_entry_chain(entry: dict | None, *, quick_model: str, deep_model: str, global_fallback: list[str], default_mode: str) -> list[str]:
    cfg = dict(entry or {})
    mode = str(cfg.get("mode") or "").strip().lower()
    if mode not in {"quick", "deep"}:
        mode = default_mode
    base = str(cfg.get("primary_model") or "").strip() or (quick_model if mode == "quick" else deep_model)
    fallback_raw = cfg.get("fallback_models") or []
    if isinstance(fallback_raw, str):
        fallback = [x.strip() for x in fallback_raw.split(",") if x.strip()]
    elif isinstance(fallback_raw, list):
        fallback = [str(x).strip() for x in fallback_raw if str(x).strip()]
    else:
        fallback = []
    if not fallback:
        fallback = list(global_fallback or [])
    out: list[str] = []
    for item in [base, *fallback]:
        normalized = normalize_model_name(str(item or ""))
        if normalized and normalized not in out:
            out.append(normalized)
    return out


def _multi_role_v3_model_chain(*, node_key: str, strong: bool) -> list[str]:
    policies = _load_multi_role_v3_policies()
    stage, role_key = _multi_role_v3_resolve_stage_role(node_key, strong)
    quick_model = normalize_model_name(str(policies.get("quick_think_llm") or "kimi-k2.5"))
    deep_model = normalize_model_name(str(policies.get("deep_think_llm") or "gpt-5.4-multi-role"))
    global_fallback = [normalize_model_name(str(x or "")) for x in list(policies.get("fallback_models") or []) if str(x or "").strip()]

    default_mode = "deep" if strong else "quick"
    base_chain = _multi_role_v3_entry_chain(
        None,
        quick_model=quick_model,
        deep_model=deep_model,
        global_fallback=global_fallback,
        default_mode=default_mode,
    )
    stage_chain = _multi_role_v3_entry_chain(
        dict((policies.get("stage_models") or {}).get(stage) or {}),
        quick_model=quick_model,
        deep_model=deep_model,
        global_fallback=global_fallback,
        default_mode=default_mode,
    )
    role_chain = _multi_role_v3_entry_chain(
        dict((policies.get("role_models") or {}).get(role_key.lower()) or {}),
        quick_model=quick_model,
        deep_model=deep_model,
        global_fallback=global_fallback,
        default_mode=default_mode,
    )

    chain: list[str] = []
    for bucket in (role_chain, stage_chain, base_chain):
        for item in bucket:
            if item and item not in chain:
                chain.append(item)
    return chain or [normalize_model_name(DEFAULT_LLM_MODEL)]


def _multi_role_v3_call_llm(*, node_key: str, prompt: str, strong: bool, timeout_s: int = 60) -> dict:
    chain = _multi_role_v3_model_chain(node_key=node_key, strong=strong)
    attempts: list[dict] = []
    last_err = ""
    for model in chain:
        try:
            result = chat_completion_with_fallback(
                model=model,
                temperature=0.2 if strong else 0.1,
                timeout_s=max(20, int(timeout_s or 60)),
                max_retries=0,
                messages=[
                    {
                        "role": "system",
                        "content": "你是结构化投研节点执行器。只输出 JSON，不要额外解释。",
                    },
                    {"role": "user", "content": str(prompt or "")},
                ],
            )
            for item in list(result.attempts or []):
                attempts.append({"model": item.model, "base_url": item.base_url, "error": item.error})
            return {
                "text": str(result.text or ""),
                "used_model": str(result.used_model or model),
                "requested_model": str(result.requested_model or model),
                "attempts": attempts,
            }
        except Exception as exc:
            last_err = str(exc)
            attempts.append({"model": model, "base_url": "", "error": last_err})
            continue
    raise RuntimeError(f"{node_key} 调用失败: {last_err or 'unknown error'}")


def _multi_role_v3_build_context(*, ts_code: str, lookback: int) -> dict:
    return build_multi_role_context(ts_code=ts_code, lookback=lookback)


def start_multi_role_v3_job(payload: dict) -> dict:
    return create_multi_role_v3_job(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        payload=payload,
    )


def get_multi_role_v3_job_by_id(job_id: str):
    return get_multi_role_v3_job(sqlite3_module=sqlite3, db_path=DB_PATH, job_id=job_id)


def decide_multi_role_v3_job(*, job_id: str, action: str):
    return control_multi_role_v3_job(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        job_id=job_id,
        action=action,
    )


def action_multi_role_v3_job(*, job_id: str, action: str, stage: str = ""):
    return control_multi_role_v3_job(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        job_id=job_id,
        action=action,
        stage=stage,
    )


def run_multi_role_v3_worker_once() -> None:
    run_multi_role_v3_worker_loop(
        sqlite3_module=sqlite3,
        db_path=DB_PATH,
        runtime={
            "build_context": _multi_role_v3_build_context,
            "llm_call": _multi_role_v3_call_llm,
        },
        once=True,
    )


# ---------------------------------------------------------------------------
# Chief Roundtable API helpers
# ---------------------------------------------------------------------------

def _roundtable_create(payload: dict) -> dict:
    ts_code = str(payload.get("ts_code") or "").strip().upper()
    if not ts_code:
        raise ValueError("ts_code 不能为空")
    return _create_roundtable_job(
        sqlite3_module=sqlite3,
        ts_code=ts_code,
        trigger=str(payload.get("trigger") or "manual"),
        source_job_id=str(payload.get("source_job_id") or ""),
        owner=str(payload.get("_owner") or ""),
    )


def _roundtable_get(job_id: str) -> dict | None:
    return _get_roundtable_job(sqlite3_module=sqlite3, job_id=job_id)


def _roundtable_list(ts_code: str = "", owner: str = "", page: int = 1, page_size: int = 20) -> dict:
    return _list_roundtable_jobs(
        sqlite3_module=sqlite3,
        ts_code=ts_code, owner=owner, page=page, page_size=page_size,
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

    def _route_deps(self) -> dict:
        return {
            "api_endpoints_catalog": API_ENDPOINTS_CATALOG,
            "api_layer_contracts": list_api_layer_contracts,
            "assert_write_allowed": assert_layer_write_allowed,
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
            **self._agent_service_deps(),
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
            **build_layered_route_deps(
                build_stock_news_service_runtime_deps=build_stock_news_service_runtime_deps,
                query_news_sources=query_news_sources,
                query_news=query_news,
                build_reporting_service_deps=build_reporting_service_deps,
                build_chatrooms_service_runtime_deps=build_chatrooms_service_runtime_deps,
                build_signals_service_runtime_deps=build_signals_service_runtime_deps,
                build_quantaalpha_runtime_deps=build_quantaalpha_runtime_deps,
                build_decision_runtime_deps=build_decision_runtime_deps,
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
            ),
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
        if portfolio_routes.dispatch_patch(self, parsed, payload, deps):
            return
        if macro_regime_routes.dispatch_patch(self, parsed, payload, deps):
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

        if self._serve_frontend_static(parsed):
            return

        self._send_json({"error": "Not Found"}, status=404)


if __name__ == "__main__":
    assert_database_ready()
    conn = sqlite3.connect(DB_PATH)
    try:
        _ensure_auth_tables(conn)
        ensure_multi_role_v3_tables(conn)
        _ensure_roundtable_tables(conn)
    finally:
        conn.close()
    ai_retrieval_ensure_tables(sqlite3_module=sqlite3, db_path=DB_PATH)

    server = ThreadingHTTPServer((HOST, PORT), ApiHandler)
    print(f"Backend API running on http://{HOST}:{PORT}")
    server.serve_forever()
