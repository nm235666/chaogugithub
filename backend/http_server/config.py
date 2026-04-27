#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

# Repo root: backend/http_server/config.py -> parents[2]
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

WEB_DIST_DIR = ROOT_DIR / "apps" / "web" / "dist"

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
    "/api/agents/health",
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
    "/app/data/intelligence": "public",
    "/app/data/intelligence/global-news": "news_read",
    "/app/data/intelligence/cn-news": "news_read",
    "/app/data/intelligence/stock-news": "stock_news_read",
    "/app/data/intelligence/daily-summaries": "daily_summary_read",
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
    "/system/agents-ops": "admin_system",
    "/system/agent-governance": "admin_system",
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
            {"to": "/system/agents-ops", "label": "Agent 运营台", "desc": "Agent 运行、审批、步骤与审计追踪", "permission": "admin_system"},
            {"to": "/system/agent-governance", "label": "Agent 治理中心", "desc": "质量评分、策略闸门、降级与阻断", "permission": "admin_system"},
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
