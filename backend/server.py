#!/usr/bin/env python3
from __future__ import annotations

import bisect
import hashlib
import json
import math
import os
import re
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
from job_orchestrator import query_job_definitions, query_job_runs, run_job
from llm_gateway import (
    DEFAULT_LLM_API_KEY,
    DEFAULT_LLM_BASE_URL,
    DEFAULT_LLM_MODEL,
    build_route,
    normalize_model_name,
    normalize_temperature_for_model,
    post_chat_completions,
    resolve_provider,
)
from realtime_streams import publish_app_event

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8000"))
DB_PATH = ROOT_DIR / "stock_codes.db"
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
ASYNC_JOB_TTL_SECONDS = 3600
ASYNC_MULTI_ROLE_JOBS: dict[str, dict] = {}
ASYNC_MULTI_ROLE_LOCK = threading.Lock()
ASYNC_DAILY_SUMMARY_JOBS: dict[str, dict] = {}
ASYNC_DAILY_SUMMARY_LOCK = threading.Lock()
TMP_DIR = Path("/tmp")
STOCK_SCORE_CACHE_TTL_SECONDS = 300
STOCK_SCORE_CACHE: dict[str, object] = {
    "generated_at": 0.0,
    "items": [],
    "summary": {},
}
REDIS_CACHE_TTL_SOURCE_MONITOR = 30
REDIS_CACHE_TTL_DASHBOARD = 30
AUDIT_REPORT_PATH = ROOT_DIR / "docs" / "database_audit_report.md"

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

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "items": data,
    }


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
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    where_clauses = []
    params: list[object] = []

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

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
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


def query_stock_news(ts_code: str, company_name: str, keyword: str, page: int, page_size: int):
    ts_code = ts_code.strip().upper()
    company_name = company_name.strip()
    keyword = keyword.strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    where_clauses = []
    params: list[object] = []
    if ts_code:
        where_clauses.append("ts_code = ?")
        params.append(ts_code)
    if company_name:
        where_clauses.append("company_name LIKE ?")
        params.append(f"%{company_name}%")
    if keyword:
        where_clauses.append("(title LIKE ? OR summary LIKE ?)")
        kw = f"%{keyword}%"
        params.extend([kw, kw])

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_news_items'"
        ).fetchone()[0]
        if not table_exists:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        cols = {r[1] for r in conn.execute("PRAGMA table_info(stock_news_items)").fetchall()}
        count_sql = f"SELECT COUNT(*) FROM stock_news_items{where_sql}"
        select_sentiment = ", ".join(
            [
                "llm_sentiment_score" if "llm_sentiment_score" in cols else "NULL AS llm_sentiment_score",
                "llm_sentiment_label" if "llm_sentiment_label" in cols else "NULL AS llm_sentiment_label",
                "llm_sentiment_reason" if "llm_sentiment_reason" in cols else "NULL AS llm_sentiment_reason",
                "llm_sentiment_confidence" if "llm_sentiment_confidence" in cols else "NULL AS llm_sentiment_confidence",
                "llm_sentiment_model" if "llm_sentiment_model" in cols else "NULL AS llm_sentiment_model",
                "llm_sentiment_scored_at" if "llm_sentiment_scored_at" in cols else "NULL AS llm_sentiment_scored_at",
            ]
        )
        data_sql = f"""
        SELECT
            id, ts_code, company_name, source, news_code, title, summary, link, pub_time,
            comment_num, relation_stock_tags_json,
            llm_system_score, llm_finance_impact_score, llm_finance_importance, llm_impacts_json,
            llm_summary, llm_model, llm_scored_at,
            {select_sentiment},
            fetched_at, update_time
        FROM stock_news_items
        {where_sql}
        ORDER BY pub_time DESC, id DESC
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
    talker = (talker or "").strip()
    sender_name = (sender_name or "").strip()
    keyword = (keyword or "").strip()
    is_quote = (is_quote or "").strip()
    query_date_start = (query_date_start or "").strip()
    query_date_end = (query_date_end or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='wechat_chatlog_clean_items'"
        ).fetchone()[0]
        if not table_exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "filters": {"talkers": [], "senders": []},
            }

        where_clauses = []
        params: list[object] = []
        if talker:
            where_clauses.append("talker = ?")
            params.append(talker)
        if sender_name:
            where_clauses.append("sender_name = ?")
            params.append(sender_name)
        if keyword:
            where_clauses.append("(content_clean LIKE ? OR quote_content LIKE ? OR sender_name LIKE ?)")
            kw = f"%{keyword}%"
            params.extend([kw, kw, kw])
        if is_quote in {"0", "1"}:
            where_clauses.append("is_quote = ?")
            params.append(int(is_quote))
        if query_date_start:
            where_clauses.append("query_date_start >= ?")
            params.append(query_date_start)
        if query_date_end:
            where_clauses.append("query_date_end <= ?")
            params.append(query_date_end)

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        total = conn.execute(
            f"SELECT COUNT(*) FROM wechat_chatlog_clean_items{where_sql}",
            params,
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT
                id, talker, query_date_start, query_date_end, message_date, message_time,
                sender_name, sender_id, message_type, content, content_clean, is_quote,
                quote_sender_name, quote_sender_id, quote_time_text, quote_content,
                raw_block, source_url, fetched_at, update_time
            FROM wechat_chatlog_clean_items
            {where_sql}
            ORDER BY query_date_start DESC, message_time DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
        talkers = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT talker FROM wechat_chatlog_clean_items WHERE talker IS NOT NULL AND talker <> '' ORDER BY talker"
            ).fetchall()
        ]
        sender_params: list[object] = []
        sender_where_clauses = ["sender_name IS NOT NULL", "sender_name <> ''"]
        if talker:
            sender_where_clauses.append("talker = ?")
            sender_params.append(talker)
        sender_where_sql = " WHERE " + " AND ".join(sender_where_clauses)
        senders = [
            r[0]
            for r in conn.execute(
                f"SELECT DISTINCT sender_name FROM wechat_chatlog_clean_items{sender_where_sql} ORDER BY sender_name",
                sender_params,
            ).fetchall()
        ]
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": data,
        "filters": {
            "talkers": talkers,
            "senders": senders,
        },
    }


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
    keyword = (keyword or "").strip()
    primary_category = (primary_category or "").strip()
    activity_level = (activity_level or "").strip()
    risk_level = (risk_level or "").strip()
    skip_realtime_monitor = (skip_realtime_monitor or "").strip()
    fetch_status = (fetch_status or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        room_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chatroom_list_items'"
        ).fetchone()[0]
        if not room_exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "summary": {},
                "filters": {"primary_categories": [], "activity_levels": [], "risk_levels": [], "fetch_statuses": []},
            }

        where_clauses = []
        params: list[object] = []
        if keyword:
            kw = f"%{keyword}%"
            where_clauses.append(
                "("
                "c.room_id LIKE ? OR c.remark LIKE ? OR c.nick_name LIKE ? OR "
                "c.llm_chatroom_summary LIKE ? OR c.llm_chatroom_tags_json LIKE ?"
                ")"
            )
            params.extend([kw, kw, kw, kw, kw])
        if primary_category:
            where_clauses.append("COALESCE(c.llm_chatroom_primary_category, '') = ?")
            params.append(primary_category)
        if activity_level:
            where_clauses.append("COALESCE(c.llm_chatroom_activity_level, '') = ?")
            params.append(activity_level)
        if risk_level:
            where_clauses.append("COALESCE(c.llm_chatroom_risk_level, '') = ?")
            params.append(risk_level)
        if skip_realtime_monitor in {"0", "1"}:
            where_clauses.append("COALESCE(c.skip_realtime_monitor, 0) = ?")
            params.append(int(skip_realtime_monitor))
        if fetch_status == "failed":
            where_clauses.append(
                "COALESCE(c.last_chatlog_backfill_status, '') <> '' AND COALESCE(c.last_chatlog_backfill_status, '') <> 'ok'"
            )
        elif fetch_status == "ok":
            where_clauses.append("COALESCE(c.last_chatlog_backfill_status, '') = 'ok'")
        elif fetch_status == "unknown":
            where_clauses.append("COALESCE(c.last_chatlog_backfill_status, '') = ''")

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        count_sql = f"""
        SELECT COUNT(*)
        FROM chatroom_list_items c
        {where_sql}
        """
        data_sql = f"""
        SELECT
            c.room_id,
            c.remark,
            c.nick_name,
            c.owner,
            c.user_count,
            c.first_seen_at,
            c.last_seen_at,
            c.update_time,
            c.skip_realtime_monitor,
            c.skip_realtime_reason,
            c.skip_realtime_marked_at,
            c.last_message_date,
            c.last_chatlog_backfill_at,
            c.last_chatlog_backfill_status,
            c.last_30d_raw_message_count,
            c.last_30d_clean_message_count,
            c.last_30d_fetch_fail_count,
            c.silent_candidate_runs,
            c.silent_candidate_since,
            c.llm_chatroom_summary,
            c.llm_chatroom_tags_json,
            c.llm_chatroom_primary_category,
            c.llm_chatroom_activity_level,
            c.llm_chatroom_risk_level,
            c.llm_chatroom_confidence,
            c.llm_chatroom_model,
            c.llm_chatroom_tagged_at,
            c.last_chatlog_backfill_at,
            c.last_chatlog_backfill_status,
            COALESCE(logs.message_row_count, 0) AS message_row_count
        FROM chatroom_list_items c
        LEFT JOIN (
            SELECT talker, COUNT(*) AS message_row_count
            FROM wechat_chatlog_clean_items
            GROUP BY talker
        ) logs
          ON logs.talker = COALESCE(NULLIF(c.remark, ''), NULLIF(c.nick_name, ''), c.room_id)
        {where_sql}
        ORDER BY
            COALESCE(c.last_message_date, '') DESC,
            COALESCE(logs.message_row_count, 0) DESC,
            COALESCE(c.user_count, 0) DESC,
            c.room_id
        LIMIT ? OFFSET ?
        """
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()

        summary = {
            "room_total": conn.execute("SELECT COUNT(*) FROM chatroom_list_items").fetchone()[0],
            "room_with_logs": conn.execute(
                """
                SELECT COUNT(*)
                FROM chatroom_list_items c
                WHERE EXISTS (
                    SELECT 1
                    FROM wechat_chatlog_clean_items l
                    WHERE l.talker = COALESCE(NULLIF(c.remark, ''), NULLIF(c.nick_name, ''), c.room_id)
                )
                """
            ).fetchone()[0],
            "skip_total": conn.execute(
                "SELECT COUNT(*) FROM chatroom_list_items WHERE COALESCE(skip_realtime_monitor, 0) = 1"
            ).fetchone()[0],
            "tagged_total": conn.execute(
                "SELECT COUNT(*) FROM chatroom_list_items WHERE llm_chatroom_primary_category IS NOT NULL AND llm_chatroom_primary_category <> ''"
            ).fetchone()[0],
        }
        filters = {
            "primary_categories": [
                r[0]
                for r in conn.execute(
                    """
                    SELECT DISTINCT llm_chatroom_primary_category
                    FROM chatroom_list_items
                    WHERE llm_chatroom_primary_category IS NOT NULL AND llm_chatroom_primary_category <> ''
                    ORDER BY llm_chatroom_primary_category
                    """
                ).fetchall()
            ],
            "activity_levels": [
                r[0]
                for r in conn.execute(
                    """
                    SELECT DISTINCT llm_chatroom_activity_level
                    FROM chatroom_list_items
                    WHERE llm_chatroom_activity_level IS NOT NULL AND llm_chatroom_activity_level <> ''
                    ORDER BY llm_chatroom_activity_level
                    """
                ).fetchall()
            ],
            "risk_levels": [
                r[0]
                for r in conn.execute(
                    """
                    SELECT DISTINCT llm_chatroom_risk_level
                    FROM chatroom_list_items
                    WHERE llm_chatroom_risk_level IS NOT NULL AND llm_chatroom_risk_level <> ''
                    ORDER BY llm_chatroom_risk_level
                    """
                ).fetchall()
            ],
            "fetch_statuses": ["failed", "ok", "unknown"],
        }
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": data,
        "summary": summary,
        "filters": filters,
    }


def fetch_single_chatroom_now(room_id: str, fetch_yesterday_and_today: bool):
    room_id = (room_id or "").strip()
    if not room_id:
        raise ValueError("room_id 不能为空")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT room_id, remark, nick_name, skip_realtime_monitor
            FROM chatroom_list_items
            WHERE room_id = ?
            """,
            (room_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        raise ValueError(f"未找到群聊: {room_id}")
    if int(row["skip_realtime_monitor"] or 0) != 0:
        raise ValueError("该群当前未处于监控中")

    talker = str(row["remark"] or row["nick_name"] or row["room_id"] or "").strip()
    cmd = [
        "python3",
        str(Path(__file__).resolve().parent.parent / "fetch_monitored_chatlogs_once.py"),
        "--only-room",
        talker,
    ]
    if fetch_yesterday_and_today:
        cmd.append("--yesterday-and-today")
    publish_app_event(
        event="chatroom_fetch_update",
        payload={
            "room_id": room_id,
            "talker": talker,
            "status": "running",
            "mode": "yesterday_and_today" if fetch_yesterday_and_today else "today",
        },
        producer="backend.server",
    )
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    output = ((proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")).strip()
    if proc.returncode != 0:
        publish_app_event(
            event="chatroom_fetch_update",
            payload={
                "room_id": room_id,
                "talker": talker,
                "status": "error",
                "mode": "yesterday_and_today" if fetch_yesterday_and_today else "today",
                "error": output or "立即拉取失败",
            },
            producer="backend.server",
        )
        raise RuntimeError(output or "立即拉取失败")
    publish_app_event(
        event="chatroom_fetch_update",
        payload={
            "room_id": room_id,
            "talker": talker,
            "status": "done",
            "mode": "yesterday_and_today" if fetch_yesterday_and_today else "today",
        },
        producer="backend.server",
    )
    return {
        "ok": True,
        "room_id": room_id,
        "talker": talker,
        "mode": "yesterday_and_today" if fetch_yesterday_and_today else "today",
        "output": output,
    }


def query_chatroom_investment_analysis(
    keyword: str,
    final_bias: str,
    target_keyword: str,
    page: int,
    page_size: int,
):
    keyword = (keyword or "").strip()
    final_bias = (final_bias or "").strip()
    target_keyword = (target_keyword or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chatroom_investment_analysis'"
        ).fetchone()[0]
        if not table_exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "summary": {},
                "filters": {"final_biases": []},
            }
        cols = {r[1] for r in conn.execute("PRAGMA table_info(chatroom_investment_analysis)").fetchall()}
        select_chatroom_sentiment = ", ".join(
            [
                "a.llm_sentiment_score" if "llm_sentiment_score" in cols else "NULL AS llm_sentiment_score",
                "a.llm_sentiment_label" if "llm_sentiment_label" in cols else "NULL AS llm_sentiment_label",
                "a.llm_sentiment_reason" if "llm_sentiment_reason" in cols else "NULL AS llm_sentiment_reason",
                "a.llm_sentiment_confidence" if "llm_sentiment_confidence" in cols else "NULL AS llm_sentiment_confidence",
                "a.llm_sentiment_model" if "llm_sentiment_model" in cols else "NULL AS llm_sentiment_model",
                "a.llm_sentiment_scored_at" if "llm_sentiment_scored_at" in cols else "NULL AS llm_sentiment_scored_at",
            ]
        )

        where_clauses = []
        params: list[object] = []
        if keyword:
            kw = f"%{keyword}%"
            where_clauses.append("(a.talker LIKE ? OR a.room_summary LIKE ?)")
            params.extend([kw, kw])
        if final_bias in {"看多", "看空"}:
            where_clauses.append("a.final_bias = ?")
            params.append(final_bias)
        if target_keyword:
            where_clauses.append("a.targets_json LIKE ?")
            params.append(f"%{target_keyword}%")

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        latest_subquery = """
        SELECT room_id, MAX(update_time) AS max_update_time
        FROM chatroom_investment_analysis
        GROUP BY room_id
        """
        count_sql = f"""
        SELECT COUNT(*)
        FROM chatroom_investment_analysis a
        JOIN ({latest_subquery}) latest
          ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
        {where_sql}
        """
        data_sql = f"""
        SELECT
            a.id,
            a.room_id,
            a.talker,
            a.analysis_date,
            a.analysis_window_days,
            a.message_count,
            a.sender_count,
            a.latest_message_date,
            a.room_summary,
            a.targets_json,
            a.final_bias,
            {select_chatroom_sentiment},
            a.model,
            a.prompt_version,
            a.created_at,
            a.update_time,
            c.remark,
            c.nick_name,
            c.user_count,
            c.skip_realtime_monitor
        FROM chatroom_investment_analysis a
        JOIN ({latest_subquery}) latest
          ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
        LEFT JOIN chatroom_list_items c
          ON c.room_id = a.room_id
        {where_sql}
        ORDER BY a.latest_message_date DESC, a.update_time DESC, a.id DESC
        LIMIT ? OFFSET ?
        """
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        summary = {
            "analysis_total": conn.execute(f"SELECT COUNT(*) FROM ({latest_subquery}) latest_all").fetchone()[0],
            "bullish_total": conn.execute(
                f"""
                SELECT COUNT(*)
                FROM chatroom_investment_analysis a
                JOIN ({latest_subquery}) latest
                  ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
                WHERE a.final_bias = '看多'
                """
            ).fetchone()[0],
            "bearish_total": conn.execute(
                f"""
                SELECT COUNT(*)
                FROM chatroom_investment_analysis a
                JOIN ({latest_subquery}) latest
                  ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
                WHERE a.final_bias = '看空'
                """
            ).fetchone()[0],
            "with_targets_total": conn.execute(
                f"""
                SELECT COUNT(*)
                FROM chatroom_investment_analysis a
                JOIN ({latest_subquery}) latest
                  ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
                WHERE a.targets_json IS NOT NULL AND a.targets_json <> '' AND a.targets_json <> '[]'
                """
            ).fetchone()[0],
        }
        filters = {
            "final_biases": [
                r[0]
                for r in conn.execute(
                    """
                    SELECT DISTINCT final_bias
                    FROM chatroom_investment_analysis
                    WHERE final_bias IS NOT NULL AND final_bias <> ''
                    ORDER BY final_bias
                    """
                ).fetchall()
            ]
        }
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": data,
        "summary": summary,
        "filters": filters,
    }


def query_chatroom_candidate_pool(
    keyword: str,
    dominant_bias: str,
    candidate_type: str,
    page: int,
    page_size: int,
):
    keyword = (keyword or "").strip()
    dominant_bias = (dominant_bias or "").strip()
    candidate_type = (candidate_type or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chatroom_stock_candidate_pool'"
        ).fetchone()[0]
        if not table_exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "summary": {},
                "filters": {"dominant_biases": [], "candidate_types": []},
            }
        where_clauses = []
        params: list[object] = []
        if keyword:
            kw = f"%{keyword}%"
            where_clauses.append("(candidate_name LIKE ? OR sample_reasons_json LIKE ?)")
            params.extend([kw, kw])
        if dominant_bias in {"看多", "看空"}:
            where_clauses.append("dominant_bias = ?")
            params.append(dominant_bias)
        if candidate_type:
            where_clauses.append("candidate_type = ?")
            params.append(candidate_type)
        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        total = conn.execute(
            f"SELECT COUNT(*) FROM chatroom_stock_candidate_pool{where_sql}",
            params,
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT
                id, candidate_name, candidate_type, bullish_room_count, bearish_room_count,
                net_score, dominant_bias, mention_count, room_count, latest_analysis_date, ts_code,
                sample_reasons_json, source_room_ids_json, source_talkers_json, created_at, update_time
            FROM chatroom_stock_candidate_pool
            {where_sql}
            ORDER BY ABS(net_score) DESC, room_count DESC, mention_count DESC, candidate_name
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
        summary = {
            "candidate_total": conn.execute("SELECT COUNT(*) FROM chatroom_stock_candidate_pool").fetchone()[0],
            "bullish_total": conn.execute(
                "SELECT COUNT(*) FROM chatroom_stock_candidate_pool WHERE dominant_bias = '看多'"
            ).fetchone()[0],
            "bearish_total": conn.execute(
                "SELECT COUNT(*) FROM chatroom_stock_candidate_pool WHERE dominant_bias = '看空'"
            ).fetchone()[0],
            "stock_like_total": conn.execute(
                "SELECT COUNT(*) FROM chatroom_stock_candidate_pool WHERE candidate_type IN ('股票', '标的')"
            ).fetchone()[0],
        }
        filters = {
            "dominant_biases": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT dominant_bias FROM chatroom_stock_candidate_pool WHERE dominant_bias IS NOT NULL AND dominant_bias <> '' ORDER BY dominant_bias"
                ).fetchall()
            ],
            "candidate_types": [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT candidate_type FROM chatroom_stock_candidate_pool WHERE candidate_type IS NOT NULL AND candidate_type <> '' ORDER BY candidate_type"
                ).fetchall()
            ],
        }
        stock_name_map = {}
        try:
            stock_name_map = {
                str(r[0] or "").strip().upper(): str(r[1] or "").strip()
                for r in conn.execute("SELECT ts_code, name FROM stock_codes").fetchall()
            }
        except Exception:
            stock_name_map = {}
        data = []
        for r in rows:
            item = dict(r)
            ts_code = str(item.get("ts_code") or "").strip().upper()
            display_name = item.get("candidate_name") or ""
            if ts_code and stock_name_map.get(ts_code):
                display_name = stock_name_map.get(ts_code) or display_name
            item["display_name"] = display_name
            data.append(item)
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": data,
        "summary": summary,
        "filters": filters,
    }


def query_investment_signals(
    keyword: str,
    signal_type: str,
    signal_group: str,
    scope: str,
    source_filter: str,
    direction: str,
    signal_status: str,
    page: int,
    page_size: int,
):
    keyword = (keyword or "").strip()
    signal_type = (signal_type or "").strip()
    signal_group = (signal_group or "").strip()
    scope = (scope or "").strip().lower()
    source_filter = (source_filter or "").strip()
    direction = (direction or "").strip()
    signal_status = (signal_status or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_name, normalized_scope = resolve_signal_table(conn, scope)
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()[0]
        if not table_exists:
            return {
                "page": page,
                "page_size": page_size,
                "total": 0,
                "total_pages": 0,
                "items": [],
                "summary": {},
                "filters": {"signal_types": [], "directions": [], "signal_statuses": []},
                "scope": normalized_scope,
            }
        where_clauses = []
        params: list[object] = []
        if keyword:
            kw = f"%{keyword}%"
            where_clauses.append("(subject_name LIKE ? OR COALESCE(ts_code, '') LIKE ? OR COALESCE(evidence_json, '') LIKE ?)")
            params.extend([kw, kw, kw])
        if signal_type:
            where_clauses.append("signal_type = ?")
            params.append(signal_type)
        elif signal_group == "stock":
            where_clauses.append("signal_type = 'stock'")
        elif signal_group == "non_stock":
            where_clauses.append("signal_type <> 'stock'")
        elif signal_group == "chatroom_stock":
            where_clauses.append("signal_type = 'stock'")
            where_clauses.append("COALESCE(chatroom_count, 0) > 0")
        if source_filter == "chatroom":
            where_clauses.append("COALESCE(chatroom_count, 0) > 0")
        if direction:
            where_clauses.append("direction = ?")
            params.append(direction)
        if signal_status:
            where_clauses.append("signal_status = ?")
            params.append(signal_status)
        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        total = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}{where_sql}",
            params,
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT
                id, signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                evidence_count, news_count, stock_news_count, chatroom_count, signal_status,
                latest_signal_date, evidence_json, source_summary_json, created_at, update_time
            FROM {table_name}
            {where_sql}
            ORDER BY signal_strength DESC, confidence DESC, latest_signal_date DESC, subject_name
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
        summary = {
            "signal_total": conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0],
            "bullish_total": conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE direction = '看多'"
            ).fetchone()[0],
            "bearish_total": conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE direction = '看空'"
            ).fetchone()[0],
            "active_total": conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE signal_status = '活跃'"
            ).fetchone()[0],
            "stock_total": conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE signal_type = 'stock'"
            ).fetchone()[0],
        }
        filters = {
            "signal_types": [
                r[0]
                for r in conn.execute(
                    f"SELECT DISTINCT signal_type FROM {table_name} WHERE signal_type IS NOT NULL AND signal_type <> '' ORDER BY signal_type"
                ).fetchall()
            ],
            "directions": [
                r[0]
                for r in conn.execute(
                    f"SELECT DISTINCT direction FROM {table_name} WHERE direction IS NOT NULL AND direction <> '' ORDER BY direction"
                ).fetchall()
            ],
            "signal_statuses": [
                r[0]
                for r in conn.execute(
                    f"SELECT DISTINCT signal_status FROM {table_name} WHERE signal_status IS NOT NULL AND signal_status <> '' ORDER BY signal_status"
                ).fetchall()
            ],
        }
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": data,
        "summary": summary,
        "filters": filters,
        "scope": normalized_scope,
    }


def query_investment_signal_timeline(signal_key: str, page: int, page_size: int):
    signal_key = (signal_key or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size
    if not signal_key:
        raise ValueError("缺少 signal_key")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        events_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='investment_signal_events'"
        ).fetchone()[0]
        snapshots_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='investment_signal_daily_snapshots'"
        ).fetchone()[0]
        tracker_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='investment_signal_tracker'"
        ).fetchone()[0]
        if not tracker_exists:
            return {"signal": None, "events": [], "snapshots": [], "page": page, "page_size": page_size, "total": 0, "total_pages": 0}

        signal_row = conn.execute(
            """
            SELECT id, signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                   evidence_count, news_count, stock_news_count, chatroom_count, signal_status, latest_signal_date,
                   evidence_json, source_summary_json, created_at, update_time
            FROM investment_signal_tracker
            WHERE signal_key = ?
            LIMIT 1
            """,
            (signal_key,),
        ).fetchone()
        total = 0
        events: list[dict] = []
        if events_exists:
            total = conn.execute(
                "SELECT COUNT(*) FROM investment_signal_events WHERE signal_key = ?",
                (signal_key,),
            ).fetchone()[0]
            rows = conn.execute(
                """
                SELECT id, signal_key, event_time, event_date, event_type, old_direction, new_direction,
                       old_strength, new_strength, delta_strength, old_confidence, new_confidence,
                       delta_confidence, event_level, driver_type, driver_source, driver_ref_id,
                       driver_title, status_after_event, event_summary, evidence_json,
                       snapshot_before_json, snapshot_after_json, created_at
                FROM investment_signal_events
                WHERE signal_key = ?
                ORDER BY event_time DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (signal_key, page_size, offset),
            ).fetchall()
            events = []
            for r in rows:
                item = dict(r)
                event_logic = get_or_build_cached_logic_view(
                    conn,
                    entity_type="investment_signal_event",
                    entity_key=str(item.get("id") or ""),
                    source_payload={
                        "event_time": item.get("event_time"),
                        "event_type": item.get("event_type"),
                        "driver_type": item.get("driver_type"),
                        "driver_source": item.get("driver_source"),
                        "event_summary": item.get("event_summary"),
                        "new_direction": item.get("new_direction"),
                        "status_after_event": item.get("status_after_event"),
                        "evidence_json": item.get("evidence_json"),
                    },
                    builder=lambda row=item: build_signal_event_logic_view(row),
                )
                item["logic_view"] = event_logic
                item["evidence_items"] = event_logic.get("evidence_chain", [])
                events.append(item)
        snapshots: list[dict] = []
        if snapshots_exists:
            rows = conn.execute(
                """
                SELECT snapshot_at, snapshot_date, signal_key, signal_type, subject_name, ts_code, direction,
                       signal_strength, confidence, evidence_count, news_count, stock_news_count, chatroom_count,
                       signal_status, latest_signal_date, source_summary_json
                FROM investment_signal_daily_snapshots
                WHERE signal_key = ?
                ORDER BY snapshot_at DESC, id DESC
                LIMIT 60
                """,
                (signal_key,),
            ).fetchall()
            snapshots = [dict(r) for r in rows]
        signal = dict(signal_row) if signal_row else None
        signal_logic = (
            get_or_build_cached_logic_view(
                conn,
                entity_type="investment_signal",
                entity_key=str(signal.get("signal_key") or ""),
                source_payload={
                    "signal_key": signal.get("signal_key"),
                    "subject_name": signal.get("subject_name"),
                    "direction": signal.get("direction"),
                    "signal_strength": signal.get("signal_strength"),
                    "confidence": signal.get("confidence"),
                    "signal_status": signal.get("signal_status"),
                    "source_summary_json": signal.get("source_summary_json"),
                    "evidence_json": signal.get("evidence_json"),
                },
                builder=lambda row=signal: build_signal_logic_view(row),
            )
            if signal
            else {"summary": {}, "chains": [], "has_logic": False, "evidence_chain": []}
        )
        if signal is not None:
            signal["logic_view"] = signal_logic
    finally:
        conn.close()

    return {
        "signal": signal,
        "events": events,
        "snapshots": snapshots,
        "logic_view": signal_logic,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
    }


def _top_market_expectations_for_theme(conn, theme_name: str, limit: int = 8):
    if not theme_name:
        return []
    exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='market_expectation_items'"
    ).fetchone()[0]
    if not exists:
        return []
    rows = conn.execute(
        """
        SELECT question, volume, liquidity, end_date, source_url, related_theme_names_json, outcome_prices_json
        FROM market_expectation_items
        WHERE related_theme_names_json LIKE ?
        ORDER BY COALESCE(volume, 0) DESC, COALESCE(liquidity, 0) DESC, id DESC
        LIMIT ?
        """,
        (f'%"{theme_name}"%', limit),
    ).fetchall()
    return [dict(r) for r in rows]


def query_theme_hotspots(keyword: str, theme_group: str, direction: str, heat_level: str, state_filter: str, page: int, page_size: int):
    keyword = (keyword or "").strip()
    theme_group = (theme_group or "").strip()
    direction = (direction or "").strip()
    heat_level = (heat_level or "").strip()
    state_filter = (state_filter or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        tracker_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='theme_hotspot_tracker'"
        ).fetchone()[0]
        if not tracker_exists:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": [], "summary": {}, "filters": {}}
        where = []
        params: list[object] = []
        if keyword:
            kw = f"%{keyword}%"
            where.append("(t.theme_name LIKE ? OR COALESCE(t.theme_group,'') LIKE ? OR COALESCE(t.top_terms_json,'') LIKE ?)")
            params.extend([kw, kw, kw])
        if theme_group:
            where.append("t.theme_group = ?")
            params.append(theme_group)
        if direction:
            where.append("t.direction = ?")
            params.append(direction)
        if heat_level:
            where.append("t.heat_level = ?")
            params.append(heat_level)
        if state_filter:
            where.append("COALESCE(s.current_state, '') = ?")
            params.append(state_filter)
        where_sql = " WHERE " + " AND ".join(where) if where else ""
        total = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM theme_hotspot_tracker t
            LEFT JOIN signal_state_tracker s
              ON s.signal_scope = 'theme' AND s.signal_key = ('theme:' || t.theme_name)
            {where_sql}
            """,
            params,
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT
              t.theme_name, t.theme_group, t.direction, t.theme_strength, t.confidence, t.evidence_count,
              t.intl_news_count, t.domestic_news_count, t.stock_news_count, t.chatroom_count,
              t.stock_link_count, t.latest_evidence_time, t.heat_level, t.top_terms_json, t.top_stocks_json,
              t.source_summary_json, t.evidence_json,
              s.current_state, s.prev_state, s.driver_type, s.driver_title
            FROM theme_hotspot_tracker t
            LEFT JOIN signal_state_tracker s
              ON s.signal_scope = 'theme' AND s.signal_key = ('theme:' || t.theme_name)
            {where_sql}
            ORDER BY t.theme_strength DESC, t.confidence DESC, t.theme_name
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
        items = []
        for r in rows:
            item = dict(r)
            item["market_expectations"] = _top_market_expectations_for_theme(conn, str(item.get("theme_name") or ""), limit=5)
            items.append(item)
        summary = {
            "theme_total": conn.execute("SELECT COUNT(*) FROM theme_hotspot_tracker").fetchone()[0],
            "bullish_total": conn.execute("SELECT COUNT(*) FROM theme_hotspot_tracker WHERE direction='看多'").fetchone()[0],
            "bearish_total": conn.execute("SELECT COUNT(*) FROM theme_hotspot_tracker WHERE direction='看空'").fetchone()[0],
            "high_heat_total": conn.execute("SELECT COUNT(*) FROM theme_hotspot_tracker WHERE heat_level IN ('极高','高')").fetchone()[0],
        }
        filters = {
            "theme_groups": [r[0] for r in conn.execute("SELECT DISTINCT theme_group FROM theme_hotspot_tracker WHERE COALESCE(theme_group,'')<>'' ORDER BY theme_group").fetchall()],
            "directions": [r[0] for r in conn.execute("SELECT DISTINCT direction FROM theme_hotspot_tracker WHERE COALESCE(direction,'')<>'' ORDER BY direction").fetchall()],
            "heat_levels": [r[0] for r in conn.execute("SELECT DISTINCT heat_level FROM theme_hotspot_tracker WHERE COALESCE(heat_level,'')<>'' ORDER BY heat_level").fetchall()],
            "states": [r[0] for r in conn.execute("SELECT DISTINCT current_state FROM signal_state_tracker WHERE signal_scope='theme' AND COALESCE(current_state,'')<>'' ORDER BY current_state").fetchall()],
        }
    finally:
        conn.close()
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": items,
        "summary": summary,
        "filters": filters,
    }


def query_signal_state_timeline(signal_scope: str, signal_key: str, page: int, page_size: int):
    signal_scope = (signal_scope or "").strip()
    signal_key = (signal_key or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size
    if not signal_key:
        raise ValueError("缺少 signal_key")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        state_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='signal_state_tracker'"
        ).fetchone()[0]
        event_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='signal_state_events'"
        ).fetchone()[0]
        if not state_exists:
            return {"signal": None, "events": [], "page": page, "page_size": page_size, "total": 0, "total_pages": 0}
        where_extra = ""
        params: list[object] = [signal_key]
        if signal_scope:
            where_extra = " AND signal_scope = ?"
            params.append(signal_scope)
        signal = conn.execute(
            f"""
            SELECT *
            FROM signal_state_tracker
            WHERE signal_key = ?{where_extra}
            ORDER BY update_time DESC, id DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
        total = 0
        events = []
        if event_exists:
            total = conn.execute(
                f"SELECT COUNT(*) FROM signal_state_events WHERE signal_key = ?{where_extra}",
                params,
            ).fetchone()[0]
            rows = conn.execute(
                f"""
                SELECT *
                FROM signal_state_events
                WHERE signal_key = ?{where_extra}
                ORDER BY event_time DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                [*params, page_size, offset],
            ).fetchall()
            events = [dict(r) for r in rows]
        expectations = []
        if signal and str(signal["signal_scope"] or "") == "theme":
            expectations = _top_market_expectations_for_theme(conn, str(signal["subject_name"] or ""), limit=8)
        return {
            "signal": dict(signal) if signal else None,
            "events": events,
            "market_expectations": expectations,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size if total else 0,
        }
    finally:
        conn.close()


def _market_expectations_for_report(conn, report_type: str, subject_key: str, context_json_text: str, limit: int = 8):
    exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='market_expectation_items'"
    ).fetchone()[0]
    if not exists:
        return []
    themes: list[str] = []
    if report_type == "theme":
        themes = [subject_key]
    elif report_type == "market":
        ctx = _parse_json_text(context_json_text) or {}
        for row in (ctx.get("theme_rows") or [])[:5]:
            if isinstance(row, (list, tuple)) and row:
                themes.append(str(row[0]))
    elif report_type == "stock":
        ctx = _parse_json_text(context_json_text) or {}
        for row in (ctx.get("themes") or [])[:5]:
            if isinstance(row, (list, tuple)) and row:
                themes.append(str(row[0]))
    if not themes:
        return []
    seen = set()
    out = []
    for theme in themes:
        for item in _top_market_expectations_for_theme(conn, theme, limit=limit):
            q = str(item.get("question") or "")
            if q in seen:
                continue
            seen.add(q)
            out.append(item)
            if len(out) >= limit:
                return out
    return out


def query_research_reports(report_type: str, keyword: str, report_date: str, page: int, page_size: int):
    report_type = (report_type or "").strip()
    keyword = (keyword or "").strip()
    report_date = (report_date or "").strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 100)
    offset = (page - 1) * page_size
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='research_reports'"
        ).fetchone()[0]
        if not exists:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": [], "filters": {}}
        where = []
        params: list[object] = []
        if report_type:
            where.append("report_type = ?")
            params.append(report_type)
        if keyword:
            kw = f"%{keyword}%"
            where.append("(subject_key LIKE ? OR COALESCE(subject_name,'') LIKE ? OR COALESCE(markdown_content,'') LIKE ?)")
            params.extend([kw, kw, kw])
        if report_date:
            where.append("report_date = ?")
            params.append(report_date)
        where_sql = " WHERE " + " AND ".join(where) if where else ""
        total = conn.execute(f"SELECT COUNT(*) FROM research_reports{where_sql}", params).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT id, report_date, report_type, subject_key, subject_name, model, markdown_content, context_json, created_at, update_time
            FROM research_reports
            {where_sql}
            ORDER BY report_date DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
        items = []
        for r in rows:
            item = dict(r)
            item["market_expectations"] = _market_expectations_for_report(conn, str(item.get("report_type") or ""), str(item.get("subject_key") or ""), str(item.get("context_json") or ""), limit=6)
            items.append(item)
        filters = {
            "report_types": [r[0] for r in conn.execute("SELECT DISTINCT report_type FROM research_reports ORDER BY report_type").fetchall()],
            "report_dates": [r[0] for r in conn.execute("SELECT DISTINCT report_date FROM research_reports ORDER BY report_date DESC LIMIT 30").fetchall()],
        }
    finally:
        conn.close()
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
        "items": items,
        "filters": filters,
    }


def query_signal_audit(scope: str = "7d"):
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_name, normalized_scope = resolve_signal_table(conn, scope)
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()[0]
        if not table_exists:
            return {
                "scope": normalized_scope,
                "table_name": table_name,
                "summary": {},
                "stats": {},
                "sections": {},
                "generated_at": generated_at,
            }
        rows = conn.execute(
            f"""
            SELECT signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
                   evidence_count, news_count, stock_news_count, chatroom_count, signal_status,
                   latest_signal_date, evidence_json, source_summary_json
            FROM {table_name}
            ORDER BY signal_strength DESC, confidence DESC, latest_signal_date DESC
            """
        ).fetchall()
        stock_name_map = {
            str(r[0] or "").strip().upper(): str(r[1] or "").strip()
            for r in conn.execute("SELECT ts_code, name FROM stock_codes").fetchall()
        }
    finally:
        conn.close()

    def parse_obj(raw):
        try:
            obj = json.loads(raw or "{}")
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def parse_list(raw):
        try:
            obj = json.loads(raw or "[]")
            return obj if isinstance(obj, list) else []
        except Exception:
            return []

    def top_evidence_title(item: dict) -> str:
        evidences = parse_list(item.get("evidence_json"))
        if evidences:
            ev = evidences[0] if isinstance(evidences[0], dict) else {}
            return str(ev.get("title") or ev.get("theme_name") or ev.get("source") or "").strip()
        return ""

    def dominant_source(item: dict) -> str:
        source_summary = parse_obj(item.get("source_summary_json"))
        counts = {
            "国际新闻": int(source_summary.get("intl_news", 0)),
            "国内新闻": int(source_summary.get("domestic_news", 0)),
            "个股新闻": int(source_summary.get("stock_news", 0)),
            "群聊": int(source_summary.get("chatroom", 0)),
            "主题映射": int(source_summary.get("theme_mapping", 0)),
        }
        best = max(counts.items(), key=lambda kv: kv[1]) if counts else ("无", 0)
        return best[0] if best[1] > 0 else "无"

    all_items = [dict(r) for r in rows]
    stock_items = [r for r in all_items if str(r.get("signal_type") or "") == "stock"]
    active_items = [r for r in all_items if str(r.get("signal_status") or "") == "活跃"]
    code_name_re = re.compile(r"^\d{6}\.(SZ|SH|BJ)$")

    code_named_stocks = []
    theme_only_stocks = []
    low_conf_active = []
    missing_ts_stock = []
    weak_chatroom_stocks = []
    no_direct_source_stocks = []

    for item in stock_items:
        subject_name = str(item.get("subject_name") or "").strip()
        ts_code = str(item.get("ts_code") or "").strip().upper()
        source_summary = parse_obj(item.get("source_summary_json"))
        news_ct = int(item.get("news_count") or 0)
        stock_news_ct = int(item.get("stock_news_count") or 0)
        chatroom_ct = int(item.get("chatroom_count") or 0)
        theme_ct = int(source_summary.get("theme_mapping", 0))
        confidence = float(item.get("confidence") or 0.0)
        strength = float(item.get("signal_strength") or 0.0)
        base = {
            "signal_key": item.get("signal_key"),
            "subject_name": subject_name,
            "display_name": stock_name_map.get(ts_code) or subject_name or ts_code,
            "ts_code": ts_code,
            "direction": item.get("direction"),
            "signal_strength": strength,
            "confidence": confidence,
            "signal_status": item.get("signal_status"),
            "latest_signal_date": item.get("latest_signal_date"),
            "dominant_source": dominant_source(item),
            "top_evidence": top_evidence_title(item),
            "news_count": news_ct,
            "stock_news_count": stock_news_ct,
            "chatroom_count": chatroom_ct,
            "theme_mapping_count": theme_ct,
        }
        if code_name_re.fullmatch(subject_name):
            code_named_stocks.append(base)
        if not ts_code:
            missing_ts_stock.append(base)
        if news_ct == 0 and stock_news_ct == 0 and chatroom_ct == 0 and theme_ct > 0:
            theme_only_stocks.append(base)
        if str(item.get("signal_status") or "") == "活跃" and confidence < 40:
            low_conf_active.append(base)
        if chatroom_ct > 0 and strength < 3.0:
            weak_chatroom_stocks.append(base)
        if news_ct == 0 and stock_news_ct == 0 and chatroom_ct == 0:
            no_direct_source_stocks.append(base)

    def sort_rows(items: list[dict]) -> list[dict]:
        return sorted(
            items,
            key=lambda x: (-float(x.get("signal_strength") or 0.0), -float(x.get("confidence") or 0.0), str(x.get("display_name") or "")),
        )

    summary = {
        "signal_total": len(all_items),
        "stock_total": len(stock_items),
        "active_total": len(active_items),
        "chatroom_stock_total": sum(1 for x in stock_items if int(x.get("chatroom_count") or 0) > 0),
        "theme_only_stock_total": len(theme_only_stocks),
        "code_named_stock_total": len(code_named_stocks),
        "low_conf_active_total": len(low_conf_active),
        "missing_ts_stock_total": len(missing_ts_stock),
    }
    stats = {
        "scope_label": "最近1天" if normalized_scope == "1d" else "近7天累计" if normalized_scope == "7d" else "主表",
        "table_name": table_name,
        "quality_score": max(
            0,
            min(
                100,
                int(
                    100
                    - len(code_named_stocks) * 2
                    - len(missing_ts_stock) * 4
                    - len(low_conf_active) * 3
                    - len(theme_only_stocks)
                ),
            ),
        ),
    }
    sections = {
        "code_named_stocks": sort_rows(code_named_stocks)[:30],
        "missing_ts_stock": sort_rows(missing_ts_stock)[:30],
        "theme_only_stocks": sort_rows(theme_only_stocks)[:30],
        "low_conf_active": sort_rows(low_conf_active)[:30],
        "weak_chatroom_stocks": sort_rows(weak_chatroom_stocks)[:30],
        "no_direct_source_stocks": sort_rows(no_direct_source_stocks)[:30],
    }
    return {
        "scope": normalized_scope,
        "table_name": table_name,
        "summary": summary,
        "stats": stats,
        "sections": sections,
        "generated_at": generated_at,
    }


def query_signal_quality_config():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rules_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='signal_quality_rules'"
        ).fetchone()[0]
        block_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='signal_mapping_blocklist'"
        ).fetchone()[0]
        rules = []
        blocklist = []
        if rules_exists:
            rules = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT rule_key, rule_value, value_type, category, description, enabled, update_time
                    FROM signal_quality_rules
                    ORDER BY category, rule_key
                    """
                ).fetchall()
            ]
        if block_exists:
            blocklist = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT id, term, target_type, match_type, source, reason, enabled, update_time
                    FROM signal_mapping_blocklist
                    ORDER BY enabled DESC, target_type, match_type, term
                    """
                ).fetchall()
            ]
    finally:
        conn.close()
    return {
        "rules": rules,
        "blocklist": blocklist,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def save_signal_quality_rules(items: list[dict]):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_quality_rules (
                rule_key TEXT PRIMARY KEY,
                rule_value TEXT,
                value_type TEXT DEFAULT 'number',
                category TEXT,
                description TEXT,
                enabled INTEGER DEFAULT 1,
                created_at TEXT,
                update_time TEXT
            )
            """
        )
        affected = 0
        for item in items:
            rule_key = str(item.get("rule_key") or "").strip()
            if not rule_key:
                continue
            rule_value = str(item.get("rule_value") if item.get("rule_value") is not None else "").strip()
            value_type = str(item.get("value_type") or "number").strip()
            category = str(item.get("category") or "").strip()
            description = str(item.get("description") or "").strip()
            enabled = 1 if str(item.get("enabled", 1)).strip().lower() not in {"0", "false", "no"} else 0
            updated = conn.execute(
                """
                UPDATE signal_quality_rules
                SET rule_value = ?, value_type = ?, category = ?, description = ?, enabled = ?, update_time = ?
                WHERE rule_key = ?
                """,
                (rule_value, value_type, category, description, enabled, now, rule_key),
            ).rowcount
            if not updated:
                conn.execute(
                    """
                    INSERT INTO signal_quality_rules (
                        rule_key, rule_value, value_type, category, description, enabled, created_at, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (rule_key, rule_value, value_type, category, description, enabled, now, now),
                )
            affected += 1
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "affected": affected, "updated_at": now}


def save_signal_mapping_blocklist(items: list[dict]):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_mapping_blocklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                term TEXT NOT NULL,
                target_type TEXT NOT NULL DEFAULT 'stock',
                match_type TEXT NOT NULL DEFAULT 'exact',
                source TEXT,
                reason TEXT,
                enabled INTEGER DEFAULT 1,
                created_at TEXT,
                update_time TEXT,
                UNIQUE(term, target_type, match_type)
            )
            """
        )
        affected = 0
        for item in items:
            term = str(item.get("term") or "").strip()
            if not term:
                continue
            target_type = str(item.get("target_type") or "stock").strip()
            match_type = str(item.get("match_type") or "exact").strip()
            source = str(item.get("source") or "signal_quality_admin").strip()
            reason = str(item.get("reason") or "").strip()
            enabled = 1 if str(item.get("enabled", 1)).strip().lower() not in {"0", "false", "no"} else 0
            updated = conn.execute(
                """
                UPDATE signal_mapping_blocklist
                SET source = ?, reason = ?, enabled = ?, update_time = ?
                WHERE term = ? AND target_type = ? AND match_type = ?
                """,
                (source, reason, enabled, now, term, target_type, match_type),
            ).rowcount
            if not updated:
                conn.execute(
                    """
                    INSERT INTO signal_mapping_blocklist (
                        term, target_type, match_type, source, reason, enabled, created_at, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (term, target_type, match_type, source, reason, enabled, now, now),
                )
            affected += 1
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "affected": affected, "updated_at": now}


def query_news_daily_summaries(
    summary_date: str,
    source_filter: str,
    model: str,
    page: int,
    page_size: int,
):
    summary_date = summary_date.strip()
    source_filter = source_filter.strip()
    model = model.strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    where_clauses = []
    params: list[object] = []
    if summary_date:
        where_clauses.append("summary_date = ?")
        params.append(summary_date)
    if source_filter:
        where_clauses.append("source_filter LIKE ?")
        params.append(f"%{source_filter}%")
    if model:
        where_clauses.append("model = ?")
        params.append(model)

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='news_daily_summaries'"
        ).fetchone()[0]
        if not table_exists:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        count_sql = f"SELECT COUNT(*) FROM news_daily_summaries{where_sql}"
        data_sql = f"""
        SELECT id, summary_date, filter_importance, source_filter, news_count, model, prompt_version, summary_markdown, created_at
        FROM news_daily_summaries
        {where_sql}
        ORDER BY summary_date DESC, id DESC
        LIMIT ? OFFSET ?
        """
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        data = []
        for r in rows:
            item = dict(r)
            item["logic_view"] = get_or_build_cached_logic_view(
                conn,
                entity_type="news_daily_summary",
                entity_key=str(item.get("id") or ""),
                source_payload=item.get("summary_markdown", ""),
                builder=lambda text=item.get("summary_markdown", ""): extract_logic_view_from_markdown(text),
            )
            data.append(item)
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "items": data,
    }


def get_daily_summary_by_date(summary_date: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='news_daily_summaries'"
        ).fetchone()[0]
        if not table_exists:
            return None
        row = conn.execute(
            """
            SELECT id, summary_date, filter_importance, source_filter, news_count, model, prompt_version, summary_markdown, created_at
            FROM news_daily_summaries
            WHERE summary_date = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (summary_date,),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["logic_view"] = get_or_build_cached_logic_view(
            conn,
            entity_type="news_daily_summary",
            entity_key=str(item.get("id") or ""),
            source_payload=item.get("summary_markdown", ""),
            builder=lambda text=item.get("summary_markdown", ""): extract_logic_view_from_markdown(text),
        )
        return item
    finally:
        conn.close()


def generate_daily_summary(model: str, summary_date: str):
    script_path = Path(__file__).resolve().parent.parent / "llm_summarize_daily_important_news.py"
    cmd = [
        "python3",
        str(script_path),
        "--date",
        summary_date,
        "--model",
        model,
        "--max-news",
        "12",
        "--min-news",
        "6",
        "--max-prompt-chars",
        "9000",
        "--title-max-len",
        "80",
        "--summary-max-len",
        "100",
        "--request-timeout",
        "90",
        "--max-retries",
        "1",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(f"日报总结生成失败: {proc.stderr.strip() or proc.stdout.strip()}")
    return {"stdout": proc.stdout, "stderr": proc.stderr}


def fetch_stock_news_now(ts_code: str, company_name: str, page_size: int, timeout_s: int = 180):
    script_path = Path(__file__).resolve().parent.parent / "fetch_stock_news_eastmoney_to_db.py"
    cmd = [
        "python3",
        str(script_path),
        "--db-path",
        str(DB_PATH),
        "--page-size",
        str(page_size),
    ]
    if ts_code.strip():
        cmd.extend(["--ts-code", ts_code.strip().upper()])
    if company_name.strip():
        cmd.extend(["--name", company_name.strip()])
    publish_app_event(
        event="stock_news_fetch_update",
        payload={
            "ts_code": ts_code.strip().upper(),
            "company_name": company_name.strip(),
            "status": "running",
            "page_size": page_size,
        },
        producer="backend.server",
    )
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    if proc.returncode != 0:
        publish_app_event(
            event="stock_news_fetch_update",
            payload={
                "ts_code": ts_code.strip().upper(),
                "company_name": company_name.strip(),
                "status": "error",
                "error": proc.stderr.strip() or proc.stdout.strip() or "采集失败",
            },
            producer="backend.server",
        )
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "采集失败")
    publish_app_event(
        event="stock_news_fetch_update",
        payload={
            "ts_code": ts_code.strip().upper(),
            "company_name": company_name.strip(),
            "status": "done",
        },
        producer="backend.server",
    )
    return {"stdout": proc.stdout, "stderr": proc.stderr}


def score_stock_news_now(ts_code: str, limit: int, model: str, timeout_s: int = 300):
    script_path = Path(__file__).resolve().parent.parent / "llm_score_stock_news.py"
    cmd = [
        "python3",
        str(script_path),
        "--db-path",
        str(DB_PATH),
        "--ts-code",
        ts_code.strip().upper(),
        "--limit",
        str(limit),
        "--retry",
        "1",
        "--sleep",
        "0.05",
        "--model",
        normalize_model_name(model),
    ]
    publish_app_event(
        event="stock_news_score_update",
        payload={
            "ts_code": ts_code.strip().upper(),
            "status": "running",
            "limit": int(limit),
            "model": normalize_model_name(model),
        },
        producer="backend.server",
    )
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    if proc.returncode != 0:
        publish_app_event(
            event="stock_news_score_update",
            payload={
                "ts_code": ts_code.strip().upper(),
                "status": "error",
                "model": normalize_model_name(model),
                "error": proc.stderr.strip() or proc.stdout.strip() or "评分失败",
            },
            producer="backend.server",
        )
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "评分失败")
    publish_app_event(
        event="stock_news_score_update",
        payload={
            "ts_code": ts_code.strip().upper(),
            "status": "done",
            "model": normalize_model_name(model),
        },
        producer="backend.server",
    )
    return {"stdout": proc.stdout, "stderr": proc.stderr}


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
    ts_code = (ts_code or "").strip().upper()
    keyword = (keyword or "").strip()
    lookback = min(max(int(lookback or 60), 20), 240)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        profile = None
        if ts_code:
            profile = conn.execute(
                """
                SELECT ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status
                FROM stock_codes
                WHERE ts_code = ?
                LIMIT 1
                """,
                (ts_code,),
            ).fetchone()
        if not profile and keyword:
            kw = f"%{keyword}%"
            profile = conn.execute(
                """
                SELECT ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status
                FROM stock_codes
                WHERE ts_code LIKE ? OR symbol LIKE ? OR name LIKE ?
                ORDER BY CASE WHEN list_status = 'L' THEN 0 ELSE 1 END, ts_code
                LIMIT 1
                """,
                (kw, kw, kw),
            ).fetchone()
        if not profile:
            raise ValueError(f"未找到股票: {ts_code or keyword}")

        profile_dict = dict(profile)
        resolved_ts_code = str(profile["ts_code"])
        name = str(profile["name"] or "")
        symbol = str(profile["symbol"] or "")

        recent_prices = [
            dict(r)
            for r in conn.execute(
                """
                SELECT trade_date, open, high, low, close, pct_chg, vol, amount
                FROM stock_daily_prices
                WHERE ts_code = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                (resolved_ts_code, lookback),
            ).fetchall()
        ]
        recent_prices.reverse()

        minute_rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT trade_date, minute_time, price, avg_price, volume, total_volume
                FROM stock_minline
                WHERE ts_code = ?
                ORDER BY trade_date DESC, minute_time DESC
                LIMIT 120
                """,
                (resolved_ts_code,),
            ).fetchall()
        ]
        minute_rows.reverse()
        latest_minline = minute_rows[-1] if minute_rows else None

        latest_score = None
        score_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_scores_daily'"
        ).fetchone()[0]
        if score_exists:
            latest_score = conn.execute(
                """
                SELECT *
                FROM stock_scores_daily
                WHERE ts_code = ?
                ORDER BY score_date DESC
                LIMIT 1
                """,
                (resolved_ts_code,),
            ).fetchone()
        score_dict = dict(latest_score) if latest_score else {}
        score_payload = _parse_json_text(score_dict.get("score_payload_json") or "")
        if score_dict:
            score_dict["score_summary"] = score_payload.get("score_summary", {})
            score_dict["raw_metrics"] = score_payload.get("raw_metrics", {})

        financial_summary = _build_financial_summary(conn, resolved_ts_code)
        valuation_summary = _build_valuation_summary(conn, resolved_ts_code)
        capital_flow_summary = _build_capital_flow_summary(conn, resolved_ts_code)
        event_summary = _build_event_summary(conn, resolved_ts_code)
        governance_summary = _build_governance_summary(conn, resolved_ts_code)
        risk_summary = _build_risk_summary(conn, resolved_ts_code)
        stock_news_summary = _build_stock_news_summary(conn, resolved_ts_code)

        candidate_pool_item = conn.execute(
            """
            SELECT candidate_name, candidate_type, bullish_room_count, bearish_room_count, net_score,
                   dominant_bias, mention_count, room_count, latest_analysis_date
            FROM chatroom_stock_candidate_pool
            WHERE candidate_name IN (?, ?, ?)
            ORDER BY
              CASE candidate_name WHEN ? THEN 0 WHEN ? THEN 1 ELSE 2 END,
              ABS(COALESCE(net_score, 0)) DESC
            LIMIT 1
            """,
            (name, resolved_ts_code, symbol, name, resolved_ts_code),
        ).fetchone()

        latest_subquery = """
        SELECT room_id, MAX(update_time) AS max_update_time
        FROM chatroom_investment_analysis
        GROUP BY room_id
        """
        chatroom_mentions = [
            dict(r)
            for r in conn.execute(
                f"""
                SELECT a.room_id, a.talker, a.analysis_date, a.latest_message_date, a.final_bias,
                       a.targets_json, a.room_summary, a.update_time
                FROM chatroom_investment_analysis a
                JOIN ({latest_subquery}) latest
                  ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
                WHERE a.targets_json LIKE ?
                ORDER BY a.latest_message_date DESC, a.update_time DESC
                LIMIT 8
                """,
                (f"%{name}%",),
            ).fetchall()
        ] if conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chatroom_investment_analysis'"
        ).fetchone()[0] else []

        price_summary = {}
        if recent_prices:
            closes = [_safe_float(x.get("close")) for x in recent_prices if _safe_float(x.get("close")) is not None]
            latest_bar = recent_prices[-1]
            first_close = closes[0] if closes else None
            last_close = closes[-1] if closes else None
            price_summary = {
                "latest_trade_date": latest_bar.get("trade_date"),
                "latest_close": _round_or_none(latest_bar.get("close"), 3),
                "latest_pct_chg": _round_or_none(latest_bar.get("pct_chg"), 2),
                "range_return_pct": (
                    round((last_close - first_close) / first_close * 100, 2)
                    if first_close not in (None, 0) and last_close is not None
                    else None
                ),
                "high_lookback": max(closes) if closes else None,
                "low_lookback": min(closes) if closes else None,
            }

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "profile": profile_dict,
            "price_summary": price_summary,
            "recent_prices": recent_prices,
            "recent_minline": minute_rows,
            "latest_minline": latest_minline,
            "score": score_dict,
            "financial_summary": financial_summary,
            "valuation_summary": valuation_summary,
            "capital_flow_summary": capital_flow_summary,
            "event_summary": event_summary,
            "governance_summary": governance_summary,
            "risk_summary": risk_summary,
            "stock_news_summary": stock_news_summary,
            "candidate_pool_item": dict(candidate_pool_item) if candidate_pool_item else None,
            "chatroom_mentions": chatroom_mentions,
        }
    finally:
        conn.close()


def _calc_ma(values: list[float], n: int):
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def _sanitize_json_value(value):
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


def build_trend_features(ts_code: str, lookback: int):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT p.trade_date, p.open, p.high, p.low, p.close, p.pct_chg, p.vol, p.amount, s.name
            FROM stock_daily_prices p
            LEFT JOIN stock_codes s ON s.ts_code = p.ts_code
            WHERE p.ts_code = ?
            ORDER BY p.trade_date DESC
            LIMIT ?
            """,
            (ts_code, lookback),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        raise ValueError(f"未找到 {ts_code} 的日线数据")

    data = [dict(r) for r in rows]
    data.reverse()

    closes = [_safe_float(r["close"]) for r in data if _safe_float(r["close"]) is not None]
    pct_chg = [_safe_float(r["pct_chg"]) for r in data if _safe_float(r["pct_chg"]) is not None]
    vols = [_safe_float(r["vol"]) for r in data if _safe_float(r["vol"]) is not None]
    if len(closes) < 2:
        raise ValueError("数据不足，至少需要2条日线")

    first_close = closes[0]
    last_close = closes[-1]
    total_return = (last_close - first_close) / first_close * 100 if first_close else None

    daily_returns = []
    for i in range(1, len(closes)):
        prev, curr = closes[i - 1], closes[i]
        if prev:
            daily_returns.append((curr - prev) / prev)
    vol_annualized = (
        statistics.pstdev(daily_returns) * math.sqrt(252) * 100
        if len(daily_returns) >= 2
        else None
    )

    latest = data[-1]
    latest_close = _safe_float(latest["close"])
    ma20 = _calc_ma(closes, 20)

    return {
        "name": latest.get("name") or "",
        "samples": len(data),
        "date_range": {"start": data[0]["trade_date"], "end": data[-1]["trade_date"]},
        "latest": {
            "trade_date": latest["trade_date"],
            "close": latest_close,
            "pct_chg": _safe_float(latest["pct_chg"]),
            "vol": _safe_float(latest["vol"]),
        },
        "trend_metrics": {
            "total_return_pct": total_return,
            "ma5": _calc_ma(closes, 5),
            "ma10": _calc_ma(closes, 10),
            "ma20": ma20,
            "ma60": _calc_ma(closes, 60),
            "distance_to_ma20_pct": ((latest_close - ma20) / ma20 * 100) if (latest_close and ma20) else None,
            "annualized_volatility_pct": vol_annualized,
            "avg_daily_pct_chg": (sum(pct_chg) / len(pct_chg)) if pct_chg else None,
            "avg_volume": (sum(vols) / len(vols)) if vols else None,
        },
        "recent_bars": data[-20:],
    }


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


def _percentile_rank(values: list[float], current: float):
    clean = sorted([v for v in values if v is not None])
    if not clean or current is None:
        return None
    count = sum(1 for v in clean if v <= current)
    return round(count / len(clean) * 100, 2)


def _latest_macro_row(conn: sqlite3.Connection, indicator_codes: list[str]):
    for code in indicator_codes:
        row = conn.execute(
            """
            SELECT indicator_code, indicator_name, period, value, unit, source
            FROM macro_series
            WHERE indicator_code = ?
            ORDER BY period DESC
            LIMIT 1
            """,
            (code,),
        ).fetchone()
        if row:
            return dict(row)
    return None


def _build_financial_summary(conn: sqlite3.Connection, ts_code: str):
    rows = conn.execute(
        """
        SELECT report_period, report_type, ann_date, revenue, op_profit, net_profit, net_profit_excl_nr,
               roe, gross_margin, debt_to_assets, operating_cf, free_cf, eps, bps
        FROM stock_financials
        WHERE ts_code = ?
        ORDER BY report_period DESC
        LIMIT 8
        """,
        (ts_code,),
    ).fetchall()
    if not rows:
        return {}
    items = [dict(r) for r in rows]
    latest = items[0]
    yoy_base = None
    latest_suffix = str(latest["report_period"])[-4:]
    for item in items[1:]:
        if str(item["report_period"]).endswith(latest_suffix):
            yoy_base = item
            break

    def _yoy(field: str):
        curr = _safe_float(latest.get(field))
        prev = _safe_float(yoy_base.get(field)) if yoy_base else None
        if curr is None or prev in (None, 0):
            return None
        return round((curr - prev) / abs(prev) * 100, 2)

    latest_clean = {
        "report_period": latest.get("report_period"),
        "report_type": latest.get("report_type"),
        "ann_date": latest.get("ann_date"),
        "revenue": _round_or_none(latest.get("revenue"), 2),
        "op_profit": _round_or_none(latest.get("op_profit"), 2),
        "net_profit": _round_or_none(latest.get("net_profit"), 2),
        "net_profit_excl_nr": _round_or_none(latest.get("net_profit_excl_nr"), 2),
        "roe": _round_or_none(latest.get("roe"), 2),
        "gross_margin": _round_or_none(latest.get("gross_margin"), 2),
        "debt_to_assets": _round_or_none(latest.get("debt_to_assets"), 2),
        "operating_cf": _round_or_none(latest.get("operating_cf"), 2),
        "free_cf": _round_or_none(latest.get("free_cf"), 2),
        "eps": _round_or_none(latest.get("eps"), 4),
        "bps": _round_or_none(latest.get("bps"), 4),
    }
    trend = {
        "revenue_yoy_pct": _yoy("revenue"),
        "net_profit_yoy_pct": _yoy("net_profit"),
        "net_profit_excl_nr_yoy_pct": _yoy("net_profit_excl_nr"),
        "operating_cf_yoy_pct": _yoy("operating_cf"),
        "roe_change": (
            round(_safe_float(latest.get("roe")) - _safe_float(yoy_base.get("roe")), 2)
            if yoy_base and _safe_float(latest.get("roe")) is not None and _safe_float(yoy_base.get("roe")) is not None
            else None
        ),
        "debt_to_assets_change": (
            round(_safe_float(latest.get("debt_to_assets")) - _safe_float(yoy_base.get("debt_to_assets")), 2)
            if yoy_base
            and _safe_float(latest.get("debt_to_assets")) is not None
            and _safe_float(yoy_base.get("debt_to_assets")) is not None
            else None
        ),
    }
    recent_reports = []
    for item in items[:4]:
        recent_reports.append(
            {
                "report_period": item.get("report_period"),
                "revenue": _round_or_none(item.get("revenue"), 2),
                "net_profit": _round_or_none(item.get("net_profit"), 2),
                "roe": _round_or_none(item.get("roe"), 2),
                "operating_cf": _round_or_none(item.get("operating_cf"), 2),
                "eps": _round_or_none(item.get("eps"), 4),
            }
        )
    return {
        "latest_report_period": latest.get("report_period"),
        "latest": latest_clean,
        "trend": trend,
        "recent_4_reports": recent_reports,
    }


def _build_valuation_summary(conn: sqlite3.Connection, ts_code: str):
    rows = conn.execute(
        """
        SELECT trade_date, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_mv, circ_mv
        FROM stock_valuation_daily
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT 250
        """,
        (ts_code,),
    ).fetchall()
    if not rows:
        return {}
    items = [dict(r) for r in rows]
    latest = items[0]
    pe_ttm_values = [_safe_float(r["pe_ttm"]) for r in items if _safe_float(r["pe_ttm"]) is not None]
    pb_values = [_safe_float(r["pb"]) for r in items if _safe_float(r["pb"]) is not None]
    dv_values = [_safe_float(r["dv_ttm"]) for r in items if _safe_float(r["dv_ttm"]) is not None]
    return {
        "trade_date": latest.get("trade_date"),
        "current": {
            "pe": _round_or_none(latest.get("pe"), 4),
            "pe_ttm": _round_or_none(latest.get("pe_ttm"), 4),
            "pb": _round_or_none(latest.get("pb"), 4),
            "ps": _round_or_none(latest.get("ps"), 4),
            "ps_ttm": _round_or_none(latest.get("ps_ttm"), 4),
            "dv_ratio": _round_or_none(latest.get("dv_ratio"), 4),
            "dv_ttm": _round_or_none(latest.get("dv_ttm"), 4),
            "total_mv": _round_or_none(latest.get("total_mv"), 2),
            "circ_mv": _round_or_none(latest.get("circ_mv"), 2),
        },
        "history_percentile": {
            "pe_ttm_pct": _percentile_rank(pe_ttm_values, _safe_float(latest.get("pe_ttm"))),
            "pb_pct": _percentile_rank(pb_values, _safe_float(latest.get("pb"))),
            "dv_ttm_pct": _percentile_rank(dv_values, _safe_float(latest.get("dv_ttm"))),
        },
    }


def _build_capital_flow_summary(conn: sqlite3.Connection, ts_code: str):
    stock_rows = conn.execute(
        """
        SELECT trade_date, net_inflow, main_inflow, super_large_inflow, large_inflow, medium_inflow, small_inflow
        FROM capital_flow_stock
        WHERE ts_code = ?
        ORDER BY trade_date DESC
        LIMIT 5
        """,
        (ts_code,),
    ).fetchall()
    market_rows = conn.execute(
        """
        SELECT trade_date, flow_type, net_inflow
        FROM capital_flow_market
        ORDER BY trade_date DESC
        LIMIT 10
        """
    ).fetchall()
    out = {}
    if stock_rows:
        stock_items = [dict(r) for r in stock_rows]
        latest = stock_items[0]
        out["stock_flow"] = {
            "latest": {
                "trade_date": latest.get("trade_date"),
                "net_inflow": _round_or_none(latest.get("net_inflow"), 2),
                "main_inflow": _round_or_none(latest.get("main_inflow"), 2),
                "super_large_inflow": _round_or_none(latest.get("super_large_inflow"), 2),
                "large_inflow": _round_or_none(latest.get("large_inflow"), 2),
            },
            "recent_5d_sum": {
                "net_inflow": round(sum(_safe_float(x["net_inflow"]) or 0 for x in stock_items), 2),
                "main_inflow": round(sum(_safe_float(x["main_inflow"]) or 0 for x in stock_items), 2),
            },
        }
    if market_rows:
        grouped: dict[str, dict] = {}
        for row in market_rows:
            d = dict(row)
            flow_type = d["flow_type"]
            if flow_type not in grouped:
                grouped[flow_type] = {
                    "trade_date": d["trade_date"],
                    "net_inflow": _round_or_none(d["net_inflow"], 2),
                }
        out["market_flow"] = grouped
    return out


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


def _build_macro_context(conn: sqlite3.Connection):
    shibor_1m = _latest_macro_row(conn, ["shibor.1m"])
    shibor_3m = _latest_macro_row(conn, ["shibor.3m"])
    shibor_1y = _latest_macro_row(conn, ["shibor.1y"])
    m1_yoy = _latest_macro_row(conn, ["cn_m.m1_yoy", "cn_m.m1"])
    m0_yoy = _latest_macro_row(conn, ["cn_m.m0_yoy", "cn_m.m0"])
    cpi = _latest_macro_row(conn, ["cn_cpi.nt_yoy", "cn_cpi.nt_val"])
    return {
        "asof": max(
            [x["period"] for x in [shibor_1m, shibor_3m, shibor_1y, m1_yoy, m0_yoy, cpi] if x],
            default="",
        ),
        "liquidity": {
            "shibor_1m": _round_or_none(shibor_1m["value"], 4) if shibor_1m else None,
            "shibor_3m": _round_or_none(shibor_3m["value"], 4) if shibor_3m else None,
            "shibor_1y": _round_or_none(shibor_1y["value"], 4) if shibor_1y else None,
        },
        "money_supply": {
            "m1_value": _round_or_none(m1_yoy["value"], 4) if m1_yoy else None,
            "m1_code": m1_yoy["indicator_code"] if m1_yoy else None,
            "m0_value": _round_or_none(m0_yoy["value"], 4) if m0_yoy else None,
            "m0_code": m0_yoy["indicator_code"] if m0_yoy else None,
        },
        "inflation": {
            "cpi_value": _round_or_none(cpi["value"], 4) if cpi else None,
            "cpi_code": cpi["indicator_code"] if cpi else None,
        },
    }


def _build_fx_context(conn: sqlite3.Connection):
    pairs = ["USDCNH.FXCM", "USDJPY.FXCM", "EURUSD.FXCM"]
    out = {}
    for pair in pairs:
        rows = conn.execute(
            """
            SELECT trade_date, close, pct_chg
            FROM fx_daily
            WHERE pair_code = ?
            ORDER BY trade_date DESC
            LIMIT 20
            """,
            (pair,),
        ).fetchall()
        if not rows:
            continue
        items = [dict(r) for r in rows]
        latest = items[0]
        oldest = items[-1]
        latest_close = _safe_float(latest["close"])
        oldest_close = _safe_float(oldest["close"])
        out[pair] = {
            "trade_date": latest.get("trade_date"),
            "close": _round_or_none(latest_close, 6),
            "pct_chg": _round_or_none(latest.get("pct_chg"), 4),
            "return_20d_pct": (
                round((latest_close - oldest_close) / oldest_close * 100, 4)
                if latest_close is not None and oldest_close not in (None, 0)
                else None
            ),
        }
    return out


def _build_rate_spread_context(conn: sqlite3.Connection):
    latest_curve_date_row = conn.execute("SELECT MAX(trade_date) FROM rate_curve_points").fetchone()
    latest_spread_date_row = conn.execute("SELECT MAX(trade_date) FROM spread_daily").fetchone()
    latest_curve_date = latest_curve_date_row[0] if latest_curve_date_row else None
    latest_spread_date = latest_spread_date_row[0] if latest_spread_date_row else None
    out = {}
    if latest_curve_date:
        curve_rows = conn.execute(
            """
            SELECT market, curve_code, tenor, value
            FROM rate_curve_points
            WHERE trade_date = ?
            AND ((market='CN' AND curve_code='shibor') OR (market='US' AND curve_code='treasury'))
            """,
            (latest_curve_date,),
        ).fetchall()
        grouped: dict[str, dict] = {}
        for row in curve_rows:
            d = dict(row)
            key = f"{d['market']}_{d['curve_code']}"
            grouped.setdefault(key, {})[d["tenor"]] = _round_or_none(d["value"], 4)
        out["curve_date"] = latest_curve_date
        out["curves"] = grouped
    if latest_spread_date:
        spread_rows = conn.execute(
            """
            SELECT spread_code, value
            FROM spread_daily
            WHERE trade_date = ?
            """,
            (latest_spread_date,),
        ).fetchall()
        out["spread_date"] = latest_spread_date
        out["spreads"] = {r["spread_code"]: _round_or_none(r["value"], 4) for r in spread_rows}
    return out


def _build_governance_summary(conn: sqlite3.Connection, ts_code: str):
    row = conn.execute(
        """
        SELECT asof_date, holder_structure_json, board_structure_json, mgmt_change_json, incentive_plan_json, governance_score
        FROM company_governance
        WHERE ts_code = ?
        ORDER BY asof_date DESC
        LIMIT 1
        """,
        (ts_code,),
    ).fetchone()
    if not row:
        return {}
    d = dict(row)
    holder = _parse_json_text(d.get("holder_structure_json"))
    board = _parse_json_text(d.get("board_structure_json"))
    mgmt = _parse_json_text(d.get("mgmt_change_json"))
    incentive = _parse_json_text(d.get("incentive_plan_json"))
    return {
        "asof_date": d.get("asof_date"),
        "governance_score": _round_or_none(d.get("governance_score"), 2),
        "holder_summary": {
            "top1_ratio": _round_or_none(holder.get("top1_ratio"), 4),
            "top10_ratio_sum": _round_or_none(holder.get("top10_ratio_sum"), 4),
            "holder_num_latest": holder.get("holder_num_latest"),
            "pledge_stat_latest": holder.get("pledge_stat_latest"),
            "top10_holders": (holder.get("top10_holders") or [])[:5],
        },
        "board_summary": {
            "reward_period": board.get("reward_period"),
            "total_reward": _round_or_none(board.get("total_reward"), 2),
            "members": (board.get("members") or [])[:10],
        },
        "mgmt_changes": (mgmt.get("recent_holder_trades") or [])[:5],
        "incentive_plan": incentive,
    }


def _build_risk_summary(conn: sqlite3.Connection, ts_code: str):
    latest_row = conn.execute(
        "SELECT MAX(scenario_date) FROM risk_scenarios WHERE ts_code = ?",
        (ts_code,),
    ).fetchone()
    latest_date = latest_row[0] if latest_row else None
    if not latest_date:
        return {}
    rows = conn.execute(
        """
        SELECT scenario_name, horizon, pnl_impact, max_drawdown, var_95, cvar_95, assumptions_json
        FROM risk_scenarios
        WHERE ts_code = ? AND scenario_date = ?
        ORDER BY scenario_name
        """,
        (ts_code, latest_date),
    ).fetchall()
    items = []
    for row in rows:
        d = dict(row)
        items.append(
            {
                "scenario_name": d["scenario_name"],
                "horizon": d["horizon"],
                "pnl_impact": _round_or_none(d["pnl_impact"], 4),
                "max_drawdown": _round_or_none(d["max_drawdown"], 4),
                "var_95": _round_or_none(d["var_95"], 4),
                "cvar_95": _round_or_none(d["cvar_95"], 4),
                "assumptions": _parse_json_text(d["assumptions_json"]),
            }
        )
    return {"scenario_date": latest_date, "items": items}


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


def _ensure_stock_news_fresh(ts_code: str, company_name: str, page_size: int = 20, score_model: str = DEFAULT_LLM_MODEL):
    conn = sqlite3.connect(DB_PATH)
    try:
        fresh, latest_pub = _stock_news_is_fresh(conn, ts_code)
    finally:
        conn.close()
    if fresh:
        return {"fetched": False, "scored": False, "latest_pub": latest_pub}
    fetch_info = fetch_stock_news_now(ts_code=ts_code, company_name=company_name, page_size=page_size)
    score_info = score_stock_news_now(ts_code=ts_code, limit=min(page_size, 10), model=score_model)
    conn = sqlite3.connect(DB_PATH)
    try:
        latest_pub = _stock_news_latest_pub(conn, ts_code)
    finally:
        conn.close()
    return {
        "fetched": True,
        "scored": True,
        "latest_pub": latest_pub,
        "fetch_stdout": fetch_info.get("stdout", ""),
        "score_stdout": score_info.get("stdout", ""),
    }


def _build_stock_news_summary(conn: sqlite3.Connection, ts_code: str):
    rows = conn.execute(
        """
        SELECT pub_time, title, summary, link, llm_system_score, llm_finance_impact_score,
               llm_finance_importance, llm_summary, llm_impacts_json
        FROM stock_news_items
        WHERE ts_code = ?
        ORDER BY pub_time DESC, id DESC
        LIMIT 8
        """,
        (ts_code,),
    ).fetchall()
    if not rows:
        return {}
    items = []
    high_count = 0
    for row in rows:
        d = dict(row)
        imp = d.get("llm_finance_importance") or ""
        if imp in {"极高", "高", "中"}:
            high_count += 1
        items.append(
            {
                "pub_time": d.get("pub_time"),
                "title": d.get("title"),
                "summary": d.get("summary"),
                "llm_summary": d.get("llm_summary"),
                "finance_importance": imp,
                "system_score": d.get("llm_system_score"),
                "finance_impact_score": d.get("llm_finance_impact_score"),
                "impacts": _parse_json_text(d.get("llm_impacts_json") or ""),
                "link": d.get("link"),
            }
        )
    return {
        "latest_pub_time": items[0].get("pub_time"),
        "high_importance_count_recent_8": high_count,
        "recent_items": items,
    }


def resolve_llm_endpoint(model: str):
    return resolve_provider(model)


def _post_chat_completions(url: str, payload: dict, api_key: str, timeout_s: int = 120) -> str:
    route = build_route(
        model=str(payload.get("model") or DEFAULT_LLM_MODEL),
        base_url=url.rsplit("/chat/completions", 1)[0],
        api_key=api_key,
        temperature=float(payload.get("temperature") or 0.2),
    )
    return post_chat_completions(route=route, payload=payload, timeout_s=timeout_s)


def call_llm_trend(ts_code: str, features: dict, model: str, temperature: float = 0.2):
    features = _sanitize_json_value(features)
    model = normalize_model_name(model)
    temperature = normalize_temperature_for_model(model, temperature)
    base_url, api_key = resolve_llm_endpoint(model)
    url = base_url.rstrip("/") + "/chat/completions"
    system_prompt = (
        "你是专业的A股量化研究助手。请基于给定特征做趋势分析，"
        "输出客观、结构化结论，并明确不确定性。"
    )
    user_prompt = (
        f"请分析股票 {ts_code} 的走势。\n"
        "请按以下结构输出：\n"
        "1) 趋势判断（上涨/震荡/下跌）\n"
        "2) 置信度（0-100）\n"
        "3) 依据（3-5条）\n"
        "4) 风险点（2-4条）\n"
        "5) 未来5-20个交易日观察要点\n"
        "6) 免责声明（非投资建议）\n\n"
        f"输入特征JSON：\n{json.dumps(features, ensure_ascii=False, allow_nan=False)}"
    )
    payload = {
        "model": model or DEFAULT_LLM_MODEL,
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    try:
        text = _post_chat_completions(url, payload, api_key, timeout_s=120)
    except Exception as e:
        raise RuntimeError(f"LLM接口错误: {e}") from e
    obj = json.loads(text)
    return obj["choices"][0]["message"]["content"]


def _resolve_roles(raw: str) -> list[str]:
    roles = [x.strip() for x in (raw or "").split(",") if x.strip()]
    return roles or list(DEFAULT_MULTI_ROLES)


def build_multi_role_context(ts_code: str, lookback: int):
    ts_code = ts_code.strip().upper()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        profile = conn.execute(
            """
            SELECT ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status
            FROM stock_codes
            WHERE ts_code = ?
            """,
            (ts_code,),
        ).fetchone()
        company_name = profile["name"] if profile else ""
    finally:
        conn.close()

    stock_news_freshness = {"fetched": False, "scored": False, "latest_pub": ""}
    if profile:
        stock_news_freshness = _ensure_stock_news_fresh(
            ts_code=ts_code,
            company_name=company_name,
            page_size=20,
            score_model=DEFAULT_LLM_MODEL,
        )

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        stock_news_summary = _build_stock_news_summary(conn, ts_code)
        financial_summary = _build_financial_summary(conn, ts_code)
        valuation_summary = _build_valuation_summary(conn, ts_code)
        capital_flow_summary = _build_capital_flow_summary(conn, ts_code)
        event_summary = _build_event_summary(conn, ts_code)
        macro_context = _build_macro_context(conn)
        fx_context = _build_fx_context(conn)
        rate_spread_context = _build_rate_spread_context(conn)
        governance_summary = _build_governance_summary(conn, ts_code)
        risk_summary = _build_risk_summary(conn, ts_code)
    finally:
        conn.close()

    if not profile:
        raise ValueError(f"未找到股票基础信息: {ts_code}")

    features = build_trend_features(ts_code, lookback)

    return {
        "company_profile": dict(profile),
        "price_summary": {
            "samples": features["samples"],
            "date_range": features["date_range"],
            "latest": features["latest"],
            "metrics": features["trend_metrics"],
            "recent_20_bars": features["recent_bars"],
        },
        "financial_summary": financial_summary,
        "valuation_summary": valuation_summary,
        "capital_flow_summary": capital_flow_summary,
        "event_summary": event_summary,
        "macro_context": macro_context,
        "fx_context": fx_context,
        "rate_spread_context": rate_spread_context,
        "governance_summary": governance_summary,
        "risk_summary": risk_summary,
        "stock_news_summary": stock_news_summary,
        "stock_news_freshness": stock_news_freshness,
    }


def build_multi_role_prompt(context: dict, roles: list[str]) -> str:
    context = _sanitize_json_value(context)
    role_lines = "\n".join([f"- {r}" for r in roles])
    role_specs = []
    for role in roles:
        spec = ROLE_PROFILES.get(role, {})
        role_specs.append(
            {
                "role": role,
                "focus": spec.get("focus", "围绕该角色的标准职责进行分析"),
                "framework": spec.get("framework", "使用该角色常用分析框架"),
                "indicators": spec.get("indicators", []),
                "risk_bias": spec.get("risk_bias", "识别与该角色相关的核心风险"),
            }
        )
    role_specs = _sanitize_json_value(role_specs)
    return (
        "请你以“投研委员会会议纪要”的形式，基于下面数据进行多角色分析。\n\n"
        f"参与角色：\n{role_lines}\n\n"
        "每个角色必须严格按其角色设定发言，不得混淆口径。\n"
        "请综合使用公司画像、价格行为、财务、估值、个股/市场资金流、公司事件、宏观流动性、汇率、利率利差、公司治理、风险情景等信息。\n"
        "如提供了股票相关新闻，请优先识别其中的事件催化、风险暴露、情绪扰动和与公司基本面的冲突或共振。\n"
        "如果某部分数据为空或不足，请明确指出“该维度数据暂缺/不足”，不要假装已看到数据。\n"
        "分析时优先引用数据里的最新日期、最新值、变化方向和分位水平，避免空泛表述。\n"
        "输出要求：\n"
        "1) 每个角色单独一节，给出观点、核心依据、主要风险、关注指标。\n"
        "2) 角色观点可以有分歧，但要明确分歧点。\n"
        "3) 最后给出“综合结论”：短期(5-20交易日)、中期(1-3个月)两个层面的概率判断。\n"
        "4) 给出“行动清单”：继续观察/择时/风控阈值（价格、波动、量能、估值、资金流、汇率或利率信号）。\n"
        "5) 若发现基本面、估值、资金流、宏观或治理之间存在冲突，请单列“关键分歧”。\n"
        "6) 必须附上非投资建议免责声明。\n"
        "7) 输出格式必须严格使用 Markdown 二级标题；每个角色都必须以“## 角色名”单独起一节，标题文字必须与角色名完全一致，不要改写，不要加编号。\n"
        "8) 公共部分必须使用以下固定二级标题：## 综合结论、## 行动清单、## 关键分歧、## 非投资建议免责声明。\n\n"
        "请严格按如下骨架输出，不要遗漏任何标题：\n"
        + "".join([f"## {role}\n" for role in roles])
        + "## 综合结论\n## 行动清单\n## 关键分歧\n## 非投资建议免责声明\n\n"
        f"角色设定(JSON)：\n{json.dumps(role_specs, ensure_ascii=False, allow_nan=False)}\n\n"
        f"输入数据(JSON)：\n{json.dumps(context, ensure_ascii=False, allow_nan=False)}"
    )


def call_llm_multi_role(context: dict, roles: list[str], model: str, temperature: float = 0.2):
    model = normalize_model_name(model)
    temperature = normalize_temperature_for_model(model, temperature)
    base_url, api_key = resolve_llm_endpoint(model)
    url = base_url.rstrip("/") + "/chat/completions"
    prompt = build_multi_role_prompt(context, roles)
    payload = {
        "model": model or DEFAULT_LLM_MODEL,
        "temperature": temperature,
        "messages": [
            {
                "role": "system",
                "content": "你是专业投研团队的总协调人，擅长多角色观点整合，表达清晰、客观、可执行。",
            },
            {"role": "user", "content": prompt},
        ],
    }
    try:
        text = _post_chat_completions(url, payload, api_key, timeout_s=180)
    except Exception as e:
        raise RuntimeError(f"LLM接口错误: {e}") from e
    obj = json.loads(text)
    return obj["choices"][0]["message"]["content"]


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
    text = _normalize_markdown_lines(markdown).strip()
    normalized_roles = [str(role).strip() for role in (roles or []) if str(role).strip()]
    common_markdown = _build_common_sections_markdown(text)
    common_names = ["综合结论", "行动清单", "关键分歧", "非投资建议免责声明"]
    role_sections = []
    if not text:
        return {"role_sections": role_sections, "common_sections_markdown": common_markdown}
    for role in normalized_roles:
        start = _find_section_start(text, role)
        if start == -1:
            fallback = (
                f"## {role}\n\n未从大模型原始输出中稳定切分出该角色的独立段落，以下附上公共结论部分供参考。\n\n{common_markdown}"
                if common_markdown
                else f"## {role}\n\n未从大模型原始输出中稳定切分出该角色的独立段落，以下附上完整分析原文供参考。\n\n{text}"
            )
            role_sections.append(
                {
                    "role": role,
                    "content": fallback,
                    "matched": False,
                    "logic_view": extract_logic_view_from_markdown(fallback),
                }
            )
            continue
        end = len(text)
        for name in [x for x in normalized_roles if x != role] + common_names:
            pos = _find_section_start(text[start + 1 :], name)
            if pos != -1:
                end = min(end, start + 1 + pos)
        content = text[start:end].strip()
        compact_content = re.sub(r"\s+", "", content)
        compact_common = re.sub(r"\s+", "", common_markdown)
        if common_markdown and compact_common and compact_common not in compact_content:
            content = f"{content}\n\n{common_markdown}"
        role_sections.append(
            {
                "role": role,
                "content": content,
                "matched": True,
                "logic_view": extract_logic_view_from_markdown(content),
            }
        )
    return {
        "role_sections": role_sections,
        "common_sections_markdown": common_markdown,
        "logic_view": extract_logic_view_from_markdown(text),
    }


def _cleanup_async_multi_role_jobs():
    cutoff = time.time() - ASYNC_JOB_TTL_SECONDS
    with ASYNC_MULTI_ROLE_LOCK:
        expired = [
            job_id
            for job_id, job in ASYNC_MULTI_ROLE_JOBS.items()
            if float(job.get("updated_at_ts", 0)) < cutoff
        ]
        for job_id in expired:
            ASYNC_MULTI_ROLE_JOBS.pop(job_id, None)


def _serialize_async_job(job: dict):
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
        "model": job.get("model"),
        "roles": job.get("roles"),
        "context": job.get("context"),
        "analysis": job.get("analysis"),
        "logic_view": job.get("logic_view"),
        "role_sections": job.get("role_sections"),
        "common_sections_markdown": job.get("common_sections_markdown"),
        "error": job.get("error"),
    }


def _create_async_multi_role_job(ts_code: str, lookback: int, model: str, roles: list[str], context: dict):
    _cleanup_async_multi_role_jobs()
    now = datetime.now(timezone.utc).isoformat()
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "status": "queued",
        "progress": 5,
        "stage": "queued",
        "message": "任务已创建，等待后台分析",
        "created_at": now,
        "updated_at": now,
        "finished_at": "",
        "updated_at_ts": time.time(),
        "ts_code": ts_code,
        "name": context.get("company_profile", {}).get("name", ""),
        "lookback": lookback,
        "model": model,
        "roles": roles,
        "context": context,
        "analysis": "",
        "logic_view": {"summary": {}, "chains": [], "has_logic": False},
        "role_sections": [],
        "common_sections_markdown": "",
        "error": "",
    }
    with ASYNC_MULTI_ROLE_LOCK:
        ASYNC_MULTI_ROLE_JOBS[job_id] = job
    publish_app_event(
        event="multi_role_job_update",
        payload={
            "job_id": job_id,
            "status": "queued",
            "progress": 5,
            "stage": "queued",
            "ts_code": ts_code,
            "model": model,
        },
        producer="backend.server",
    )
    return job


def _run_async_multi_role_job(job_id: str):
    with ASYNC_MULTI_ROLE_LOCK:
        job = ASYNC_MULTI_ROLE_JOBS.get(job_id)
        if not job:
            return
        job["status"] = "running"
        job["progress"] = 20
        job["stage"] = "llm"
        job["message"] = "大模型分析中，请稍候"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()
        context = job["context"]
        roles = list(job["roles"])
        model = str(job["model"])
        ts_code = str(job.get("ts_code") or "")
    publish_app_event(
        event="multi_role_job_update",
        payload={
            "job_id": job_id,
            "status": "running",
            "progress": 20,
            "stage": "llm",
            "ts_code": ts_code,
            "model": model,
        },
        producer="backend.server",
    )

    try:
        analysis = call_llm_multi_role(context, roles, model=model, temperature=0.2)
        split_payload = split_multi_role_analysis(analysis, roles)
        conn = sqlite3.connect(DB_PATH)
        try:
            logic_view = get_or_build_cached_logic_view(
                conn,
                entity_type="llm_multi_role",
                entity_key=f"{ts_code}|{job.get('lookback', '')}|{model}|{','.join(roles)}",
                source_payload=analysis,
                builder=lambda text=analysis: split_payload.get("logic_view", extract_logic_view_from_markdown(text)),
            )
        finally:
            conn.close()
        now = datetime.now(timezone.utc).isoformat()
        with ASYNC_MULTI_ROLE_LOCK:
            job = ASYNC_MULTI_ROLE_JOBS.get(job_id)
            if not job:
                return
            job["status"] = "done"
            job["progress"] = 100
            job["stage"] = "done"
            job["message"] = "分析完成"
            job["analysis"] = analysis
            job["logic_view"] = logic_view
            job["role_sections"] = split_payload.get("role_sections", [])
            job["common_sections_markdown"] = split_payload.get("common_sections_markdown", "")
            job["finished_at"] = now
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="multi_role_job_update",
            payload={
                "job_id": job_id,
                "status": "done",
                "progress": 100,
                "stage": "done",
                "ts_code": ts_code,
                "model": model,
            },
            producer="backend.server",
        )
    except Exception as e:
        now = datetime.now(timezone.utc).isoformat()
        with ASYNC_MULTI_ROLE_LOCK:
            job = ASYNC_MULTI_ROLE_JOBS.get(job_id)
            if not job:
                return
            job["status"] = "error"
            job["progress"] = 100
            job["stage"] = "error"
            job["message"] = "分析失败"
            job["error"] = str(e)
            job["finished_at"] = now
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="multi_role_job_update",
            payload={
                "job_id": job_id,
                "status": "error",
                "progress": 100,
                "stage": "error",
                "ts_code": ts_code,
                "model": model,
                "error": str(e),
            },
            producer="backend.server",
        )


def start_async_multi_role_job(ts_code: str, lookback: int, model: str, roles: list[str]):
    context = build_multi_role_context(ts_code, lookback)
    job = _create_async_multi_role_job(ts_code, lookback, model, roles, context)
    worker = threading.Thread(
        target=_run_async_multi_role_job,
        args=(job["job_id"],),
        daemon=True,
        name=f"multi_role_{job['job_id'][:8]}",
    )
    worker.start()
    return _serialize_async_job(job)


def get_async_multi_role_job(job_id: str):
    _cleanup_async_multi_role_jobs()
    with ASYNC_MULTI_ROLE_LOCK:
        job = ASYNC_MULTI_ROLE_JOBS.get(job_id)
        if not job:
            return None
        return _serialize_async_job(job)


def _cleanup_async_daily_summary_jobs():
    cutoff = time.time() - ASYNC_JOB_TTL_SECONDS
    with ASYNC_DAILY_SUMMARY_LOCK:
        expired = [
            job_id
            for job_id, job in ASYNC_DAILY_SUMMARY_JOBS.items()
            if float(job.get("updated_at_ts", 0)) < cutoff
        ]
        for job_id in expired:
            ASYNC_DAILY_SUMMARY_JOBS.pop(job_id, None)


def _serialize_async_daily_summary_job(job: dict):
    return {
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "progress": job.get("progress"),
        "stage": job.get("stage"),
        "message": job.get("message"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "finished_at": job.get("finished_at"),
        "summary_date": job.get("summary_date"),
        "model": job.get("model"),
        "item": job.get("item"),
        "run_stdout": job.get("run_stdout"),
        "error": job.get("error"),
    }


def _create_async_daily_summary_job(model: str, summary_date: str):
    _cleanup_async_daily_summary_jobs()
    now = datetime.now(timezone.utc).isoformat()
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "status": "queued",
        "progress": 5,
        "stage": "queued",
        "message": "任务已创建，等待后台生成日报总结",
        "created_at": now,
        "updated_at": now,
        "finished_at": "",
        "updated_at_ts": time.time(),
        "summary_date": summary_date,
        "model": model,
        "item": None,
        "run_stdout": "",
        "error": "",
    }
    with ASYNC_DAILY_SUMMARY_LOCK:
        ASYNC_DAILY_SUMMARY_JOBS[job_id] = job
    publish_app_event(
        event="daily_summary_job_update",
        payload={
            "job_id": job_id,
            "status": "queued",
            "progress": 5,
            "stage": "queued",
            "summary_date": summary_date,
            "model": model,
        },
        producer="backend.server",
    )
    return job


def _run_async_daily_summary_job(job_id: str):
    with ASYNC_DAILY_SUMMARY_LOCK:
        job = ASYNC_DAILY_SUMMARY_JOBS.get(job_id)
        if not job:
            return
        job["status"] = "running"
        job["progress"] = 15
        job["stage"] = "llm"
        job["message"] = "正在生成日报总结，请稍候"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()
        model = str(job["model"])
        summary_date = str(job["summary_date"])
    publish_app_event(
        event="daily_summary_job_update",
        payload={
            "job_id": job_id,
            "status": "running",
            "progress": 15,
            "stage": "llm",
            "summary_date": summary_date,
            "model": model,
        },
        producer="backend.server",
    )

    try:
        run_info = generate_daily_summary(model=model, summary_date=summary_date)
        item = get_daily_summary_by_date(summary_date)
        now = datetime.now(timezone.utc).isoformat()
        with ASYNC_DAILY_SUMMARY_LOCK:
            job = ASYNC_DAILY_SUMMARY_JOBS.get(job_id)
            if not job:
                return
            job["status"] = "done"
            job["progress"] = 100
            job["stage"] = "done"
            job["message"] = "日报总结生成完成"
            job["item"] = item
            job["run_stdout"] = run_info.get("stdout", "")
            job["finished_at"] = now
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="daily_summary_job_update",
            payload={
                "job_id": job_id,
                "status": "done",
                "progress": 100,
                "stage": "done",
                "summary_date": summary_date,
                "model": model,
            },
            producer="backend.server",
        )
    except Exception as e:
        now = datetime.now(timezone.utc).isoformat()
        with ASYNC_DAILY_SUMMARY_LOCK:
            job = ASYNC_DAILY_SUMMARY_JOBS.get(job_id)
            if not job:
                return
            job["status"] = "error"
            job["progress"] = 100
            job["stage"] = "error"
            job["message"] = "日报总结生成失败"
            job["error"] = str(e)
            job["finished_at"] = now
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="daily_summary_job_update",
            payload={
                "job_id": job_id,
                "status": "error",
                "progress": 100,
                "stage": "error",
                "summary_date": summary_date,
                "model": model,
                "error": str(e),
            },
            producer="backend.server",
        )


def start_async_daily_summary_job(model: str, summary_date: str):
    job = _create_async_daily_summary_job(model=model, summary_date=summary_date)
    worker = threading.Thread(
        target=_run_async_daily_summary_job,
        args=(job["job_id"],),
        daemon=True,
        name=f"daily_summary_{job['job_id'][:8]}",
    )
    worker.start()
    return _serialize_async_daily_summary_job(job)


def get_async_daily_summary_job(job_id: str):
    _cleanup_async_daily_summary_jobs()
    with ASYNC_DAILY_SUMMARY_LOCK:
        job = ASYNC_DAILY_SUMMARY_JOBS.get(job_id)
        if not job:
            return None
        return _serialize_async_daily_summary_job(job)


class ApiHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = 200):
        clean_payload = _sanitize_json_value(payload)
        body = json.dumps(clean_payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
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

        if parsed.path == "/api/signal-quality/rules/save":
            items = payload.get("items", [])
            if not isinstance(items, list):
                self._send_json({"error": "items 必须是数组"}, status=400)
                return
            try:
                result = save_signal_quality_rules(items)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"规则保存失败: {exc}"}, status=500)
                return
            self._send_json(result)
            return

        if parsed.path == "/api/signal-quality/blocklist/save":
            items = payload.get("items", [])
            if not isinstance(items, list):
                self._send_json({"error": "items 必须是数组"}, status=400)
                return
            try:
                result = save_signal_mapping_blocklist(items)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"黑名单保存失败: {exc}"}, status=500)
                return
            self._send_json(result)
            return

        self._send_json({"error": "Not Found"}, status=404)

    def do_GET(self):
        parsed = urlparse(self.path)
        host = self.headers.get("Host", f"127.0.0.1:{PORT}").split(":")[0]

        if parsed.path in {"/", "/api"}:
            self._send_json(
                {
                    "service": "stock-codes-api",
                    "message": "这是后端 API 服务，不是前端页面。",
                    "frontend_url": f"http://{host}:8080/",
                    "endpoints": {
                        "health": "/api/health",
                        "dashboard": "/api/dashboard",
                        "stock_detail": "/api/stock-detail?ts_code=000001.SZ&lookback=60",
                        "stocks": "/api/stocks?keyword=&status=&market=&area=&page=1&page_size=20",
                        "stocks_filters": "/api/stocks/filters",
                        "stock_scores": "/api/stock-scores?keyword=&market=&area=&industry=&min_score=0&page=1&page_size=20&sort_by=total_score&sort_order=desc",
                        "stock_scores_filters": "/api/stock-scores/filters",
                        "stock_news": "/api/stock-news?ts_code=601100.SH&company_name=&keyword=&page=1&page_size=20",
                        "wechat_chatlog": "/api/wechat-chatlog?talker=&sender_name=&keyword=&is_quote=&query_date_start=&query_date_end=&page=1&page_size=20",
                        "chatroom_overview": "/api/chatrooms?keyword=&primary_category=&activity_level=&risk_level=&skip_realtime_monitor=&fetch_status=&page=1&page_size=20",
                        "chatroom_fetch_now": "/api/chatrooms/fetch?room_id=<room_id>&mode=today|yesterday_and_today",
                        "chatroom_investment_analysis": "/api/chatrooms/investment?keyword=&final_bias=&target_keyword=&page=1&page_size=20",
                        "chatroom_candidate_pool": "/api/chatrooms/candidate-pool?keyword=&dominant_bias=&candidate_type=&page=1&page_size=20",
                        "investment_signals": "/api/investment-signals?keyword=&signal_type=&signal_group=all|stock|non_stock&direction=&signal_status=&page=1&page_size=20",
                        "investment_signal_timeline": "/api/investment-signals/timeline?signal_key=<signal_key>&page=1&page_size=20",
                        "stock_news_fetch": "/api/stock-news/fetch?ts_code=601100.SH&company_name=&page_size=20&score=1&model=GPT-5.4",
                        "news": "/api/news?source=&exclude_sources=cn_sina_7x24&exclude_source_prefixes=cn_sina_&keyword=&date_from=&date_to=&finance_levels=高,极高&page=1&page_size=20",
                        "news_sources": "/api/news/sources",
                        "news_daily_summaries": "/api/news/daily-summaries?summary_date=2026-03-25&source_filter=cn_sina_&model=GPT-5.4&page=1&page_size=20",
                        "news_daily_summaries_generate": "/api/news/daily-summaries/generate?model=GPT-5.4|deepseek-chat",
                        "news_daily_summaries_task": "/api/news/daily-summaries/task?job_id=<job_id>",
                        "prices": (
                            "/api/prices?ts_code=000001.SZ&start_date=20260223"
                            "&end_date=20260325&page=1&page_size=20"
                        ),
                        "minline": "/api/minline?ts_code=600114.SH&trade_date=20260325&page=1&page_size=240",
                        "llm_trend": "/api/llm/trend?ts_code=000001.SZ&lookback=120&model=GPT-5.4|deepseek-chat",
                        "llm_multi_role": (
                            "/api/llm/multi-role?ts_code=000001.SZ&lookback=120"
                            "&model=GPT-5.4|deepseek-chat&roles=宏观经济分析师,股票分析师,国际资本分析师,汇率分析师"
                        ),
                        "llm_multi_role_start": (
                            "/api/llm/multi-role/start?ts_code=000001.SZ&lookback=120"
                            "&model=GPT-5.4|deepseek-chat&roles=宏观经济分析师,股票分析师"
                        ),
                        "llm_multi_role_task": "/api/llm/multi-role/task?job_id=<job_id>",
                        "macro_indicators": "/api/macro/indicators",
                        "macro_series": "/api/macro?indicator_code=cn_cpi.nt_yoy&freq=M&period_start=202001&period_end=202512&page=1&page_size=200",
                        "source_monitor": "/api/source-monitor",
                        "database_audit": "/api/database-audit?refresh=0|1",
                        "signal_audit": "/api/signal-audit?scope=7d|1d",
                        "signal_quality_config": "/api/signal-quality/config",
                        "signal_quality_rules_save": "/api/signal-quality/rules/save",
                        "signal_quality_blocklist_save": "/api/signal-quality/blocklist/save",
                    },
                }
            )
            return

        if parsed.path == "/api/health":
            self._send_json({"ok": True, "db": db_label()})
            return

        if parsed.path == "/api/jobs":
            try:
                payload = query_job_definitions()
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"任务定义查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/job-runs":
            params = parse_qs(parsed.query)
            job_key = params.get("job_key", [""])[0]
            status = params.get("status", [""])[0]
            try:
                limit = int(params.get("limit", ["50"])[0])
            except ValueError:
                self._send_json({"error": "limit 必须是整数"}, status=400)
                return
            try:
                payload = query_job_runs(job_key=job_key, status=status, limit=limit)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"任务运行记录查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/jobs/trigger":
            params = parse_qs(parsed.query)
            job_key = params.get("job_key", [""])[0].strip()
            if not job_key:
                self._send_json({"error": "缺少 job_key"}, status=400)
                return
            try:
                payload = run_job(job_key, trigger_mode="api")
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"任务触发失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/dashboard":
            try:
                payload = query_dashboard()
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"工作台查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/source-monitor":
            try:
                payload = query_source_monitor()
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"监控查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/database-audit":
            params = parse_qs(parsed.query)
            refresh_raw = params.get("refresh", ["0"])[0].strip().lower()
            refresh = refresh_raw in {"1", "true", "yes", "y", "on"}
            try:
                payload = query_database_audit(refresh=refresh)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"审核报告查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/db-health":
            try:
                payload = query_database_health()
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"数据库健康查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/signal-audit":
            params = parse_qs(parsed.query)
            scope = params.get("scope", ["7d"])[0]
            try:
                payload = query_signal_audit(scope=scope)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"信号审计查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/signal-quality/config":
            try:
                payload = query_signal_quality_config()
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"信号质量配置查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/stock-detail":
            params = parse_qs(parsed.query)
            ts_code = params.get("ts_code", [""])[0].strip().upper()
            keyword = params.get("keyword", [""])[0].strip()
            try:
                lookback = int(params.get("lookback", ["60"])[0])
            except ValueError:
                self._send_json({"error": "lookback 必须是整数"}, status=400)
                return
            if not ts_code and not keyword:
                self._send_json({"error": "缺少 ts_code 或 keyword"}, status=400)
                return
            try:
                payload = query_stock_detail(ts_code=ts_code, keyword=keyword, lookback=lookback)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"详情查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/stocks":
            params = parse_qs(parsed.query)
            keyword = params.get("keyword", [""])[0]
            status = params.get("status", [""])[0]
            market = params.get("market", [""])[0]
            area = params.get("area", [""])[0]

            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return

            try:
                payload = query_stocks(keyword, status, market, area, page, page_size)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return

            self._send_json(payload)
            return

        if parsed.path == "/api/stocks/filters":
            try:
                payload = query_stock_filters()
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/stock-scores/filters":
            try:
                payload = query_stock_score_filters()
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/stock-scores":
            params = parse_qs(parsed.query)
            keyword = params.get("keyword", [""])[0]
            market = params.get("market", [""])[0]
            area = params.get("area", [""])[0]
            industry = params.get("industry", [""])[0]
            sort_by = params.get("sort_by", ["total_score"])[0]
            sort_order = params.get("sort_order", ["desc"])[0]
            try:
                min_score = float(params.get("min_score", ["0"])[0] or 0)
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "min_score/page/page_size 参数格式错误"}, status=400)
                return
            try:
                payload = query_stock_scores(
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
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/stock-news":
            params = parse_qs(parsed.query)
            ts_code = params.get("ts_code", [""])[0]
            company_name = params.get("company_name", [""])[0]
            keyword = params.get("keyword", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_stock_news(ts_code, company_name, keyword, page, page_size)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/wechat-chatlog":
            params = parse_qs(parsed.query)
            talker = params.get("talker", [""])[0]
            sender_name = params.get("sender_name", [""])[0]
            keyword = params.get("keyword", [""])[0]
            is_quote = params.get("is_quote", [""])[0]
            query_date_start = params.get("query_date_start", [""])[0]
            query_date_end = params.get("query_date_end", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_wechat_chatlog(
                    talker=talker,
                    sender_name=sender_name,
                    keyword=keyword,
                    is_quote=is_quote,
                    query_date_start=query_date_start,
                    query_date_end=query_date_end,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/chatrooms":
            params = parse_qs(parsed.query)
            keyword = params.get("keyword", [""])[0]
            primary_category = params.get("primary_category", [""])[0]
            activity_level = params.get("activity_level", [""])[0]
            risk_level = params.get("risk_level", [""])[0]
            skip_realtime_monitor = params.get("skip_realtime_monitor", [""])[0]
            fetch_status = params.get("fetch_status", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_chatroom_overview(
                    keyword=keyword,
                    primary_category=primary_category,
                    activity_level=activity_level,
                    risk_level=risk_level,
                    skip_realtime_monitor=skip_realtime_monitor,
                    fetch_status=fetch_status,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/chatrooms/fetch":
            params = parse_qs(parsed.query)
            room_id = params.get("room_id", [""])[0]
            mode = params.get("mode", ["today"])[0].strip()
            try:
                payload = fetch_single_chatroom_now(
                    room_id=room_id,
                    fetch_yesterday_and_today=(mode == "yesterday_and_today"),
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"立即拉取失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/chatrooms/investment":
            params = parse_qs(parsed.query)
            keyword = params.get("keyword", [""])[0]
            final_bias = params.get("final_bias", [""])[0]
            target_keyword = params.get("target_keyword", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_chatroom_investment_analysis(
                    keyword=keyword,
                    final_bias=final_bias,
                    target_keyword=target_keyword,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/chatrooms/candidate-pool":
            params = parse_qs(parsed.query)
            keyword = params.get("keyword", [""])[0]
            dominant_bias = params.get("dominant_bias", [""])[0]
            candidate_type = params.get("candidate_type", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_chatroom_candidate_pool(
                    keyword=keyword,
                    dominant_bias=dominant_bias,
                    candidate_type=candidate_type,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/investment-signals":
            params = parse_qs(parsed.query)
            keyword = params.get("keyword", [""])[0]
            signal_type = params.get("signal_type", [""])[0]
            signal_group = params.get("signal_group", [""])[0]
            scope = params.get("scope", ["7d"])[0]
            source_filter = params.get("source_filter", [""])[0]
            direction = params.get("direction", [""])[0]
            signal_status = params.get("signal_status", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_investment_signals(
                    keyword=keyword,
                    signal_type=signal_type,
                    signal_group=signal_group,
                    scope=scope,
                    source_filter=source_filter,
                    direction=direction,
                    signal_status=signal_status,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/theme-hotspots":
            params = parse_qs(parsed.query)
            keyword = params.get("keyword", [""])[0]
            theme_group = params.get("theme_group", [""])[0]
            direction = params.get("direction", [""])[0]
            heat_level = params.get("heat_level", [""])[0]
            state_filter = params.get("state", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_theme_hotspots(
                    keyword=keyword,
                    theme_group=theme_group,
                    direction=direction,
                    heat_level=heat_level,
                    state_filter=state_filter,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/signal-state/timeline":
            params = parse_qs(parsed.query)
            signal_scope = params.get("signal_scope", [""])[0]
            signal_key = params.get("signal_key", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_signal_state_timeline(
                    signal_scope=signal_scope,
                    signal_key=signal_key,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"时间线查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/research-reports":
            params = parse_qs(parsed.query)
            report_type = params.get("report_type", [""])[0]
            keyword = params.get("keyword", [""])[0]
            report_date = params.get("report_date", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_research_reports(
                    report_type=report_type,
                    keyword=keyword,
                    report_date=report_date,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/investment-signals/timeline":
            params = parse_qs(parsed.query)
            signal_key = params.get("signal_key", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_investment_signal_timeline(
                    signal_key=signal_key,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"时间线查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/stock-news/fetch":
            params = parse_qs(parsed.query)
            ts_code = params.get("ts_code", [""])[0].strip().upper()
            company_name = params.get("company_name", [""])[0].strip()
            model = normalize_model_name(params.get("model", [DEFAULT_LLM_MODEL])[0])
            score = params.get("score", ["1"])[0].strip() not in {"0", "false", "False"}
            try:
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page_size 必须是整数"}, status=400)
                return
            if not ts_code and not company_name:
                self._send_json({"error": "缺少 ts_code 或 company_name"}, status=400)
                return
            try:
                fetch_info = fetch_stock_news_now(
                    ts_code=ts_code,
                    company_name=company_name,
                    page_size=page_size,
                )
                score_info = None
                if score and ts_code:
                    score_info = score_stock_news_now(
                        ts_code=ts_code,
                        limit=min(page_size, 10),
                        model=model,
                    )
                payload = query_stock_news(ts_code, company_name, "", 1, min(page_size, 20))
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"采集失败: {exc}"}, status=500)
                return
            self._send_json(
                {
                    "ok": True,
                    "ts_code": ts_code,
                    "company_name": company_name,
                    "model": model,
                    "fetch_stdout": fetch_info.get("stdout", ""),
                    "score_stdout": score_info.get("stdout", "") if score_info else "",
                    "items": payload.get("items", []),
                    "total": payload.get("total", 0),
                }
            )
            return

        if parsed.path == "/api/news/sources":
            try:
                payload = {"items": query_news_sources()}
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/news":
            params = parse_qs(parsed.query)
            source = params.get("source", [""])[0]
            source_prefixes = params.get("source_prefixes", [""])[0]
            keyword = params.get("keyword", [""])[0]
            date_from = params.get("date_from", [""])[0]
            date_to = params.get("date_to", [""])[0]
            finance_levels = params.get("finance_levels", [""])[0]
            exclude_sources = params.get("exclude_sources", [""])[0]
            exclude_source_prefixes = params.get("exclude_source_prefixes", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_news(
                    source=source,
                    source_prefixes=source_prefixes,
                    keyword=keyword,
                    date_from=date_from,
                    date_to=date_to,
                    finance_levels=finance_levels,
                    exclude_sources=exclude_sources,
                    exclude_source_prefixes=exclude_source_prefixes,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/news/daily-summaries":
            params = parse_qs(parsed.query)
            summary_date = params.get("summary_date", [""])[0]
            source_filter = params.get("source_filter", [""])[0]
            model = params.get("model", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_news_daily_summaries(
                    summary_date=summary_date,
                    source_filter=source_filter,
                    model=model,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/news/daily-summaries/generate":
            params = parse_qs(parsed.query)
            model = normalize_model_name(params.get("model", [DEFAULT_LLM_MODEL])[0])
            summary_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            try:
                job = start_async_daily_summary_job(model=model, summary_date=summary_date)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"启动生成失败: {exc}"}, status=500)
                return
            self._send_json(
                {
                    "ok": True,
                    **job,
                }
            )
            return

        if parsed.path == "/api/news/daily-summaries/task":
            params = parse_qs(parsed.query)
            job_id = params.get("job_id", [""])[0].strip()
            if not job_id:
                self._send_json({"error": "缺少 job_id"}, status=400)
                return
            job = get_async_daily_summary_job(job_id)
            if not job:
                self._send_json({"error": f"任务不存在或已过期: {job_id}"}, status=404)
                return
            self._send_json({"ok": True, **job})
            return

        if parsed.path == "/api/prices":
            params = parse_qs(parsed.query)
            ts_code = params.get("ts_code", [""])[0]
            start_date = params.get("start_date", [""])[0]
            end_date = params.get("end_date", [""])[0]

            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["20"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return

            try:
                payload = query_prices(ts_code, start_date, end_date, page, page_size)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return

            self._send_json(payload)
            return

        if parsed.path == "/api/minline":
            params = parse_qs(parsed.query)
            ts_code = params.get("ts_code", [""])[0]
            trade_date = params.get("trade_date", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["240"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_minline(ts_code, trade_date, page, page_size)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/llm/trend":
            params = parse_qs(parsed.query)
            ts_code = params.get("ts_code", [""])[0].strip().upper()
            model = normalize_model_name(params.get("model", [DEFAULT_LLM_MODEL])[0])
            if not ts_code:
                self._send_json({"error": "缺少 ts_code"}, status=400)
                return
            try:
                lookback = int(params.get("lookback", ["120"])[0])
            except ValueError:
                self._send_json({"error": "lookback 必须是整数"}, status=400)
                return
            try:
                features = build_trend_features(ts_code, lookback)
                analysis = call_llm_trend(ts_code, features, model=model, temperature=0.2)
                conn = sqlite3.connect(DB_PATH)
                try:
                    logic_view = get_or_build_cached_logic_view(
                        conn,
                        entity_type="llm_trend",
                        entity_key=f"{ts_code}|{lookback}|{model}",
                        source_payload=analysis,
                        builder=lambda text=analysis: extract_logic_view_from_markdown(text),
                    )
                finally:
                    conn.close()
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"分析失败: {exc}"}, status=500)
                return
            self._send_json(
                {
                    "ts_code": ts_code,
                    "name": features.get("name", ""),
                    "lookback": lookback,
                    "model": model,
                    "features": features,
                    "analysis": analysis,
                    "logic_view": logic_view,
                }
            )
            return

        if parsed.path == "/api/llm/multi-role":
            params = parse_qs(parsed.query)
            ts_code = params.get("ts_code", [""])[0].strip().upper()
            model = normalize_model_name(params.get("model", [DEFAULT_LLM_MODEL])[0])
            roles_raw = params.get("roles", [""])[0]
            if not ts_code:
                self._send_json({"error": "缺少 ts_code"}, status=400)
                return
            try:
                lookback = int(params.get("lookback", ["120"])[0])
            except ValueError:
                self._send_json({"error": "lookback 必须是整数"}, status=400)
                return
            roles = _resolve_roles(roles_raw)
            try:
                context = build_multi_role_context(ts_code, lookback)
                analysis = call_llm_multi_role(context, roles, model=model, temperature=0.2)
                split_payload = split_multi_role_analysis(analysis, roles)
                conn = sqlite3.connect(DB_PATH)
                try:
                    logic_view = get_or_build_cached_logic_view(
                        conn,
                        entity_type="llm_multi_role",
                        entity_key=f"{ts_code}|{lookback}|{model}|{','.join(roles)}",
                        source_payload=analysis,
                        builder=lambda text=analysis: split_payload.get("logic_view", extract_logic_view_from_markdown(text)),
                    )
                finally:
                    conn.close()
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"分析失败: {exc}"}, status=500)
                return
            self._send_json(
                {
                    "ts_code": ts_code,
                    "name": context.get("company_profile", {}).get("name", ""),
                    "lookback": lookback,
                    "model": model,
                    "roles": roles,
                    "context": context,
                    "analysis": analysis,
                    "logic_view": logic_view,
                    "role_sections": split_payload.get("role_sections", []),
                    "common_sections_markdown": split_payload.get("common_sections_markdown", ""),
                }
            )
            return

        if parsed.path == "/api/llm/multi-role/start":
            params = parse_qs(parsed.query)
            ts_code = params.get("ts_code", [""])[0].strip().upper()
            model = normalize_model_name(params.get("model", [DEFAULT_LLM_MODEL])[0])
            roles_raw = params.get("roles", [""])[0]
            if not ts_code:
                self._send_json({"error": "缺少 ts_code"}, status=400)
                return
            try:
                lookback = int(params.get("lookback", ["120"])[0])
            except ValueError:
                self._send_json({"error": "lookback 必须是整数"}, status=400)
                return
            roles = _resolve_roles(roles_raw)
            try:
                job = start_async_multi_role_job(ts_code, lookback, model, roles)
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"启动分析失败: {exc}"}, status=500)
                return
            self._send_json({"ok": True, **job})
            return

        if parsed.path == "/api/llm/multi-role/task":
            params = parse_qs(parsed.query)
            job_id = params.get("job_id", [""])[0].strip()
            if not job_id:
                self._send_json({"error": "缺少 job_id"}, status=400)
                return
            job = get_async_multi_role_job(job_id)
            if not job:
                self._send_json({"error": f"任务不存在或已过期: {job_id}"}, status=404)
                return
            self._send_json({"ok": True, **job})
            return

        if parsed.path == "/api/macro/indicators":
            try:
                payload = {"items": query_macro_indicators(limit=1000)}
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        if parsed.path == "/api/macro":
            params = parse_qs(parsed.query)
            indicator_code = params.get("indicator_code", [""])[0]
            freq = params.get("freq", [""])[0]
            period_start = params.get("period_start", [""])[0]
            period_end = params.get("period_end", [""])[0]
            keyword = params.get("keyword", [""])[0]
            try:
                page = int(params.get("page", ["1"])[0])
                page_size = int(params.get("page_size", ["200"])[0])
            except ValueError:
                self._send_json({"error": "page/page_size 必须是整数"}, status=400)
                return
            try:
                payload = query_macro_series(
                    indicator_code=indicator_code,
                    freq=freq,
                    period_start=period_start,
                    period_end=period_end,
                    keyword=keyword,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:  # pragma: no cover
                self._send_json({"error": f"查询失败: {exc}"}, status=500)
                return
            self._send_json(payload)
            return

        self._send_json({"error": "Not Found"}, status=404)


if __name__ == "__main__":
    assert_database_ready()

    server = ThreadingHTTPServer((HOST, PORT), ApiHandler)
    print(f"Backend API running on http://{HOST}:{PORT}")
    server.serve_forever()
