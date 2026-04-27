#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

import db_compat as sqlite3
from db_compat import cache_get_json, cache_set_json, get_redis_client
from job_orchestrator import dry_run_job, query_job_alerts, query_job_definitions, query_job_runs, run_job
from llm_gateway import normalize_model_name
from realtime_streams import publish_app_event

from services.notifications import build_notification_payload, notify_with_wecom
from services.quantaalpha_service import get_quantaalpha_runtime_health
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
from services.chatrooms_service import (
build_chatrooms_service_deps,
fetch_single_chatroom_now as chatrooms_fetch_single_now,
query_chatroom_candidate_pool as chatrooms_query_candidate_pool,
query_chatroom_investment_analysis as chatrooms_query_investment_analysis,
query_chatroom_overview as chatrooms_query_overview,
query_wechat_chatlog as chatrooms_query_wechat_chatlog,
)
from services.signals_service import query_signal_chain_graph
from services.reporting import (
generate_daily_summary as reporting_generate_daily_summary,
get_daily_summary_by_date as reporting_get_daily_summary_by_date,
query_research_reports as reporting_query_research_reports,
query_news_daily_summaries as reporting_query_news_daily_summaries,
)
from services.decision_service import build_decision_runtime_deps as build_decision_service_runtime_deps
from skills.strategies import load_strategy_template_text

from backend.http_server import config

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

    conn = sqlite3.connect(config.DB_PATH)
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
    cache_set_json(cache_key, payload, config.REDIS_CACHE_TTL_STOCKS)
    return payload


def query_stock_filters():
    conn = sqlite3.connect(config.DB_PATH)
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
    conn = sqlite3.connect(config.DB_PATH)
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
    conn = sqlite3.connect(config.DB_PATH)
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
    if not force_refresh and now_ts - float(config.STOCK_SCORE_CACHE.get("generated_at", 0.0)) < config.STOCK_SCORE_CACHE_TTL_SECONDS:
        return {
            "generated_at": config.STOCK_SCORE_CACHE.get("generated_at"),
            "items": list(config.STOCK_SCORE_CACHE.get("items", [])),
            "summary": dict(config.STOCK_SCORE_CACHE.get("summary", {})),
        }

    conn = sqlite3.connect(config.DB_PATH)
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
    config.STOCK_SCORE_CACHE["generated_at"] = now_ts
    config.STOCK_SCORE_CACHE["items"] = items
    config.STOCK_SCORE_CACHE["summary"] = summary
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
    page_size = min(max(page_size, 1), config.PRICES_MAX_PAGE_SIZE)
    offset = (page - 1) * page_size

    applied_default_lookback = False

    where_clauses = []
    params: list[object] = []

    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        if not ts_code and not start_date and not end_date:
            latest_trade_date = conn.execute("SELECT MAX(trade_date) FROM stock_daily_prices").fetchone()[0]
            if latest_trade_date:
                end_date = str(latest_trade_date)
                try:
                    latest_dt = datetime.strptime(end_date, "%Y%m%d")
                    start_date = (latest_dt - timedelta(days=config.PRICES_DEFAULT_LOOKBACK_DAYS)).strftime("%Y%m%d")
                except Exception:
                    start_date = (datetime.utcnow() - timedelta(days=config.PRICES_DEFAULT_LOOKBACK_DAYS)).strftime("%Y%m%d")
            else:
                start_date = (datetime.utcnow() - timedelta(days=config.PRICES_DEFAULT_LOOKBACK_DAYS)).strftime("%Y%m%d")
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
                f"未传查询条件，默认返回最近{config.PRICES_DEFAULT_LOOKBACK_DAYS}天日线数据。"
                if applied_default_lookback
                else ""
            ),
            "cache": {"hit": False, "ttl_seconds": config.REDIS_CACHE_TTL_PRICES},
        }
        cache_set_json(cache_key, payload, config.REDIS_CACHE_TTL_PRICES)
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

    conn = sqlite3.connect(config.DB_PATH)
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
    conn = sqlite3.connect(config.DB_PATH)
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

    conn = sqlite3.connect(config.DB_PATH)
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
    conn = sqlite3.connect(config.DB_PATH)
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

    conn = sqlite3.connect(config.DB_PATH)
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
        db_path=config.DB_PATH,
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
    return stock_news_query_sources(sqlite3_module=sqlite3, db_path=config.DB_PATH)


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
        db_path=config.DB_PATH,
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
        db_path=config.DB_PATH,
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
        db_path=config.DB_PATH,
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
        db_path=config.DB_PATH,
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
        db_path=config.DB_PATH,
        keyword=keyword,
        dominant_bias=dominant_bias,
        candidate_type=candidate_type,
        page=page,
        page_size=page_size,
    )


def query_research_reports(report_type: str, keyword: str, report_date: str, page: int, page_size: int):
    return reporting_query_research_reports(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
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
        db_path=config.DB_PATH,
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

    conn = sqlite3.connect(config.DB_PATH)
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
        db_path=config.DB_PATH,
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
        db_path=config.DB_PATH,
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
        db_path=config.DB_PATH,
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

    conn = sqlite3.connect(config.DB_PATH)
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

    eastmoney_running, eastmoney_pid = _pid_running(config.TMP_DIR / "cn_eastmoney_10s.pid")
    eastmoney_data_age = _age_seconds_from_dt(_parse_iso_datetime(latest_eastmoney_fetch or latest_eastmoney_news))
    eastmoney_status = _combine_process_data_status(
        process_running=eastmoney_running,
        data_age_seconds=eastmoney_data_age,
        ok_within=1800,
        warn_within=3 * 3600,
    )
    ws_health = _fetch_local_json("http://127.0.0.1:8010/health", timeout_s=2)
    try:
        quant_health = get_quantaalpha_runtime_health(sqlite3_module=sqlite3, db_path=config.DB_PATH)
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

    counts_conn = sqlite3.connect(config.DB_PATH)
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
            "last_update": _iso_from_mtime(config.TMP_DIR / "cn_eastmoney_10s_supervisor.log"),
        },
        {
            "key": "eastmoney_watchdog",
            "name": "东方财富守护脚本",
            "status": "ok" if (config.TMP_DIR / "cn_eastmoney_10s_watchdog.log").exists() else "warn",
            "detail": "每分钟巡检一次",
            "last_update": _iso_from_mtime(config.TMP_DIR / "cn_eastmoney_10s_watchdog.log"),
        },
        {
            "key": "main_backend",
            "name": "主后端8002",
            "status": "ok" if (config.TMP_DIR / "stock_backend.log").exists() else "warn",
            "detail": "股票/新闻主API",
            "last_update": _iso_from_mtime(config.TMP_DIR / "stock_backend.log"),
        },
        {
            "key": "multi_role_backend",
            "name": "多角色后端8006",
            "status": "ok" if (config.TMP_DIR / "stock_backend_multi_role.log").exists() else "warn",
            "detail": "LLM多角色分析API",
            "last_update": _iso_from_mtime(config.TMP_DIR / "stock_backend_multi_role.log"),
        },
        {
            "key": "news_stream_worker",
            "name": "新闻 Stream Worker",
            "status": "ok" if latest_ws_broadcast else "warn",
            "detail": "消费 stream:news_events 并广播",
            "last_update": latest_ws_broadcast or _iso_from_mtime(config.TMP_DIR / "stream_news_worker.log"),
        },
        {
            "key": "news_ws_service",
            "name": "新闻 WebSocket 服务8010",
            "status": "ok" if ws_health else "error",
            "detail": f"clients={((ws_health or {}).get('clients')) if ws_health else '-'}",
            "last_update": ((ws_health or {}).get("ts")) or _iso_from_mtime(config.TMP_DIR / "ws_realtime.log"),
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
            "path": str(config.TMP_DIR / "cn_eastmoney_fetch.log"),
            "last_update": _iso_from_mtime(config.TMP_DIR / "cn_eastmoney_fetch.log"),
            "tail": _tail_text(config.TMP_DIR / "cn_eastmoney_fetch.log"),
        },
        {
            "name": "东方财富守护日志",
            "path": str(config.TMP_DIR / "cn_eastmoney_10s_watchdog.log"),
            "last_update": _iso_from_mtime(config.TMP_DIR / "cn_eastmoney_10s_watchdog.log"),
            "tail": _tail_text(config.TMP_DIR / "cn_eastmoney_10s_watchdog.log"),
        },
        {
            "name": "国内新闻cron日志",
            "path": str(config.TMP_DIR / "cn_news_fetch_cron.log"),
            "last_update": _iso_from_mtime(config.TMP_DIR / "cn_news_fetch_cron.log"),
            "tail": _tail_text(config.TMP_DIR / "cn_news_fetch_cron.log"),
        },
        {
            "name": "国际新闻cron日志",
            "path": str(config.TMP_DIR / "news_fetch_cron.log"),
            "last_update": _iso_from_mtime(config.TMP_DIR / "news_fetch_cron.log"),
            "tail": _tail_text(config.TMP_DIR / "news_fetch_cron.log"),
        },
        {
            "name": "新闻 Stream Worker 日志",
            "path": str(config.TMP_DIR / "stream_news_worker.log"),
            "last_update": _iso_from_mtime(config.TMP_DIR / "stream_news_worker.log"),
            "tail": _tail_text(config.TMP_DIR / "stream_news_worker.log"),
        },
        {
            "name": "新闻 WebSocket 日志",
            "path": str(config.TMP_DIR / "ws_realtime.log"),
            "last_update": _iso_from_mtime(config.TMP_DIR / "ws_realtime.log"),
            "tail": _tail_text(config.TMP_DIR / "ws_realtime.log"),
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
    cache_set_json("api:source-monitor:v1", payload, config.REDIS_CACHE_TTL_SOURCE_MONITOR)
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

    if not config.AUDIT_REPORT_PATH.exists():
        raise FileNotFoundError(f"审核报告不存在: {config.AUDIT_REPORT_PATH}")

    markdown = config.AUDIT_REPORT_PATH.read_text(encoding="utf-8")
    stat = config.AUDIT_REPORT_PATH.stat()
    generated_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "path": str(config.AUDIT_REPORT_PATH),
        "generated_at": generated_at,
        "markdown": markdown,
        "summary": _parse_audit_summary(markdown),
    }


def query_database_health():
    conn = sqlite3.connect(config.DB_PATH)
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

    conn = sqlite3.connect(config.DB_PATH)
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
    cache_set_json("api:dashboard:v2", payload, config.REDIS_CACHE_TTL_DASHBOARD)
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

__all__ = ['resolve_signal_table', 'query_stocks', 'query_stock_filters', 'query_stock_score_filters', '_stock_score_weights', '_load_stock_scores_from_table', '_parse_any_date', '_days_since', '_safe_div', '_mean_or_default', '_score_grade', '_percentile_scores', '_percentile_scores_by_group', '_build_stock_score_universe', 'query_stock_scores', 'query_prices', 'query_minline', 'query_macro_indicators', 'query_macro_series', 'query_news_sources', 'query_news', 'query_stock_news', 'query_stock_news_sources', 'query_wechat_chatlog', 'query_chatroom_overview', 'fetch_single_chatroom_now', 'query_chatroom_investment_analysis', 'query_chatroom_candidate_pool', 'query_research_reports', 'query_news_daily_summaries', 'query_multi_role_analysis_history', 'get_daily_summary_by_date', 'generate_daily_summary', 'fetch_stock_news_now', 'score_stock_news_now', '_parse_iso_datetime', '_parse_yyyymmdd', '_age_seconds_from_dt', '_status_by_age', '_status_by_lag', '_trading_day_lag', '_status_text', '_max_iso_datetime', '_combine_process_data_status', '_iso_from_mtime', '_tail_text', '_pid_running', '_table_count', '_table_max', '_fetch_local_json', 'query_source_monitor', '_parse_audit_summary', 'query_database_audit', 'query_database_health', '_safe_float', '_probe_backend_port_health', 'query_api_stack_consistency', 'query_dashboard', 'query_stock_detail', '_calc_ma', '_sanitize_json_value']
