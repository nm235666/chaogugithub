#!/usr/bin/env python3
"""
生成股票综合评分日快照并落库。

示例：
  python3 backfill_stock_scores_daily.py
  python3 backfill_stock_scores_daily.py --score-date 20260325
  python3 backfill_stock_scores_daily.py --limit-stocks 100
"""

from __future__ import annotations

import argparse
import json
import db_compat as sqlite3
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

import backend.http_server.config as http_server_config
import backend.server as score_server
from realtime_streams import publish_app_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成股票综合评分日快照")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--table-name", default="stock_scores_daily", help="目标表名")
    parser.add_argument("--score-date", default="", help="评分日期 YYYYMMDD，默认取评分结果中的最新交易日")
    parser.add_argument("--limit-stocks", type=int, default=0, help="仅写入前 N 只股票，0 表示全部")
    parser.add_argument("--truncate-date", action="store_true", help="写入前先删除该 score_date 的旧数据")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [to_json_safe(v) for v in value]
    return value


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            score_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            name TEXT,
            symbol TEXT,
            market TEXT,
            area TEXT,
            industry TEXT,
            industry_rank INTEGER,
            industry_count INTEGER,
            score_grade TEXT,
            industry_score_grade TEXT,
            total_score REAL,
            industry_total_score REAL,
            trend_score REAL,
            industry_trend_score REAL,
            financial_score REAL,
            industry_financial_score REAL,
            valuation_score REAL,
            industry_valuation_score REAL,
            capital_flow_score REAL,
            industry_capital_flow_score REAL,
            event_score REAL,
            industry_event_score REAL,
            news_score REAL,
            industry_news_score REAL,
            risk_score REAL,
            industry_risk_score REAL,
            latest_trade_date TEXT,
            latest_report_period TEXT,
            latest_valuation_date TEXT,
            latest_flow_date TEXT,
            latest_event_date TEXT,
            latest_news_time TEXT,
            latest_risk_date TEXT,
            score_payload_json TEXT,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (score_date, ts_code),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_score_date ON {table_name}(score_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_total_score ON {table_name}(score_date, total_score DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_market_area ON {table_name}(score_date, market, area)"
    )
    existing_cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    required_cols = {
        "industry_rank": "INTEGER",
        "industry_count": "INTEGER",
        "industry_score_grade": "TEXT",
        "industry_total_score": "REAL",
        "industry_trend_score": "REAL",
        "industry_financial_score": "REAL",
        "industry_valuation_score": "REAL",
        "industry_capital_flow_score": "REAL",
        "industry_event_score": "REAL",
        "industry_news_score": "REAL",
        "industry_risk_score": "REAL",
    }
    for col, col_type in required_cols.items():
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}")
    conn.commit()


def resolve_score_date(items: list[dict], explicit_score_date: str) -> str:
    if explicit_score_date:
        return explicit_score_date.strip()
    latest_dates = [str(item.get("latest_trade_date") or "").strip() for item in items]
    latest_dates = [d for d in latest_dates if d]
    if latest_dates:
        return max(latest_dates)
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def build_rows(items: list[dict], score_date: str) -> list[tuple]:
    update_time = utc_now()
    rows = []
    for item in items:
        payload = score_server._sanitize_json_value(
            {
                "score_summary": item.get("score_summary", {}),
                "raw_metrics": {
                    "ret_5d_pct": item.get("ret_5d_pct"),
                    "ret_20d_pct": item.get("ret_20d_pct"),
                    "ma20_gap_pct": item.get("ma20_gap_pct"),
                    "vol20_abs_pct": item.get("vol20_abs_pct"),
                    "roe": item.get("roe"),
                    "gross_margin": item.get("gross_margin"),
                    "debt_to_assets": item.get("debt_to_assets"),
                    "operating_cf_margin_pct": item.get("operating_cf_margin_pct"),
                    "free_cf_margin_pct": item.get("free_cf_margin_pct"),
                    "net_margin_pct": item.get("net_margin_pct"),
                    "pe_ttm": item.get("pe_ttm"),
                    "pb": item.get("pb"),
                    "ps_ttm": item.get("ps_ttm"),
                    "dv_ttm": item.get("dv_ttm"),
                    "main_flow_ratio_5d_pct": item.get("main_flow_ratio_5d_pct"),
                    "net_flow_ratio_20d_pct": item.get("net_flow_ratio_20d_pct"),
                    "event_balance_90d": item.get("event_balance_90d"),
                    "avg_news_impact": item.get("avg_news_impact"),
                    "avg_news_system": item.get("avg_news_system"),
                    "avg_drawdown": item.get("avg_drawdown"),
                    "avg_var95": item.get("avg_var95"),
                    "avg_cvar95": item.get("avg_cvar95"),
                },
            }
        )
        rows.append(
            (
                score_date,
                item.get("ts_code"),
                item.get("name"),
                item.get("symbol"),
                item.get("market"),
                item.get("area"),
                item.get("industry"),
                item.get("industry_rank"),
                item.get("industry_count"),
                item.get("score_grade"),
                item.get("industry_score_grade"),
                item.get("total_score"),
                item.get("industry_total_score"),
                item.get("trend_score"),
                item.get("industry_trend_score"),
                item.get("financial_score"),
                item.get("industry_financial_score"),
                item.get("valuation_score"),
                item.get("industry_valuation_score"),
                item.get("capital_flow_score"),
                item.get("industry_capital_flow_score"),
                item.get("event_score"),
                item.get("industry_event_score"),
                item.get("news_score"),
                item.get("industry_news_score"),
                item.get("risk_score"),
                item.get("industry_risk_score"),
                item.get("latest_trade_date"),
                item.get("latest_report_period"),
                item.get("latest_valuation_date"),
                item.get("latest_flow_date"),
                item.get("latest_event_date"),
                item.get("latest_news_time"),
                item.get("latest_risk_date"),
                json.dumps(to_json_safe(payload), ensure_ascii=False, allow_nan=False),
                "stock_score_v1",
                update_time,
            )
        )
    return rows


def attach_industry_ranks(items: list[dict]) -> None:
    groups: dict[str, list[dict]] = {}
    for item in items:
        industry = str(item.get("industry") or "").strip()
        groups.setdefault(industry, []).append(item)
    for _, group_items in groups.items():
        group_items.sort(key=lambda x: (-float(x.get("industry_total_score") or 0.0), str(x.get("ts_code") or "")))
        total = len(group_items)
        for idx, item in enumerate(group_items, start=1):
            item["industry_rank"] = idx
            item["industry_count"] = total


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[tuple], truncate_date: bool) -> None:
    if not rows:
        return
    score_date = rows[0][0]
    if truncate_date:
        conn.execute(f"DELETE FROM {table_name} WHERE score_date = ?", (score_date,))
    conn.executemany(
        f"""
        INSERT INTO {table_name} (
            score_date, ts_code, name, symbol, market, area, industry, industry_rank, industry_count, score_grade, industry_score_grade,
            total_score, industry_total_score, trend_score, industry_trend_score, financial_score, industry_financial_score, valuation_score, industry_valuation_score, capital_flow_score, industry_capital_flow_score,
            event_score, industry_event_score, news_score, industry_news_score, risk_score, industry_risk_score, latest_trade_date, latest_report_period,
            latest_valuation_date, latest_flow_date, latest_event_date, latest_news_time,
            latest_risk_date, score_payload_json, source, update_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(score_date, ts_code) DO UPDATE SET
            name=excluded.name,
            symbol=excluded.symbol,
            market=excluded.market,
            area=excluded.area,
            industry=excluded.industry,
            industry_rank=excluded.industry_rank,
            industry_count=excluded.industry_count,
            score_grade=excluded.score_grade,
            industry_score_grade=excluded.industry_score_grade,
            total_score=excluded.total_score,
            industry_total_score=excluded.industry_total_score,
            trend_score=excluded.trend_score,
            industry_trend_score=excluded.industry_trend_score,
            financial_score=excluded.financial_score,
            industry_financial_score=excluded.industry_financial_score,
            valuation_score=excluded.valuation_score,
            industry_valuation_score=excluded.industry_valuation_score,
            capital_flow_score=excluded.capital_flow_score,
            industry_capital_flow_score=excluded.industry_capital_flow_score,
            event_score=excluded.event_score,
            industry_event_score=excluded.industry_event_score,
            news_score=excluded.news_score,
            industry_news_score=excluded.industry_news_score,
            risk_score=excluded.risk_score,
            industry_risk_score=excluded.industry_risk_score,
            latest_trade_date=excluded.latest_trade_date,
            latest_report_period=excluded.latest_report_period,
            latest_valuation_date=excluded.latest_valuation_date,
            latest_flow_date=excluded.latest_flow_date,
            latest_event_date=excluded.latest_event_date,
            latest_news_time=excluded.latest_news_time,
            latest_risk_date=excluded.latest_risk_date,
            score_payload_json=excluded.score_payload_json,
            source=excluded.source,
            update_time=excluded.update_time
        """,
        rows,
    )
    conn.commit()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)
    if (not sqlite3.using_postgres()) and not db_path.exists():
        raise SystemExit(f"数据库不存在: {db_path}")

    old_db_path = http_server_config.DB_PATH
    http_server_config.DB_PATH = db_path
    try:
        universe = score_server._build_stock_score_universe(force_refresh=True)
    finally:
        http_server_config.DB_PATH = old_db_path

    items = list(universe["items"])
    if args.limit_stocks > 0:
        items = items[: args.limit_stocks]
    attach_industry_ranks(items)

    score_date = resolve_score_date(items, args.score_date)
    rows = build_rows(items, score_date)

    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        upsert_rows(conn, args.table_name, rows, truncate_date=args.truncate_date)
        total = conn.execute(
            f"SELECT COUNT(*) FROM {args.table_name} WHERE score_date = ?",
            (score_date,),
        ).fetchone()[0]
    finally:
        conn.close()

    print(f"score_date={score_date} rows_written={len(rows)} rows_in_table_for_date={total}")
    publish_app_event(
        event="stock_scores_update",
        payload={
            "score_date": score_date,
            "rows_written": int(len(rows)),
            "rows_in_table_for_date": int(total),
        },
        producer="backfill_stock_scores_daily.py",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
