#!/usr/bin/env python3
"""
回填股票财务核心指标到 stock_financials。

数据来源：
- tushare.pro.fina_indicator
- tushare.pro.income
- tushare.pro.cashflow
- tushare.pro.balancesheet

默认行为：
- 仅抓取当前上市股票
- 每只股票抓最近 8 个报告期
- 写入表 stock_financials

用法示例：
  python3 backfill_stock_financials.py
  python3 backfill_stock_financials.py --ts-code 000001.SZ
  python3 backfill_stock_financials.py --start-from 300750.SZ --recent-periods 12
  python3 backfill_stock_financials.py --limit-stocks 100 --pause 0.15
"""

from __future__ import annotations

import argparse
import math
import db_compat as sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from runtime_secrets import TUSHARE_TOKEN, resolve_tushare_token

LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import tushare as ts

DEFAULT_TOKEN = TUSHARE_TOKEN


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回填股票财务核心指标到 stock_financials")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--table-name", default="stock_financials", help="目标表名")
    parser.add_argument("--ts-code", default="", help="仅抓取单只股票，如 000001.SZ")
    parser.add_argument("--start-from", default="", help="从指定 ts_code 开始续跑")
    parser.add_argument("--limit-stocks", type=int, default=0, help="最多抓取多少只股票，0 表示不限")
    parser.add_argument("--recent-periods", type=int, default=8, help="每只股票抓最近多少个报告期")
    parser.add_argument("--pause", type=float, default=0.12, help="每只股票请求后暂停秒数")
    parser.add_argument("--retry", type=int, default=3, help="单接口失败最大重试次数")
    parser.add_argument("--listed-only", action="store_true", help="仅抓当前上市股票")
    parser.add_argument("--all-status", action="store_true", help="抓全部状态股票")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_code TEXT NOT NULL,
            report_period TEXT NOT NULL,
            report_type TEXT,
            ann_date TEXT,
            revenue REAL,
            op_profit REAL,
            net_profit REAL,
            net_profit_excl_nr REAL,
            roe REAL,
            gross_margin REAL,
            debt_to_assets REAL,
            operating_cf REAL,
            free_cf REAL,
            eps REAL,
            bps REAL,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (ts_code, report_period),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_period ON {table_name}(report_period)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_period ON {table_name}(ts_code, report_period)"
    )
    conn.commit()


def load_target_codes(
    conn: sqlite3.Connection,
    ts_code: str,
    start_from: str,
    limit_stocks: int,
    listed_only: bool,
) -> list[str]:
    if ts_code.strip():
        return [ts_code.strip().upper()]

    where = []
    params: list[object] = []
    if listed_only:
        where.append("list_status = 'L'")
    if start_from.strip():
        where.append("ts_code >= ?")
        params.append(start_from.strip().upper())

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    limit_sql = f" LIMIT {int(limit_stocks)}" if limit_stocks > 0 else ""
    sql = f"SELECT ts_code FROM stock_codes{where_sql} ORDER BY ts_code{limit_sql}"
    rows = conn.execute(sql, params).fetchall()
    return [r[0] for r in rows]


def fetch_with_retry(fetch_fn, retry: int):
    last_exc = None
    for attempt in range(retry + 1):
        try:
            return fetch_fn()
        except Exception as exc:
            last_exc = exc
            if attempt < retry:
                time.sleep(1.5 * (2**attempt))
                continue
            raise last_exc


def safe_num(value):
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def quarter_report_type(end_date: str, fallback) -> str | None:
    suffix = (end_date or "")[-4:]
    mapping = {
        "0331": "q1",
        "0630": "h1",
        "0930": "q3",
        "1231": "annual",
    }
    if suffix in mapping:
        return mapping[suffix]
    if fallback is None:
        return None
    return str(fallback)


def choose_ann_date(*values) -> str | None:
    dates = [str(v).strip() for v in values if v not in (None, "", "None")]
    if not dates:
        return None
    return max(dates)


def fetch_financial_context(pro, ts_code: str, recent_periods: int, retry: int) -> list[dict]:
    indicator_df = fetch_with_retry(
        lambda: pro.fina_indicator(ts_code=ts_code, limit=recent_periods),
        retry=retry,
    )
    income_df = fetch_with_retry(
        lambda: pro.income(ts_code=ts_code, limit=recent_periods),
        retry=retry,
    )
    cashflow_df = fetch_with_retry(
        lambda: pro.cashflow(ts_code=ts_code, limit=recent_periods),
        retry=retry,
    )
    balancesheet_df = fetch_with_retry(
        lambda: pro.balancesheet(ts_code=ts_code, limit=recent_periods),
        retry=retry,
    )

    merged: dict[str, dict] = {}

    def ensure_period(period: str) -> dict:
        item = merged.get(period)
        if item is None:
            item = {
                "ts_code": ts_code,
                "report_period": period,
                "report_type": None,
                "ann_date": None,
                "revenue": None,
                "op_profit": None,
                "net_profit": None,
                "net_profit_excl_nr": None,
                "roe": None,
                "gross_margin": None,
                "debt_to_assets": None,
                "operating_cf": None,
                "free_cf": None,
                "eps": None,
                "bps": None,
                "source": "tushare",
                "update_time": utc_now(),
            }
            merged[period] = item
        return item

    if indicator_df is not None and not indicator_df.empty:
        for row in indicator_df.to_dict("records"):
            period = str(row.get("end_date") or "").strip()
            if not period:
                continue
            item = ensure_period(period)
            item["report_type"] = quarter_report_type(period, row.get("report_type"))
            item["ann_date"] = choose_ann_date(item["ann_date"], row.get("ann_date"))
            item["net_profit_excl_nr"] = safe_num(row.get("profit_dedt"))
            item["roe"] = safe_num(row.get("roe"))
            item["gross_margin"] = safe_num(row.get("grossprofit_margin"))
            item["debt_to_assets"] = safe_num(row.get("debt_to_assets"))
            item["eps"] = safe_num(row.get("eps"))
            item["bps"] = safe_num(row.get("bps"))

    if income_df is not None and not income_df.empty:
        for row in income_df.to_dict("records"):
            period = str(row.get("end_date") or "").strip()
            if not period:
                continue
            item = ensure_period(period)
            item["report_type"] = quarter_report_type(period, row.get("report_type")) or item["report_type"]
            item["ann_date"] = choose_ann_date(
                item["ann_date"], row.get("ann_date"), row.get("f_ann_date")
            )
            item["revenue"] = safe_num(row.get("total_revenue")) or safe_num(row.get("revenue"))
            item["op_profit"] = safe_num(row.get("operate_profit"))
            item["net_profit"] = safe_num(row.get("n_income_attr_p")) or safe_num(row.get("n_income"))

    if cashflow_df is not None and not cashflow_df.empty:
        for row in cashflow_df.to_dict("records"):
            period = str(row.get("end_date") or "").strip()
            if not period:
                continue
            item = ensure_period(period)
            item["report_type"] = quarter_report_type(period, row.get("report_type")) or item["report_type"]
            item["ann_date"] = choose_ann_date(
                item["ann_date"], row.get("ann_date"), row.get("f_ann_date")
            )
            item["operating_cf"] = safe_num(row.get("n_cashflow_act"))
            item["free_cf"] = safe_num(row.get("free_cashflow"))

    if balancesheet_df is not None and not balancesheet_df.empty:
        for row in balancesheet_df.to_dict("records"):
            period = str(row.get("end_date") or "").strip()
            if not period:
                continue
            item = ensure_period(period)
            item["report_type"] = quarter_report_type(period, row.get("report_type")) or item["report_type"]
            item["ann_date"] = choose_ann_date(
                item["ann_date"], row.get("ann_date"), row.get("f_ann_date")
            )
            if item["debt_to_assets"] is None:
                total_assets = safe_num(row.get("total_assets"))
                total_liab = safe_num(row.get("total_liab"))
                if total_assets not in (None, 0) and total_liab is not None:
                    item["debt_to_assets"] = total_liab / total_assets * 100

    rows = list(merged.values())
    rows.sort(key=lambda x: x["report_period"], reverse=True)
    return rows[: max(recent_periods, 1)]


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {table_name} (
        ts_code, report_period, report_type, ann_date, revenue, op_profit, net_profit,
        net_profit_excl_nr, roe, gross_margin, debt_to_assets, operating_cf, free_cf,
        eps, bps, source, update_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, report_period) DO UPDATE SET
        report_type=excluded.report_type,
        ann_date=excluded.ann_date,
        revenue=excluded.revenue,
        op_profit=excluded.op_profit,
        net_profit=excluded.net_profit,
        net_profit_excl_nr=excluded.net_profit_excl_nr,
        roe=excluded.roe,
        gross_margin=excluded.gross_margin,
        debt_to_assets=excluded.debt_to_assets,
        operating_cf=excluded.operating_cf,
        free_cf=excluded.free_cf,
        eps=excluded.eps,
        bps=excluded.bps,
        source=excluded.source,
        update_time=excluded.update_time
    """
    values = [
        (
            row["ts_code"],
            row["report_period"],
            row["report_type"],
            row["ann_date"],
            row["revenue"],
            row["op_profit"],
            row["net_profit"],
            row["net_profit_excl_nr"],
            row["roe"],
            row["gross_margin"],
            row["debt_to_assets"],
            row["operating_cf"],
            row["free_cf"],
            row["eps"],
            row["bps"],
            row["source"],
            row["update_time"],
        )
        for row in rows
    ]
    cur = conn.cursor()
    cur.executemany(sql, values)
    conn.commit()
    return len(rows)


def main() -> int:
    args = parse_args()
    listed_only = True
    if args.all_status:
        listed_only = False
    elif args.listed_only:
        listed_only = True

    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"错误: 数据库不存在: {db_path}", file=sys.stderr)
        return 1

    pro = ts.pro_api(resolve_tushare_token(args.token))
    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        codes = load_target_codes(
            conn=conn,
            ts_code=args.ts_code,
            start_from=args.start_from,
            limit_stocks=args.limit_stocks,
            listed_only=listed_only,
        )
        if not codes:
            print("没有可处理的股票。")
            return 0

        print(
            f"待处理股票数: {len(codes)}, listed_only={listed_only}, recent_periods={args.recent_periods}"
        )

        total_rows = 0
        failed = 0
        for idx, ts_code in enumerate(codes, start=1):
            try:
                rows = fetch_financial_context(
                    pro=pro,
                    ts_code=ts_code,
                    recent_periods=args.recent_periods,
                    retry=args.retry,
                )
                n = upsert_rows(conn, args.table_name, rows)
                total_rows += n
                latest_period = rows[0]["report_period"] if rows else "-"
                print(
                    f"[{idx}/{len(codes)}] {ts_code}: ok periods={len(rows)} latest={latest_period}"
                )
            except Exception as exc:
                failed += 1
                print(f"[{idx}/{len(codes)}] {ts_code}: 失败 -> {exc}", file=sys.stderr)

            if args.pause > 0:
                time.sleep(args.pause)

        final_rows = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(
            f"完成: failed={failed}, upsert_rows={total_rows}, table_rows={final_rows}, db={db_path}"
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
