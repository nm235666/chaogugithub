#!/usr/bin/env python3
"""
按交易日批量回填个股资金流到 capital_flow_stock。

数据来源：
- tushare.pro.moneyflow

字段映射：
- net_inflow         = net_mf_amount
- super_large_inflow = buy_elg_amount - sell_elg_amount
- large_inflow       = buy_lg_amount - sell_lg_amount
- medium_inflow      = buy_md_amount - sell_md_amount
- small_inflow       = buy_sm_amount - sell_sm_amount
- main_inflow        = super_large_inflow + large_inflow

用法示例：
  python3 backfill_capital_flow_stock.py
  python3 backfill_capital_flow_stock.py --lookback-days 30
  python3 backfill_capital_flow_stock.py --start-date 20260301 --end-date 20260325
"""

from __future__ import annotations

import argparse
import db_compat as sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from runtime_secrets import TUSHARE_TOKEN, resolve_tushare_token

LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import tushare as ts

DEFAULT_TOKEN = TUSHARE_TOKEN


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按交易日批量回填个股资金流")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--table-name", default="capital_flow_stock", help="目标表名")
    parser.add_argument("--lookback-days", type=int, default=30, help="回溯天数，默认30")
    parser.add_argument("--start-date", default="", help="开始日期(YYYYMMDD)")
    parser.add_argument("--end-date", default="", help="结束日期(YYYYMMDD)")
    parser.add_argument("--pause", type=float, default=0.05, help="每个交易日请求后暂停秒数")
    parser.add_argument("--truncate", action="store_true", help="执行前清空目标表")
    parser.add_argument("--all-status", action="store_true", help="抓全部状态股票")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def calc_start(end_date: str, lookback_days: int) -> str:
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    return (end_dt - timedelta(days=max(lookback_days, 1))).strftime("%Y%m%d")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            net_inflow REAL,
            main_inflow REAL,
            super_large_inflow REAL,
            large_inflow REAL,
            medium_inflow REAL,
            small_inflow REAL,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (ts_code, trade_date),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_trade ON {table_name}(ts_code, trade_date)"
    )
    conn.commit()


def load_code_set(conn: sqlite3.Connection, all_status: bool) -> set[str]:
    if all_status:
        sql = "SELECT ts_code FROM stock_codes"
    else:
        sql = "SELECT ts_code FROM stock_codes WHERE list_status='L'"
    rows = conn.execute(sql).fetchall()
    return {row[0] for row in rows}


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {table_name} (
        ts_code, trade_date, net_inflow, main_inflow, super_large_inflow, large_inflow,
        medium_inflow, small_inflow, source, update_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, trade_date) DO UPDATE SET
        net_inflow=excluded.net_inflow,
        main_inflow=excluded.main_inflow,
        super_large_inflow=excluded.super_large_inflow,
        large_inflow=excluded.large_inflow,
        medium_inflow=excluded.medium_inflow,
        small_inflow=excluded.small_inflow,
        source=excluded.source,
        update_time=excluded.update_time
    """
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def main() -> int:
    args = parse_args()
    end_date = args.end_date.strip() or utc_today()
    start_date = args.start_date.strip() or calc_start(end_date, args.lookback_days)

    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"错误: 数据库不存在: {db_path}", file=sys.stderr)
        return 1

    pro = ts.pro_api(resolve_tushare_token(args.token))
    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        if args.truncate:
            conn.execute(f"DELETE FROM {args.table_name}")
            conn.commit()
            print(f"已清空旧数据: {args.table_name}")

        code_set = load_code_set(conn, args.all_status)
        print(f"目标股票数: {len(code_set)}, all_status={args.all_status}")

        cal = pro.trade_cal(
            exchange="SSE",
            start_date=start_date,
            end_date=end_date,
            is_open="1",
            fields="cal_date",
        )
        if cal is None or cal.empty:
            print("未获取到交易日，结束。")
            return 0

        trade_dates = sorted(cal["cal_date"].tolist())
        print(f"交易日数量: {len(trade_dates)} ({start_date}~{end_date})")

        total_rows = 0
        failed_days = 0
        update_time = utc_now()
        for idx, trade_date in enumerate(trade_dates, start=1):
            try:
                df = pro.moneyflow(trade_date=trade_date)
                if df is None or df.empty:
                    print(f"[{idx}/{len(trade_dates)}] {trade_date}: 无数据")
                else:
                    filtered = df[df["ts_code"].isin(code_set)]
                    rows = []
                    for row in filtered.itertuples(index=False):
                        super_large = (row.buy_elg_amount or 0) - (row.sell_elg_amount or 0)
                        large = (row.buy_lg_amount or 0) - (row.sell_lg_amount or 0)
                        medium = (row.buy_md_amount or 0) - (row.sell_md_amount or 0)
                        small = (row.buy_sm_amount or 0) - (row.sell_sm_amount or 0)
                        main = super_large + large
                        rows.append(
                            (
                                row.ts_code,
                                row.trade_date,
                                row.net_mf_amount,
                                main,
                                super_large,
                                large,
                                medium,
                                small,
                                "tushare.moneyflow",
                                update_time,
                            )
                        )
                    n = upsert_rows(conn, args.table_name, rows)
                    total_rows += n
                    print(
                        f"[{idx}/{len(trade_dates)}] {trade_date}: raw={len(df)} filtered={len(rows)} upsert={n}"
                    )
            except Exception as exc:
                failed_days += 1
                print(f"[{idx}/{len(trade_dates)}] {trade_date}: 失败 -> {exc}", file=sys.stderr)

            if args.pause > 0:
                time.sleep(args.pause)

        final_count = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(
            f"完成: failed_days={failed_days}, upsert_rows={total_rows}, table_rows={final_count}, db={db_path}"
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
