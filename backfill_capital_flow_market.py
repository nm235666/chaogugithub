#!/usr/bin/env python3
"""
回填市场级资金流到 capital_flow_market。

数据来源：
- tushare.pro.moneyflow_hsgt

当前写入两类 flow_type：
- northbound  北向资金
- southbound  南向资金

说明：
- Tushare moneyflow_hsgt 提供的是净流向及分通道数据
- 由于当前表结构的 buy_amount / sell_amount 更适合“买入/卖出”口径，
  这里保守写法为：
  - net_inflow: 北向/南向总额
  - buy_amount / sell_amount: 留空
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
    parser = argparse.ArgumentParser(description="回填市场级资金流到 capital_flow_market")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--table-name", default="capital_flow_market", help="目标表名")
    parser.add_argument("--lookback-days", type=int, default=30, help="回溯天数，默认30")
    parser.add_argument("--start-date", default="", help="开始日期(YYYYMMDD)")
    parser.add_argument("--end-date", default="", help="结束日期(YYYYMMDD)")
    parser.add_argument("--pause", type=float, default=0.05, help="每个区间请求后暂停秒数")
    parser.add_argument("--truncate", action="store_true", help="执行前清空目标表")
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
            trade_date TEXT NOT NULL,
            flow_type TEXT NOT NULL,
            net_inflow REAL,
            buy_amount REAL,
            sell_amount REAL,
            unit TEXT,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (trade_date, flow_type)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date)"
    )
    conn.commit()


def to_float(value):
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {table_name} (
        trade_date, flow_type, net_inflow, buy_amount, sell_amount, unit, source, update_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(trade_date, flow_type) DO UPDATE SET
        net_inflow=excluded.net_inflow,
        buy_amount=excluded.buy_amount,
        sell_amount=excluded.sell_amount,
        unit=excluded.unit,
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

        df = pro.moneyflow_hsgt(start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            print("未获取到市场级资金流数据。")
            return 0

        update_time = utc_now()
        rows = []
        for row in df.itertuples(index=False):
            rows.append(
                (
                    row.trade_date,
                    "northbound",
                    to_float(row.north_money),
                    None,
                    None,
                    "million_cny",
                    "tushare.moneyflow_hsgt",
                    update_time,
                )
            )
            rows.append(
                (
                    row.trade_date,
                    "southbound",
                    to_float(row.south_money),
                    None,
                    None,
                    "million_cny",
                    "tushare.moneyflow_hsgt",
                    update_time,
                )
            )

        n = upsert_rows(conn, args.table_name, rows)
        final_count = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(
            f"完成: dates={len(df)}, upsert_rows={n}, table_rows={final_count}, range={start_date}~{end_date}"
        )
        if args.pause > 0:
            time.sleep(args.pause)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
