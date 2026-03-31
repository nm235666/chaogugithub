#!/usr/bin/env python3
"""
更快版本：按“交易日”批量拉取全市场日线，再过滤为上市股票写入 PostgreSQL。

默认写入表: stock_daily_prices
默认区间: 最近30天

用法:
  python3 fast_backfill_listed_prices.py --truncate
  python3 fast_backfill_listed_prices.py --lookback-days 30 --pause 0.03
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
    parser = argparse.ArgumentParser(description="按交易日快速回填上市股票日线")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--table-name", default="stock_daily_prices", help="目标价格表名")
    parser.add_argument("--lookback-days", type=int, default=30, help="回溯天数，默认30")
    parser.add_argument("--start-date", default="", help="开始日期(YYYYMMDD)")
    parser.add_argument("--end-date", default="", help="结束日期(YYYYMMDD)")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--pause", type=float, default=0.03, help="每个交易日请求后暂停秒数")
    parser.add_argument("--truncate", action="store_true", help="执行前清空目标表")
    return parser.parse_args()


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
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            pre_close REAL,
            change REAL,
            pct_chg REAL,
            vol REAL,
            amount REAL,
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


def load_listed_set(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT ts_code FROM stock_codes WHERE list_status='L'").fetchall()
    return {row[0] for row in rows}


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[tuple]) -> int:
    sql = f"""
    INSERT INTO {table_name} (
        ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, trade_date) DO UPDATE SET
        open=excluded.open,
        high=excluded.high,
        low=excluded.low,
        close=excluded.close,
        pre_close=excluded.pre_close,
        change=excluded.change,
        pct_chg=excluded.pct_chg,
        vol=excluded.vol,
        amount=excluded.amount
    """
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    return cur.rowcount if cur.rowcount != -1 else 0


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

        listed_set = load_listed_set(conn)
        print(f"上市股票数量: {len(listed_set)}")

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
        for idx, d in enumerate(trade_dates, start=1):
            try:
                df = pro.daily(
                    trade_date=d,
                    fields=(
                        "ts_code,trade_date,open,high,low,close,pre_close,"
                        "change,pct_chg,vol,amount"
                    ),
                )
                if df is None or df.empty:
                    print(f"[{idx}/{len(trade_dates)}] {d}: 无数据")
                else:
                    # 仅保留当前仍为上市状态的股票
                    filtered = df[df["ts_code"].isin(listed_set)]
                    rows = list(filtered.itertuples(index=False, name=None))
                    affected = upsert_rows(conn, args.table_name, rows) if rows else 0
                    total_rows += len(rows)
                    print(
                        f"[{idx}/{len(trade_dates)}] {d}: raw={len(df)} filtered={len(rows)} upsert={affected}"
                    )
            except Exception as exc:
                failed_days += 1
                print(f"[{idx}/{len(trade_dates)}] {d}: 失败 -> {exc}", file=sys.stderr)

            if args.pause > 0:
                time.sleep(args.pause)

        final_count = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(
            f"完成: failed_days={failed_days}, fetched_rows={total_rows}, table_rows={final_count}, db={db_path}"
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
