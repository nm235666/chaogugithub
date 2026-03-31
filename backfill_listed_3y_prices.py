#!/usr/bin/env python3
"""
把上市状态股票最近一段时间（日线）写入 PostgreSQL 新表，并与 stock_codes.ts_code 关联。

默认库: /home/zanbo/zanbotest/stock_codes.db
默认表: stock_daily_prices

用法:
  python3 backfill_listed_3y_prices.py
  python3 backfill_listed_3y_prices.py --max-stocks 10
  python3 backfill_listed_3y_prices.py --lookback-days 30
  python3 backfill_listed_3y_prices.py --start-date 20260301 --end-date 20260325

可选环境变量:
  TUSHARE_TOKEN=你的token
"""

from __future__ import annotations

import argparse
import db_compat as sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from runtime_secrets import TUSHARE_TOKEN, resolve_tushare_token

# 自动加载项目本地依赖目录，避免系统环境缺包
LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import tushare as ts

DEFAULT_TOKEN = TUSHARE_TOKEN


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="回填上市股票最近一段时间日线数据到 stock_daily_prices 表"
    )
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--table-name", default="stock_daily_prices", help="目标价格表名")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="当未指定 start-date 时，回溯天数（默认30天）",
    )
    parser.add_argument(
        "--start-date",
        default="",
        help="开始日期(YYYYMMDD)，默认=由 lookback-days 推导",
    )
    parser.add_argument("--end-date", default="", help="结束日期(YYYYMMDD)，默认=今天")
    parser.add_argument("--pause", type=float, default=0.08, help="每只股票请求后暂停秒数")
    parser.add_argument("--max-stocks", type=int, default=0, help="最多处理多少只股票，0=全部")
    parser.add_argument(
        "--resume-from",
        default="",
        help="从某个 ts_code 开始(含该代码)，用于断点续跑",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="执行前清空目标价格表数据",
    )
    return parser.parse_args()


def yyyymmdd_utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def default_start_date_by_lookback(end_date: str, lookback_days: int) -> str:
    dt = datetime.strptime(end_date, "%Y%m%d")
    return (dt - timedelta(days=max(lookback_days, 1))).strftime("%Y%m%d")


def ensure_price_table(conn: sqlite3.Connection, table_name: str) -> None:
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


def load_listed_codes(
    conn: sqlite3.Connection, max_stocks: int, resume_from: str
) -> list[str]:
    sql = "SELECT ts_code FROM stock_codes WHERE list_status = 'L'"
    params: list[object] = []
    if resume_from:
        sql += " AND ts_code >= ?"
        params.append(resume_from)
    sql += " ORDER BY ts_code"
    if max_stocks and max_stocks > 0:
        sql += " LIMIT ?"
        params.append(max_stocks)
    rows = conn.execute(sql, params).fetchall()
    return [r[0] for r in rows]


def chunked_rows(df) -> Iterable[tuple]:
    for row in df.itertuples(index=False):
        yield (
            row.ts_code,
            row.trade_date,
            row.open,
            row.high,
            row.low,
            row.close,
            row.pre_close,
            row.change,
            row.pct_chg,
            row.vol,
            row.amount,
        )


def upsert_prices(conn: sqlite3.Connection, table_name: str, rows: Iterable[tuple]) -> int:
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
    token = resolve_tushare_token(args.token)

    end_date = args.end_date.strip() or yyyymmdd_utc_today()
    start_date = args.start_date.strip() or default_start_date_by_lookback(
        end_date, args.lookback_days
    )

    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"错误: 数据库不存在: {db_path}", file=sys.stderr)
        return 1

    pro = ts.pro_api(token)

    conn = sqlite3.connect(db_path)
    try:
        ensure_price_table(conn, args.table_name)
        if args.truncate:
            conn.execute(f"DELETE FROM {args.table_name}")
            conn.commit()
            print(f"已清空旧数据: {args.table_name}")
        codes = load_listed_codes(conn, args.max_stocks, args.resume_from.strip())
        total_codes = len(codes)
        if total_codes == 0:
            print("没有可处理的上市股票。")
            return 0

        print(
            f"开始回填: {total_codes} 只股票, 区间 {start_date}~{end_date}, 表 {args.table_name}"
        )
        processed = 0
        failed = 0
        total_rows = 0

        for ts_code in codes:
            processed += 1
            try:
                df = pro.daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields=(
                        "ts_code,trade_date,open,high,low,close,pre_close,"
                        "change,pct_chg,vol,amount"
                    ),
                )
                if df is None or df.empty:
                    print(f"[{processed}/{total_codes}] {ts_code}: 无数据")
                else:
                    affected = upsert_prices(conn, args.table_name, chunked_rows(df))
                    total_rows += len(df)
                    print(
                        f"[{processed}/{total_codes}] {ts_code}: {len(df)} 行 "
                        f"(upsert={affected})"
                    )
            except Exception as exc:
                failed += 1
                print(f"[{processed}/{total_codes}] {ts_code}: 失败 -> {exc}", file=sys.stderr)

            if args.pause > 0:
                time.sleep(args.pause)

        print(
            f"完成: processed={processed}, failed={failed}, fetched_rows={total_rows}, db={db_path}"
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
