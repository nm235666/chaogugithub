#!/usr/bin/env python3
"""
自动更新:
1) stock_codes: 更新全部股票基础信息(上市/退市/暂停)
2) stock_daily_prices: 增量更新上市股票日线

默认行为:
- 如果价格表已有数据: 从 max(trade_date) 开始增量更新到今天
- 如果价格表为空: 回填最近30天
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
    parser = argparse.ArgumentParser(description="自动更新股票列表和日线数据")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--price-table", default="stock_daily_prices", help="价格表名")
    parser.add_argument("--bootstrap-days", type=int, default=30, help="价格表为空时回溯天数")
    parser.add_argument("--pause", type=float, default=0.03, help="每个交易日请求间隔秒数")
    parser.add_argument("--end-date", default="", help="结束日期(YYYYMMDD)，默认今天")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    return parser.parse_args()


def utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def ensure_tables(conn: sqlite3.Connection, price_table: str) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_codes (
            ts_code TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            area TEXT,
            industry TEXT,
            market TEXT,
            list_date TEXT,
            delist_date TEXT,
            list_status TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_codes_status ON stock_codes(list_status)")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {price_table} (
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
        f"CREATE INDEX IF NOT EXISTS idx_{price_table}_trade_date ON {price_table}(trade_date)"
    )
    conn.commit()


def upsert_stock_codes(conn: sqlite3.Connection, pro) -> int:
    df = pro.stock_basic(
        exchange="",
        list_status="L,D,P",
        fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,list_status",
    )
    if df is None or df.empty:
        return 0
    rows = list(df.itertuples(index=False, name=None))
    sql = """
    INSERT INTO stock_codes (
        ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code) DO UPDATE SET
        symbol=excluded.symbol,
        name=excluded.name,
        area=excluded.area,
        industry=excluded.industry,
        market=excluded.market,
        list_date=excluded.list_date,
        delist_date=excluded.delist_date,
        list_status=excluded.list_status
    """
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def get_start_date(conn: sqlite3.Connection, price_table: str, end_date: str, bootstrap_days: int) -> str:
    row = conn.execute(f"SELECT MAX(trade_date) FROM {price_table}").fetchone()
    max_date = row[0] if row else None
    if max_date:
        return max_date
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    return (end_dt - timedelta(days=max(bootstrap_days, 1))).strftime("%Y%m%d")


def load_listed_set(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT ts_code FROM stock_codes WHERE list_status='L'").fetchall()
    return {r[0] for r in rows}


def upsert_price_rows(conn: sqlite3.Connection, price_table: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {price_table} (
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
    return len(rows)


def main() -> int:
    args = parse_args()
    end_date = args.end_date.strip() or utc_today()
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"错误: 数据库不存在: {db_path}", file=sys.stderr)
        return 1

    pro = ts.pro_api(resolve_tushare_token(args.token))
    conn = sqlite3.connect(db_path)
    try:
        ensure_tables(conn, args.price_table)

        total_codes = upsert_stock_codes(conn, pro)
        listed_set = load_listed_set(conn)
        print(f"股票列表更新完成: total={total_codes}, listed={len(listed_set)}")

        start_date = get_start_date(conn, args.price_table, end_date, args.bootstrap_days)
        print(f"开始增量更新日线: {start_date} ~ {end_date}")

        cal = pro.trade_cal(
            exchange="SSE",
            start_date=start_date,
            end_date=end_date,
            is_open="1",
            fields="cal_date",
        )
        if cal is None or cal.empty:
            print("无可更新交易日。")
            return 0

        trade_dates = sorted(cal["cal_date"].tolist())
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
                    filtered = df[df["ts_code"].isin(listed_set)]
                    rows = list(filtered.itertuples(index=False, name=None))
                    n = upsert_price_rows(conn, args.price_table, rows)
                    total_rows += n
                    print(f"[{idx}/{len(trade_dates)}] {d}: upsert={n}")
            except Exception as exc:
                failed_days += 1
                print(f"[{idx}/{len(trade_dates)}] {d}: 失败 -> {exc}", file=sys.stderr)
            if args.pause > 0:
                time.sleep(args.pause)

        table_rows = conn.execute(f"SELECT COUNT(*) FROM {args.price_table}").fetchone()[0]
        print(
            f"完成: failed_days={failed_days}, updated_rows={total_rows}, table_rows={table_rows}"
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
