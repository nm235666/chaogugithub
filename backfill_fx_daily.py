#!/usr/bin/env python3
"""
回填汇率日线到 fx_daily。

数据来源：
- tushare.pro.fx_daily

默认抓取：
- USDCNH.FXCM
- USDJPY.FXCM
- EURUSD.FXCM
- GBPUSD.FXCM
- AUDUSD.FXCM

写入策略：
- 采用 bid / ask 中间价作为 open/high/low/close
- pct_chg 使用 close 对前一日 close 的变化率
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
DEFAULT_PAIRS = [
    "USDCNH.FXCM",
    "USDJPY.FXCM",
    "EURUSD.FXCM",
    "GBPUSD.FXCM",
    "AUDUSD.FXCM",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回填汇率日线到 fx_daily")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--table-name", default="fx_daily", help="目标表名")
    parser.add_argument("--pairs", default=",".join(DEFAULT_PAIRS), help="币对列表，逗号分隔")
    parser.add_argument("--lookback-days", type=int, default=365, help="回溯天数，默认365")
    parser.add_argument("--start-date", default="", help="开始日期(YYYYMMDD)")
    parser.add_argument("--end-date", default="", help="结束日期(YYYYMMDD)")
    parser.add_argument("--pause", type=float, default=0.08, help="每个币对请求后暂停秒数")
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
            pair_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            pct_chg REAL,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (pair_code, trade_date)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date)")
    conn.commit()


def mid(a, b):
    if a is None and b is None:
        return None
    if a is None:
        return float(b)
    if b is None:
        return float(a)
    return (float(a) + float(b)) / 2.0


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {table_name} (
        pair_code, trade_date, open, high, low, close, pct_chg, source, update_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(pair_code, trade_date) DO UPDATE SET
        open=excluded.open,
        high=excluded.high,
        low=excluded.low,
        close=excluded.close,
        pct_chg=excluded.pct_chg,
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
    pairs = [x.strip().upper() for x in args.pairs.split(",") if x.strip()]

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

        total_rows = 0
        failed = 0
        update_time = utc_now()
        for idx, pair_code in enumerate(pairs, start=1):
            try:
                df = pro.fx_daily(ts_code=pair_code, start_date=start_date, end_date=end_date)
                if df is None or df.empty:
                    print(f"[{idx}/{len(pairs)}] {pair_code}: 无数据")
                else:
                    df = df.sort_values("trade_date").reset_index(drop=True)
                    rows = []
                    prev_close = None
                    for row in df.itertuples(index=False):
                        open_mid = mid(row.bid_open, row.ask_open)
                        close_mid = mid(row.bid_close, row.ask_close)
                        high_mid = mid(row.bid_high, row.ask_high)
                        low_mid = mid(row.bid_low, row.ask_low)
                        pct_chg = None
                        if prev_close not in (None, 0) and close_mid is not None:
                            pct_chg = (close_mid - prev_close) / prev_close * 100
                        rows.append(
                            (
                                pair_code,
                                row.trade_date,
                                open_mid,
                                high_mid,
                                low_mid,
                                close_mid,
                                pct_chg,
                                "tushare.fx_daily",
                                update_time,
                            )
                        )
                        prev_close = close_mid
                    n = upsert_rows(conn, args.table_name, rows)
                    total_rows += n
                    print(f"[{idx}/{len(pairs)}] {pair_code}: rows={len(rows)} upsert={n}")
            except Exception as exc:
                failed += 1
                print(f"[{idx}/{len(pairs)}] {pair_code}: 失败 -> {exc}", file=sys.stderr)

            if args.pause > 0:
                time.sleep(args.pause)

        final_rows = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(
            f"完成: failed={failed}, upsert_rows={total_rows}, table_rows={final_rows}, range={start_date}~{end_date}"
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
