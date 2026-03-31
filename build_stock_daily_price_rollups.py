#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import db_compat as sqlite3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="预计算股票日线常用窗口汇总（30/90/365天）")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--table-name", default="stock_daily_price_rollups", help="目标汇总表名")
    parser.add_argument("--window-days", default="30,90,365", help="窗口天数，逗号分隔")
    parser.add_argument("--latest-trade-date", default="", help="指定汇总终止交易日(YYYYMMDD)，默认自动取库内最大交易日")
    parser.add_argument("--limit-stocks", type=int, default=0, help="仅处理前 N 只股票（0=全量）")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_code TEXT NOT NULL,
            window_days INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            rows_count INTEGER NOT NULL,
            close_first REAL,
            close_last REAL,
            close_change_pct REAL,
            high_max REAL,
            low_min REAL,
            vol_avg REAL,
            amount_avg REAL,
            update_time TEXT NOT NULL,
            PRIMARY KEY (ts_code, window_days, end_date)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_window_end ON {table_name}(window_days, end_date)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_window ON {table_name}(ts_code, window_days)")
    conn.commit()


def resolve_latest_trade_date(conn: sqlite3.Connection, arg_value: str) -> str:
    if arg_value:
        return arg_value.strip()
    row = conn.execute("SELECT MAX(trade_date) FROM stock_daily_prices").fetchone()
    latest = (row[0] if row else "") or ""
    if not latest:
        raise RuntimeError("stock_daily_prices 暂无数据，无法构建汇总。")
    return str(latest)


def calc_start_date(end_date: str, days: int) -> str:
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    return (end_dt - timedelta(days=max(days, 1))).strftime("%Y%m%d")


def build_window_rollups(
    conn: sqlite3.Connection,
    end_date: str,
    start_date: str,
    limit_stocks: int,
) -> list[dict]:
    limit_sql = ""
    params: list[object] = []
    if limit_stocks > 0:
        limit_sql = "LIMIT ?"
        params.append(limit_stocks)
    params.extend([start_date, end_date])
    sql = f"""
    WITH scope_codes AS (
        SELECT ts_code
        FROM stock_codes
        WHERE list_status = 'L'
        ORDER BY ts_code
        {limit_sql}
    ),
    scoped AS (
        SELECT p.ts_code, p.trade_date, p.close, p.high, p.low, p.vol, p.amount
        FROM stock_daily_prices p
        INNER JOIN scope_codes c ON c.ts_code = p.ts_code
        WHERE p.trade_date >= ? AND p.trade_date <= ?
    ),
    ranked AS (
        SELECT
            ts_code,
            trade_date,
            close,
            high,
            low,
            vol,
            amount,
            ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn_desc,
            ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date ASC) AS rn_asc
        FROM scoped
    )
    SELECT
        ts_code,
        MIN(trade_date) AS start_date,
        MAX(trade_date) AS end_date,
        COUNT(*) AS rows_count,
        MAX(CASE WHEN rn_asc = 1 THEN close END) AS close_first,
        MAX(CASE WHEN rn_desc = 1 THEN close END) AS close_last,
        MAX(high) AS high_max,
        MIN(low) AS low_min,
        AVG(vol) AS vol_avg,
        AVG(amount) AS amount_avg
    FROM ranked
    GROUP BY ts_code
    """
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def upsert_rollups(conn: sqlite3.Connection, table_name: str, window_days: int, rows: list[dict]) -> int:
    if not rows:
        return 0
    now = utc_now()
    payload = []
    for row in rows:
        close_first = row.get("close_first")
        close_last = row.get("close_last")
        close_change_pct = None
        try:
            if close_first not in (None, 0, 0.0) and close_last is not None:
                close_change_pct = (float(close_last) - float(close_first)) / float(close_first) * 100.0
        except Exception:
            close_change_pct = None
        payload.append(
            (
                row.get("ts_code"),
                window_days,
                row.get("start_date"),
                row.get("end_date"),
                int(row.get("rows_count") or 0),
                close_first,
                close_last,
                close_change_pct,
                row.get("high_max"),
                row.get("low_min"),
                row.get("vol_avg"),
                row.get("amount_avg"),
                now,
            )
        )

    sql = f"""
    INSERT INTO {table_name} (
        ts_code, window_days, start_date, end_date, rows_count,
        close_first, close_last, close_change_pct, high_max, low_min, vol_avg, amount_avg, update_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, window_days, end_date) DO UPDATE SET
        start_date=excluded.start_date,
        rows_count=excluded.rows_count,
        close_first=excluded.close_first,
        close_last=excluded.close_last,
        close_change_pct=excluded.close_change_pct,
        high_max=excluded.high_max,
        low_min=excluded.low_min,
        vol_avg=excluded.vol_avg,
        amount_avg=excluded.amount_avg,
        update_time=excluded.update_time
    """
    cur = conn.cursor()
    cur.executemany(sql, payload)
    conn.commit()
    return len(payload)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"数据库不存在: {db_path}", file=sys.stderr)
        return 1

    try:
        windows = [int(x.strip()) for x in args.window_days.split(",") if x.strip()]
    except ValueError:
        print("window-days 格式错误，示例: 30,90,365", file=sys.stderr)
        return 1
    windows = sorted(set([x for x in windows if x > 0]))
    if not windows:
        print("window-days 至少要有一个正整数", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_table(conn, args.table_name)
        latest_trade_date = resolve_latest_trade_date(conn, args.latest_trade_date)
        print(f"开始构建日线预聚合: latest_trade_date={latest_trade_date}, windows={windows}")
        total = 0
        for window in windows:
            start_date = calc_start_date(latest_trade_date, window)
            rows = build_window_rollups(
                conn=conn,
                end_date=latest_trade_date,
                start_date=start_date,
                limit_stocks=max(args.limit_stocks, 0),
            )
            n = upsert_rollups(conn, args.table_name, window, rows)
            total += n
            print(f"  window={window}d start={start_date} end={latest_trade_date}: upsert={n}")
        print(f"完成: total_upsert={total}, table={args.table_name}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
