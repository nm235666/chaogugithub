#!/usr/bin/env python3
"""
回填利率曲线点到 rate_curve_points。

当前接入：
- CN / shibor      : ON, 1W, 2W, 1M, 3M, 6M, 9M, 1Y
- US / treasury    : 1M, 2M, 3M, 6M, 1Y, 2Y, 3Y, 5Y, 7Y, 10Y, 20Y, 30Y

用法示例：
  python3 backfill_rate_curve_points.py
  python3 backfill_rate_curve_points.py --lookback-days 365
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

CN_SHIBOR_MAP = {
    "on": "ON",
    "1w": "1W",
    "2w": "2W",
    "1m": "1M",
    "3m": "3M",
    "6m": "6M",
    "9m": "9M",
    "1y": "1Y",
}

US_TY_MAP = {
    "m1": "1M",
    "m2": "2M",
    "m3": "3M",
    "m6": "6M",
    "y1": "1Y",
    "y2": "2Y",
    "y3": "3Y",
    "y5": "5Y",
    "y7": "7Y",
    "y10": "10Y",
    "y20": "20Y",
    "y30": "30Y",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回填利率曲线点到 rate_curve_points")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--table-name", default="rate_curve_points", help="目标表名")
    parser.add_argument("--lookback-days", type=int, default=365, help="回溯天数，默认365")
    parser.add_argument("--start-date", default="", help="开始日期(YYYYMMDD)")
    parser.add_argument("--end-date", default="", help="结束日期(YYYYMMDD)")
    parser.add_argument("--pause", type=float, default=0.05, help="接口间暂停秒数")
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
            market TEXT NOT NULL,
            curve_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            tenor TEXT NOT NULL,
            value REAL,
            unit TEXT DEFAULT 'pct',
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (market, curve_code, trade_date, tenor)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(trade_date)")
    conn.commit()


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {table_name} (
        market, curve_code, trade_date, tenor, value, unit, source, update_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(market, curve_code, trade_date, tenor) DO UPDATE SET
        value=excluded.value,
        unit=excluded.unit,
        source=excluded.source,
        update_time=excluded.update_time
    """
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def build_curve_rows(df, market: str, curve_code: str, tenor_map: dict[str, str], source: str, update_time: str) -> list[tuple]:
    rows: list[tuple] = []
    if df is None or df.empty:
        return rows
    date_col = "date" if "date" in df.columns else "trade_date"
    for item in df.to_dict("records"):
        trade_date = str(item.get(date_col) or "").strip()
        if not trade_date:
            continue
        for raw_key, tenor in tenor_map.items():
            value = item.get(raw_key)
            if value in (None, "", "None"):
                continue
            rows.append(
                (
                    market,
                    curve_code,
                    trade_date,
                    tenor,
                    float(value),
                    "pct",
                    source,
                    update_time,
                )
            )
    return rows


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

        update_time = utc_now()
        total_rows = 0

        shibor_df = pro.shibor(start_date=start_date, end_date=end_date)
        shibor_rows = build_curve_rows(
            shibor_df,
            market="CN",
            curve_code="shibor",
            tenor_map=CN_SHIBOR_MAP,
            source="tushare.shibor",
            update_time=update_time,
        )
        total_rows += upsert_rows(conn, args.table_name, shibor_rows)
        print(f"CN shibor: dates={0 if shibor_df is None else len(shibor_df)} rows={len(shibor_rows)}")

        if args.pause > 0:
            time.sleep(args.pause)

        us_df = pro.us_tycr(start_date=start_date, end_date=end_date)
        us_rows = build_curve_rows(
            us_df,
            market="US",
            curve_code="treasury",
            tenor_map=US_TY_MAP,
            source="tushare.us_tycr",
            update_time=update_time,
        )
        total_rows += upsert_rows(conn, args.table_name, us_rows)
        print(f"US treasury: dates={0 if us_df is None else len(us_df)} rows={len(us_rows)}")

        final_count = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(
            f"完成: upsert_rows={total_rows}, table_rows={final_count}, range={start_date}~{end_date}"
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
