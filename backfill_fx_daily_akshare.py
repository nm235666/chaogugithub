#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import db_compat as sqlite3
from runtime_secrets import TUSHARE_TOKEN, resolve_tushare_token

LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import akshare as ak
import pandas as pd
import tushare as ts

DEFAULT_TOKEN = TUSHARE_TOKEN
DEFAULT_PAIRS = ["USDJPY", "EURUSD", "DXY", "USDCNY"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="使用 AKShare + Tushare 双源回填汇率日线到 fx_daily")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--table-name", default="fx_daily", help="目标表名")
    parser.add_argument("--pairs", default=",".join(DEFAULT_PAIRS), help="标准币对列表，逗号分隔")
    parser.add_argument("--lookback-days", type=int, default=365, help="回溯天数")
    parser.add_argument("--start-date", default="", help="开始日期 YYYYMMDD")
    parser.add_argument("--end-date", default="", help="结束日期 YYYYMMDD")
    parser.add_argument("--pause", type=float, default=0.08, help="每个品种处理后暂停秒数")
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


def safe_float(value):
    try:
        if value is None:
            return None
        num = float(value)
        if math.isnan(num):
            return None
        return num
    except Exception:
        return None


def normalize_trade_date(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value or "").strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return text


def compute_pct(rows: list[dict]) -> list[dict]:
    rows = sorted(rows, key=lambda x: x["trade_date"])
    prev_close = None
    for row in rows:
        close = row.get("close")
        pct = None
        if prev_close not in (None, 0) and close is not None:
            pct = (close - prev_close) / prev_close * 100
        row["pct_chg"] = pct
        if close is not None:
            prev_close = close
    return rows


def fetch_tushare_pair(pro, canonical_pair: str, start_date: str, end_date: str) -> tuple[str, list[dict]]:
    ts_code_map = {
        "USDJPY": "USDJPY.FXCM",
        "EURUSD": "EURUSD.FXCM",
        "USDCNH": "USDCNH.FXCM",
    }
    ts_code = ts_code_map[canonical_pair]
    df = pro.fx_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return "tushare.fx_daily", []
    rows: list[dict] = []
    for row in df.itertuples(index=False):
        open_mid = mid(row.bid_open, row.ask_open)
        close_mid = mid(row.bid_close, row.ask_close)
        high_mid = mid(row.bid_high, row.ask_high)
        low_mid = mid(row.bid_low, row.ask_low)
        rows.append(
            {
                "trade_date": str(row.trade_date),
                "open": open_mid,
                "high": high_mid,
                "low": low_mid,
                "close": close_mid,
            }
        )
    return "tushare.fx_daily", compute_pct(rows)


def mid(a, b):
    if a is None and b is None:
        return None
    if a is None:
        return float(b)
    if b is None:
        return float(a)
    return (float(a) + float(b)) / 2.0


def fetch_akshare_usdcny(start_date: str, end_date: str) -> tuple[str, list[dict]]:
    df = ak.currency_boc_sina(symbol="美元", start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return "akshare.currency_boc_sina", []
    rows: list[dict] = []
    for item in df.to_dict("records"):
        trade_date = normalize_trade_date(item.get("日期"))
        if not trade_date:
            continue
        close = safe_float(item.get("央行中间价"))
        if close is None:
            close = safe_float(item.get("中行折算价"))
        if close is None:
            close = safe_float(item.get("中行汇买价"))
        if close is None:
            continue
        close = close / 100.0
        rows.append(
            {
                "trade_date": trade_date,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
            }
        )
    return "akshare.currency_boc_sina", compute_pct(rows)


def fetch_akshare_dxy(start_date: str, end_date: str) -> tuple[str, list[dict]]:
    df = ak.index_global_hist_em(symbol="美元指数")
    if df is None or df.empty:
        return "akshare.index_global_hist_em", []
    rows: list[dict] = []
    for item in df.to_dict("records"):
        trade_date = normalize_trade_date(item.get("日期"))
        if not trade_date:
            continue
        if trade_date < start_date or trade_date > end_date:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "open": safe_float(item.get("今开")),
                "high": safe_float(item.get("最高")),
                "low": safe_float(item.get("最低")),
                "close": safe_float(item.get("最新价")),
                "pct_chg": safe_float(item.get("涨跌幅")),
            }
        )
    rows = sorted(rows, key=lambda x: x["trade_date"])
    if rows and all(r.get("pct_chg") is None for r in rows):
        rows = compute_pct(rows)
    return "akshare.index_global_hist_em", rows


def fetch_pair(pro, pair_code: str, start_date: str, end_date: str) -> tuple[str, list[dict]]:
    pair = pair_code.strip().upper()
    if pair == "USDCNY":
        return fetch_akshare_usdcny(start_date=start_date, end_date=end_date)
    if pair == "DXY":
        return fetch_akshare_dxy(start_date=start_date, end_date=end_date)
    if pair in {"USDJPY", "EURUSD"}:
        return fetch_tushare_pair(pro, canonical_pair=pair, start_date=start_date, end_date=end_date)
    raise ValueError(f"暂不支持的 pair_code: {pair}")


def upsert_rows(conn: sqlite3.Connection, table_name: str, pair_code: str, rows: list[dict], source: str) -> int:
    if not rows:
        return 0
    update_time = utc_now()
    values = [
        (
            pair_code,
            row["trade_date"],
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            row.get("pct_chg"),
            source,
            update_time,
        )
        for row in rows
    ]
    conn.executemany(
        f"""
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
        """,
        values,
    )
    conn.commit()
    return len(values)


def main() -> int:
    args = parse_args()
    end_date = args.end_date.strip() or utc_today()
    start_date = args.start_date.strip() or calc_start(end_date, args.lookback_days)
    pairs = [x.strip().upper() for x in args.pairs.split(",") if x.strip()]

    pro = ts.pro_api(resolve_tushare_token(args.token))
    conn = sqlite3.connect(args.db_path)
    try:
        ensure_table(conn, args.table_name)
        if args.truncate:
            conn.execute(f"DELETE FROM {args.table_name}")
            conn.commit()

        total_rows = 0
        failed = 0
        for idx, pair_code in enumerate(pairs, start=1):
            try:
                source, rows = fetch_pair(pro=pro, pair_code=pair_code, start_date=start_date, end_date=end_date)
                n = upsert_rows(conn, args.table_name, pair_code=pair_code, rows=rows, source=source)
                total_rows += n
                print(f"[{idx}/{len(pairs)}] {pair_code}: rows={len(rows)} upsert={n} source={source}")
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"[{idx}/{len(pairs)}] {pair_code}: 失败 -> {exc}", file=sys.stderr)
            if args.pause > 0:
                time.sleep(args.pause)

        final_rows = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(f"完成: failed={failed}, upsert_rows={total_rows}, table_rows={final_rows}, range={start_date}~{end_date}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
