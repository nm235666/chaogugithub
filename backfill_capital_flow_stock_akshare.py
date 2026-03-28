#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3

ROOT_DIR = Path(__file__).resolve().parent
DEPS_DIR = ROOT_DIR / ".deps"
if DEPS_DIR.exists() and str(DEPS_DIR) not in sys.path:
    sys.path.insert(0, str(DEPS_DIR))

try:
    import akshare as ak  # type: ignore
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"无法导入 akshare: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="使用 AKShare 回填个股资金流到 capital_flow_stock")
    parser.add_argument(
        "--db-path",
        default=str(ROOT_DIR / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--table-name", default="capital_flow_stock", help="目标表名")
    parser.add_argument("--ts-code", default="", help="单只股票代码，如 920001.BJ")
    parser.add_argument("--resume-from", default="", help="从某个 ts_code 开始(含)")
    parser.add_argument("--limit-stocks", type=int, default=0, help="最多处理多少只，0=不限制")
    parser.add_argument("--only-bj", action="store_true", help="仅处理北交所股票")
    parser.add_argument("--missing-only", action="store_true", help="仅处理当前库里还没有资金流记录的股票")
    parser.add_argument("--pause", type=float, default=0.1, help="每只股票抓取后暂停秒数")
    parser.add_argument("--start-date", default="", help="开始日期 YYYYMMDD，可选")
    parser.add_argument("--end-date", default="", help="结束日期 YYYYMMDD，可选")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_trade ON {table_name}(ts_code, trade_date)")
    conn.commit()


def load_target_codes(
    conn: sqlite3.Connection,
    ts_code: str,
    resume_from: str,
    limit_stocks: int,
    only_bj: bool,
    missing_only: bool,
) -> list[tuple[str, str]]:
    ts_code = (ts_code or "").strip().upper()
    if ts_code:
        row = conn.execute("SELECT ts_code, name FROM stock_codes WHERE ts_code = ?", (ts_code,)).fetchone()
        return [(row[0], row[1] if len(row) > 1 else "")] if row else []

    where = ["list_status='L'"]
    params: list[object] = []
    if only_bj:
        where.append("split_part(ts_code, '.', 2) = 'BJ'")
    if resume_from:
        where.append("ts_code >= ?")
        params.append(resume_from.strip().upper())
    if missing_only:
        where.append(
            f"NOT EXISTS (SELECT 1 FROM capital_flow_stock c WHERE c.ts_code = stock_codes.ts_code)"
        )
    sql = "SELECT ts_code, name FROM stock_codes"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts_code"
    if limit_stocks > 0:
        sql += " LIMIT ?"
        params.append(limit_stocks)
    return [(r[0], r[1]) for r in conn.execute(sql, params).fetchall()]


def ts_to_parts(ts_code: str) -> tuple[str, str]:
    code = (ts_code or "").strip().upper()
    if "." not in code:
        raise ValueError("ts_code 格式应为 920001.BJ / 000001.SZ / 600000.SH")
    symbol, exch = code.split(".", 1)
    return symbol, exch


def market_for_ak(exch: str) -> str:
    mapping = {"SH": "sh", "SZ": "sz", "BJ": "bj"}
    if exch not in mapping:
        raise ValueError(f"不支持的交易所: {exch}")
    return mapping[exch]


def fetch_rows_from_ak(ts_code: str, start_date: str, end_date: str) -> list[tuple]:
    symbol, exch = ts_to_parts(ts_code)
    market = market_for_ak(exch)
    df = ak.stock_individual_fund_flow(stock=symbol, market=market)
    if df is None or getattr(df, "empty", True):
        return []

    update_time = utc_now()
    rows: list[tuple] = []
    for item in df.to_dict("records"):
        dt = item.get("日期")
        if not dt:
            continue
        trade_date = str(dt).replace("-", "")
        if start_date and trade_date < start_date:
            continue
        if end_date and trade_date > end_date:
            continue
        rows.append(
            (
                ts_code,
                trade_date,
                _to_float(item.get("主力净流入-净额")),
                _to_float(item.get("主力净流入-净额")),
                _to_float(item.get("超大单净流入-净额")),
                _to_float(item.get("大单净流入-净额")),
                _to_float(item.get("中单净流入-净额")),
                _to_float(item.get("小单净流入-净额")),
                "akshare.stock_individual_fund_flow",
                update_time,
            )
        )
    return rows


def _to_float(value):
    if value in (None, ""):
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
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        raise SystemExit(f"数据库不存在: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        targets = load_target_codes(
            conn,
            ts_code=args.ts_code,
            resume_from=args.resume_from,
            limit_stocks=args.limit_stocks,
            only_bj=args.only_bj,
            missing_only=args.missing_only,
        )
        if not targets:
            print("没有找到待处理股票")
            return 0

        ok = 0
        failed = 0
        inserted_rows = 0
        for idx, (ts_code, _name) in enumerate(targets, start=1):
            try:
                rows = fetch_rows_from_ak(
                    ts_code=ts_code,
                    start_date=(args.start_date or "").strip(),
                    end_date=(args.end_date or "").strip(),
                )
                n = upsert_rows(conn, args.table_name, rows)
                inserted_rows += n
                ok += 1
                print(f"[{idx}/{len(targets)}] {ts_code}: fetched={len(rows)} upsert={n}")
            except Exception as exc:
                failed += 1
                print(f"[{idx}/{len(targets)}] {ts_code}: failed -> {exc}")
            if args.pause > 0:
                time.sleep(args.pause)

        print(
            f"完成: total={len(targets)}, ok={ok}, failed={failed}, "
            f"upsert_rows={inserted_rows}, table={args.table_name}"
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
