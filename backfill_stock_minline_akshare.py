#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
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

from market_calendar import DEFAULT_TOKEN, resolve_trade_date
from realtime_streams import publish_app_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="使用 AKShare 回填股票分钟线到 stock_minline")
    parser.add_argument(
        "--db-path",
        default=str(ROOT_DIR / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--table-name", default="stock_minline", help="分钟线表名")
    parser.add_argument("--ts-code", default="", help="单只股票代码，如 000001.SZ")
    parser.add_argument("--resume-from", default="", help="从某个 ts_code 开始(含)")
    parser.add_argument("--limit-stocks", type=int, default=0, help="最多处理多少只，0=不限制")
    parser.add_argument("--trade-date", default="", help="交易日 YYYYMMDD，默认自动按交易日历解析")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token，用于解析交易日")
    parser.add_argument("--adjust", default="", choices=["", "qfq", "hfq"], help="AKShare 复权方式")
    parser.add_argument("--skip-existing", action="store_true", help="若该股票该交易日已有数据则跳过")
    return parser.parse_args()


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            minute_time TEXT NOT NULL,
            price REAL,
            avg_price REAL,
            volume REAL,
            total_volume REAL,
            source TEXT DEFAULT 'akshare.stock_zh_a_hist_min_em',
            PRIMARY KEY (ts_code, trade_date, minute_time),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_td_time ON {table_name}(trade_date, minute_time)"
    )
    conn.commit()


def load_target_codes(
    conn: sqlite3.Connection, ts_code: str, resume_from: str, limit_stocks: int
) -> list[str]:
    ts_code = (ts_code or "").strip().upper()
    if ts_code:
        return [ts_code]
    sql = "SELECT ts_code FROM stock_codes WHERE list_status='L'"
    params: list[object] = []
    if resume_from:
        sql += " AND ts_code >= ?"
        params.append(resume_from.strip().upper())
    sql += " ORDER BY ts_code"
    if limit_stocks > 0:
        sql += " LIMIT ?"
        params.append(limit_stocks)
    return [r[0] for r in conn.execute(sql, params).fetchall()]


def ts_to_symbol(ts_code: str) -> str:
    code = (ts_code or "").strip().upper()
    if "." not in code:
        raise ValueError("ts_code 格式应为 000001.SZ / 600000.SH / 430047.BJ")
    symbol, _exch = code.split(".", 1)
    return symbol


def existing_rows(conn: sqlite3.Connection, table_name: str, ts_code: str, trade_date: str) -> int:
    return int(
        conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE ts_code = ? AND trade_date = ?",
            (ts_code, trade_date),
        ).fetchone()[0]
    )


def fetch_akshare_rows(ts_code: str, trade_date: str, adjust: str) -> list[tuple]:
    symbol = ts_to_symbol(ts_code)
    start_dt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]} 09:30:00"
    end_dt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]} 15:00:00"
    df = ak.stock_zh_a_hist_min_em(
        symbol=symbol,
        start_date=start_dt,
        end_date=end_dt,
        period="1",
        adjust=adjust,
    )
    if df is None or getattr(df, "empty", True):
        return []

    rows: list[tuple] = []
    total_volume = 0.0
    for item in df.to_dict("records"):
        time_text = str(item.get("时间", "")).strip()
        if not time_text:
            continue
        time_part = time_text.split(" ")[-1]
        if len(time_part) == 5:
            time_part = f"{time_part}:00"
        price = _to_float(item.get("收盘"))
        avg_price = _to_float(item.get("均价"))
        volume = _to_float(item.get("成交量"))
        if volume is not None:
            total_volume += volume
        rows.append(
            (
                ts_code,
                trade_date,
                time_part,
                price,
                avg_price,
                volume,
                total_volume if volume is not None else None,
                "akshare.stock_zh_a_hist_min_em",
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
        ts_code, trade_date, minute_time, price, avg_price, volume, total_volume, source
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, trade_date, minute_time) DO UPDATE SET
        price=excluded.price,
        avg_price=excluded.avg_price,
        volume=excluded.volume,
        total_volume=excluded.total_volume,
        source=excluded.source
    """
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        raise SystemExit(f"数据库不存在: {db_path}")

    trade_date = resolve_trade_date(args.trade_date, args.token)

    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        codes = load_target_codes(conn, args.ts_code, args.resume_from, args.limit_stocks)
        if not codes:
            print("没有找到待处理股票")
            return 0

        ok = 0
        skipped = 0
        failed = 0
        for idx, ts_code in enumerate(codes, start=1):
            try:
                if args.skip_existing and existing_rows(conn, args.table_name, ts_code, trade_date) > 0:
                    skipped += 1
                    print(f"[{idx}/{len(codes)}] {ts_code}: skip existing")
                    continue
                rows = fetch_akshare_rows(ts_code=ts_code, trade_date=trade_date, adjust=args.adjust)
                if not rows:
                    failed += 1
                    print(f"[{idx}/{len(codes)}] {ts_code}: no data")
                    continue
                count = upsert_rows(conn, args.table_name, rows)
                ok += 1
                print(f"[{idx}/{len(codes)}] {ts_code}: ok rows={count}")
            except Exception as exc:
                failed += 1
                print(f"[{idx}/{len(codes)}] {ts_code}: failed -> {exc}")

        publish_app_event(
            event="minline_batch_update",
            payload={
                "trade_date": trade_date,
                "rows_source": "akshare.stock_zh_a_hist_min_em",
                "stocks_total": len(codes),
                "stocks_ok": ok,
                "stocks_skipped": skipped,
                "stocks_failed": failed,
                "table": args.table_name,
            },
            producer="backfill_stock_minline_akshare.py",
        )
        print(
            f"完成: trade_date={trade_date}, total={len(codes)}, ok={ok}, "
            f"skipped={skipped}, failed={failed}, table={args.table_name}"
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
