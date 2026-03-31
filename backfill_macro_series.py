#!/usr/bin/env python3
from __future__ import annotations

import argparse
import db_compat as sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from runtime_secrets import TUSHARE_TOKEN, resolve_tushare_token

LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import pandas as pd
import tushare as ts

DEFAULT_TOKEN = TUSHARE_TOKEN

# 先给一个“可扩展”的默认宏观接口清单
# 备注: 不同接口参数不同，脚本按“尽量抓 + 失败跳过”策略执行
DEFAULT_API_SPECS = [
    {"api": "cn_gdp", "freq": "Q"},
    {"api": "cn_cpi", "freq": "M"},
    {"api": "cn_ppi", "freq": "M"},
    {"api": "cn_pmi", "freq": "M"},
    {"api": "cn_sf", "freq": "M"},  # 社融
    {"api": "cn_m", "freq": "M"},   # 货币供应
    {"api": "shibor", "freq": "D"}, # 利率
]

ID_LIKE_COLS = {
    "ts_code",
    "symbol",
    "code",
    "name",
    "country",
    "area",
    "market",
    "source",
}

DATE_CANDIDATES = [
    "trade_date",
    "date",
    "month",
    "year",
    "quarter",
    "period",
    "stat_month",
    "ann_date",
    "end_date",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="补宏观数据到 macro_series（Tushare -> PostgreSQL）")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument(
        "--apis",
        default=",".join([x["api"] for x in DEFAULT_API_SPECS]),
        help="要抓取的接口名，逗号分隔；默认抓取常用宏观接口",
    )
    parser.add_argument("--max-apis", type=int, default=0, help="最多处理多少个接口，0=全部")
    parser.add_argument("--retry", type=int, default=3, help="单接口失败重试次数")
    parser.add_argument("--backoff", type=float, default=1.0, help="重试退避基数（秒）")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS macro_series (
            indicator_code TEXT NOT NULL,
            indicator_name TEXT,
            freq TEXT NOT NULL,
            period TEXT NOT NULL,
            value REAL,
            unit TEXT,
            source TEXT,
            publish_date TEXT,
            update_time TEXT,
            PRIMARY KEY (indicator_code, freq, period)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_macro_series_period ON macro_series(period)")
    conn.commit()


def normalize_period(row: pd.Series) -> str:
    for c in DATE_CANDIDATES:
        if c in row.index and pd.notna(row[c]) and str(row[c]).strip():
            return str(row[c]).strip()
    return ""


def iter_numeric_fields(df: pd.DataFrame) -> list[str]:
    fields: list[str] = []
    for col in df.columns:
        lc = col.lower()
        if lc in ID_LIKE_COLS or lc in DATE_CANDIDATES:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        if s.notna().any():
            fields.append(col)
    return fields


def to_rows(api_name: str, freq: str, df: pd.DataFrame) -> list[tuple]:
    rows: list[tuple] = []
    numeric_fields = iter_numeric_fields(df)
    update_time = now_utc_str()

    for _, row in df.iterrows():
        period = normalize_period(row)
        if not period:
            continue

        publish_date = ""
        for c in ("ann_date", "publish_date"):
            if c in row.index and pd.notna(row[c]) and str(row[c]).strip():
                publish_date = str(row[c]).strip()
                break

        for f in numeric_fields:
            v = pd.to_numeric(row.get(f), errors="coerce")
            if pd.isna(v):
                continue
            indicator_code = f"{api_name}.{f}"
            rows.append(
                (
                    indicator_code,
                    f,
                    freq,
                    period,
                    float(v),
                    "",
                    "tushare",
                    publish_date,
                    update_time,
                )
            )
    return rows


def upsert_rows(conn: sqlite3.Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT INTO macro_series (
            indicator_code, indicator_name, freq, period, value, unit, source, publish_date, update_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(indicator_code, freq, period) DO UPDATE SET
            indicator_name=excluded.indicator_name,
            value=excluded.value,
            unit=excluded.unit,
            source=excluded.source,
            publish_date=excluded.publish_date,
            update_time=excluded.update_time
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"数据库不存在: {db_path}", file=sys.stderr)
        return 1

    api_names = [x.strip() for x in args.apis.split(",") if x.strip()]
    if args.max_apis and args.max_apis > 0:
        api_names = api_names[: args.max_apis]
    if not api_names:
        print("没有可执行的接口名。", file=sys.stderr)
        return 2

    spec_map = {x["api"]: x for x in DEFAULT_API_SPECS}
    pro = ts.pro_api(resolve_tushare_token(args.token))

    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn)
        total_written = 0
        success = 0
        failed = 0

        for i, api_name in enumerate(api_names, start=1):
            freq = spec_map.get(api_name, {}).get("freq", "M")
            last_err = None
            for attempt in range(args.retry + 1):
                try:
                    # 统一通过 query 调用，避免某些环境缺方法属性
                    df = pro.query(api_name)
                    if df is None or df.empty:
                        print(f"[{i}/{len(api_names)}] {api_name}: 空数据")
                        success += 1
                        last_err = None
                        break
                    rows = to_rows(api_name, freq, df)
                    n = upsert_rows(conn, rows)
                    total_written += n
                    success += 1
                    print(
                        f"[{i}/{len(api_names)}] {api_name}: raw_rows={len(df)} metric_rows={n}"
                    )
                    last_err = None
                    break
                except Exception as exc:
                    last_err = exc
                    if attempt < args.retry:
                        sleep_s = args.backoff * (2**attempt)
                        print(
                            f"[{i}/{len(api_names)}] {api_name}: 失败重试 {attempt + 1}/{args.retry}, "
                            f"等待 {sleep_s:.1f}s -> {exc}",
                            file=sys.stderr,
                        )
                        time.sleep(sleep_s)
            if last_err is not None:
                failed += 1
                print(f"[{i}/{len(api_names)}] {api_name}: 最终失败 -> {last_err}", file=sys.stderr)

        total = conn.execute("SELECT COUNT(*) FROM macro_series").fetchone()[0]
        print(
            f"完成: success={success}, failed={failed}, written={total_written}, table_rows={total}"
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
