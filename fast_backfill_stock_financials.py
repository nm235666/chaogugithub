#!/usr/bin/env python3
"""
高速版：按报告期批量回填财务核心指标到 stock_financials。

核心思路：
- 使用 tushare 的 *_vip 接口按 period 批量抓全市场
- 再过滤为 stock_codes 中目标股票
- 比逐股 4 接口抓取快很多

默认行为：
- 仅抓当前上市股票
- 回填最近 8 个报告期

用法示例：
  python3 fast_backfill_stock_financials.py
  python3 fast_backfill_stock_financials.py --recent-periods 12
  python3 fast_backfill_stock_financials.py --periods 20251231,20250930,20250630,20250331
"""

from __future__ import annotations

import argparse
import math
import db_compat as sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from runtime_secrets import TUSHARE_TOKEN, resolve_tushare_token

LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import tushare as ts

DEFAULT_TOKEN = TUSHARE_TOKEN
QUARTER_ENDS = ("1231", "0930", "0630", "0331")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按报告期批量回填财务核心指标到 stock_financials")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--table-name", default="stock_financials", help="目标表名")
    parser.add_argument("--recent-periods", type=int, default=8, help="最近多少期，默认8")
    parser.add_argument("--periods", default="", help="手工指定报告期，逗号分隔，如 20251231,20250930")
    parser.add_argument("--page-size", type=int, default=2000, help="每页抓取条数")
    parser.add_argument("--pause", type=float, default=0.08, help="每次请求后暂停秒数")
    parser.add_argument("--retry", type=int, default=3, help="单请求失败最大重试次数")
    parser.add_argument("--all-status", action="store_true", help="抓全部状态股票")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_code TEXT NOT NULL,
            report_period TEXT NOT NULL,
            report_type TEXT,
            ann_date TEXT,
            revenue REAL,
            op_profit REAL,
            net_profit REAL,
            net_profit_excl_nr REAL,
            roe REAL,
            gross_margin REAL,
            debt_to_assets REAL,
            operating_cf REAL,
            free_cf REAL,
            eps REAL,
            bps REAL,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (ts_code, report_period),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_period ON {table_name}(report_period)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_period ON {table_name}(ts_code, report_period)"
    )
    conn.commit()


def load_code_set(conn: sqlite3.Connection, all_status: bool) -> set[str]:
    if all_status:
        sql = "SELECT ts_code FROM stock_codes"
    else:
        sql = "SELECT ts_code FROM stock_codes WHERE list_status='L'"
    return {row[0] for row in conn.execute(sql).fetchall()}


def safe_num(value):
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def choose_ann_date(*values) -> str | None:
    dates = [str(v).strip() for v in values if v not in (None, "", "None")]
    if not dates:
        return None
    return max(dates)


def quarter_report_type(period: str, fallback) -> str | None:
    suffix = (period or "")[-4:]
    mapping = {
        "0331": "q1",
        "0630": "h1",
        "0930": "q3",
        "1231": "annual",
    }
    if suffix in mapping:
        return mapping[suffix]
    if fallback is None:
        return None
    return str(fallback)


def get_recent_periods(n: int) -> list[str]:
    now = datetime.now(timezone.utc)
    year = now.year
    periods: list[str] = []
    while len(periods) < max(n, 1):
        for suffix in QUARTER_ENDS:
            periods.append(f"{year}{suffix}")
            if len(periods) >= max(n, 1):
                break
        year -= 1
    return periods


def fetch_with_retry(fetch_fn, retry: int):
    last_exc = None
    for attempt in range(retry + 1):
        try:
            return fetch_fn()
        except Exception as exc:
            last_exc = exc
            if attempt < retry:
                time.sleep(1.5 * (2**attempt))
                continue
            raise last_exc


def fetch_all_pages(pro, api_name: str, period: str, page_size: int, retry: int, pause: float):
    offset = 0
    frames = []
    while True:
        df = fetch_with_retry(
            lambda: getattr(pro, api_name)(period=period, limit=page_size, offset=offset),
            retry=retry,
        )
        if df is None or df.empty:
            break
        frames.append(df)
        got = len(df)
        if got < page_size:
            break
        offset += got
        if pause > 0:
            time.sleep(pause)
    if not frames:
        return None
    import pandas as pd

    return pd.concat(frames, ignore_index=True)


def merge_period_rows(period: str, indicator_df, income_df, cashflow_df, balancesheet_df, code_set: set[str]) -> list[dict]:
    merged: dict[tuple[str, str], dict] = {}

    def ensure_item(ts_code: str) -> dict:
        key = (ts_code, period)
        item = merged.get(key)
        if item is None:
            item = {
                "ts_code": ts_code,
                "report_period": period,
                "report_type": quarter_report_type(period, None),
                "ann_date": None,
                "revenue": None,
                "op_profit": None,
                "net_profit": None,
                "net_profit_excl_nr": None,
                "roe": None,
                "gross_margin": None,
                "debt_to_assets": None,
                "operating_cf": None,
                "free_cf": None,
                "eps": None,
                "bps": None,
                "source": "tushare_vip",
                "update_time": utc_now(),
            }
            merged[key] = item
        return item

    if indicator_df is not None and not indicator_df.empty:
        for row in indicator_df.to_dict("records"):
            ts_code = str(row.get("ts_code") or "").strip()
            if ts_code not in code_set:
                continue
            item = ensure_item(ts_code)
            item["ann_date"] = choose_ann_date(item["ann_date"], row.get("ann_date"))
            item["net_profit_excl_nr"] = safe_num(row.get("profit_dedt"))
            item["roe"] = safe_num(row.get("roe"))
            item["gross_margin"] = safe_num(row.get("grossprofit_margin"))
            item["debt_to_assets"] = safe_num(row.get("debt_to_assets"))
            item["eps"] = safe_num(row.get("eps"))
            item["bps"] = safe_num(row.get("bps"))

    if income_df is not None and not income_df.empty:
        for row in income_df.to_dict("records"):
            ts_code = str(row.get("ts_code") or "").strip()
            if ts_code not in code_set:
                continue
            item = ensure_item(ts_code)
            item["report_type"] = quarter_report_type(period, row.get("report_type")) or item["report_type"]
            item["ann_date"] = choose_ann_date(item["ann_date"], row.get("ann_date"), row.get("f_ann_date"))
            item["revenue"] = safe_num(row.get("total_revenue")) or safe_num(row.get("revenue"))
            item["op_profit"] = safe_num(row.get("operate_profit"))
            item["net_profit"] = safe_num(row.get("n_income_attr_p")) or safe_num(row.get("n_income"))

    if cashflow_df is not None and not cashflow_df.empty:
        for row in cashflow_df.to_dict("records"):
            ts_code = str(row.get("ts_code") or "").strip()
            if ts_code not in code_set:
                continue
            item = ensure_item(ts_code)
            item["report_type"] = quarter_report_type(period, row.get("report_type")) or item["report_type"]
            item["ann_date"] = choose_ann_date(item["ann_date"], row.get("ann_date"), row.get("f_ann_date"))
            item["operating_cf"] = safe_num(row.get("n_cashflow_act"))
            item["free_cf"] = safe_num(row.get("free_cashflow"))

    if balancesheet_df is not None and not balancesheet_df.empty:
        for row in balancesheet_df.to_dict("records"):
            ts_code = str(row.get("ts_code") or "").strip()
            if ts_code not in code_set:
                continue
            item = ensure_item(ts_code)
            item["report_type"] = quarter_report_type(period, row.get("report_type")) or item["report_type"]
            item["ann_date"] = choose_ann_date(item["ann_date"], row.get("ann_date"), row.get("f_ann_date"))
            if item["debt_to_assets"] is None:
                total_assets = safe_num(row.get("total_assets"))
                total_liab = safe_num(row.get("total_liab"))
                if total_assets not in (None, 0) and total_liab is not None:
                    item["debt_to_assets"] = total_liab / total_assets * 100

    return list(merged.values())


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {table_name} (
        ts_code, report_period, report_type, ann_date, revenue, op_profit, net_profit,
        net_profit_excl_nr, roe, gross_margin, debt_to_assets, operating_cf, free_cf,
        eps, bps, source, update_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ts_code, report_period) DO UPDATE SET
        report_type=excluded.report_type,
        ann_date=excluded.ann_date,
        revenue=excluded.revenue,
        op_profit=excluded.op_profit,
        net_profit=excluded.net_profit,
        net_profit_excl_nr=excluded.net_profit_excl_nr,
        roe=excluded.roe,
        gross_margin=excluded.gross_margin,
        debt_to_assets=excluded.debt_to_assets,
        operating_cf=excluded.operating_cf,
        free_cf=excluded.free_cf,
        eps=excluded.eps,
        bps=excluded.bps,
        source=excluded.source,
        update_time=excluded.update_time
    """
    values = [
        (
            row["ts_code"],
            row["report_period"],
            row["report_type"],
            row["ann_date"],
            row["revenue"],
            row["op_profit"],
            row["net_profit"],
            row["net_profit_excl_nr"],
            row["roe"],
            row["gross_margin"],
            row["debt_to_assets"],
            row["operating_cf"],
            row["free_cf"],
            row["eps"],
            row["bps"],
            row["source"],
            row["update_time"],
        )
        for row in rows
    ]
    cur = conn.cursor()
    cur.executemany(sql, values)
    conn.commit()
    return len(rows)


def main() -> int:
    args = parse_args()
    periods = [x.strip() for x in args.periods.split(",") if x.strip()] if args.periods else get_recent_periods(args.recent_periods)
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"错误: 数据库不存在: {db_path}", file=sys.stderr)
        return 1

    pro = ts.pro_api(resolve_tushare_token(args.token))
    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        code_set = load_code_set(conn, args.all_status)
        print(f"目标股票数: {len(code_set)}, periods={periods}, all_status={args.all_status}")

        total_rows = 0
        failed_periods = 0
        for idx, period in enumerate(periods, start=1):
            try:
                indicator_df = fetch_all_pages(pro, "fina_indicator_vip", period, args.page_size, args.retry, args.pause)
                income_df = fetch_all_pages(pro, "income_vip", period, args.page_size, args.retry, args.pause)
                cashflow_df = fetch_all_pages(pro, "cashflow_vip", period, args.page_size, args.retry, args.pause)
                balancesheet_df = fetch_all_pages(pro, "balancesheet_vip", period, args.page_size, args.retry, args.pause)
                rows = merge_period_rows(period, indicator_df, income_df, cashflow_df, balancesheet_df, code_set)
                n = upsert_rows(conn, args.table_name, rows)
                total_rows += n
                print(
                    f"[{idx}/{len(periods)}] {period}: indicator={0 if indicator_df is None else len(indicator_df)} "
                    f"income={0 if income_df is None else len(income_df)} cashflow={0 if cashflow_df is None else len(cashflow_df)} "
                    f"balancesheet={0 if balancesheet_df is None else len(balancesheet_df)} merged={len(rows)} upsert={n}"
                )
            except Exception as exc:
                failed_periods += 1
                print(f"[{idx}/{len(periods)}] {period}: 失败 -> {exc}", file=sys.stderr)

            if args.pause > 0:
                time.sleep(args.pause)

        final_rows = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(
            f"完成: failed_periods={failed_periods}, upsert_rows={total_rows}, table_rows={final_rows}, db={db_path}"
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
