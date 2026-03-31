#!/usr/bin/env python3
"""
回填股票事件数据到 stock_events。

当前接入事件源：
- forecast        业绩预告
- express         业绩快报
- dividend        分红实施/预案
- repurchase      回购
- stk_holdertrade 股东增减持

默认行为：
- 仅抓取当前上市股票
- 每只股票每类事件抓最近若干条

用法示例：
  python3 backfill_stock_events.py
  python3 backfill_stock_events.py --ts-code 000001.SZ
  python3 backfill_stock_events.py --limit-stocks 100 --rows-per-source 5
  python3 backfill_stock_events.py --start-from 300750.SZ
"""

from __future__ import annotations

import argparse
import json
import db_compat as sqlite3
import sys
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from runtime_secrets import TUSHARE_TOKEN, resolve_tushare_token

LOCAL_DEPS = Path(__file__).resolve().parent / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

import tushare as ts

DEFAULT_TOKEN = TUSHARE_TOKEN


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回填股票事件数据到 stock_events")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--table-name", default="stock_events", help="目标表名")
    parser.add_argument("--ts-code", default="", help="仅抓取单只股票，如 000001.SZ")
    parser.add_argument("--start-from", default="", help="从指定 ts_code 开始续跑")
    parser.add_argument("--limit-stocks", type=int, default=0, help="最多抓取多少只股票，0 表示不限")
    parser.add_argument("--rows-per-source", type=int, default=8, help="每只股票每类事件最多抓多少条")
    parser.add_argument("--pause", type=float, default=0.12, help="每只股票请求后暂停秒数")
    parser.add_argument("--retry", type=int, default=3, help="单接口失败最大重试次数")
    parser.add_argument("--all-status", action="store_true", help="抓全部状态股票")
    parser.add_argument("--missing-only", action="store_true", help="仅处理当前没有事件记录的股票")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_date TEXT,
            ann_date TEXT,
            title TEXT,
            detail_json TEXT,
            source TEXT,
            update_time TEXT,
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if "event_key" not in cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN event_key TEXT")
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_code_date ON {table_name}(ts_code, event_date)"
    )
    conn.execute(f"DROP INDEX IF EXISTS uq_{table_name}_event")
    conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{table_name}_event_key ON {table_name}(event_key)"
    )
    conn.commit()


def load_target_codes(
    conn: sqlite3.Connection,
    table_name: str,
    ts_code: str,
    start_from: str,
    limit_stocks: int,
    all_status: bool,
    missing_only: bool,
) -> list[str]:
    if ts_code.strip():
        return [ts_code.strip().upper()]

    where = []
    params: list[object] = []
    if not all_status:
        where.append("list_status = 'L'")
    if missing_only:
        where.append(
            f"NOT EXISTS (SELECT 1 FROM {table_name} e WHERE e.ts_code = stock_codes.ts_code)"
        )
    if start_from.strip():
        where.append("ts_code >= ?")
        params.append(start_from.strip().upper())

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    limit_sql = f" LIMIT {int(limit_stocks)}" if limit_stocks > 0 else ""
    sql = f"SELECT ts_code FROM stock_codes{where_sql} ORDER BY ts_code{limit_sql}"
    rows = conn.execute(sql, params).fetchall()
    return [r[0] for r in rows]


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


def normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def compact_json(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def make_event_key(*parts) -> str:
    raw = "||".join(normalize_text(p) for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def build_forecast_events(df, ts_code: str) -> list[dict]:
    events = []
    if df is None or df.empty:
        return events
    for row in df.to_dict("records"):
        end_date = normalize_text(row.get("end_date"))
        ann_date = normalize_text(row.get("ann_date") or row.get("first_ann_date"))
        change_min = row.get("p_change_min")
        change_max = row.get("p_change_max")
        summary = normalize_text(row.get("summary"))
        title = f"业绩预告 {end_date} {normalize_text(row.get('type'))}".strip()
        if change_min not in (None, "") or change_max not in (None, ""):
            title += f" 变动区间{change_min}~{change_max}%"
        events.append(
            {
                "ts_code": ts_code,
                "event_type": "earnings_forecast",
                "event_date": end_date,
                "ann_date": ann_date,
                "title": title[:200],
                "detail_json": compact_json(
                    {
                        "end_date": end_date,
                        "forecast_type": row.get("type"),
                        "p_change_min": change_min,
                        "p_change_max": change_max,
                        "net_profit_min": row.get("net_profit_min"),
                        "net_profit_max": row.get("net_profit_max"),
                        "last_parent_net": row.get("last_parent_net"),
                        "summary": summary,
                        "change_reason": normalize_text(row.get("change_reason")),
                    }
                ),
                "source": "tushare.forecast",
                "update_time": utc_now(),
                "event_key": make_event_key(
                    ts_code, "earnings_forecast", end_date, ann_date, row.get("type"), change_min, change_max
                ),
            }
        )
    return events


def build_express_events(df, ts_code: str) -> list[dict]:
    events = []
    if df is None or df.empty:
        return events
    for row in df.to_dict("records"):
        end_date = normalize_text(row.get("end_date"))
        ann_date = normalize_text(row.get("ann_date"))
        title = f"业绩快报 {end_date}"
        if row.get("n_income") not in (None, ""):
            title += f" 净利润{row.get('n_income')}"
        events.append(
            {
                "ts_code": ts_code,
                "event_type": "earnings_express",
                "event_date": end_date,
                "ann_date": ann_date,
                "title": title[:200],
                "detail_json": compact_json(
                    {
                        "end_date": end_date,
                        "revenue": row.get("revenue"),
                        "operate_profit": row.get("operate_profit"),
                        "total_profit": row.get("total_profit"),
                        "net_profit": row.get("n_income"),
                        "yoy_net_profit": row.get("yoy_net_profit"),
                        "diluted_eps": row.get("diluted_eps"),
                        "diluted_roe": row.get("diluted_roe"),
                        "bps": row.get("bps"),
                        "perf_summary": normalize_text(row.get("perf_summary")),
                    }
                ),
                "source": "tushare.express",
                "update_time": utc_now(),
                "event_key": make_event_key(ts_code, "earnings_express", end_date, ann_date),
            }
        )
    return events


def build_dividend_events(df, ts_code: str) -> list[dict]:
    events = []
    if df is None or df.empty:
        return events
    for row in df.to_dict("records"):
        end_date = normalize_text(row.get("end_date"))
        ann_date = normalize_text(row.get("ann_date") or row.get("imp_ann_date"))
        event_date = (
            normalize_text(row.get("ex_date"))
            or normalize_text(row.get("pay_date"))
            or normalize_text(row.get("record_date"))
            or end_date
        )
        cash_div = row.get("cash_div")
        title = f"分红 {normalize_text(row.get('div_proc'))}"
        title_cash = cash_div
        if title_cash in (None, "", 0, 0.0):
            title_cash = row.get("cash_div_tax")
        if title_cash not in (None, ""):
            title += f" 每股派现{title_cash}"
        events.append(
            {
                "ts_code": ts_code,
                "event_type": "dividend",
                "event_date": event_date,
                "ann_date": ann_date,
                "title": title[:200],
                "detail_json": compact_json(
                    {
                        "end_date": end_date,
                        "div_proc": normalize_text(row.get("div_proc")),
                        "stk_div": row.get("stk_div"),
                        "stk_bo_rate": row.get("stk_bo_rate"),
                        "stk_co_rate": row.get("stk_co_rate"),
                        "cash_div": cash_div,
                        "cash_div_tax": row.get("cash_div_tax"),
                        "record_date": normalize_text(row.get("record_date")),
                        "ex_date": normalize_text(row.get("ex_date")),
                        "pay_date": normalize_text(row.get("pay_date")),
                        "div_listdate": normalize_text(row.get("div_listdate")),
                    }
                ),
                "source": "tushare.dividend",
                "update_time": utc_now(),
                "event_key": make_event_key(
                    ts_code,
                    "dividend",
                    event_date,
                    ann_date,
                    row.get("div_proc"),
                    row.get("cash_div_tax"),
                    row.get("stk_div"),
                ),
            }
        )
    return events


def build_repurchase_events(df, ts_code: str) -> list[dict]:
    events = []
    if df is None or df.empty:
        return events
    for row in df.to_dict("records"):
        ann_date = normalize_text(row.get("ann_date"))
        end_date = normalize_text(row.get("end_date"))
        event_date = normalize_text(row.get("exp_date")) or end_date or ann_date
        title = f"回购 {normalize_text(row.get('proc'))}"
        if row.get("amount") not in (None, ""):
            title += f" 金额{row.get('amount')}"
        events.append(
            {
                "ts_code": ts_code,
                "event_type": "repurchase",
                "event_date": event_date,
                "ann_date": ann_date,
                "title": title[:200],
                "detail_json": compact_json(
                    {
                        "proc": normalize_text(row.get("proc")),
                        "end_date": end_date,
                        "exp_date": normalize_text(row.get("exp_date")),
                        "vol": row.get("vol"),
                        "amount": row.get("amount"),
                        "high_limit": row.get("high_limit"),
                        "low_limit": row.get("low_limit"),
                    }
                ),
                "source": "tushare.repurchase",
                "update_time": utc_now(),
                "event_key": make_event_key(
                    ts_code, "repurchase", ann_date, event_date, row.get("proc"), row.get("vol"), row.get("amount")
                ),
            }
        )
    return events


def build_holdertrade_events(df, ts_code: str) -> list[dict]:
    events = []
    if df is None or df.empty:
        return events
    for row in df.to_dict("records"):
        ann_date = normalize_text(row.get("ann_date"))
        in_de = normalize_text(row.get("in_de"))
        holder_name = normalize_text(row.get("holder_name"))
        title = f"股东{in_de or '变动'} {holder_name} 变动{row.get('change_ratio')}%"
        events.append(
            {
                "ts_code": ts_code,
                "event_type": "holder_trade",
                "event_date": ann_date,
                "ann_date": ann_date,
                "title": title[:200],
                "detail_json": compact_json(
                    {
                        "holder_name": holder_name,
                        "holder_type": normalize_text(row.get("holder_type")),
                        "in_de": in_de,
                        "change_vol": row.get("change_vol"),
                        "change_ratio": row.get("change_ratio"),
                        "after_share": row.get("after_share"),
                        "after_ratio": row.get("after_ratio"),
                        "avg_price": row.get("avg_price"),
                        "total_share": row.get("total_share"),
                    }
                ),
                "source": "tushare.stk_holdertrade",
                "update_time": utc_now(),
                "event_key": make_event_key(
                    ts_code,
                    "holder_trade",
                    ann_date,
                    holder_name,
                    in_de,
                    row.get("change_vol"),
                    row.get("change_ratio"),
                ),
            }
        )
    return events


def fetch_stock_events(pro, ts_code: str, rows_per_source: int, retry: int) -> list[dict]:
    rows: list[dict] = []
    source_builders = [
        ("forecast", build_forecast_events),
        ("express", build_express_events),
        ("dividend", build_dividend_events),
        ("repurchase", build_repurchase_events),
        ("stk_holdertrade", build_holdertrade_events),
    ]

    for api_name, builder in source_builders:
        df = fetch_with_retry(
            lambda api_name=api_name: getattr(pro, api_name)(ts_code=ts_code, limit=rows_per_source),
            retry=retry,
        )
        rows.extend(builder(df, ts_code))

    rows.sort(key=lambda x: (x["event_date"], x["ann_date"], x["title"]), reverse=True)
    return rows


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {table_name} (
        ts_code, event_type, event_date, ann_date, title, detail_json, source, update_time, event_key
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT DO NOTHING
    """
    values = [
        (
            row["ts_code"],
            row["event_type"],
            normalize_text(row["event_date"]),
            normalize_text(row["ann_date"]),
            normalize_text(row["title"]),
            row["detail_json"],
            row["source"],
            row["update_time"],
            row["event_key"],
        )
        for row in rows
    ]
    cur = conn.cursor()
    cur.executemany(sql, values)
    conn.commit()
    return cur.rowcount if cur.rowcount != -1 else 0


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"错误: 数据库不存在: {db_path}", file=sys.stderr)
        return 1

    pro = ts.pro_api(resolve_tushare_token(args.token))
    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        codes = load_target_codes(
            conn=conn,
            table_name=args.table_name,
            ts_code=args.ts_code,
            start_from=args.start_from,
            limit_stocks=args.limit_stocks,
            all_status=args.all_status,
            missing_only=args.missing_only,
        )
        if not codes:
            print("没有可处理的股票。")
            return 0

        print(
            f"待处理股票数: {len(codes)}, rows_per_source={args.rows_per_source}, all_status={args.all_status}, missing_only={args.missing_only}"
        )

        total_inserted = 0
        failed = 0
        for idx, ts_code in enumerate(codes, start=1):
            try:
                rows = fetch_stock_events(
                    pro=pro,
                    ts_code=ts_code,
                    rows_per_source=args.rows_per_source,
                    retry=args.retry,
                )
                inserted = upsert_rows(conn, args.table_name, rows)
                total_inserted += inserted
                print(
                    f"[{idx}/{len(codes)}] {ts_code}: fetched={len(rows)} inserted={inserted}"
                )
            except Exception as exc:
                failed += 1
                print(f"[{idx}/{len(codes)}] {ts_code}: 失败 -> {exc}", file=sys.stderr)

            if args.pause > 0:
                time.sleep(args.pause)

        final_rows = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(
            f"完成: failed={failed}, inserted={total_inserted}, table_rows={final_rows}, db={db_path}"
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
