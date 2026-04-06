#!/usr/bin/env python3
"""
回填市场级资金流到 capital_flow_market。

数据来源：
- tushare.pro.moneyflow_hsgt

当前写入两类 flow_type：
- northbound  北向资金
- southbound  南向资金

说明：
- Tushare moneyflow_hsgt 提供的是净流向及分通道数据
- 由于当前表结构的 buy_amount / sell_amount 更适合“买入/卖出”口径，
  这里保守写法为：
  - net_inflow: 北向/南向总额
  - buy_amount / sell_amount: 留空
"""

from __future__ import annotations

import argparse
import math
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
ROOT_DIR = Path(__file__).resolve().parent
LOCAL_DEPS = ROOT_DIR / ".deps"
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回填市场级资金流到 capital_flow_market")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token（默认从 TUSHARE_TOKEN 读取）")
    parser.add_argument("--table-name", default="capital_flow_market", help="目标表名")
    parser.add_argument("--lookback-days", type=int, default=30, help="回溯天数，默认30")
    parser.add_argument("--start-date", default="", help="开始日期(YYYYMMDD)")
    parser.add_argument("--end-date", default="", help="结束日期(YYYYMMDD)")
    parser.add_argument("--pause", type=float, default=0.05, help="每个区间请求后暂停秒数")
    parser.add_argument("--truncate", action="store_true", help="执行前清空目标表")
    parser.add_argument(
        "--provider-chain",
        default="tushare,akshare",
        help="数据源链路，按优先级逗号分隔，如 tushare,akshare 或 akshare",
    )
    parser.add_argument(
        "--akshare-summary-fallback",
        action="store_true",
        help="启用 AKShare 当日摘要兜底（补最新交易日/当天）",
    )
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
            trade_date TEXT NOT NULL,
            flow_type TEXT NOT NULL,
            net_inflow REAL,
            buy_amount REAL,
            sell_amount REAL,
            unit TEXT,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (trade_date, flow_type)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date)"
    )
    conn.commit()


def to_float(value):
    if value in (None, "", "None"):
        return None
    try:
        f = float(value)
        if math.isnan(f):
            return None
        return f
    except Exception:
        return None


def to_trade_date(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if "-" in text:
        return text.replace("-", "")[:8]
    return text[:8]


def normalize_flow_type(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"northbound", "north", "北向", "北向资金"}:
        return "northbound"
    if text in {"southbound", "south", "南向", "南向资金"}:
        return "southbound"
    return text


def parse_provider_chain(raw: str) -> list[str]:
    items = [x.strip().lower() for x in str(raw or "").split(",") if x.strip()]
    allowed = {"tushare", "akshare"}
    chain = [x for x in items if x in allowed]
    return chain or ["tushare", "akshare"]


def fetch_from_tushare(start_date: str, end_date: str, token: str) -> list[tuple[str, str, float | None, float | None, float | None, str]]:
    pro = ts.pro_api(resolve_tushare_token(token))
    df = pro.moneyflow_hsgt(start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return []
    rows: list[tuple[str, str, float | None, float | None, float | None, str]] = []
    for row in df.itertuples(index=False):
        trade_date = to_trade_date(getattr(row, "trade_date", ""))
        if not trade_date:
            continue
        rows.append(
            (
                trade_date,
                "northbound",
                to_float(getattr(row, "north_money", None)),
                None,
                None,
                "tushare.moneyflow_hsgt",
            )
        )
        rows.append(
            (
                trade_date,
                "southbound",
                to_float(getattr(row, "south_money", None)),
                None,
                None,
                "tushare.moneyflow_hsgt",
            )
        )
    return rows


def fetch_from_akshare_hist(start_date: str, end_date: str) -> list[tuple[str, str, float | None, float | None, float | None, str]]:
    try:
        import akshare as ak  # type: ignore
    except Exception:
        return []
    rows: list[tuple[str, str, float | None, float | None, float | None, str]] = []
    symbol_map = [("北向资金", "northbound"), ("南向资金", "southbound")]
    for symbol, flow_type in symbol_map:
        df = ak.stock_hsgt_hist_em(symbol=symbol)
        if df is None or getattr(df, "empty", True):
            continue
        for item in df.to_dict("records"):
            trade_date = to_trade_date(item.get("日期"))
            if not trade_date:
                continue
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue
            rows.append(
                (
                    trade_date,
                    flow_type,
                    to_float(item.get("当日成交净买额")),
                    to_float(item.get("买入成交额")),
                    to_float(item.get("卖出成交额")),
                    "akshare.stock_hsgt_hist_em",
                )
            )
    return rows


def fetch_from_akshare_summary() -> list[tuple[str, str, float | None, float | None, float | None, str]]:
    try:
        import akshare as ak  # type: ignore
    except Exception:
        return []
    df = ak.stock_hsgt_fund_flow_summary_em()
    if df is None or getattr(df, "empty", True):
        return []
    grouped: dict[tuple[str, str], dict[str, float]] = {}
    for item in df.to_dict("records"):
        trade_date = to_trade_date(item.get("交易日"))
        flow_type = normalize_flow_type(item.get("资金方向"))
        if flow_type not in {"northbound", "southbound"} or not trade_date:
            continue
        key = (trade_date, flow_type)
        if key not in grouped:
            grouped[key] = {"net": 0.0, "buy": 0.0, "sell": 0.0}
        grouped[key]["net"] += to_float(item.get("成交净买额")) or 0.0
    rows: list[tuple[str, str, float | None, float | None, float | None, str]] = []
    for (trade_date, flow_type), v in grouped.items():
        rows.append(
            (
                trade_date,
                flow_type,
                to_float(v.get("net")),
                to_float(v.get("buy")),
                to_float(v.get("sell")),
                "akshare.stock_hsgt_fund_flow_summary_em",
            )
        )
    return rows


def merge_rows_by_chain(source_rows: list[tuple[str, str, float | None, float | None, float | None, str]], chain: list[str]) -> list[tuple]:
    priority = {name: idx for idx, name in enumerate(chain)}

    def provider_rank(src: str) -> int:
        text = str(src or "").lower()
        if "tushare" in text:
            return priority.get("tushare", 999)
        if "akshare" in text:
            return priority.get("akshare", 999)
        return 999

    chosen: dict[tuple[str, str], tuple[str, str, float | None, float | None, float | None, str]] = {}
    for row in source_rows:
        trade_date, flow_type, net_inflow, buy_amount, sell_amount, source = row
        key = (trade_date, flow_type)
        existing = chosen.get(key)
        if existing is None:
            chosen[key] = row
            continue
        e_trade_date, e_flow_type, e_net, e_buy, e_sell, e_source = existing
        new_rank = provider_rank(source)
        old_rank = provider_rank(e_source)
        # Prefer higher-priority provider when it has a valid net value.
        if new_rank < old_rank and net_inflow is not None:
            chosen[key] = row
            continue
        # Fill null net with any available provider value.
        if e_net is None and net_inflow is not None:
            chosen[key] = row
            continue
        # For same provider priority, keep the row with more complete fields.
        if new_rank == old_rank:
            existing_score = sum(x is not None for x in [e_net, e_buy, e_sell])
            new_score = sum(x is not None for x in [net_inflow, buy_amount, sell_amount])
            if new_score > existing_score:
                chosen[key] = row

    update_time = utc_now()
    return [
        (
            trade_date,
            flow_type,
            net_inflow,
            buy_amount,
            sell_amount,
            "million_cny",
            source,
            update_time,
        )
        for (trade_date, flow_type), (_, _, net_inflow, buy_amount, sell_amount, source) in sorted(chosen.items())
    ]


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {table_name} (
        trade_date, flow_type, net_inflow, buy_amount, sell_amount, unit, source, update_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(trade_date, flow_type) DO UPDATE SET
        net_inflow=excluded.net_inflow,
        buy_amount=excluded.buy_amount,
        sell_amount=excluded.sell_amount,
        unit=excluded.unit,
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

    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"错误: 数据库不存在: {db_path}", file=sys.stderr)
        return 1

    provider_chain = parse_provider_chain(args.provider_chain)
    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        if args.truncate:
            conn.execute(f"DELETE FROM {args.table_name}")
            conn.commit()
            print(f"已清空旧数据: {args.table_name}")

        raw_rows: list[tuple[str, str, float | None, float | None, float | None, str]] = []
        for provider in provider_chain:
            if provider == "tushare":
                try:
                    rows = fetch_from_tushare(start_date=start_date, end_date=end_date, token=args.token)
                    raw_rows.extend(rows)
                    print(f"provider=tushare fetched={len(rows)}")
                except Exception as exc:
                    print(f"provider=tushare failed: {exc}")
            elif provider == "akshare":
                try:
                    rows = fetch_from_akshare_hist(start_date=start_date, end_date=end_date)
                    raw_rows.extend(rows)
                    print(f"provider=akshare(hist) fetched={len(rows)}")
                except Exception as exc:
                    print(f"provider=akshare(hist) failed: {exc}")

        if args.akshare_summary_fallback:
            try:
                summary_rows = fetch_from_akshare_summary()
                raw_rows.extend(summary_rows)
                print(f"provider=akshare(summary) fetched={len(summary_rows)}")
            except Exception as exc:
                print(f"provider=akshare(summary) failed: {exc}")

        if not raw_rows:
            print("未获取到市场级资金流数据。")
            return 0

        rows = merge_rows_by_chain(raw_rows, provider_chain)
        n = upsert_rows(conn, args.table_name, rows)
        final_count = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(
            f"完成: raw_rows={len(raw_rows)}, upsert_rows={n}, table_rows={final_count}, "
            f"range={start_date}~{end_date}, providers={','.join(provider_chain)}"
        )
        if args.pause > 0:
            time.sleep(args.pause)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
