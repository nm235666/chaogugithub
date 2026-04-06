#!/usr/bin/env python3
"""
基于 stock_daily_prices 派生风控场景到 risk_scenarios。

当前生成的场景：
- base_var95_5d
- base_var95_20d
- stress_down_5pct
- stress_down_10pct
- vol_spike_1_5x

说明：
- pnl_impact / max_drawdown / var_95 / cvar_95 统一使用百分比口径
- scenario_date 使用该股票最新交易日
"""

from __future__ import annotations

import argparse
import json
import math
import db_compat as sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="基于日线派生 risk_scenarios")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--table-name", default="risk_scenarios", help="目标表名")
    parser.add_argument("--lookback-bars", type=int, default=120, help="使用最近多少根日线")
    parser.add_argument("--limit-stocks", type=int, default=0, help="最多处理多少只股票")
    parser.add_argument("--all-status", action="store_true", help="处理全部状态股票")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT,
            scenario_date TEXT NOT NULL,
            scenario_name TEXT NOT NULL,
            horizon TEXT,
            pnl_impact REAL,
            max_drawdown REAL,
            var_95 REAL,
            cvar_95 REAL,
            assumptions_json TEXT,
            source TEXT,
            update_time TEXT,
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name}(scenario_date)"
    )
    conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{table_name}_scenario ON {table_name}(ts_code, scenario_date, scenario_name, horizon)"
    )
    conn.commit()


def load_codes(conn: sqlite3.Connection, all_status: bool, limit_stocks: int) -> list[str]:
    where_sql = "" if all_status else " WHERE list_status='L'"
    limit_sql = f" LIMIT {int(limit_stocks)}" if limit_stocks > 0 else ""
    sql = f"SELECT ts_code FROM stock_codes{where_sql} ORDER BY ts_code{limit_sql}"
    return [row[0] for row in conn.execute(sql).fetchall()]


def fetch_price_rows(conn: sqlite3.Connection, codes: list[str], lookback_bars: int) -> dict[str, list[tuple[str, float]]]:
    if not codes:
        return {}
    placeholders = ",".join(["?"] * len(codes))
    # Use CTE form for cross-DB compatibility (PostgreSQL requires explicit aliases
    # for derived tables and has stricter parser behavior than SQLite).
    sql = f"""
    WITH ranked_prices AS (
      SELECT
        ts_code,
        trade_date,
        close,
        ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
      FROM stock_daily_prices
      WHERE ts_code IN ({placeholders})
    )
    SELECT ts_code, trade_date, close
    FROM ranked_prices
    WHERE rn <= ?
    ORDER BY ts_code, trade_date
    """
    rows = conn.execute(sql, [*codes, lookback_bars]).fetchall()
    grouped: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for ts_code, trade_date, close in rows:
        if close is None:
            continue
        grouped[ts_code].append((trade_date, float(close)))
    return grouped


def compute_stats(series: list[tuple[str, float]]) -> dict | None:
    if len(series) < 20:
        return None
    dates = [d for d, _ in series]
    closes = [c for _, c in series]
    returns = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        curr = closes[i]
        if prev:
            returns.append((curr - prev) / prev)
    if len(returns) < 10:
        return None

    sorted_returns = sorted(returns)
    tail_count = max(1, math.ceil(len(sorted_returns) * 0.05))
    var95 = sorted_returns[tail_count - 1]
    cvar95 = sum(sorted_returns[:tail_count]) / tail_count

    peak = closes[0]
    max_drawdown = 0.0
    for close in closes:
        if close > peak:
            peak = close
        drawdown = (close - peak) / peak
        if drawdown < max_drawdown:
            max_drawdown = drawdown

    daily_vol = statistics.pstdev(returns) if len(returns) >= 2 else 0.0
    return {
        "scenario_date": dates[-1],
        "latest_close": closes[-1],
        "returns": returns,
        "var95_pct": var95 * 100,
        "cvar95_pct": cvar95 * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "daily_vol_pct": daily_vol * 100,
        "bars": len(closes),
    }


def build_scenarios(ts_code: str, stats: dict) -> list[dict]:
    var95_1d = stats["var95_pct"]
    cvar95_1d = stats["cvar95_pct"]
    max_drawdown = stats["max_drawdown_pct"]
    daily_vol = stats["daily_vol_pct"]
    scenario_date = stats["scenario_date"]

    var95_5d = var95_1d * math.sqrt(5)
    cvar95_5d = cvar95_1d * math.sqrt(5)
    var95_20d = var95_1d * math.sqrt(20)
    cvar95_20d = cvar95_1d * math.sqrt(20)

    base_assumptions = {
        "lookback_bars": stats["bars"],
        "daily_vol_pct": daily_vol,
        "historical_var95_1d_pct": var95_1d,
        "historical_cvar95_1d_pct": cvar95_1d,
    }

    return [
        {
            "ts_code": ts_code,
            "scenario_date": scenario_date,
            "scenario_name": "base_var95_5d",
            "horizon": "5d",
            "pnl_impact": var95_5d,
            "max_drawdown": max_drawdown,
            "var_95": var95_5d,
            "cvar_95": cvar95_5d,
            "assumptions_json": json.dumps(
                {**base_assumptions, "method": "historical_var_sqrt_time"}, ensure_ascii=False
            ),
        },
        {
            "ts_code": ts_code,
            "scenario_date": scenario_date,
            "scenario_name": "base_var95_20d",
            "horizon": "20d",
            "pnl_impact": var95_20d,
            "max_drawdown": max_drawdown,
            "var_95": var95_20d,
            "cvar_95": cvar95_20d,
            "assumptions_json": json.dumps(
                {**base_assumptions, "method": "historical_var_sqrt_time"}, ensure_ascii=False
            ),
        },
        {
            "ts_code": ts_code,
            "scenario_date": scenario_date,
            "scenario_name": "stress_down_5pct",
            "horizon": "1d",
            "pnl_impact": -5.0,
            "max_drawdown": max_drawdown,
            "var_95": var95_1d,
            "cvar_95": cvar95_1d,
            "assumptions_json": json.dumps(
                {**base_assumptions, "shock_pct": -5.0, "method": "deterministic_shock"}, ensure_ascii=False
            ),
        },
        {
            "ts_code": ts_code,
            "scenario_date": scenario_date,
            "scenario_name": "stress_down_10pct",
            "horizon": "1d",
            "pnl_impact": -10.0,
            "max_drawdown": max_drawdown,
            "var_95": var95_1d,
            "cvar_95": cvar95_1d,
            "assumptions_json": json.dumps(
                {**base_assumptions, "shock_pct": -10.0, "method": "deterministic_shock"}, ensure_ascii=False
            ),
        },
        {
            "ts_code": ts_code,
            "scenario_date": scenario_date,
            "scenario_name": "vol_spike_1_5x",
            "horizon": "5d",
            "pnl_impact": var95_5d * 1.5,
            "max_drawdown": max_drawdown,
            "var_95": var95_5d * 1.5,
            "cvar_95": cvar95_5d * 1.5,
            "assumptions_json": json.dumps(
                {**base_assumptions, "vol_multiplier": 1.5, "method": "volatility_spike"}, ensure_ascii=False
            ),
        },
    ]


def upsert_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    update_time = utc_now()
    values = [
        (
            row["ts_code"],
            row["scenario_date"],
            row["scenario_name"],
            row["horizon"],
            row["pnl_impact"],
            row["max_drawdown"],
            row["var_95"],
            row["cvar_95"],
            row["assumptions_json"],
            "derived.stock_daily_prices",
            update_time,
        )
        for row in rows
    ]
    cur = conn.cursor()
    if sqlite3.using_postgres():
        # PostgreSQL 当前线上表使用表达式唯一索引（md5+COALESCE）而非列级唯一约束，
        # 列式 ON CONFLICT 无法命中，采用“先删后插”保证幂等与兼容。
        delete_sql = f"""
        DELETE FROM {table_name}
        WHERE COALESCE(ts_code, '') = COALESCE(?, '')
          AND COALESCE(scenario_date, '') = COALESCE(?, '')
          AND COALESCE(scenario_name, '') = COALESCE(?, '')
          AND COALESCE(horizon, '') = COALESCE(?, '')
        """
        insert_sql = f"""
        INSERT INTO {table_name} (
            ts_code, scenario_date, scenario_name, horizon, pnl_impact, max_drawdown,
            var_95, cvar_95, assumptions_json, source, update_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        keys = [(v[0], v[1], v[2], v[3]) for v in values]
        cur.executemany(delete_sql, keys)
        cur.executemany(insert_sql, values)
    else:
        sql = f"""
        INSERT INTO {table_name} (
            ts_code, scenario_date, scenario_name, horizon, pnl_impact, max_drawdown,
            var_95, cvar_95, assumptions_json, source, update_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ts_code, scenario_date, scenario_name, horizon) DO UPDATE SET
            pnl_impact=excluded.pnl_impact,
            max_drawdown=excluded.max_drawdown,
            var_95=excluded.var_95,
            cvar_95=excluded.cvar_95,
            assumptions_json=excluded.assumptions_json,
            source=excluded.source,
            update_time=excluded.update_time
        """
        cur.executemany(sql, values)
    conn.commit()
    return len(rows)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"数据库不存在: {db_path}")
        return 1

    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn, args.table_name)
        codes = load_codes(conn, args.all_status, args.limit_stocks)
        if not codes:
            print("没有可处理的股票。")
            return 0
        grouped = fetch_price_rows(conn, codes, args.lookback_bars)
        total_rows = 0
        used_stocks = 0
        for ts_code, series in grouped.items():
            stats = compute_stats(series)
            if not stats:
                continue
            scenarios = build_scenarios(ts_code, stats)
            total_rows += upsert_rows(conn, args.table_name, scenarios)
            used_stocks += 1
        final_rows = conn.execute(f"SELECT COUNT(*) FROM {args.table_name}").fetchone()[0]
        print(
            f"完成: stocks={used_stocks}, upsert_rows={total_rows}, table_rows={final_rows}, lookback_bars={args.lookback_bars}"
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
