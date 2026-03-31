#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import db_compat as sqlite3
from market_calendar import DEFAULT_TOKEN, resolve_trade_date


ROOT = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按重点股票池刷新分钟线")
    parser.add_argument("--trade-date", default="", help="交易日 YYYYMMDD，默认北京时间今天")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Tushare Token，用于按交易日历解析默认日期")
    parser.add_argument("--limit-scores", type=int, default=80, help="综合评分榜抓取股票数")
    parser.add_argument("--limit-candidates", type=int, default=40, help="候选池抓取股票数")
    parser.add_argument("--max-targets", type=int, default=100, help="最终最多处理股票数")
    return parser.parse_args()

def latest_score_date(conn) -> str:
    row = conn.execute("SELECT MAX(score_date) FROM stock_scores_daily").fetchone()
    return str(row[0] or "").strip()


def load_targets(conn, args: argparse.Namespace) -> list[str]:
    targets: list[str] = []
    seen: set[str] = set()
    score_date = latest_score_date(conn)
    if score_date:
        rows = conn.execute(
            """
            SELECT ts_code
            FROM stock_scores_daily
            WHERE score_date = ?
            ORDER BY industry_total_score DESC NULLS LAST, total_score DESC NULLS LAST, ts_code
            LIMIT ?
            """,
            (score_date, args.limit_scores),
        ).fetchall()
        for row in rows:
            code = str(row[0]).strip().upper()
            if code and code not in seen:
                seen.add(code)
                targets.append(code)

    rows = conn.execute(
        """
        SELECT candidate_name
        FROM chatroom_stock_candidate_pool
        WHERE candidate_type IN ('股票', '标的')
        ORDER BY bullish_room_count DESC NULLS LAST, mention_count DESC NULLS LAST, candidate_name
        LIMIT ?
        """,
        (args.limit_candidates,),
    ).fetchall()
    for row in rows:
        name = str(row[0] or "").strip()
        if not name:
            continue
        resolved = conn.execute(
            """
            SELECT ts_code
            FROM stock_codes
            WHERE name = ?
            ORDER BY CASE list_status WHEN 'L' THEN 0 ELSE 1 END, ts_code
            LIMIT 1
            """,
            (name,),
        ).fetchone()
        if not resolved:
            continue
        code = str(resolved[0]).strip().upper()
        if code and code not in seen:
            seen.add(code)
            targets.append(code)
    return targets[: max(args.max_targets, 1)]


def main() -> int:
    args = parse_args()
    trade_date = resolve_trade_date(args.trade_date, args.token)
    conn = sqlite3.connect(ROOT / "stocks.db")
    try:
        targets = load_targets(conn, args)
    finally:
        conn.close()
    if not targets:
        print("没有可刷新的重点股票。")
        return 0
    failed = 0
    print(f"目标股票数: {len(targets)}, trade_date={trade_date}")
    for idx, ts_code in enumerate(targets, start=1):
        print(f"[{idx}/{len(targets)}] 分钟线刷新: {ts_code}")
        rc = subprocess.run(
            [
                sys.executable,
                str(ROOT / "fetch_sina_minline_one.py"),
                "--ts-code",
                ts_code,
                "--trade-date",
                trade_date,
            ],
            cwd=str(ROOT),
        ).returncode
        if rc != 0:
            failed += 1
    print(f"完成: failed={failed}, total={len(targets)}")
    return 0 if failed < len(targets) else 1


if __name__ == "__main__":
    raise SystemExit(main())
