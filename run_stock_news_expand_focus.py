#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import db_compat as sqlite3


ROOT = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按重点股票池扩面抓取个股新闻")
    parser.add_argument("--limit-scores", type=int, default=100, help="综合评分榜抓取股票数")
    parser.add_argument("--limit-candidates", type=int, default=50, help="候选池抓取股票数")
    parser.add_argument("--page-size", type=int, default=20, help="每只股票抓取新闻条数")
    parser.add_argument("--max-targets", type=int, default=120, help="最终最多处理股票数")
    parser.add_argument("--score-after-fetch", action="store_true", help="抓取后顺带补一轮个股新闻评分")
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
        SELECT candidate_name, ts_code
        FROM chatroom_stock_candidate_pool
        WHERE candidate_type IN ('股票', '标的')
        ORDER BY bullish_room_count DESC NULLS LAST, mention_count DESC NULLS LAST, candidate_name
        LIMIT ?
        """,
        (args.limit_candidates,),
    ).fetchall()
    for row in rows:
        name = str(row[0] or "").strip()
        direct_code = str(row[1] or "").strip().upper()
        code = direct_code
        if not code and name:
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
            if resolved:
                code = str(resolved[0]).strip().upper()
        if code and code not in seen:
            seen.add(code)
            targets.append(code)

    return targets[: max(args.max_targets, 1)]


def run_cmd(argv: list[str]) -> int:
    proc = subprocess.run(argv, cwd=str(ROOT))
    return proc.returncode


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(ROOT / "stocks.db")
    try:
        targets = load_targets(conn, args)
    finally:
        conn.close()

    if not targets:
        print("没有可抓取的重点股票。")
        return 0

    print(f"目标股票数: {len(targets)}")
    failed = 0
    for idx, ts_code in enumerate(targets, start=1):
        print(f"[{idx}/{len(targets)}] 抓取个股新闻: {ts_code}")
        rc = run_cmd(
            [
                sys.executable,
                str(ROOT / "fetch_stock_news_eastmoney_to_db.py"),
                "--ts-code",
                ts_code,
                "--page-size",
                str(args.page_size),
            ]
        )
        if rc != 0:
            failed += 1

    if args.score_after_fetch:
        run_cmd(
            [
                sys.executable,
                str(ROOT / "llm_score_stock_news.py"),
                "--model",
                "GPT-5.4",
                "--limit",
                "160",
                "--workers",
                "6",
                "--batch-size",
                "8",
                "--retry",
                "1",
                "--sleep",
                "0.02",
            ]
        )

    print(f"完成: failed={failed}, total={len(targets)}")
    return 0 if failed < len(targets) else 1


if __name__ == "__main__":
    raise SystemExit(main())
