#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

import db_compat as sqlite3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="循环补评分未评分新闻，直到清空为止")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--model", default="auto", help="模型名，如 auto / kimi-k2.5 / GPT-5.4 / deepseek-chat")
    parser.add_argument("--batch-size", type=int, default=100, help="每轮最多评分条数")
    parser.add_argument("--max-rounds", type=int, default=0, help="最大轮数，0 表示不限")
    parser.add_argument("--source", default="", help="仅处理指定来源")
    parser.add_argument("--retry", type=int, default=2, help="单条新闻失败重试次数")
    parser.add_argument("--sleep", type=float, default=0.3, help="单条新闻之间间隔秒数")
    parser.add_argument("--round-sleep", type=float, default=2.0, help="每轮之间间隔秒数")
    parser.add_argument("--stop-on-error", action="store_true", help="若某轮脚本失败则立即停止")
    return parser.parse_args()


def count_unscored(conn: sqlite3.Connection, source: str) -> int:
    where = ["(llm_system_score IS NULL OR llm_finance_impact_score IS NULL OR llm_finance_importance IS NULL)"]
    params: list[object] = []
    if source.strip():
        where.append("source = ?")
        params.append(source.strip().lower())
    sql = f"SELECT COUNT(*) FROM news_feed_items WHERE {' AND '.join(where)}"
    row = conn.execute(sql, params).fetchone()
    return int(row[0] if row else 0)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    script_path = Path(__file__).resolve().parent / "llm_score_news.py"

    if not script_path.exists():
        print(f"评分脚本不存在: {script_path}")
        return 1

    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"数据库不存在: {db_path}")
        return 1

    round_no = 0
    total_before = None

    while True:
        conn = sqlite3.connect(db_path)
        try:
            remaining = count_unscored(conn, args.source)
        finally:
            conn.close()

        if total_before is None:
            total_before = remaining

        if remaining <= 0:
            print(f"已完成，未评分新闻已清空。初始待处理={total_before}")
            return 0

        round_no += 1
        if args.max_rounds > 0 and round_no > args.max_rounds:
            print(f"达到最大轮数限制，剩余未评分新闻={remaining}")
            return 0

        batch_size = min(max(args.batch_size, 1), remaining)
        print(f"[round {round_no}] remaining={remaining}, batch_size={batch_size}, model={args.model}")

        cmd = [
            sys.executable,
            str(script_path),
            "--db-path",
            str(db_path),
            "--model",
            args.model,
            "--limit",
            str(batch_size),
            "--retry",
            str(args.retry),
            "--sleep",
            str(args.sleep),
        ]
        if args.source.strip():
            cmd.extend(["--source", args.source.strip()])

        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            print(f"[round {round_no}] 评分脚本退出码={result.returncode}")
            if args.stop_on_error:
                return result.returncode

        time.sleep(max(args.round_sleep, 0.0))


if __name__ == "__main__":
    raise SystemExit(main())
