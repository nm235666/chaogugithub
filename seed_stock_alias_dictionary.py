#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"

ALIAS_ROWS = [
    ("宁王", "300750.SZ", "宁德时代", "manual", 0.99, "manual_seed", "常见群聊别名"),
    ("茅台", "600519.SH", "贵州茅台", "manual", 0.99, "manual_seed", "常见简称"),
    ("东土", "300353.SZ", "东土科技", "manual", 0.97, "manual_seed", "常见简称"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="初始化股票别名字典")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_alias_dictionary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alias TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            stock_name TEXT,
            alias_type TEXT,
            confidence REAL DEFAULT 1.0,
            source TEXT,
            notes TEXT,
            used_count INTEGER DEFAULT 0,
            last_used_at TEXT,
            created_at TEXT,
            update_time TEXT,
            UNIQUE(alias)
        )
        """
    )
    conn.commit()


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    try:
        ensure_table(conn)
        now = now_utc_str()
        affected = 0
        for alias, ts_code, stock_name, alias_type, confidence, source, notes in ALIAS_ROWS:
            updated = conn.execute(
                """
                UPDATE stock_alias_dictionary
                SET ts_code = ?, stock_name = ?, alias_type = ?, confidence = ?, source = ?, notes = ?, update_time = ?
                WHERE alias = ?
                """,
                (ts_code, stock_name, alias_type, confidence, source, notes, now, alias),
            ).rowcount
            if not updated:
                conn.execute(
                    """
                    INSERT INTO stock_alias_dictionary (
                        alias, ts_code, stock_name, alias_type, confidence, source, notes, created_at, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (alias, ts_code, stock_name, alias_type, confidence, source, notes, now, now),
                )
            affected += 1
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM stock_alias_dictionary").fetchone()[0]
    finally:
        conn.close()
    print(f"seeded={affected} total={int(total or 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
