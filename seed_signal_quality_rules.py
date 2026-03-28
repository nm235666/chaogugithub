#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import argparse

import db_compat as sqlite3

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"

DEFAULT_RULES = [
    ("chatroom_min_room_count", "2", "number", "chatroom", "群聊股票信号至少涉及多少个群才算稳定"),
    ("chatroom_min_mention_count", "3", "number", "chatroom", "群聊股票信号至少累计提及多少次才算稳定"),
    ("chatroom_weak_strength_threshold", "3", "number", "chatroom", "群聊股票信号低于该强度视为弱信号"),
    ("theme_only_stock_enabled", "0", "bool", "theme_mapping", "是否允许仅主题传导股票进入股票信号"),
    ("theme_mapping_weight_cap", "5", "number", "theme_mapping", "主题传导到股票的最大权重"),
    ("active_min_confidence_pct", "40", "number", "status", "活跃信号最低置信度"),
    ("stock_min_strength_with_direct_source", "2", "number", "threshold", "有直接来源时股票最低强度"),
    ("stock_min_strength_without_direct_source", "10", "number", "threshold", "无直接来源时股票最低强度"),
]

DEFAULT_BLOCKLIST = [
    ("黄金", "stock", "exact", "manual_seed", "主题词，禁止直接映射为股票"),
    ("原油", "stock", "exact", "manual_seed", "主题词，禁止直接映射为股票"),
    ("能源", "stock", "exact", "manual_seed", "主题词，禁止直接映射为股票"),
    ("AI", "stock", "exact", "manual_seed", "主题词，禁止直接映射为股票"),
    ("航运", "stock", "exact", "manual_seed", "主题词，禁止直接映射为股票"),
    ("军工", "stock", "exact", "manual_seed", "主题词，禁止直接映射为股票"),
    ("A股", "stock", "exact", "manual_seed", "市场词，禁止直接映射为股票"),
    ("美股", "stock", "exact", "manual_seed", "市场词，禁止直接映射为股票"),
    ("港股", "stock", "exact", "manual_seed", "市场词，禁止直接映射为股票"),
    ("人民币", "stock", "exact", "manual_seed", "宏观词，禁止直接映射为股票"),
    ("美元", "stock", "exact", "manual_seed", "宏观词，禁止直接映射为股票"),
    ("利率", "stock", "contains", "manual_seed", "宏观词，禁止直接映射为股票"),
    ("通胀", "stock", "contains", "manual_seed", "宏观词，禁止直接映射为股票"),
    ("Meta", "stock", "exact", "manual_seed", "海外公司，禁止直接映射为A股"),
    ("英伟达", "stock", "exact", "manual_seed", "海外公司，禁止直接映射为A股"),
    ("特斯拉", "stock", "exact", "manual_seed", "海外公司，禁止直接映射为A股"),
    ("腾讯", "stock", "exact", "manual_seed", "非A股主体，禁止直接映射为A股"),
    ("小米", "stock", "exact", "manual_seed", "非A股主体，禁止直接映射为A股"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="初始化信号质量规则与映射黑名单")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_mapping_blocklist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            term TEXT NOT NULL,
            target_type TEXT NOT NULL DEFAULT 'stock',
            match_type TEXT NOT NULL DEFAULT 'exact',
            source TEXT,
            reason TEXT,
            enabled INTEGER DEFAULT 1,
            created_at TEXT,
            update_time TEXT,
            UNIQUE(term, target_type, match_type)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_quality_rules (
            rule_key TEXT PRIMARY KEY,
            rule_value TEXT,
            value_type TEXT DEFAULT 'number',
            category TEXT,
            description TEXT,
            enabled INTEGER DEFAULT 1,
            created_at TEXT,
            update_time TEXT
        )
        """
    )
    conn.commit()


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    now = now_utc_str()
    try:
        ensure_tables(conn)
        for rule_key, rule_value, value_type, category, description in DEFAULT_RULES:
            updated = conn.execute(
                """
                UPDATE signal_quality_rules
                SET rule_value = ?, value_type = ?, category = ?, description = ?, enabled = 1, update_time = ?
                WHERE rule_key = ?
                """,
                (rule_value, value_type, category, description, now, rule_key),
            ).rowcount
            if not updated:
                conn.execute(
                    """
                    INSERT INTO signal_quality_rules (
                        rule_key, rule_value, value_type, category, description, enabled, created_at, update_time
                    ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (rule_key, rule_value, value_type, category, description, now, now),
                )
        for term, target_type, match_type, source, reason in DEFAULT_BLOCKLIST:
            updated = conn.execute(
                """
                UPDATE signal_mapping_blocklist
                SET source = ?, reason = ?, enabled = 1, update_time = ?
                WHERE term = ? AND target_type = ? AND match_type = ?
                """,
                (source, reason, now, term, target_type, match_type),
            ).rowcount
            if not updated:
                conn.execute(
                    """
                    INSERT INTO signal_mapping_blocklist (
                        term, target_type, match_type, source, reason, enabled, created_at, update_time
                    ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (term, target_type, match_type, source, reason, now, now),
                )
        conn.commit()
        print("seeded signal quality rules and blocklist")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
