#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"

THEME_ROWS = [
    ("黄金", "600489.SH", "中金黄金", "核心标的", 1.00, "manual_seed", "黄金主题核心映射"),
    ("黄金", "601899.SH", "紫金矿业", "核心标的", 1.00, "manual_seed", "黄金/有色龙头"),
    ("黄金", "600988.SH", "赤峰黄金", "核心标的", 0.95, "manual_seed", "黄金弹性标的"),
    ("黄金", "002155.SZ", "湖南黄金", "核心标的", 0.90, "manual_seed", "黄金资源股"),
    ("黄金", "600547.SH", "山东黄金", "核心标的", 1.00, "manual_seed", "黄金龙头"),
    ("黄金", "000975.SZ", "山金国际", "扩展标的", 0.85, "manual_seed", "黄金主题扩展"),
    ("原油", "601857.SH", "中国石油", "核心标的", 1.00, "manual_seed", "油价主题核心映射"),
    ("原油", "600028.SH", "中国石化", "核心标的", 0.95, "manual_seed", "炼化与油价相关"),
    ("原油", "600938.SH", "中国海油", "核心标的", 1.00, "manual_seed", "上游油气龙头"),
    ("原油", "600871.SH", "石化油服", "扩展标的", 0.75, "manual_seed", "油服链条"),
    ("原油", "000554.SZ", "泰山石油", "扩展标的", 0.65, "manual_seed", "油价情绪弹性"),
    ("能源", "601088.SH", "中国神华", "核心标的", 1.00, "manual_seed", "煤炭能源龙头"),
    ("能源", "601225.SH", "陕西煤业", "核心标的", 0.95, "manual_seed", "煤炭龙头"),
    ("能源", "600188.SH", "兖矿能源", "核心标的", 0.95, "manual_seed", "能源主题高弹性"),
    ("能源", "600938.SH", "中国海油", "扩展标的", 0.85, "manual_seed", "油气能源"),
    ("能源", "600905.SH", "三峡能源", "扩展标的", 0.80, "manual_seed", "电力新能源"),
    ("能源", "003816.SZ", "中国广核", "扩展标的", 0.75, "manual_seed", "核电能源"),
    ("AI", "002230.SZ", "科大讯飞", "核心标的", 1.00, "manual_seed", "AI 应用龙头"),
    ("AI", "603019.SH", "中科曙光", "核心标的", 0.95, "manual_seed", "算力基础设施"),
    ("AI", "300308.SZ", "中际旭创", "核心标的", 0.90, "manual_seed", "光模块"),
    ("AI", "300502.SZ", "新易盛", "核心标的", 0.90, "manual_seed", "光模块"),
    ("AI", "300394.SZ", "天孚通信", "扩展标的", 0.85, "manual_seed", "算力链条"),
    ("AI", "603881.SH", "数据港", "扩展标的", 0.80, "manual_seed", "IDC/算力"),
    ("航运", "601919.SH", "中远海控", "核心标的", 1.00, "manual_seed", "集运龙头"),
    ("航运", "600026.SH", "中远海能", "核心标的", 0.95, "manual_seed", "油运"),
    ("航运", "601872.SH", "招商轮船", "核心标的", 0.90, "manual_seed", "航运龙头"),
    ("航运", "601083.SH", "锦江航运", "扩展标的", 0.75, "manual_seed", "集运"),
    ("航运", "601866.SH", "中远海发", "扩展标的", 0.70, "manual_seed", "航运链条"),
    ("军工", "600760.SH", "中航沈飞", "核心标的", 1.00, "manual_seed", "军工主机厂"),
    ("军工", "600893.SH", "航发动力", "核心标的", 1.00, "manual_seed", "航空发动机"),
    ("军工", "000768.SZ", "中航西飞", "核心标的", 0.95, "manual_seed", "军机制造"),
    ("军工", "600862.SH", "中航高科", "扩展标的", 0.85, "manual_seed", "军工材料"),
    ("军工", "000733.SZ", "振华科技", "扩展标的", 0.85, "manual_seed", "军工电子"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="初始化主题 -> 股票池映射")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--replace", action="store_true", help="先清空主题映射后再重建")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_stock_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme_name TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            stock_name TEXT,
            relation_type TEXT,
            weight REAL DEFAULT 1.0,
            source TEXT,
            notes TEXT,
            created_at TEXT,
            update_time TEXT,
            UNIQUE(theme_name, ts_code)
        )
        """
    )
    conn.commit()


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    try:
        ensure_table(conn)
        if args.replace:
            conn.execute("DELETE FROM theme_stock_mapping")
        now = now_utc_str()
        affected = 0
        for row in THEME_ROWS:
            theme_name, ts_code, stock_name, relation_type, weight, source, notes = row
            updated = conn.execute(
                """
                UPDATE theme_stock_mapping
                SET stock_name = ?, relation_type = ?, weight = ?, source = ?, notes = ?, update_time = ?
                WHERE theme_name = ? AND ts_code = ?
                """,
                (stock_name, relation_type, weight, source, notes, now, theme_name, ts_code),
            ).rowcount
            if not updated:
                conn.execute(
                    """
                    INSERT INTO theme_stock_mapping (
                        theme_name, ts_code, stock_name, relation_type, weight, source, notes, created_at, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (theme_name, ts_code, stock_name, relation_type, weight, source, notes, now, now),
                )
            affected += 1
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM theme_stock_mapping").fetchone()[0]
    finally:
        conn.close()
    print(f"seeded={affected} total={int(total or 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
