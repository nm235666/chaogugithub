#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

import db_compat as sqlite3
from realtime_streams import publish_app_event

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"
ITEM_TABLE = "market_expectation_items"
SNAPSHOT_TABLE = "market_expectation_snapshots"
API_URL = "https://gamma-api.polymarket.com/markets"

FINANCE_KEYWORDS = {
    "黄金": ["gold", "gc", "precious metal"],
    "原油": ["oil", "brent", "wti", "crude"],
    "能源": ["energy", "natural gas", "gas prices"],
    "航运": ["shipping", "ship", "ships", "transit", "strait of hormuz", "hormuz"],
    "汇率": ["usd", "dxy", "yuan", "cny", "jpy", "eurusd", "usdcny", "fx", "currency"],
    "利率": ["fed", "rate", "rates", "yield", "treasury", "ecb", "boj"],
    "通胀": ["inflation", "cpi", "ppi", "prices rise"],
    "风险偏好": ["recession", "market crash", "stocks fall", "risk-off", "selloff"],
    "美股": ["s&p", "nasdaq", "dow", "stocks", "wall street"],
    "港股": ["hang seng", "hong kong stocks"],
    "A股": ["china stocks", "a-share", "a shares", "shanghai composite", "shenzhen"],
    "AI": ["ai", "artificial intelligence", "nvidia"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取 Polymarket 公开市场，形成市场预期层")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--limit", type=int, default=150, help="最多抓取多少条 active 市场")
    parser.add_argument("--min-volume", type=float, default=1000.0, help="最小累计成交量过滤")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP 超时秒数")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()[0]
    )


def ensure_tables(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, ITEM_TABLE):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ITEM_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                market_id TEXT NOT NULL,
                question TEXT,
                slug TEXT,
                active INTEGER DEFAULT 1,
                closed INTEGER DEFAULT 0,
                end_date TEXT,
                liquidity REAL,
                volume REAL,
                volume_24h REAL,
                best_bid REAL,
                best_ask REAL,
                last_trade_price REAL,
                outcomes_json TEXT,
                outcome_prices_json TEXT,
                related_theme_names_json TEXT,
                source_url TEXT,
                raw_json TEXT,
                fetched_at TEXT,
                update_time TEXT,
                UNIQUE(source, market_id)
            )
            """
        )
    if not table_exists(conn, SNAPSHOT_TABLE):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_at TEXT NOT NULL,
                market_id TEXT NOT NULL,
                question TEXT,
                active INTEGER DEFAULT 1,
                closed INTEGER DEFAULT 0,
                volume REAL,
                liquidity REAL,
                last_trade_price REAL,
                outcome_prices_json TEXT,
                related_theme_names_json TEXT,
                created_at TEXT,
                UNIQUE(snapshot_at, market_id)
            )
            """
        )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{ITEM_TABLE}_volume ON {ITEM_TABLE}(volume DESC)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{SNAPSHOT_TABLE}_time ON {SNAPSHOT_TABLE}(snapshot_at DESC)")
    conn.commit()


def to_float(value) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def resolve_themes(question: str) -> list[str]:
    text = str(question or "").lower()
    hits = []
    for theme, keywords in FINANCE_KEYWORDS.items():
        if any(keyword_match(text, keyword) for keyword in keywords):
            hits.append(theme)
    return hits


def keyword_match(text: str, keyword: str) -> bool:
    key = str(keyword or "").strip().lower()
    if not key:
        return False
    if any("\u4e00" <= ch <= "\u9fff" for ch in key):
        return key in text
    pattern = r"(?<![a-z0-9])" + re.escape(key) + r"(?![a-z0-9])"
    return re.search(pattern, text) is not None


def fetch_markets(limit: int, timeout: int) -> list[dict]:
    params = {
        "active": "true",
        "closed": "false",
        "limit": str(max(limit, 1)),
        "order": "volume",
        "ascending": "false",
    }
    resp = requests.get(API_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def save_rows(conn: sqlite3.Connection, rows: list[dict]) -> tuple[int, int]:
    now = now_utc_str()
    upserts = 0
    snapshots = 0
    for row in rows:
        conn.execute(
            f"""
            INSERT INTO {ITEM_TABLE} (
                source, market_id, question, slug, active, closed, end_date, liquidity, volume, volume_24h,
                best_bid, best_ask, last_trade_price, outcomes_json, outcome_prices_json, related_theme_names_json,
                source_url, raw_json, fetched_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, market_id) DO UPDATE SET
                question=excluded.question,
                slug=excluded.slug,
                active=excluded.active,
                closed=excluded.closed,
                end_date=excluded.end_date,
                liquidity=excluded.liquidity,
                volume=excluded.volume,
                volume_24h=excluded.volume_24h,
                best_bid=excluded.best_bid,
                best_ask=excluded.best_ask,
                last_trade_price=excluded.last_trade_price,
                outcomes_json=excluded.outcomes_json,
                outcome_prices_json=excluded.outcome_prices_json,
                related_theme_names_json=excluded.related_theme_names_json,
                source_url=excluded.source_url,
                raw_json=excluded.raw_json,
                fetched_at=excluded.fetched_at,
                update_time=excluded.update_time
            """,
            (
                "polymarket",
                row["market_id"],
                row["question"],
                row["slug"],
                row["active"],
                row["closed"],
                row["end_date"],
                row["liquidity"],
                row["volume"],
                row["volume_24h"],
                row["best_bid"],
                row["best_ask"],
                row["last_trade_price"],
                row["outcomes_json"],
                row["outcome_prices_json"],
                row["related_theme_names_json"],
                row["source_url"],
                row["raw_json"],
                now,
                now,
            ),
        )
        conn.execute(
            f"""
            INSERT INTO {SNAPSHOT_TABLE} (
                snapshot_at, market_id, question, active, closed, volume, liquidity, last_trade_price,
                outcome_prices_json, related_theme_names_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_at, market_id) DO NOTHING
            """,
            (
                now,
                row["market_id"],
                row["question"],
                row["active"],
                row["closed"],
                row["volume"],
                row["liquidity"],
                row["last_trade_price"],
                row["outcome_prices_json"],
                row["related_theme_names_json"],
                now,
            ),
        )
        upserts += 1
        snapshots += 1
    conn.commit()
    return upserts, snapshots


def main() -> int:
    args = parse_args()
    raw_rows = fetch_markets(limit=args.limit, timeout=args.timeout)
    prepared = []
    for item in raw_rows:
        if not isinstance(item, dict):
            continue
        question = str(item.get("question") or "").strip()
        if not question:
            continue
        volume = to_float(item.get("volume") or item.get("volumeNum"))
        if volume < float(args.min_volume):
            continue
        themes = resolve_themes(question)
        if not themes:
            continue
        market_id = str(item.get("id") or "").strip()
        if not market_id:
            continue
        slug = str(item.get("slug") or "").strip()
        prepared.append(
            {
                "market_id": market_id,
                "question": question,
                "slug": slug,
                "active": 1 if bool(item.get("active")) else 0,
                "closed": 1 if bool(item.get("closed")) else 0,
                "end_date": str(item.get("endDate") or item.get("endDateIso") or "").strip(),
                "liquidity": to_float(item.get("liquidity") or item.get("liquidityNum")),
                "volume": volume,
                "volume_24h": to_float(item.get("volume24hr") or item.get("volume24hrClob")),
                "best_bid": to_float(item.get("bestBid")),
                "best_ask": to_float(item.get("bestAsk")),
                "last_trade_price": to_float(item.get("lastTradePrice")),
                "outcomes_json": json.dumps(json.loads(item.get("outcomes") or "[]") if isinstance(item.get("outcomes"), str) else item.get("outcomes") or [], ensure_ascii=False),
                "outcome_prices_json": json.dumps(json.loads(item.get("outcomePrices") or "[]") if isinstance(item.get("outcomePrices"), str) else item.get("outcomePrices") or [], ensure_ascii=False),
                "related_theme_names_json": json.dumps(themes, ensure_ascii=False),
                "source_url": f"https://polymarket.com/event/{slug}" if slug else "https://polymarket.com/",
                "raw_json": json.dumps(item, ensure_ascii=False),
            }
        )

    conn = sqlite3.connect(args.db_path)
    try:
        ensure_tables(conn)
        upserts, snapshots = save_rows(conn, prepared)
        publish_app_event(
            event="market_expectation_update",
            payload={"source": "polymarket", "rows": upserts, "snapshots": snapshots},
            producer="fetch_market_expectations_polymarket.py",
        )
        print(f"完成: markets={upserts}, snapshots={snapshots}, source=polymarket")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
