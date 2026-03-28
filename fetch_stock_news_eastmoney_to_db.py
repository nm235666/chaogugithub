#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import db_compat as sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from realtime_streams import publish_app_event
from query_stock_news_eastmoney import (
    fetch_news,
    normalize_items,
    resolve_name_from_ts_code,
)

SOURCE = "eastmoney_stock_news"
LOCAL_DEDUP_LOOKBACK = 14


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取东方财富个股新闻并入库")
    parser.add_argument("--name", default="", help="股票名称，如 恒立液压")
    parser.add_argument("--ts-code", default="", help="股票代码，如 601100.SH")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--page", type=int, default=1, help="页码，默认 1")
    parser.add_argument("--page-size", type=int, default=20, help="每页数量，默认 20")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP 超时秒数")
    parser.add_argument("--dry-run", action="store_true", help="只抓取解析，不入库")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean_text(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^\w\u4e00-\u9fff]+", "", text)
    return text


def _clean_link(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
        return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, "", ""))
    except Exception:
        return raw


def _pub_day(value: object) -> str:
    text = str(value or "").strip()
    return text[:10] if len(text) >= 10 else text


def _candidate_signatures(ts_code: str, item: dict) -> set[str]:
    title = _clean_text(item.get("title"))
    summary = _clean_text(item.get("summary"))
    news_code = str(item.get("news_code") or "").strip()
    link = _clean_link(item.get("link"))
    pub_day = _pub_day(item.get("pub_time"))

    signatures: set[str] = set()
    if news_code:
        signatures.add(f"code:{ts_code}:{news_code}")
    if link:
        signatures.add(f"link:{ts_code}:{link}")
    if title:
        signatures.add(f"title:{ts_code}:{title}")
        if pub_day:
            signatures.add(f"title_day:{ts_code}:{title}:{pub_day}")
    if title and summary:
        signatures.add(f"title_summary:{ts_code}:{title}:{summary[:160]}")
        if pub_day:
            signatures.add(f"title_summary_day:{ts_code}:{title}:{summary[:160]}:{pub_day}")
    return signatures


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_news_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT NOT NULL,
            company_name TEXT NOT NULL,
            source TEXT NOT NULL,
            news_code TEXT,
            title TEXT NOT NULL,
            summary TEXT,
            link TEXT,
            pub_time TEXT,
            comment_num INTEGER,
            relation_stock_tags_json TEXT,
            llm_system_score INTEGER,
            llm_finance_impact_score INTEGER,
            llm_finance_importance TEXT,
            llm_impacts_json TEXT,
            llm_summary TEXT,
            llm_model TEXT,
            llm_scored_at TEXT,
            llm_prompt_version TEXT,
            llm_raw_output TEXT,
            content_hash TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            update_time TEXT,
            UNIQUE(ts_code, source, content_hash)
        )
        """
    )
    cols = {r[1] for r in conn.execute("PRAGMA table_info(stock_news_items)").fetchall()}
    need = [
        ("llm_system_score", "INTEGER"),
        ("llm_finance_impact_score", "INTEGER"),
        ("llm_finance_importance", "TEXT"),
        ("llm_impacts_json", "TEXT"),
        ("llm_summary", "TEXT"),
        ("llm_model", "TEXT"),
        ("llm_scored_at", "TEXT"),
        ("llm_prompt_version", "TEXT"),
        ("llm_raw_output", "TEXT"),
    ]
    for name, typ in need:
        if name not in cols:
            conn.execute(f"ALTER TABLE stock_news_items ADD COLUMN {name} {typ}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_news_code_time ON stock_news_items(ts_code, pub_time)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_news_source_time ON stock_news_items(source, pub_time)"
    )
    conn.commit()


def content_hash(ts_code: str, item: dict) -> str:
    raw = "||".join(
        [
            ts_code,
            str(item.get("news_code") or "").strip(),
            _clean_text(item.get("title")),
            _clean_text(item.get("summary")),
            _clean_link(item.get("link")),
            _pub_day(item.get("pub_time")),
        ]
    ).encode(
        "utf-8",
        errors="ignore",
    )
    return hashlib.sha256(raw).hexdigest()


def _load_existing_signatures(conn: sqlite3.Connection, ts_code: str, lookback_days: int = LOCAL_DEDUP_LOOKBACK) -> set[str]:
    rows = conn.execute(
        """
        SELECT news_code, title, summary, link, pub_time
        FROM stock_news_items
        WHERE ts_code = ?
        ORDER BY pub_time DESC, id DESC
        LIMIT ?
        """,
        (ts_code, max(lookback_days * 80, 300)),
    ).fetchall()
    signatures: set[str] = set()
    for row in rows:
        signatures.update(
            _candidate_signatures(
                ts_code,
                {
                    "news_code": row[0],
                    "title": row[1],
                    "summary": row[2],
                    "link": row[3],
                    "pub_time": row[4],
                },
            )
        )
    return signatures


def deduplicate_items(conn: sqlite3.Connection, ts_code: str, items: list[dict]) -> tuple[list[dict], int]:
    existing_signatures = _load_existing_signatures(conn, ts_code)
    batch_signatures: set[str] = set()
    unique_items: list[dict] = []
    skipped = 0

    for item in items:
        signatures = _candidate_signatures(ts_code, item)
        if not signatures:
            unique_items.append(item)
            continue
        if signatures & existing_signatures or signatures & batch_signatures:
            skipped += 1
            continue
        unique_items.append(item)
        batch_signatures.update(signatures)
    return unique_items, skipped


def upsert(conn: sqlite3.Connection, ts_code: str, company_name: str, items: list[dict]) -> tuple[int, int, int]:
    fetched_at = now_utc_str()
    deduped_items, dedup_skipped = deduplicate_items(conn, ts_code, items)
    inserted = 0
    skipped = 0
    for item in deduped_items:
        h = content_hash(ts_code, item)
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO stock_news_items (
                ts_code, company_name, source, news_code, title, summary, link, pub_time,
                comment_num, relation_stock_tags_json, content_hash, fetched_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts_code,
                company_name,
                SOURCE,
                item.get("news_code", ""),
                item.get("title", ""),
                item.get("summary", ""),
                item.get("link", ""),
                item.get("pub_time", ""),
                item.get("comment_num"),
                json.dumps(item.get("relation_stock_tags") or [], ensure_ascii=False),
                h,
                fetched_at,
                fetched_at,
            ),
        )
        if cur.rowcount == 1:
            inserted += 1
        else:
            skipped += 1
    conn.commit()
    return inserted, skipped + dedup_skipped, dedup_skipped


def main() -> int:
    args = parse_args()
    ts_code = args.ts_code.strip().upper()
    company_name = args.name.strip()
    db_path = Path(args.db_path).resolve()

    if not company_name:
        if not ts_code:
            print("请传入 --name 或 --ts-code")
            return 1
        try:
            company_name = resolve_name_from_ts_code(str(db_path), ts_code)
        except Exception as exc:
            print(f"解析股票名称失败: {exc}")
            return 2

    if not ts_code:
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                """
                SELECT ts_code
                FROM stock_codes
                WHERE name = ?
                ORDER BY CASE list_status WHEN 'L' THEN 0 ELSE 1 END, ts_code
                LIMIT 1
                """,
                (company_name,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            print(f"未在 stock_codes 中找到公司: {company_name}")
            return 3
        ts_code = str(row[0]).strip().upper()

    try:
        payload = fetch_news(company_name, page=args.page, page_size=args.page_size, timeout=args.timeout)
        total, items = normalize_items(payload)
    except Exception as exc:
        print(f"抓取失败: {exc}")
        return 4

    print(f"股票: {company_name} ({ts_code})")
    print(f"总命中: {total}")
    print(f"本页返回: {len(items)}")

    if args.dry_run:
        for item in items[:5]:
            print(f"- {item['pub_time']} | {item['title']}")
        return 0

    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn)
        inserted, skipped, dedup_skipped = upsert(conn, ts_code, company_name, items)
        table_rows = conn.execute(
            "SELECT COUNT(*) FROM stock_news_items WHERE ts_code = ?",
            (ts_code,),
        ).fetchone()[0]
    finally:
        conn.close()

    print(
        f"完成: source={SOURCE}, inserted={inserted}, skipped={skipped}, "
        f"dedup_skipped={dedup_skipped}, stock_rows={table_rows}, page={args.page}, page_size={args.page_size}"
    )
    publish_app_event(
        event="stock_news_update",
        payload={
            "ts_code": ts_code,
            "company_name": company_name,
            "inserted": int(inserted),
            "skipped": int(skipped),
            "dedup_skipped": int(dedup_skipped),
            "stock_rows": int(table_rows),
            "page_size": int(args.page_size),
        },
        producer="fetch_stock_news_eastmoney_to_db.py",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
