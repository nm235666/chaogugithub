#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import html
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests

import db_compat as sqlite3
from realtime_streams import publish_news_batch

DEFAULT_SOURCE = "marketscreener_live_news"
DEFAULT_URL = "https://www.marketscreener.com/news/"

DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "zh-CN,zh;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=0, i",
    "sec-ch-ua": '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取 MarketScreener Live 国际新闻并入库")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="MarketScreener 新闻页 URL")
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="写入 news_feed_items 的 source")
    parser.add_argument("--category", default="国际实时新闻", help="新闻分类")
    parser.add_argument("--site-author", default="MarketScreener", help="默认作者/站点名")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP 超时秒数")
    parser.add_argument("--limit", type=int, default=50, help="本次最多入库条数")
    parser.add_argument("--dry-run", action="store_true", help="只抓取解析，不入库")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_feed_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            title TEXT,
            link TEXT,
            guid TEXT,
            summary TEXT,
            category TEXT,
            author TEXT,
            pub_date TEXT,
            fetched_at TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            UNIQUE(source, content_hash)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_news_source_pub_date ON news_feed_items(source, pub_date)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_news_fetched_at ON news_feed_items(fetched_at)")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uniq_news_source_hash ON news_feed_items(source, content_hash)"
    )
    conn.commit()


def clean_text(text: str) -> str:
    s = html.unescape(re.sub(r"<[^>]+>", " ", text or ""))
    return re.sub(r"\s+", " ", s).strip()


def fetch_html(url: str, timeout: int) -> str:
    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return resp.text


def extract_live_block(html_text: str) -> str:
    match = re.search(
        r'<h2[^>]*class="card-title"[^>]*>\s*Live\s*</h2>.*?<table[^>]*id="newsScreener"[^>]*>(.*?)</table>',
        html_text,
        flags=re.I | re.S,
    )
    if not match:
        raise ValueError("未找到 MarketScreener Live 新闻区块")
    return match.group(1)


def parse_items(page_url: str, html_text: str, source: str, category: str, site_author: str) -> list[dict]:
    block = extract_live_block(html_text)
    rows = re.findall(r"<tr\b[^>]*>(.*?)</tr>", block, flags=re.I | re.S)
    items: list[dict] = []
    seen: set[str] = set()

    for row in rows:
        date_match = re.search(r'data-utc-date="([^"]+)"', row, flags=re.I)
        link_match = re.search(r'<a href="([^"]+/news/[^"]+|/news/[^"]+)"[^>]*>(.*?)</a>', row, flags=re.I | re.S)
        if not date_match or not link_match:
            continue
        pub_date = str(date_match.group(1) or "").strip()
        rel_link = str(link_match.group(1) or "").strip()
        title = clean_text(link_match.group(2))
        if not pub_date or not rel_link or not title:
            continue
        full_link = urljoin(page_url, rel_link)
        badge_match = re.search(r'<span[^>]+badge[^>]+title="([^"]+)"', row, flags=re.I | re.S)
        author = clean_text(badge_match.group(1)) if badge_match else site_author
        guid = full_link
        key = f"{pub_date}|{full_link}|{title.lower()}"
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "source": source,
                "title": title,
                "link": full_link,
                "guid": guid,
                "summary": "",
                "category": category,
                "author": author or site_author,
                "pub_date": pub_date,
            }
        )
    return items


def content_hash(item: dict) -> str:
    raw = f"{item['guid']}||{item['title']}||{item['pub_date']}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()


def marketscreener_family_exists(conn: sqlite3.Connection, item: dict) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM news_feed_items
        WHERE source IN ('marketscreener_byd_news', 'marketscreener_live_news')
          AND (
            (COALESCE(link, '') <> '' AND link = ?)
            OR (
              LOWER(TRIM(COALESCE(title, ''))) = LOWER(TRIM(?))
              AND COALESCE(pub_date, '') = ?
            )
          )
        LIMIT 1
        """,
        (item["link"], item["title"], item["pub_date"]),
    ).fetchone()
    return bool(row)


def upsert(conn: sqlite3.Connection, items: list[dict], limit: int) -> tuple[int, int, list[dict]]:
    fetched_at = now_utc_str()
    inserted = 0
    skipped = 0
    inserted_items: list[dict] = []
    for it in items[: max(limit, 0)]:
        if marketscreener_family_exists(conn, it):
            skipped += 1
            continue
        h = content_hash(it)
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO news_feed_items (
                source, title, link, guid, summary, category, author, pub_date, fetched_at, content_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                it["source"],
                it["title"],
                it["link"],
                it["guid"],
                it["summary"],
                it["category"],
                it["author"],
                it["pub_date"],
                fetched_at,
                h,
            ),
        )
        if cur.rowcount == 1:
            inserted += 1
            inserted_items.append({**it, "scope": "international"})
        else:
            skipped += 1
    conn.commit()
    return inserted, skipped, inserted_items


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"数据库不存在: {db_path}")
        return 1
    try:
        html_text = fetch_html(args.url, args.timeout)
        items = parse_items(args.url, html_text, args.source, args.category, args.site_author)
    except Exception as exc:
        print(f"抓取失败: {exc}")
        return 2
    print(f"抓取并解析到 {len(items)} 条 Live 新闻")
    if args.dry_run:
        for it in items[:5]:
            print(f"- {it['pub_date']} | {it['author']} | {it['title']}")
        return 0
    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn)
        inserted, skipped, inserted_items = upsert(conn, items, args.limit)
        total = conn.execute("SELECT COUNT(*) FROM news_feed_items").fetchone()[0]
        print(
            f"完成: source={args.source}, fetched={len(items)}, try_insert={min(len(items), args.limit)}, "
            f"inserted={inserted}, skipped={skipped}, table_rows={total}"
        )
        if inserted_items:
            publish_news_batch(
                source=args.source,
                scope="international",
                items=inserted_items,
                producer="fetch_news_marketscreener_live.py",
            )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
