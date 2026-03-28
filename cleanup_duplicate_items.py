#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

import db_compat as sqlite3


ROOT = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="清理新闻与聊天记录重复数据")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--semantic-days", type=int, default=30, help="语义去重回看最近多少天")
    return parser.parse_args()


def _norm_text(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^\w\u4e00-\u9fff]+", "", text)
    return text


def _date_key(value: object) -> str:
    text = str(value or "").strip()
    return text[:10] if len(text) >= 10 else text


def _news_semantic_keys(row: dict) -> list[str]:
    source = str(row.get("source") or "").strip().lower()
    day = _date_key(row.get("pub_date"))
    title = _norm_text(row.get("title"))
    summary = _norm_text(row.get("summary"))
    keys = []
    if title:
        keys.append(f"{source}|{day}|t:{title[:72]}")
    if title and summary:
        keys.append(f"{source}|{day}|ts:{title[:48]}|{summary[:120]}")
    return keys


def _stock_news_semantic_keys(row: dict) -> list[str]:
    ts_code = str(row.get("ts_code") or "").strip().upper()
    day = _date_key(row.get("pub_time"))
    title = _norm_text(row.get("title"))
    summary = _norm_text(row.get("summary"))
    keys = []
    if title:
        keys.append(f"{ts_code}|{day}|t:{title[:72]}")
    if title and summary:
        keys.append(f"{ts_code}|{day}|ts:{title[:48]}|{summary[:120]}")
    return keys


def _score_value(row: dict) -> tuple:
    return (
        int(row.get("llm_finance_impact_score") or 0),
        int(row.get("llm_system_score") or 0),
        str(row.get("pub_date") or row.get("pub_time") or ""),
        str(row.get("fetched_at") or row.get("update_time") or ""),
        int(row.get("id") or 0),
    )


def _semantic_duplicate_ids(rows: list[dict], key_builder) -> set[int]:
    keeper_by_key: dict[str, dict] = {}
    dup_ids: set[int] = set()
    for row in rows:
        keys = key_builder(row)
        if not keys:
            continue
        best_existing = None
        best_key = None
        for key in keys:
            existing = keeper_by_key.get(key)
            if existing is not None and (best_existing is None or _score_value(existing) > _score_value(best_existing)):
                best_existing = existing
                best_key = key
        if best_existing is None:
            for key in keys:
                keeper_by_key[key] = row
            continue
        if _score_value(row) > _score_value(best_existing):
            dup_ids.add(int(best_existing["id"]))
            for key in keys:
                keeper_by_key[key] = row
        else:
            dup_ids.add(int(row["id"]))
    return dup_ids


def count_duplicate_groups(conn) -> dict[str, int]:
    return {
        "news_feed_link_groups": conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT source, COALESCE(link,''), COUNT(*) c
              FROM news_feed_items
              GROUP BY source, COALESCE(link,'')
              HAVING COALESCE(link,'') <> '' AND COUNT(*) > 1
            ) t
            """
        ).fetchone()[0],
        "stock_news_link_groups": conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT ts_code, COALESCE(link,''), COUNT(*) c
              FROM stock_news_items
              GROUP BY ts_code, COALESCE(link,'')
              HAVING COALESCE(link,'') <> '' AND COUNT(*) > 1
            ) t
            """
        ).fetchone()[0],
        "news_feed_semantic_groups": conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT source, SUBSTR(COALESCE(pub_date,''),1,10), LOWER(REGEXP_REPLACE(COALESCE(title,''), '[^a-zA-Z0-9一-龥]+', '', 'g')), COUNT(*) c
              FROM news_feed_items
              WHERE COALESCE(title,'') <> ''
              GROUP BY source, SUBSTR(COALESCE(pub_date,''),1,10), LOWER(REGEXP_REPLACE(COALESCE(title,''), '[^a-zA-Z0-9一-龥]+', '', 'g'))
              HAVING COUNT(*) > 1
            ) t
            """
        ).fetchone()[0],
        "stock_news_semantic_groups": conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT ts_code, SUBSTR(COALESCE(pub_time,''),1,10), LOWER(REGEXP_REPLACE(COALESCE(title,''), '[^a-zA-Z0-9一-龥]+', '', 'g')), COUNT(*) c
              FROM stock_news_items
              WHERE COALESCE(title,'') <> ''
              GROUP BY ts_code, SUBSTR(COALESCE(pub_time,''),1,10), LOWER(REGEXP_REPLACE(COALESCE(title,''), '[^a-zA-Z0-9一-龥]+', '', 'g'))
              HAVING COUNT(*) > 1
            ) t
            """
        ).fetchone()[0],
        "chatlog_key_groups": conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT message_key, COUNT(*) c
              FROM wechat_chatlog_clean_items
              GROUP BY message_key
              HAVING COUNT(*) > 1
            ) t
            """
        ).fetchone()[0],
    }


def cleanup_semantic_news(conn, semantic_days: int) -> tuple[int, int]:
    cutoff_ymd = conn.execute(
        "SELECT TO_CHAR(CURRENT_DATE - (%s * INTERVAL '1 day'), 'YYYYMMDD')" % max(semantic_days, 1)
    ).fetchone()[0]
    news_rows = [
        dict(r)
        for r in conn.execute(
            """
            SELECT id, source, title, summary, pub_date, fetched_at, llm_finance_impact_score, llm_system_score
            FROM news_feed_items
            WHERE SUBSTR(REPLACE(COALESCE(pub_date, fetched_at, ''), '-', ''), 1, 8) >= ?
            ORDER BY COALESCE(pub_date, fetched_at, '') DESC, id DESC
            """,
            (cutoff_ymd,),
        ).fetchall()
    ]
    stock_rows = [
        dict(r)
        for r in conn.execute(
            """
            SELECT id, ts_code, title, summary, pub_time, update_time, llm_finance_impact_score, llm_system_score
            FROM stock_news_items
            WHERE SUBSTR(REPLACE(COALESCE(pub_time, update_time, ''), '-', ''), 1, 8) >= ?
            ORDER BY COALESCE(pub_time, update_time, '') DESC, id DESC
            """,
            (cutoff_ymd,),
        ).fetchall()
    ]
    news_dup_ids = _semantic_duplicate_ids(news_rows, _news_semantic_keys)
    stock_dup_ids = _semantic_duplicate_ids(stock_rows, _stock_news_semantic_keys)
    if news_dup_ids:
        conn.execute(
            f"DELETE FROM news_feed_items WHERE id IN ({','.join(['?'] * len(news_dup_ids))})",
            tuple(sorted(news_dup_ids)),
        )
    if stock_dup_ids:
        conn.execute(
            f"DELETE FROM stock_news_items WHERE id IN ({','.join(['?'] * len(stock_dup_ids))})",
            tuple(sorted(stock_dup_ids)),
        )
    conn.commit()
    return len(news_dup_ids), len(stock_dup_ids)


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(ROOT / "stocks.db")
    conn.row_factory = sqlite3.Row
    try:
        before = count_duplicate_groups(conn)
        print("before", before)
        if args.dry_run:
            return 0

        conn.execute(
            """
            DELETE FROM news_feed_items
            WHERE id IN (
              SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                         PARTITION BY source, COALESCE(link,'')
                         ORDER BY COALESCE(pub_date,'' ) DESC, COALESCE(fetched_at,'') DESC, id DESC
                       ) AS rn
                FROM news_feed_items
                WHERE COALESCE(link,'') <> ''
              ) x
              WHERE x.rn > 1
            )
            """
        )

        conn.execute(
            """
            DELETE FROM stock_news_items
            WHERE id IN (
              SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                         PARTITION BY ts_code, COALESCE(link,'')
                         ORDER BY COALESCE(pub_time,'') DESC, COALESCE(update_time,'') DESC, id DESC
                       ) AS rn
                FROM stock_news_items
                WHERE COALESCE(link,'') <> ''
              ) x
              WHERE x.rn > 1
            )
            """
        )

        conn.execute(
            """
            DELETE FROM wechat_chatlog_clean_items
            WHERE id IN (
              SELECT id FROM (
                SELECT id,
                       ROW_NUMBER() OVER (
                         PARTITION BY message_key
                         ORDER BY COALESCE(update_time,'' ) DESC, COALESCE(fetched_at,'') DESC, id DESC
                       ) AS rn
                FROM wechat_chatlog_clean_items
              ) x
              WHERE x.rn > 1
            )
            """
        )
        conn.commit()
        semantic_news_deleted, semantic_stock_deleted = cleanup_semantic_news(conn, args.semantic_days)
        after = count_duplicate_groups(conn)
        print("after", after)
        print(
            "semantic_deleted",
            {
                "news_feed_items": semantic_news_deleted,
                "stock_news_items": semantic_stock_deleted,
            },
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
