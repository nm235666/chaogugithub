#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import db_compat as sqlite3
from realtime_streams import publish_app_event

from backend.server import (
    build_signal_event_logic_view,
    build_signal_logic_view,
    ensure_logic_view_cache_table,
    extract_logic_view_from_markdown,
    get_or_build_cached_logic_view,
)


def parse_args():
    parser = argparse.ArgumentParser(description="回填 logic_view_cache，避免页面首次访问时现算链路")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--limit-news", type=int, default=0, help="仅处理前 N 条日报总结，0 表示全部")
    parser.add_argument("--limit-signals", type=int, default=0, help="仅处理前 N 条信号，0 表示全部")
    parser.add_argument("--limit-events", type=int, default=0, help="仅处理前 N 条信号事件，0 表示全部")
    return parser.parse_args()


def iter_rows(conn, sql: str, params=()):
    for row in conn.execute(sql, params).fetchall():
        yield dict(row)


def main():
    args = parse_args()
    publish_app_event(
        event="logic_view_cache_update",
        payload={"status": "running"},
        producer="backfill_logic_view_cache.py",
    )
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_logic_view_cache_table(conn)

        news_limit_sql = f" LIMIT {int(args.limit_news)}" if args.limit_news and args.limit_news > 0 else ""
        news_count = 0
        if conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='news_daily_summaries'"
        ).fetchone()[0]:
            for item in iter_rows(
                conn,
                f"""
                SELECT id, summary_markdown
                FROM news_daily_summaries
                ORDER BY id DESC
                {news_limit_sql}
                """,
            ):
                get_or_build_cached_logic_view(
                    conn,
                    entity_type="news_daily_summary",
                    entity_key=str(item.get("id") or ""),
                    source_payload=item.get("summary_markdown", ""),
                    builder=lambda text=item.get("summary_markdown", ""): extract_logic_view_from_markdown(text),
                )
                news_count += 1

        signal_limit_sql = f" LIMIT {int(args.limit_signals)}" if args.limit_signals and args.limit_signals > 0 else ""
        signal_count = 0
        if conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='investment_signal_tracker'"
        ).fetchone()[0]:
            for item in iter_rows(
                conn,
                f"""
                SELECT signal_key, subject_name, direction, signal_strength, confidence, signal_status,
                       source_summary_json, evidence_json
                FROM investment_signal_tracker
                ORDER BY update_time DESC NULLS LAST, id DESC
                {signal_limit_sql}
                """,
            ):
                get_or_build_cached_logic_view(
                    conn,
                    entity_type="investment_signal",
                    entity_key=str(item.get("signal_key") or ""),
                    source_payload=item,
                    builder=lambda row=item: build_signal_logic_view(row),
                )
                signal_count += 1

        event_limit_sql = f" LIMIT {int(args.limit_events)}" if args.limit_events and args.limit_events > 0 else ""
        event_count = 0
        if conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='investment_signal_events'"
        ).fetchone()[0]:
            for item in iter_rows(
                conn,
                f"""
                SELECT id, event_time, event_type, driver_type, driver_source, event_summary,
                       new_direction, status_after_event, evidence_json
                FROM investment_signal_events
                ORDER BY event_time DESC NULLS LAST, id DESC
                {event_limit_sql}
                """,
            ):
                get_or_build_cached_logic_view(
                    conn,
                    entity_type="investment_signal_event",
                    entity_key=str(item.get("id") or ""),
                    source_payload=item,
                    builder=lambda row=item: build_signal_event_logic_view(row),
                )
                event_count += 1

        total = conn.execute("SELECT COUNT(*) FROM logic_view_cache").fetchone()[0]
        print(f"日报总结缓存: {news_count}")
        print(f"信号总表缓存: {signal_count}")
        print(f"信号事件缓存: {event_count}")
        print(f"logic_view_cache 总数: {total}")
        publish_app_event(
            event="logic_view_cache_update",
            payload={
                "status": "done",
                "news_count": news_count,
                "signal_count": signal_count,
                "event_count": event_count,
                "total": total,
            },
            producer="backfill_logic_view_cache.py",
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        publish_app_event(
            event="logic_view_cache_update",
            payload={"status": "error", "error": str(exc)},
            producer="backfill_logic_view_cache.py",
        )
        raise
