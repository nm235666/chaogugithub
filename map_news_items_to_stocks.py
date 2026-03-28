#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3
from realtime_streams import publish_app_event

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"
PROMPT_VERSION = "news_stock_map_v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="识别 news_feed_items 标题/摘要中的股票并映射到 ts_code")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--limit", type=int, default=200, help="单次最多处理多少条新闻")
    parser.add_argument("--source", default="", help="仅处理指定 source")
    parser.add_argument("--force", action="store_true", help="强制重跑已映射新闻")
    parser.add_argument("--days", type=int, default=7, help="仅处理最近多少天新闻")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_columns(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(news_feed_items)").fetchall()}
    need = [
        ("related_ts_codes_json", "TEXT"),
        ("related_stock_names_json", "TEXT"),
        ("stock_match_version", "TEXT"),
        ("stock_mapped_at", "TEXT"),
        ("llm_direct_related_ts_codes_json", "TEXT"),
        ("llm_direct_related_stock_names_json", "TEXT"),
    ]
    for name, typ in need:
        if name not in cols:
            conn.execute(f"ALTER TABLE news_feed_items ADD COLUMN {name} {typ}")
    conn.commit()


def fetch_target_rows(conn: sqlite3.Connection, args: argparse.Namespace) -> list[sqlite3.Row]:
    where = ["COALESCE(pub_date, '') >= ?"]
    params: list[object] = [cutoff_time(args.days)]
    if args.source.strip():
        where.append("source = ?")
        params.append(args.source.strip().lower())
    if not args.force:
        where.append(
            "("
            "related_ts_codes_json IS NULL "
            "OR TRIM(COALESCE(stock_match_version, '')) = '' "
            "OR COALESCE(llm_direct_related_ts_codes_json, '') <> ''"
            ")"
        )
    sql = f"""
    SELECT id, source, title, summary, link, pub_date,
           COALESCE(llm_direct_related_ts_codes_json, '') AS llm_direct_related_ts_codes_json,
           COALESCE(llm_direct_related_stock_names_json, '') AS llm_direct_related_stock_names_json
    FROM news_feed_items
    WHERE {' AND '.join(where)}
    ORDER BY COALESCE(pub_date, '') DESC, id DESC
    LIMIT ?
    """
    params.append(max(args.limit, 1))
    return conn.execute(sql, params).fetchall()


def cutoff_time(days: int) -> str:
    dt = datetime.now(timezone.utc).timestamp() - max(days, 1) * 86400
    return datetime.fromtimestamp(dt, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_text(text: str) -> str:
    value = str(text or "")
    value = value.replace("\u3000", " ")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def load_stock_aliases(conn: sqlite3.Connection) -> tuple[list[dict], dict[str, list[dict]]]:
    rows = conn.execute(
        """
        SELECT ts_code, symbol, name
        FROM stock_codes
        WHERE ts_code IS NOT NULL AND name IS NOT NULL AND TRIM(name) <> ''
        """
    ).fetchall()
    aliases: list[dict] = []
    by_first: dict[str, list[dict]] = {}
    for row in rows:
        ts_code = row[0]
        symbol = row[1]
        name = row[2]
        ts_code = str(ts_code or "").strip().upper()
        symbol = str(symbol or "").strip()
        name = str(name or "").strip()
        if not ts_code or not name:
            continue
        if ts_code in {"TS_CODE", "TSCODE"} or symbol.lower() == "symbol" or name.lower() == "name":
            continue
        candidates = [(name, "name")]
        if symbol and len(symbol) == 6:
            candidates.append((symbol, "symbol"))
        if "." in ts_code:
            candidates.append((ts_code.split(".")[0], "code"))
        for alias, alias_type in candidates:
            alias = str(alias).strip()
            if not alias:
                continue
            if alias_type == "name" and len(alias) < 2:
                continue
            if alias_type != "name" and len(alias) < 6:
                continue
            item = {
                "ts_code": ts_code,
                "name": name,
                "alias": alias,
                "alias_type": alias_type,
            }
            aliases.append(item)
            first = alias[0]
            by_first.setdefault(first, []).append(item)
    for key in list(by_first):
        by_first[key].sort(key=lambda x: (-len(x["alias"]), x["name"]))
    return aliases, by_first


def find_related_stocks(text: str, by_first: dict[str, list[dict]]) -> list[dict]:
    text = normalize_text(text)
    if not text:
        return []
    text_chars = {ch for ch in text}
    candidates: list[dict] = []
    seen_alias_ranges: list[tuple[int, int]] = []
    used_ts_codes: set[str] = set()
    for ch in text_chars:
        for item in by_first.get(ch, []):
            alias = item["alias"]
            start = text.find(alias)
            if start < 0:
                continue
            end = start + len(alias)
            overlap = False
            for s, e in seen_alias_ranges:
                if not (end <= s or start >= e):
                    if len(alias) <= (e - s):
                        overlap = True
                        break
            if overlap:
                continue
            ts_code = item["ts_code"]
            if ts_code in used_ts_codes:
                continue
            candidates.append(
                {
                    "ts_code": ts_code,
                    "name": item["name"],
                    "alias": alias,
                    "alias_type": item["alias_type"],
                }
            )
            used_ts_codes.add(ts_code)
            seen_alias_ranges.append((start, end))
            if len(candidates) >= 5:
                return candidates
    return candidates


def merge_matches(rule_matches: list[dict], llm_codes_raw: str, llm_names_raw: str) -> list[dict]:
    merged: dict[str, dict] = {}
    for item in rule_matches:
        if not isinstance(item, dict):
            continue
        ts_code = str(item.get("ts_code") or "").strip().upper()
        if not ts_code:
            continue
        merged[ts_code] = {
            "ts_code": ts_code,
            "name": str(item.get("name") or "").strip(),
            "alias": str(item.get("alias") or "").strip(),
            "alias_type": str(item.get("alias_type") or "").strip(),
            "sources": ["rule"],
        }
    llm_codes = json.loads(llm_codes_raw) if str(llm_codes_raw or "").strip() else []
    llm_names = json.loads(llm_names_raw) if str(llm_names_raw or "").strip() else []
    llm_name_map = {
        str(item.get("ts_code") or "").strip().upper(): str(item.get("name") or "").strip()
        for item in llm_names
        if isinstance(item, dict)
    }
    if isinstance(llm_codes, list):
        for ts_code in llm_codes:
            ts_code = str(ts_code or "").strip().upper()
            if not ts_code:
                continue
            if ts_code in merged:
                if "llm" not in merged[ts_code]["sources"]:
                    merged[ts_code]["sources"].append("llm")
            else:
                merged[ts_code] = {
                    "ts_code": ts_code,
                    "name": llm_name_map.get(ts_code, ""),
                    "alias": "",
                    "alias_type": "llm",
                    "sources": ["llm"],
                }
    return list(merged.values())


def update_news_row(conn: sqlite3.Connection, row_id: int, matches: list[dict]) -> None:
    now = now_utc_str()
    conn.execute(
        """
        UPDATE news_feed_items
        SET related_ts_codes_json = ?,
            related_stock_names_json = ?,
            stock_match_version = ?,
            stock_mapped_at = ?
        WHERE id = ?
        """,
        (
            json.dumps([m["ts_code"] for m in matches], ensure_ascii=False),
            json.dumps(matches, ensure_ascii=False),
            PROMPT_VERSION,
            now,
            row_id,
        ),
    )


def main() -> int:
    args = parse_args()
    publish_app_event(
        event="news_stock_map_update",
        payload={"status": "running", "limit": int(args.limit), "days": int(args.days), "source": args.source},
        producer="map_news_items_to_stocks.py",
    )
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_columns(conn)
        _, by_first = load_stock_aliases(conn)
        rows = fetch_target_rows(conn, args)
        mapped = 0
        for idx, row in enumerate(rows, start=1):
            text = f"{row['title'] or ''}\n{row['summary'] or ''}"
            rule_matches = find_related_stocks(text, by_first)
            matches = merge_matches(
                rule_matches,
                row["llm_direct_related_ts_codes_json"],
                row["llm_direct_related_stock_names_json"],
            )
            update_news_row(conn, row["id"], matches)
            mapped += 1 if matches else 0
            print(f"[{idx}/{len(rows)}] id={row['id']} source={row['source']} matches={len(matches)}")
        conn.commit()
        print(f"完成: processed={len(rows)} matched={mapped}")
        publish_app_event(
            event="news_stock_map_update",
            payload={"status": "done", "processed": len(rows), "matched": mapped, "source": args.source},
            producer="map_news_items_to_stocks.py",
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        publish_app_event(
            event="news_stock_map_update",
            payload={"status": "error", "error": str(exc)},
            producer="map_news_items_to_stocks.py",
        )
        raise
