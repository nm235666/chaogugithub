#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import db_compat as sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

from realtime_streams import publish_app_event

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"
DEFAULT_SOURCE_TABLE = "chatroom_investment_analysis"
DEFAULT_TARGET_TABLE = "chatroom_stock_candidate_pool"


def normalize_text(value: str) -> str:
    return str(value or "").strip().upper()


def strip_st_prefix(name: str) -> str:
    import re

    text = str(name or "").strip()
    text = re.sub(r"^\*?ST", "", text, flags=re.IGNORECASE).strip()
    return text


def add_alias(alias_map: dict[str, dict], aliases: set[str], alias: str, ts_code: str, stock_name: str, *, alias_type: str = "derived") -> None:
    text = str(alias or "").strip()
    if not text:
        return
    key = normalize_text(text)
    aliases.add(key)
    alias_map.setdefault(
        key,
        {
            "ts_code": str(ts_code or "").strip().upper(),
            "stock_name": str(stock_name or "").strip() or str(ts_code or "").strip().upper(),
            "alias": text,
            "alias_type": alias_type,
            "confidence": 1.0,
            "source": "name_variant",
        },
    )


def add_name_variants(alias_map: dict[str, dict], aliases: set[str], ts_code: str, stock_name: str) -> None:
    name = str(stock_name or "").strip()
    if not name:
        return
    add_alias(alias_map, aliases, name, ts_code, name, alias_type="official_name")
    stripped = strip_st_prefix(name)
    if stripped and normalize_text(stripped) != normalize_text(name):
        add_alias(alias_map, aliases, stripped, ts_code, stripped, alias_type="name_st_stripped")
        add_alias(alias_map, aliases, f"ST{stripped}", ts_code, name, alias_type="name_st_prefixed")
        add_alias(alias_map, aliases, f"*ST{stripped}", ts_code, name, alias_type="name_st_prefixed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="把群聊投资倾向分析汇总成股票/主题候选池")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）")
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE, help="来源表名")
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE, help="目标表名")
    parser.add_argument("--min-room-count", type=int, default=1, help="至少被多少个群提到才入池")
    parser.add_argument("--only-bias", default="", help="只汇总指定方向：看多 或 看空")
    parser.add_argument("--window-days", type=int, default=7, help="累计窗口天数；>0 表示近 N 天累计，<=0 表示每群仅取最新一条")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def cutoff_date_str(window_days: int) -> str:
    dt = datetime.now(timezone.utc).date() - timedelta(days=max(int(window_days), 1) - 1)
    return dt.isoformat()


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_name TEXT NOT NULL,
            candidate_type TEXT,
            ts_code TEXT,
            bullish_room_count INTEGER DEFAULT 0,
            bearish_room_count INTEGER DEFAULT 0,
            net_score INTEGER DEFAULT 0,
            dominant_bias TEXT,
            mention_count INTEGER DEFAULT 0,
            room_count INTEGER DEFAULT 0,
            latest_analysis_date TEXT,
            alias_hit_name TEXT,
            alias_source TEXT,
            alias_confidence REAL,
            sample_reasons_json TEXT,
            source_room_ids_json TEXT,
            source_talkers_json TEXT,
            created_at TEXT,
            update_time TEXT
        )
        """
    )
    conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{table_name}_candidate_name ON {table_name}(candidate_name)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_dominant_bias ON {table_name}(dominant_bias)"
    )
    existing = {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    for ddl in [
        ("ts_code", "TEXT"),
        ("alias_hit_name", "TEXT"),
        ("alias_source", "TEXT"),
        ("alias_confidence", "REAL"),
    ]:
        if ddl[0] not in existing:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {ddl[0]} {ddl[1]}")
    conn.commit()


def load_stock_aliases(conn: sqlite3.Connection) -> tuple[set[str], dict[str, dict]]:
    aliases: set[str] = set()
    alias_map: dict[str, dict] = {}
    code_to_name: dict[str, str] = {}
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_codes'"
    ).fetchone()[0]
    if not table_exists:
        return aliases, alias_map

    rows = conn.execute(
        """
        SELECT ts_code, symbol, name
        FROM stock_codes
        WHERE COALESCE(ts_code, '') <> ''
           OR COALESCE(symbol, '') <> ''
           OR COALESCE(name, '') <> ''
        """
    ).fetchall()
    for row in rows:
        ts_code = str(row[0] or "").strip().upper()
        symbol = str(row[1] or "").strip().upper()
        name = str(row[2] or "").strip()
        if ts_code and name:
            code_to_name[ts_code] = name
        for value, alias_type in [
            (ts_code, "official_ts_code"),
            (symbol, "official_symbol"),
            (name, "official_name"),
        ]:
            text = str(value or "").strip()
            if text:
                add_alias(alias_map, aliases, text, ts_code, name or ts_code, alias_type=alias_type)
        add_name_variants(alias_map, aliases, ts_code, name or ts_code)
        if name:
            alias_map[normalize_text(ts_code)] = {
                "ts_code": ts_code,
                "stock_name": name,
                "alias": ts_code,
                "alias_type": "official_ts_code",
                "confidence": 1.0,
                "source": "stock_codes",
            }
            if symbol:
                alias_map[normalize_text(symbol)] = {
                    "ts_code": ts_code,
                    "stock_name": name,
                    "alias": symbol,
                    "alias_type": "official_symbol",
                    "confidence": 1.0,
                    "source": "stock_codes",
                }
    if table_exists(conn, "stock_scores_daily"):
        for row in conn.execute(
            """
            SELECT ts_code, name
            FROM stock_scores_daily
            WHERE COALESCE(ts_code, '') <> '' AND COALESCE(name, '') <> ''
            GROUP BY ts_code, name
            """
        ).fetchall():
            ts_code = str(row[0] or "").strip().upper()
            name = str(row[1] or "").strip()
            if not ts_code or not name:
                continue
            add_name_variants(alias_map, aliases, ts_code, name)

    alias_table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_alias_dictionary'"
    ).fetchone()[0]
    if alias_table_exists:
        for row in conn.execute(
            """
            SELECT alias, ts_code, stock_name, alias_type, confidence, source
            FROM stock_alias_dictionary
            WHERE COALESCE(alias, '') <> '' AND COALESCE(ts_code, '') <> ''
            ORDER BY COALESCE(confidence, 0) DESC, COALESCE(used_count, 0) DESC, alias
            """
        ).fetchall():
            alias = str(row[0] or "").strip()
            ts_code = str(row[1] or "").strip().upper()
            stock_name = str(row[2] or "").strip() or ts_code
            if not alias or not ts_code:
                continue
            key = normalize_text(alias)
            aliases.add(key)
            alias_map[key] = {
                "ts_code": ts_code,
                "stock_name": stock_name or code_to_name.get(ts_code, ts_code),
                "alias": alias,
                "alias_type": str(row[3] or "").strip() or "dictionary",
                "confidence": float(row[4] or 0.0),
                "source": str(row[5] or "").strip() or "stock_alias_dictionary",
            }
    return aliases, alias_map


def resolve_stock_candidate(name: str, alias_map: dict[str, dict]) -> dict | None:
    key = normalize_text(name)
    if not key:
        return None
    item = alias_map.get(key)
    if not item:
        return None
    stock_name = str(item.get("stock_name") or "").strip()
    ts_code = str(item.get("ts_code") or "").strip().upper()
    if (not stock_name or normalize_text(stock_name) == normalize_text(ts_code)) and re_match_ts_code(key):
        stock_name = lookup_stock_name_by_code(alias_map, ts_code) or stock_name or ts_code
    return {
        "ts_code": ts_code,
        "stock_name": stock_name or ts_code,
        "alias": str(item.get("alias") or "").strip() or name,
        "alias_type": str(item.get("alias_type") or "").strip(),
        "confidence": float(item.get("confidence") or 0.0),
        "source": str(item.get("source") or "").strip(),
    }


def re_match_ts_code(text: str) -> bool:
    import re
    return bool(re.fullmatch(r"\d{6}\.(SZ|SH|BJ)", str(text or "").strip().upper()))


def lookup_stock_name_by_code(alias_map: dict[str, dict], ts_code: str) -> str:
    target = str(ts_code or "").strip().upper()
    if not target:
        return ""
    for item in alias_map.values():
        if str(item.get("ts_code") or "").strip().upper() == target:
            stock_name = str(item.get("stock_name") or "").strip()
            if stock_name and normalize_text(stock_name) != normalize_text(target):
                return stock_name
    return ""


def classify_candidate_type(name: str, stock_aliases: set[str], resolved: dict | None = None) -> str:
    text = str(name or "").strip()
    if not text:
        return "未知"
    if resolved and resolved.get("ts_code"):
        return "股票"
    if normalize_text(text) in stock_aliases:
        return "股票"
    if text.endswith((".SZ", ".SH", ".BJ")):
        return "股票"
    keywords = [
        "ETF", "原油", "黄金", "白银", "铜", "航运", "算力", "科技股", "银行股", "储能",
        "芯片", "电力", "液冷", "AI", "LNG", "LPG", "A股大盘", "恒生科技"
    ]
    for kw in keywords:
        if kw in text:
            return "主题"
    return "标的"


def load_latest_rows(conn: sqlite3.Connection, table_name: str) -> list[sqlite3.Row]:
    sql = f"""
    SELECT a.*
    FROM {table_name} a
    JOIN (
        SELECT room_id, MAX(update_time) AS max_update_time
        FROM {table_name}
        GROUP BY room_id
    ) latest
      ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
    ORDER BY a.update_time DESC, a.id DESC
    """
    return conn.execute(sql).fetchall()


def load_window_rows(conn: sqlite3.Connection, table_name: str, window_days: int) -> list[sqlite3.Row]:
    sql = f"""
    SELECT a.*
    FROM {table_name} a
    WHERE COALESCE(a.analysis_date, '') >= ?
    ORDER BY COALESCE(a.analysis_date, '') DESC, a.update_time DESC, a.id DESC
    """
    return conn.execute(sql, (cutoff_date_str(window_days),)).fetchall()


def upsert_candidate_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict]) -> int:
    now = now_utc_str()
    conn.execute(f"DELETE FROM {table_name}")
    conn.executemany(
        f"""
        INSERT INTO {table_name} (
            candidate_name, candidate_type, ts_code, bullish_room_count, bearish_room_count, net_score,
            dominant_bias, mention_count, room_count, latest_analysis_date, alias_hit_name, alias_source, alias_confidence, sample_reasons_json,
            source_room_ids_json, source_talkers_json, created_at, update_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["candidate_name"],
                row["candidate_type"],
                row.get("ts_code") or None,
                row["bullish_room_count"],
                row["bearish_room_count"],
                row["net_score"],
                row["dominant_bias"],
                row["mention_count"],
                row["room_count"],
                row["latest_analysis_date"],
                row.get("alias_hit_name") or None,
                row.get("alias_source") or None,
                row.get("alias_confidence"),
                row["sample_reasons_json"],
                row["source_room_ids_json"],
                row["source_talkers_json"],
                now,
                now,
            )
            for row in rows
        ],
    )
    conn.commit()
    return len(rows)


def record_alias_hits(conn: sqlite3.Connection, alias_hits: dict[str, int]) -> None:
    if not alias_hits:
        return
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_alias_dictionary'"
    ).fetchone()[0]
    if not table_exists:
        return
    now = now_utc_str()
    for alias, count in alias_hits.items():
        conn.execute(
            """
            UPDATE stock_alias_dictionary
            SET used_count = COALESCE(used_count, 0) + ?, last_used_at = ?, update_time = ?
            WHERE alias = ?
            """,
            (int(count or 0), now, now, alias),
        )
    conn.commit()


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_table(conn, args.target_table)
        stock_aliases, alias_map = load_stock_aliases(conn)
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (args.source_table,),
        ).fetchone()[0]
        if not table_exists:
            print(f"来源表不存在: {args.source_table}")
            return 1

        latest_rows = load_latest_rows(conn, args.source_table) if int(args.window_days) <= 0 else load_window_rows(conn, args.source_table, args.window_days)
        pool: dict[str, dict] = {}
        alias_hits: dict[str, int] = {}
        for row in latest_rows:
            try:
                targets = json.loads(row["targets_json"] or "[]")
            except Exception:
                targets = []
            if not isinstance(targets, list):
                continue

            seen_in_room: set[tuple[str, str]] = set()
            for item in targets:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                bias = str(item.get("bias") or "").strip()
                reason = str(item.get("reason") or "").strip()
                if not name or bias not in {"看多", "看空"}:
                    continue
                if args.only_bias.strip() and bias != args.only_bias.strip():
                    continue
                resolved = resolve_stock_candidate(name, alias_map)
                canonical_name = str((resolved or {}).get("stock_name") or name).strip()
                bucket_key = str((resolved or {}).get("ts_code") or canonical_name).strip()
                if resolved and not str((resolved or {}).get("alias_type") or "").startswith("official_"):
                    alias_hits[name] = alias_hits.get(name, 0) + 1

                bucket = pool.setdefault(
                    bucket_key,
                    {
                        "candidate_name": canonical_name,
                        "candidate_type": classify_candidate_type(name, stock_aliases, resolved),
                        "ts_code": str((resolved or {}).get("ts_code") or "").strip().upper(),
                        "alias_hit_name": name if resolved and normalize_text(name) != normalize_text(canonical_name) else "",
                        "alias_source": str((resolved or {}).get("alias_type") or "").strip(),
                        "alias_confidence": float((resolved or {}).get("confidence") or 0.0) if resolved else None,
                        "bullish_room_ids": set(),
                        "bearish_room_ids": set(),
                        "room_ids": set(),
                        "talkers": set(),
                        "reasons": [],
                        "mention_count": 0,
                        "latest_analysis_date": str(row["analysis_date"] or ""),
                    },
                )
                bucket["mention_count"] += 1
                bucket["room_ids"].add(str(row["room_id"] or ""))
                bucket["talkers"].add(str(row["talker"] or ""))
                if reason and len(bucket["reasons"]) < 10:
                    payload = {"bias": bias, "reason": reason, "talker": str(row["talker"] or "")}
                    if name != canonical_name:
                        payload["raw_name"] = name
                        payload["canonical_name"] = canonical_name
                    bucket["reasons"].append(payload)
                key = (str(row["room_id"] or ""), bias)
                if key not in seen_in_room:
                    if bias == "看多":
                        bucket["bullish_room_ids"].add(str(row["room_id"] or ""))
                    else:
                        bucket["bearish_room_ids"].add(str(row["room_id"] or ""))
                    seen_in_room.add(key)
                latest_date = str(row["analysis_date"] or "")
                if latest_date and latest_date > bucket["latest_analysis_date"]:
                    bucket["latest_analysis_date"] = latest_date

        final_rows: list[dict] = []
        for _, bucket in pool.items():
            bullish_room_count = len(bucket["bullish_room_ids"])
            bearish_room_count = len(bucket["bearish_room_ids"])
            room_count = len(bucket["room_ids"])
            if room_count < max(args.min_room_count, 1):
                continue
            net_score = bullish_room_count - bearish_room_count
            dominant_bias = "看多" if net_score >= 0 else "看空"
            final_rows.append(
                {
                    "candidate_name": bucket["candidate_name"],
                    "candidate_type": bucket["candidate_type"],
                    "ts_code": bucket.get("ts_code") or "",
                    "bullish_room_count": bullish_room_count,
                    "bearish_room_count": bearish_room_count,
                    "net_score": net_score,
                    "dominant_bias": dominant_bias,
                    "mention_count": bucket["mention_count"],
                    "room_count": room_count,
                    "latest_analysis_date": bucket["latest_analysis_date"],
                    "alias_hit_name": bucket.get("alias_hit_name") or "",
                    "alias_source": bucket.get("alias_source") or "",
                    "alias_confidence": bucket.get("alias_confidence"),
                    "sample_reasons_json": json.dumps(bucket["reasons"][:6], ensure_ascii=False),
                    "source_room_ids_json": json.dumps(sorted(x for x in bucket["room_ids"] if x), ensure_ascii=False),
                    "source_talkers_json": json.dumps(sorted(x for x in bucket["talkers"] if x), ensure_ascii=False),
                }
            )

        final_rows.sort(
            key=lambda x: (
                -abs(int(x["net_score"])),
                -int(x["room_count"]),
                -int(x["mention_count"]),
                x["candidate_name"],
            )
        )
        affected = upsert_candidate_rows(conn, args.target_table, final_rows)
        record_alias_hits(conn, alias_hits)
    finally:
        conn.close()

        print(f"source_rows={len(latest_rows)} candidates={affected}")
        if int(args.window_days) > 0:
            print(f"window_days={int(args.window_days)} cutoff={cutoff_date_str(args.window_days)}")
    publish_app_event(
        event="chatroom_candidate_pool_update",
        payload={
            "source_rows": int(len(latest_rows)),
            "candidates": int(affected),
            "target_table": args.target_table,
            "window_days": int(args.window_days),
        },
        producer="build_chatroom_candidate_pool.py",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
