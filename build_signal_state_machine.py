#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3
from realtime_streams import publish_app_event

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"
STATE_TABLE = "signal_state_tracker"
EVENT_TABLE = "signal_state_events"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="统一信号状态机：为主题/股票信号打初始、强化、弱化、证伪、反转状态")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--state-table", default=STATE_TABLE, help="状态当前表")
    parser.add_argument("--event-table", default=EVENT_TABLE, help="状态事件表")
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


def ensure_tables(conn: sqlite3.Connection, state_table: str, event_table: str) -> None:
    if not table_exists(conn, state_table):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {state_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_scope TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                subject_name TEXT NOT NULL,
                ts_code TEXT,
                direction TEXT,
                signal_strength REAL,
                confidence REAL,
                current_state TEXT,
                prev_state TEXT,
                source_table TEXT,
                latest_signal_date TEXT,
                evidence_count INTEGER DEFAULT 0,
                driver_type TEXT,
                driver_title TEXT,
                source_summary_json TEXT,
                snapshot_json TEXT,
                created_at TEXT,
                update_time TEXT,
                UNIQUE(signal_scope, signal_key)
            )
            """
        )
    if not table_exists(conn, event_table):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {event_table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_scope TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                subject_name TEXT NOT NULL,
                ts_code TEXT,
                event_time TEXT NOT NULL,
                event_date TEXT NOT NULL,
                event_type TEXT NOT NULL,
                old_state TEXT,
                new_state TEXT,
                old_direction TEXT,
                new_direction TEXT,
                old_strength REAL,
                new_strength REAL,
                delta_strength REAL,
                old_confidence REAL,
                new_confidence REAL,
                delta_confidence REAL,
                source_table TEXT,
                driver_type TEXT,
                driver_title TEXT,
                event_summary TEXT,
                snapshot_before_json TEXT,
                snapshot_after_json TEXT,
                created_at TEXT,
                UNIQUE(signal_scope, signal_key, event_time, event_type)
            )
            """
        )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{state_table}_scope ON {state_table}(signal_scope, current_state)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{state_table}_strength ON {state_table}(signal_strength DESC)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{event_table}_scope_time ON {event_table}(signal_scope, event_time DESC)")
    conn.commit()


def parse_json_text(raw: str):
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def load_current_signals(conn: sqlite3.Connection) -> list[dict]:
    rows: list[dict] = []
    if table_exists(conn, "theme_hotspot_tracker"):
        for row in conn.execute(
            """
            SELECT theme_name, theme_group, direction, theme_strength, confidence,
                   evidence_count, latest_evidence_time, source_summary_json, evidence_json
            FROM theme_hotspot_tracker
            """
        ).fetchall():
            rows.append(
                {
                    "signal_scope": "theme",
                    "signal_key": f"theme:{row[0]}",
                    "signal_type": "theme",
                    "subject_name": str(row[0] or ""),
                    "ts_code": "",
                    "direction": str(row[2] or ""),
                    "signal_strength": float(row[3] or 0.0),
                    "confidence": float(row[4] or 0.0),
                    "latest_signal_date": str(row[6] or ""),
                    "evidence_count": int(row[5] or 0),
                    "source_table": "theme_hotspot_tracker",
                    "source_summary_json": str(row[7] or "{}"),
                    "snapshot_json": json.dumps(
                        {
                            "theme_group": str(row[1] or ""),
                            "direction": str(row[2] or ""),
                            "signal_strength": float(row[3] or 0.0),
                            "confidence": float(row[4] or 0.0),
                            "evidence_count": int(row[5] or 0),
                            "latest_signal_date": str(row[6] or ""),
                            "source_summary_json": parse_json_text(row[7]) or {},
                            "evidence_json": parse_json_text(row[8]) or [],
                        },
                        ensure_ascii=False,
                    ),
                    "driver_title": first_driver_title(row[8]),
                    "driver_type": dominant_driver(row[7]),
                }
            )
    stock_table = "investment_signal_tracker_7d" if table_exists(conn, "investment_signal_tracker_7d") else "investment_signal_tracker"
    if table_exists(conn, stock_table):
        for row in conn.execute(
            f"""
            SELECT signal_key, signal_type, subject_name, ts_code, direction, signal_strength,
                   confidence, latest_signal_date, evidence_count, source_summary_json, evidence_json
            FROM {stock_table}
            WHERE signal_type = 'stock'
            """
        ).fetchall():
            rows.append(
                {
                    "signal_scope": "stock",
                    "signal_key": str(row[0] or ""),
                    "signal_type": str(row[1] or "stock"),
                    "subject_name": str(row[2] or ""),
                    "ts_code": str(row[3] or ""),
                    "direction": str(row[4] or ""),
                    "signal_strength": float(row[5] or 0.0),
                    "confidence": float(row[6] or 0.0),
                    "latest_signal_date": str(row[7] or ""),
                    "evidence_count": int(row[8] or 0),
                    "source_table": stock_table,
                    "source_summary_json": str(row[9] or "{}"),
                    "snapshot_json": json.dumps(
                        {
                            "direction": str(row[4] or ""),
                            "signal_strength": float(row[5] or 0.0),
                            "confidence": float(row[6] or 0.0),
                            "latest_signal_date": str(row[7] or ""),
                            "evidence_count": int(row[8] or 0),
                            "source_summary_json": parse_json_text(row[9]) or {},
                            "evidence_json": parse_json_text(row[10]) or [],
                        },
                        ensure_ascii=False,
                    ),
                    "driver_title": first_driver_title(row[10]),
                    "driver_type": dominant_driver(row[9]),
                }
            )
    return rows


def dominant_driver(source_summary_json: str) -> str:
    data = parse_json_text(source_summary_json) or {}
    if not isinstance(data, dict) or not data:
        return ""
    key = max(data.items(), key=lambda kv: float(kv[1] or 0))[0]
    return str(key or "")


def first_driver_title(evidence_json: str) -> str:
    items = parse_json_text(evidence_json) or []
    if not isinstance(items, list):
        return ""
    for item in items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or item.get("theme_name") or item.get("source") or "").strip()
        if title:
            return title
    return ""


def load_previous(conn: sqlite3.Connection, state_table: str) -> dict[str, dict]:
    if not table_exists(conn, state_table):
        return {}
    rows = conn.execute(
        f"""
        SELECT signal_scope, signal_key, signal_type, subject_name, ts_code, direction, signal_strength,
               confidence, current_state, prev_state, source_table, latest_signal_date, evidence_count,
               driver_type, driver_title, source_summary_json, snapshot_json
        FROM {state_table}
        """
    ).fetchall()
    out = {}
    for row in rows:
        key = f"{row[0]}::{row[1]}"
        out[key] = {
            "signal_scope": str(row[0] or ""),
            "signal_key": str(row[1] or ""),
            "signal_type": str(row[2] or ""),
            "subject_name": str(row[3] or ""),
            "ts_code": str(row[4] or ""),
            "direction": str(row[5] or ""),
            "signal_strength": float(row[6] or 0.0),
            "confidence": float(row[7] or 0.0),
            "current_state": str(row[8] or ""),
            "prev_state": str(row[9] or ""),
            "source_table": str(row[10] or ""),
            "latest_signal_date": str(row[11] or ""),
            "evidence_count": int(row[12] or 0),
            "driver_type": str(row[13] or ""),
            "driver_title": str(row[14] or ""),
            "source_summary_json": str(row[15] or "{}"),
            "snapshot_json": str(row[16] or "{}"),
        }
    return out


def map_event_to_state(old_row: dict | None, new_row: dict | None) -> tuple[str | None, str]:
    if old_row is None and new_row is not None:
        return "初始", "新信号首次进入状态机"
    if old_row is not None and new_row is None:
        return "证伪", "信号本轮消失或失去有效支撑"
    if old_row is None or new_row is None:
        return None, ""

    old_dir = str(old_row.get("direction") or "")
    new_dir = str(new_row.get("direction") or "")
    old_strength = float(old_row.get("signal_strength") or 0.0)
    new_strength = float(new_row.get("signal_strength") or 0.0)
    delta = new_strength - old_strength

    if old_dir in {"看多", "看空"} and new_dir in {"看多", "看空"} and old_dir != new_dir:
        return "反转", f"方向由{old_dir}切换为{new_dir}"
    if old_dir in {"看多", "看空"} and new_dir == "中性":
        return "证伪", f"方向由{old_dir}退化为中性"
    if delta >= max(5.0, old_strength * 0.25):
        return "强化", f"强度由{old_strength:.2f}升至{new_strength:.2f}"
    if delta <= -max(5.0, max(old_strength, 1.0) * 0.25):
        return "弱化", f"强度由{old_strength:.2f}降至{new_strength:.2f}"
    prev = str(old_row.get("current_state") or "")
    return (prev or "初始"), "状态延续"


def replace_state_rows(conn: sqlite3.Connection, state_table: str, rows: list[dict]) -> int:
    now = now_utc_str()
    conn.execute(f"DELETE FROM {state_table}")
    for row in rows:
        conn.execute(
            f"""
            INSERT INTO {state_table} (
                signal_scope, signal_key, signal_type, subject_name, ts_code, direction, signal_strength,
                confidence, current_state, prev_state, source_table, latest_signal_date, evidence_count,
                driver_type, driver_title, source_summary_json, snapshot_json, created_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["signal_scope"],
                row["signal_key"],
                row["signal_type"],
                row["subject_name"],
                row["ts_code"] or None,
                row["direction"],
                row["signal_strength"],
                row["confidence"],
                row["current_state"],
                row["prev_state"],
                row["source_table"],
                row["latest_signal_date"],
                row["evidence_count"],
                row["driver_type"],
                row["driver_title"],
                row["source_summary_json"],
                row["snapshot_json"],
                now,
                now,
            ),
        )
    conn.commit()
    return len(rows)


def write_events(conn: sqlite3.Connection, event_table: str, events: list[dict], now: str) -> int:
    for event in events:
        conn.execute(
            f"""
            INSERT INTO {event_table} (
                signal_scope, signal_key, signal_type, subject_name, ts_code, event_time, event_date, event_type,
                old_state, new_state, old_direction, new_direction, old_strength, new_strength, delta_strength,
                old_confidence, new_confidence, delta_confidence, source_table, driver_type, driver_title,
                event_summary, snapshot_before_json, snapshot_after_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_scope, signal_key, event_time, event_type) DO NOTHING
            """,
            (
                event["signal_scope"],
                event["signal_key"],
                event["signal_type"],
                event["subject_name"],
                event["ts_code"] or None,
                now,
                now[:10],
                event["event_type"],
                event["old_state"],
                event["new_state"],
                event["old_direction"],
                event["new_direction"],
                event["old_strength"],
                event["new_strength"],
                event["delta_strength"],
                event["old_confidence"],
                event["new_confidence"],
                event["delta_confidence"],
                event["source_table"],
                event["driver_type"],
                event["driver_title"],
                event["event_summary"],
                event["snapshot_before_json"],
                event["snapshot_after_json"],
                now,
            ),
        )
    conn.commit()
    return len(events)


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    try:
        ensure_tables(conn, args.state_table, args.event_table)
        previous = load_previous(conn, args.state_table)
        current_rows = load_current_signals(conn)
        now = now_utc_str()

        current_map = {}
        final_rows = []
        events = []
        for row in current_rows:
            map_key = f"{row['signal_scope']}::{row['signal_key']}"
            current_map[map_key] = row
            old_row = previous.get(map_key)
            new_state, reason = map_event_to_state(old_row, row)
            if new_state is None:
                continue
            final_rows.append(
                {
                    **row,
                    "current_state": new_state,
                    "prev_state": str((old_row or {}).get("current_state") or ""),
                }
            )
            if old_row is None or new_state != str(old_row.get("current_state") or "") or row["direction"] != str(old_row.get("direction") or ""):
                events.append(
                    {
                        "signal_scope": row["signal_scope"],
                        "signal_key": row["signal_key"],
                        "signal_type": row["signal_type"],
                        "subject_name": row["subject_name"],
                        "ts_code": row["ts_code"],
                        "event_type": new_state,
                        "old_state": str((old_row or {}).get("current_state") or ""),
                        "new_state": new_state,
                        "old_direction": str((old_row or {}).get("direction") or ""),
                        "new_direction": row["direction"],
                        "old_strength": float((old_row or {}).get("signal_strength") or 0.0),
                        "new_strength": float(row["signal_strength"] or 0.0),
                        "delta_strength": round(float(row["signal_strength"] or 0.0) - float((old_row or {}).get("signal_strength") or 0.0), 2),
                        "old_confidence": float((old_row or {}).get("confidence") or 0.0),
                        "new_confidence": float(row["confidence"] or 0.0),
                        "delta_confidence": round(float(row["confidence"] or 0.0) - float((old_row or {}).get("confidence") or 0.0), 2),
                        "source_table": row["source_table"],
                        "driver_type": row["driver_type"],
                        "driver_title": row["driver_title"],
                        "event_summary": reason,
                        "snapshot_before_json": json.dumps(old_row or {}, ensure_ascii=False),
                        "snapshot_after_json": json.dumps(row or {}, ensure_ascii=False),
                    }
                )

        for map_key, old_row in previous.items():
            if map_key in current_map:
                continue
            events.append(
                {
                    "signal_scope": old_row["signal_scope"],
                    "signal_key": old_row["signal_key"],
                    "signal_type": old_row["signal_type"],
                    "subject_name": old_row["subject_name"],
                    "ts_code": old_row["ts_code"],
                    "event_type": "证伪",
                    "old_state": old_row.get("current_state") or "",
                    "new_state": "证伪",
                    "old_direction": old_row.get("direction") or "",
                    "new_direction": "",
                    "old_strength": float(old_row.get("signal_strength") or 0.0),
                    "new_strength": 0.0,
                    "delta_strength": -float(old_row.get("signal_strength") or 0.0),
                    "old_confidence": float(old_row.get("confidence") or 0.0),
                    "new_confidence": 0.0,
                    "delta_confidence": -float(old_row.get("confidence") or 0.0),
                    "source_table": old_row.get("source_table") or "",
                    "driver_type": old_row.get("driver_type") or "",
                    "driver_title": old_row.get("driver_title") or "",
                    "event_summary": "信号本轮消失或失去有效支撑",
                    "snapshot_before_json": json.dumps(old_row or {}, ensure_ascii=False),
                    "snapshot_after_json": "{}",
                }
            )

        event_count = write_events(conn, args.event_table, events, now)
        row_count = replace_state_rows(conn, args.state_table, final_rows)
        publish_app_event(
            event="signal_state_machine_update",
            payload={"rows": row_count, "events": event_count, "state_table": args.state_table},
            producer="build_signal_state_machine.py",
        )
        print(f"完成: state_rows={row_count}, events={event_count}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
