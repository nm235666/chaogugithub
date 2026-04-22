#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import db_compat as sqlite3

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"
SH_TZ = ZoneInfo("Asia/Shanghai")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="群聊荐股方向每日校验与准确率标注")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="数据库路径（兼容参数，默认主库）")
    parser.add_argument("--as-of-date", default="", help="校验截止日（Asia/Shanghai, YYYY-MM-DD），默认今天")
    parser.add_argument("--lookback-days", type=int, default=3, help="抽取最近多少天的信号")
    parser.add_argument("--window-days", type=int, default=30, help="准确率统计窗口（天）")
    parser.add_argument("--min-sample", type=int, default=3, help="标签最小样本")
    parser.add_argument(
        "--room-scope",
        default="all",
        choices=["all", "weak"],
        help="参与校验群范围：all=全部群；weak=仅弱群（低活跃或高风险）",
    )
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def as_of_sh_date(text: str) -> date:
    raw = str(text or "").strip()
    if raw:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    return datetime.now(SH_TZ).date()


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chatroom_stock_signal_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talker TEXT NOT NULL,
            room_id TEXT,
            sender_name TEXT,
            sender_id TEXT,
            signal_date TEXT NOT NULL,
            signal_time TEXT,
            ts_code TEXT NOT NULL,
            stock_name TEXT,
            direction TEXT NOT NULL,
            source_message_key TEXT,
            source_content TEXT,
            source_table TEXT DEFAULT 'wechat_chatlog_clean_items',
            room_strength_label TEXT DEFAULT 'weak',
            validation_status TEXT DEFAULT 'pending',
            validation_due_date TEXT,
            target_trade_date TEXT,
            close_t0 REAL,
            close_t1 REAL,
            return_1d REAL,
            verdict TEXT,
            evaluated_at TEXT,
            created_at TEXT,
            update_time TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_chatroom_stock_signal_pred_msg_stock_dir
        ON chatroom_stock_signal_predictions(source_message_key, ts_code, direction)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chatroom_stock_signal_pred_status
        ON chatroom_stock_signal_predictions(validation_status, signal_date)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chatroom_signal_accuracy_labels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            entity_name TEXT,
            as_of_date TEXT NOT NULL,
            window_days INTEGER NOT NULL,
            sample_size INTEGER DEFAULT 0,
            hit_count INTEGER DEFAULT 0,
            hit_rate REAL DEFAULT 0,
            accuracy_label TEXT DEFAULT '样本不足',
            stats_json TEXT,
            created_at TEXT,
            update_time TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_chatroom_signal_accuracy_labels
        ON chatroom_signal_accuracy_labels(entity_type, entity_key, as_of_date, window_days)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chatroom_signal_accuracy_labels_date
        ON chatroom_signal_accuracy_labels(as_of_date, entity_type)
        """
    )
    conn.commit()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool((row[0] if row else 0) or 0)


def build_room_map(conn: sqlite3.Connection, *, room_scope: str) -> dict[str, dict]:
    if not table_exists(conn, "chatroom_list_items"):
        return {}
    rows = conn.execute(
        """
        SELECT
            room_id,
            COALESCE(NULLIF(remark, ''), NULLIF(nick_name, ''), room_id) AS talker,
            COALESCE(llm_chatroom_activity_level, '') AS activity_level,
            COALESCE(llm_chatroom_risk_level, '') AS risk_level
        FROM chatroom_list_items
        """
    ).fetchall()
    out: dict[str, dict] = {}
    for row in rows:
        room_id = str(row[0] or "").strip()
        talker = str(row[1] or "").strip()
        if not talker:
            continue
        activity = str(row[2] or "").strip()
        risk = str(row[3] or "").strip()
        is_weak = activity == "低" or risk == "高"
        if room_scope == "weak" and not is_weak:
            continue
        meta = {
            "room_id": room_id,
            "activity_level": activity,
            "risk_level": risk,
            "is_weak": is_weak,
        }
        out[talker] = meta
        if room_id:
            out.setdefault(room_id, meta)
    return out


def normalize_alias(value: str) -> str:
    return str(value or "").strip().upper()


def _strip_st_prefix(name: str) -> str:
    text = str(name or "").strip()
    text = re.sub(r"^\*?ST", "", text, flags=re.IGNORECASE).strip()
    return text


def _add_alias(alias_map: dict[str, dict], alias: str, ts_code: str, stock_name: str) -> None:
    key = normalize_alias(alias)
    if not key:
        return
    alias_map.setdefault(
        key,
        {
            "ts_code": str(ts_code or "").strip().upper(),
            "stock_name": str(stock_name or "").strip() or str(ts_code or "").strip().upper(),
        },
    )


def _add_name_variants(alias_map: dict[str, dict], ts_code: str, stock_name: str) -> None:
    base_name = str(stock_name or "").strip()
    if not base_name:
        return
    _add_alias(alias_map, base_name, ts_code, base_name)
    stripped = _strip_st_prefix(base_name)
    if stripped and normalize_alias(stripped) != normalize_alias(base_name):
        _add_alias(alias_map, stripped, ts_code, stripped)
        _add_alias(alias_map, f"ST{stripped}", ts_code, base_name)
        _add_alias(alias_map, f"*ST{stripped}", ts_code, base_name)


def load_stock_alias_map(conn: sqlite3.Connection) -> dict[str, dict]:
    alias_map: dict[str, dict] = {}
    if table_exists(conn, "stock_codes"):
        rows = conn.execute(
            "SELECT ts_code, symbol, name FROM stock_codes WHERE COALESCE(ts_code, '') <> ''"
        ).fetchall()
        for ts_code, symbol, name in rows:
            code = str(ts_code or "").strip().upper()
            stock_name = str(name or "").strip()
            symbol_value = str(symbol or "").strip().upper()
            if code:
                _add_alias(alias_map, code, code, stock_name or code)
            if symbol_value:
                _add_alias(alias_map, symbol_value, code, stock_name or code)
                if len(symbol_value) == 6:
                    _add_alias(alias_map, symbol_value, code, stock_name or code)
            _add_name_variants(alias_map, code, stock_name)
    # Backfill historical stock names (e.g. ST rename) from scores table when alias dictionary misses them.
    if table_exists(conn, "stock_scores_daily"):
        rows = conn.execute(
            """
            SELECT ts_code, name
            FROM stock_scores_daily
            WHERE COALESCE(ts_code, '') <> ''
              AND COALESCE(name, '') <> ''
            GROUP BY ts_code, name
            """
        ).fetchall()
        for ts_code, name in rows:
            code = str(ts_code or "").strip().upper()
            hist_name = str(name or "").strip()
            if not code or not hist_name:
                continue
            _add_name_variants(alias_map, code, hist_name)
    if table_exists(conn, "stock_alias_dictionary"):
        rows = conn.execute(
            """
            SELECT alias, ts_code, stock_name, confidence
            FROM stock_alias_dictionary
            WHERE COALESCE(alias, '') <> '' AND COALESCE(ts_code, '') <> ''
            ORDER BY COALESCE(confidence, 0) DESC, COALESCE(used_count, 0) DESC
            """
        ).fetchall()
        for alias, ts_code, stock_name, confidence in rows:
            conf = float(confidence or 0.0)
            if conf < 0.8:
                continue
            key = normalize_alias(str(alias or ""))
            if not key:
                continue
            code = str(ts_code or "").strip().upper()
            display_name = str(stock_name or "").strip() or code
            _add_alias(alias_map, key, code, display_name)
            _add_name_variants(alias_map, code, display_name)
    return alias_map


CODE_RE = re.compile(r"\b(\d{6})\.(SZ|SH|BJ)\b", re.I)
PURE_CODE_RE = re.compile(r"\b(\d{6})\b")
ZH_SEG_RE = re.compile(r"[\u4e00-\u9fff]{2,8}")

BULLISH_TERMS = ("看多", "做多", "买入", "加仓", "看涨", "低吸", "看好")
BEARISH_TERMS = ("看空", "做空", "卖出", "减仓", "看跌", "清仓", "回避", "止损")


def detect_direction(text: str) -> str:
    content = str(text or "")
    bull = any(term in content for term in BULLISH_TERMS)
    bear = any(term in content for term in BEARISH_TERMS)
    if bull and not bear:
        return "看多"
    if bear and not bull:
        return "看空"
    return ""


def detect_stocks(text: str, alias_map: dict[str, dict]) -> list[dict]:
    matched: dict[str, dict] = {}
    content = str(text or "")
    for code, market in CODE_RE.findall(content):
        key = f"{code}.{market.upper()}"
        item = alias_map.get(normalize_alias(key)) or {"ts_code": key, "stock_name": key}
        matched[item["ts_code"]] = item
    for code in PURE_CODE_RE.findall(content):
        item = alias_map.get(code) or alias_map.get(normalize_alias(code))
        if item:
            matched[item["ts_code"]] = item
    for seg in ZH_SEG_RE.findall(content):
        normalized = normalize_alias(seg)
        item = alias_map.get(normalized)
        if item:
            matched[item["ts_code"]] = item
            continue
        for width in range(6, 1, -1):
            if len(seg) < width:
                continue
            for idx in range(0, len(seg) - width + 1):
                token = seg[idx : idx + width]
                item2 = alias_map.get(normalize_alias(token))
                if item2:
                    matched[item2["ts_code"]] = item2
    return list(matched.values())


def fetch_recent_chat_messages(conn: sqlite3.Connection, start_date: str) -> list[sqlite3.Row]:
    if not table_exists(conn, "wechat_chatlog_clean_items"):
        return []
    return conn.execute(
        """
        SELECT
            id, talker, message_date, message_time, sender_name, sender_id, content_clean, message_key
        FROM wechat_chatlog_clean_items
        WHERE COALESCE(message_date, '') >= ?
          AND COALESCE(content_clean, '') <> ''
        ORDER BY message_date ASC, message_time ASC, id ASC
        """,
        (start_date,),
    ).fetchall()


def _resolve_room_meta(room_map: dict[str, dict], *, talker: str, room_id: str) -> dict | None:
    talker_key = str(talker or "").strip()
    room_id_key = str(room_id or "").strip()
    if talker_key and talker_key in room_map:
        return room_map[talker_key]
    if room_id_key and room_id_key in room_map:
        return room_map[room_id_key]
    return None


def upsert_predictions(
    conn: sqlite3.Connection,
    *,
    room_map: dict[str, dict],
    alias_map: dict[str, dict],
    start_date: str,
) -> int:
    rows = fetch_recent_chat_messages(conn, start_date)
    inserted = 0
    now_text = now_utc_str()
    for row in rows:
        talker = str(row[1] or "").strip()
        room_meta = _resolve_room_meta(room_map, talker=talker, room_id="")
        if not room_meta:
            continue
        text = str(row[6] or "").strip()
        direction = detect_direction(text)
        if not direction:
            continue
        stocks = detect_stocks(text, alias_map)
        if not stocks:
            continue
        for stock in stocks:
            ts_code = str(stock.get("ts_code") or "").strip().upper()
            if not ts_code:
                continue
            stock_name = str(stock.get("stock_name") or ts_code).strip()
            message_key = str(row[7] or f"row-{row[0]}")
            conn.execute(
                """
                INSERT INTO chatroom_stock_signal_predictions (
                    talker, room_id, sender_name, sender_id,
                    signal_date, signal_time, ts_code, stock_name, direction,
                    source_message_key, source_content, source_table, room_strength_label,
                    created_at, update_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'wechat_chatlog_clean_items', ?, ?, ?)
                ON CONFLICT(source_message_key, ts_code, direction) DO UPDATE SET
                    room_id=excluded.room_id,
                    sender_name=excluded.sender_name,
                    sender_id=excluded.sender_id,
                    source_content=excluded.source_content,
                    update_time=excluded.update_time
                """,
                (
                    talker,
                    room_meta.get("room_id") or "",
                    str(row[4] or "").strip(),
                    str(row[5] or "").strip(),
                    str(row[2] or "").strip(),
                    str(row[3] or "").strip(),
                    ts_code,
                    stock_name,
                    direction,
                    message_key,
                    text[:300],
                    "weak" if bool(room_meta.get("is_weak")) else "normal",
                    now_text,
                    now_text,
                ),
            )
            inserted += 1
    conn.commit()
    return inserted


def upsert_predictions_from_analysis(
    conn: sqlite3.Connection,
    *,
    room_map: dict[str, dict],
    alias_map: dict[str, dict],
    start_date: str,
) -> int:
    if not table_exists(conn, "chatroom_investment_analysis"):
        return 0
    rows = conn.execute(
        """
        SELECT room_id, talker, analysis_date, targets_json
        FROM chatroom_investment_analysis
        WHERE COALESCE(analysis_date, '') >= ?
          AND COALESCE(targets_json, '') <> ''
          AND COALESCE(targets_json, '') <> '[]'
        ORDER BY analysis_date ASC, update_time ASC, id ASC
        """,
        (start_date,),
    ).fetchall()
    now_text = now_utc_str()
    inserted = 0
    for room_id, talker, analysis_date, targets_json in rows:
        room_meta = _resolve_room_meta(room_map, talker=str(talker or ""), room_id=str(room_id or ""))
        if not room_meta:
            continue
        try:
            targets = json.loads(str(targets_json or "[]"))
        except Exception:
            targets = []
        if not isinstance(targets, list):
            continue
        for idx, item in enumerate(targets):
            if not isinstance(item, dict):
                continue
            target_name = str(item.get("name") or "").strip()
            direction = str(item.get("bias") or "").strip()
            if direction not in {"看多", "看空"} or not target_name:
                continue
            matched = detect_stocks(target_name, alias_map)
            if not matched:
                continue
            stock = matched[0]
            ts_code = str(stock.get("ts_code") or "").strip().upper()
            stock_name = str(stock.get("stock_name") or target_name).strip()
            if not ts_code:
                continue
            source_key = f"analysis:{str(room_id or '').strip()}:{str(analysis_date or '').strip()}:{idx}:{target_name}:{direction}"
            conn.execute(
                """
                INSERT INTO chatroom_stock_signal_predictions (
                    talker, room_id, sender_name, sender_id,
                    signal_date, signal_time, ts_code, stock_name, direction,
                    source_message_key, source_content, source_table, room_strength_label,
                    created_at, update_time
                ) VALUES (?, ?, '', '', ?, '', ?, ?, ?, ?, ?, 'chatroom_investment_analysis', ?, ?, ?)
                ON CONFLICT(source_message_key, ts_code, direction) DO UPDATE SET
                    stock_name=excluded.stock_name,
                    source_content=excluded.source_content,
                    update_time=excluded.update_time
                """,
                (
                    str(talker or "").strip() or str(room_id or "").strip(),
                    str(room_id or "").strip(),
                    str(analysis_date or "").strip(),
                    ts_code,
                    stock_name,
                    direction,
                    source_key,
                    str(item.get("reason") or "")[:300],
                    "weak" if bool(room_meta.get("is_weak")) else "normal",
                    now_text,
                    now_text,
                ),
            )
            inserted += 1
    conn.commit()
    return inserted


def load_price_series(conn: sqlite3.Connection) -> dict[str, list[tuple[str, float]]]:
    if not table_exists(conn, "stock_daily_prices"):
        return {}
    rows = conn.execute(
        "SELECT ts_code, trade_date, close FROM stock_daily_prices WHERE close IS NOT NULL ORDER BY ts_code, trade_date"
    ).fetchall()
    out: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for ts_code, trade_date, close in rows:
        code = str(ts_code or "").strip().upper()
        date_text = str(trade_date or "").strip()
        if not code or not date_text:
            continue
        out[code].append((date_text, float(close)))
    return out


def evaluate_predictions(conn: sqlite3.Connection, as_of: date) -> dict[str, int]:
    price_map = load_price_series(conn)
    rows = conn.execute(
        """
        SELECT id, ts_code, direction, signal_date
        FROM chatroom_stock_signal_predictions
        WHERE COALESCE(validation_status, 'pending') IN ('pending', 'missing_price')
        ORDER BY signal_date ASC, id ASC
        """
    ).fetchall()
    now_text = now_utc_str()
    updated = 0
    pending = 0
    missing_price = 0
    for row in rows:
        row_id = int(row[0])
        ts_code = str(row[1] or "").strip().upper()
        direction = str(row[2] or "").strip()
        signal_date = str(row[3] or "").strip().replace("-", "")
        series = price_map.get(ts_code) or []
        if len(signal_date) != 8 or not series:
            conn.execute(
                """
                UPDATE chatroom_stock_signal_predictions
                SET validation_status='missing_price', update_time=?
                WHERE id=?
                """,
                (now_text, row_id),
            )
            missing_price += 1
            updated += 1
            continue
        base = None
        nxt = None
        for trade_date, close in series:
            td = trade_date.replace("-", "")
            if td <= signal_date:
                base = (trade_date, close)
            if td > signal_date:
                if datetime.strptime(trade_date, "%Y%m%d" if len(trade_date) == 8 else "%Y-%m-%d").date() <= as_of:
                    nxt = (trade_date, close)
                break
        if not base or not nxt:
            pending += 1
            continue
        b_close = float(base[1])
        n_close = float(nxt[1])
        ret = 0.0 if b_close == 0 else round((n_close - b_close) / b_close * 100, 3)
        if direction == "看多":
            verdict = "hit" if ret > 0 else ("flat" if ret == 0 else "miss")
        else:
            verdict = "hit" if ret < 0 else ("flat" if ret == 0 else "miss")
        conn.execute(
            """
            UPDATE chatroom_stock_signal_predictions
            SET validation_status='evaluated',
                target_trade_date=?,
                close_t0=?,
                close_t1=?,
                return_1d=?,
                verdict=?,
                evaluated_at=?,
                update_time=?
            WHERE id=?
            """,
            (nxt[0], b_close, n_close, ret, verdict, now_text, now_text, row_id),
        )
        updated += 1
    conn.commit()
    return {"updated": updated, "pending": pending, "missing_price": missing_price}


def label_by_rate(hit_rate: float, sample_size: int, min_sample: int) -> str:
    if sample_size < min_sample:
        return "样本不足"
    if hit_rate >= 0.65:
        return "高可信"
    if hit_rate >= 0.5:
        return "中可信"
    return "低可信"


def rebuild_accuracy_labels(conn: sqlite3.Connection, *, as_of: date, window_days: int, min_sample: int) -> dict[str, int]:
    start_date = (as_of - timedelta(days=max(window_days, 1) - 1)).isoformat()
    as_of_text = as_of.isoformat()
    now_text = now_utc_str()
    rows = conn.execute(
        """
        SELECT room_id, talker, sender_name, verdict
        FROM chatroom_stock_signal_predictions
        WHERE validation_status='evaluated'
          AND COALESCE(signal_date, '') >= ?
        """,
        (start_date,),
    ).fetchall()
    buckets: dict[tuple[str, str], dict[str, object]] = {}
    for room_id, talker, sender_name, verdict in rows:
        room_id_key = str(room_id or "").strip()
        room_key = str(talker or "").strip()
        sender_key = str(sender_name or "").strip()
        verdict_text = str(verdict or "").strip()
        resolved_room_key = room_id_key or room_key
        resolved_room_name = room_key or room_id_key
        if resolved_room_key:
            key = ("room", resolved_room_key)
            bucket = buckets.setdefault(key, {"sample": 0, "hit": 0, "name": resolved_room_name})
            if not str(bucket.get("name") or "").strip() and resolved_room_name:
                bucket["name"] = resolved_room_name
            bucket["sample"] = int(bucket.get("sample") or 0) + 1
            if verdict_text == "hit":
                bucket["hit"] = int(bucket.get("hit") or 0) + 1
        if sender_key:
            key = ("sender", sender_key)
            bucket = buckets.setdefault(key, {"sample": 0, "hit": 0, "name": sender_key})
            bucket["sample"] = int(bucket.get("sample") or 0) + 1
            if verdict_text == "hit":
                bucket["hit"] = int(bucket.get("hit") or 0) + 1

    conn.execute(
        """
        DELETE FROM chatroom_signal_accuracy_labels
        WHERE as_of_date=? AND window_days=?
        """,
        (as_of_text, window_days),
    )
    inserted = 0
    for (entity_type, entity_key), metric in buckets.items():
        sample = int(metric.get("sample") or 0)
        hit = int(metric.get("hit") or 0)
        entity_name = str(metric.get("name") or entity_key).strip()
        rate = round(hit / sample, 4) if sample else 0.0
        label = label_by_rate(rate, sample, min_sample)
        conn.execute(
            """
            INSERT INTO chatroom_signal_accuracy_labels (
                entity_type, entity_key, entity_name, as_of_date, window_days,
                sample_size, hit_count, hit_rate, accuracy_label, stats_json, created_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity_type,
                entity_key,
                entity_name,
                as_of_text,
                window_days,
                sample,
                hit,
                rate,
                label,
                json.dumps({"sample_size": sample, "hit_count": hit, "hit_rate": rate}, ensure_ascii=False),
                now_text,
                now_text,
            ),
        )
        inserted += 1
    conn.commit()
    return {"inserted": inserted, "entity_total": len(buckets)}


def run_refresh(
    *,
    db_path: str,
    as_of_date_text: str,
    lookback_days: int,
    window_days: int,
    min_sample: int,
    room_scope: str,
) -> dict:
    as_of = as_of_sh_date(as_of_date_text)
    start_date = (as_of - timedelta(days=max(lookback_days, 1) - 1)).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        ensure_tables(conn)
        room_map = build_room_map(conn, room_scope=room_scope)
        alias_map = load_stock_alias_map(conn)
        inserted_msg = upsert_predictions(conn, room_map=room_map, alias_map=alias_map, start_date=start_date)
        inserted_analysis = upsert_predictions_from_analysis(conn, room_map=room_map, alias_map=alias_map, start_date=start_date)
        inserted = inserted_msg + inserted_analysis
        eval_stats = evaluate_predictions(conn, as_of=as_of)
        label_stats = rebuild_accuracy_labels(
            conn,
            as_of=as_of,
            window_days=max(window_days, 1),
            min_sample=max(min_sample, 1),
        )
        return {
            "ok": True,
            "as_of_date": as_of.isoformat(),
            "room_scope": room_scope,
            "room_count": len(room_map),
            "predictions_upserted": inserted,
            "predictions_upserted_from_messages": inserted_msg,
            "predictions_upserted_from_analysis": inserted_analysis,
            "evaluation": eval_stats,
            "labels": label_stats,
        }
    finally:
        conn.close()


def main() -> int:
    args = parse_args()
    result = run_refresh(
        db_path=str(args.db_path),
        as_of_date_text=str(args.as_of_date or ""),
        lookback_days=int(args.lookback_days),
        window_days=int(args.window_days),
        min_sample=int(args.min_sample),
        room_scope=str(args.room_scope or "all"),
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
