#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import db_compat as sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

from realtime_streams import publish_app_event

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"
DEFAULT_TARGET_TABLE = "investment_signal_tracker"
SNAPSHOT_TABLE = "investment_signal_daily_snapshots"
EVENT_TABLE = "investment_signal_events"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="聚合新闻/个股新闻/群聊候选池，生成投资信号追踪表")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE, help="目标表名")
    parser.add_argument("--lookback-days", type=int, default=30, help="回看天数")
    parser.add_argument("--limit-news", type=int, default=800, help="纳入聚合的新闻上限")
    parser.add_argument("--limit-stock-news", type=int, default=1200, help="纳入聚合的个股新闻上限")
    parser.add_argument("--min-strength", type=float, default=10.0, help="最低信号强度")
    parser.add_argument("--skip-history", action="store_true", help="仅刷新目标表，不写快照/事件表")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def today_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def cutoff_news_time(lookback_days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=max(lookback_days, 1))
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def cutoff_stock_news_time(lookback_days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=max(lookback_days, 1))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()[0]
    if table_exists:
        return
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            subject_name TEXT NOT NULL,
            ts_code TEXT,
            direction TEXT,
            signal_strength REAL,
            confidence REAL,
            evidence_count INTEGER DEFAULT 0,
            news_count INTEGER DEFAULT 0,
            stock_news_count INTEGER DEFAULT 0,
            chatroom_count INTEGER DEFAULT 0,
            signal_status TEXT,
            latest_signal_date TEXT,
            evidence_json TEXT,
            source_summary_json TEXT,
            created_at TEXT,
            update_time TEXT,
            UNIQUE(signal_key),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_subject ON {table_name}(subject_name)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_type_dir ON {table_name}(signal_type, direction)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_strength ON {table_name}(signal_strength DESC)")
    conn.commit()


def ensure_snapshot_and_event_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_at TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            signal_key TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            subject_name TEXT NOT NULL,
            ts_code TEXT,
            direction TEXT,
            signal_strength REAL,
            confidence REAL,
            evidence_count INTEGER DEFAULT 0,
            news_count INTEGER DEFAULT 0,
            stock_news_count INTEGER DEFAULT 0,
            chatroom_count INTEGER DEFAULT 0,
            signal_status TEXT,
            latest_signal_date TEXT,
            evidence_json TEXT,
            source_summary_json TEXT,
            created_at TEXT,
            UNIQUE(snapshot_at, signal_key),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SNAPSHOT_TABLE}_signal_time ON {SNAPSHOT_TABLE}(signal_key, snapshot_at DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SNAPSHOT_TABLE}_date ON {SNAPSHOT_TABLE}(snapshot_date)"
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {EVENT_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL,
            event_time TEXT NOT NULL,
            event_date TEXT NOT NULL,
            event_type TEXT NOT NULL,
            old_direction TEXT,
            new_direction TEXT,
            old_strength REAL,
            new_strength REAL,
            delta_strength REAL,
            old_confidence REAL,
            new_confidence REAL,
            delta_confidence REAL,
            event_level TEXT,
            driver_type TEXT,
            driver_source TEXT,
            driver_ref_id TEXT,
            driver_title TEXT,
            status_after_event TEXT,
            event_summary TEXT,
            evidence_json TEXT,
            snapshot_before_json TEXT,
            snapshot_after_json TEXT,
            created_at TEXT,
            UNIQUE(signal_key, event_time, event_type)
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{EVENT_TABLE}_signal_time ON {EVENT_TABLE}(signal_key, event_time DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{EVENT_TABLE}_date ON {EVENT_TABLE}(event_date)"
    )
    conn.commit()


def parse_json_text(raw: str):
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def normalize_subject(text: str) -> str:
    value = str(text or "").strip()
    value = re.sub(r"\s+", " ", value)
    return value


def build_signal_key(signal_type: str, subject_name: str, ts_code: str) -> str:
    if ts_code:
        return f"{signal_type}:{ts_code}"
    return f"{signal_type}:{normalize_subject(subject_name)}"


def subject_to_type(name: str) -> str:
    text = normalize_subject(name)
    if not text:
        return "theme"
    macro_keywords = ["通胀", "利率", "汇率", "流动性", "风险偏好", "美元", "人民币"]
    commodity_keywords = ["黄金", "原油", "天然气", "铜", "白银", "煤炭"]
    fx_keywords = ["USDCNY", "USDJPY", "EURUSD", "DXY", "人民币", "美元指数"]
    if any(k in text for k in fx_keywords):
        return "fx"
    if any(k in text for k in commodity_keywords):
        return "commodity"
    if any(k in text for k in macro_keywords):
        return "macro"
    return "theme"


def _signal_bucket(subject_name: str, signal_type: str, ts_code: str = "") -> dict:
    return {
        "signal_key": build_signal_key(signal_type, subject_name, ts_code),
        "signal_type": signal_type,
        "subject_name": subject_name,
        "ts_code": ts_code,
        "bullish": 0.0,
        "bearish": 0.0,
        "weak_bullish": 0.0,
        "weak_bearish": 0.0,
        "neutral": 0.0,
        "news_count": 0,
        "intl_news_count": 0,
        "domestic_news_count": 0,
        "stock_news_count": 0,
        "chatroom_count": 0,
        "chatroom_room_count": 0,
        "chatroom_mention_count": 0,
        "latest_signal_date": "",
        "evidence": [],
        "sources": {
            "news": 0,
            "intl_news": 0,
            "domestic_news": 0,
            "stock_news": 0,
            "chatroom": 0,
            "theme_mapping": 0,
        },
    }


def ensure_quality_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_mapping_blocklist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            term TEXT NOT NULL,
            target_type TEXT NOT NULL DEFAULT 'stock',
            match_type TEXT NOT NULL DEFAULT 'exact',
            source TEXT,
            reason TEXT,
            enabled INTEGER DEFAULT 1,
            created_at TEXT,
            update_time TEXT,
            UNIQUE(term, target_type, match_type)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_quality_rules (
            rule_key TEXT PRIMARY KEY,
            rule_value TEXT,
            value_type TEXT DEFAULT 'number',
            category TEXT,
            description TEXT,
            enabled INTEGER DEFAULT 1,
            created_at TEXT,
            update_time TEXT
        )
        """
    )
    conn.commit()


def load_quality_rules(conn: sqlite3.Connection) -> dict[str, object]:
    defaults: dict[str, object] = {
        "chatroom_min_room_count": 2.0,
        "chatroom_min_mention_count": 3.0,
        "chatroom_weak_strength_threshold": 3.0,
        "theme_only_stock_enabled": False,
        "theme_mapping_weight_cap": 5.0,
        "active_min_confidence_pct": 40.0,
        "stock_min_strength_with_direct_source": 2.0,
        "stock_min_strength_without_direct_source": 10.0,
    }
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='signal_quality_rules'"
    ).fetchone()[0]
    if not table_exists:
        return defaults
    rows = conn.execute(
        """
        SELECT rule_key, rule_value, value_type
        FROM signal_quality_rules
        WHERE COALESCE(enabled, 1) = 1
        """
    ).fetchall()
    for row in rows:
        key = str(row[0] or "").strip()
        raw = str(row[1] or "").strip()
        value_type = str(row[2] or "").strip().lower()
        if not key:
            continue
        if value_type == "bool":
            defaults[key] = raw.lower() in {"1", "true", "yes", "on"}
        elif value_type == "number":
            try:
                defaults[key] = float(raw)
            except Exception:
                pass
        else:
            defaults[key] = raw
    return defaults


def load_signal_blocklist(conn: sqlite3.Connection, target_type: str = "stock") -> dict[str, set[str]]:
    out = {"exact": set(), "contains": set()}
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='signal_mapping_blocklist'"
    ).fetchone()[0]
    if not table_exists:
        return out
    rows = conn.execute(
        """
        SELECT term, match_type
        FROM signal_mapping_blocklist
        WHERE COALESCE(enabled, 1) = 1
          AND target_type = ?
        """,
        (target_type,),
    ).fetchall()
    for row in rows:
        term = normalize_subject(row[0])
        match_type = str(row[1] or "").strip().lower()
        if not term:
            continue
        if match_type == "contains":
            out["contains"].add(term)
        else:
            out["exact"].add(term)
    return out


def is_blocked_stock_subject(subject_name: str, blocklist: dict[str, set[str]]) -> bool:
    text = normalize_subject(subject_name)
    if not text:
        return False
    if text in blocklist.get("exact", set()):
        return True
    return any(term and term in text for term in blocklist.get("contains", set()))


def classify_direction_bias(direction: str) -> str:
    text = str(direction or "").strip()
    if text in {"利多", "看多"}:
        return "bullish"
    if text in {"利空", "看空"}:
        return "bearish"
    if text in {"偏利多", "中性偏多", "偏多", "弱看多"}:
        return "weak_bullish"
    if text in {"偏利空", "中性偏空", "偏空", "弱看空"}:
        return "weak_bearish"
    return "neutral"


def register_evidence(bucket: dict, *, source: str, direction: str, weight: float, latest_date: str, evidence: dict) -> None:
    bias = classify_direction_bias(direction)
    if bias == "bullish":
        bucket["bullish"] += weight
    elif bias == "bearish":
        bucket["bearish"] += weight
    elif bias == "weak_bullish":
        bucket["weak_bullish"] += weight
    elif bias == "weak_bearish":
        bucket["weak_bearish"] += weight
    else:
        bucket["neutral"] += weight
    if latest_date and latest_date > bucket["latest_signal_date"]:
        bucket["latest_signal_date"] = latest_date
    bucket["sources"][source] = int(bucket["sources"].get(source, 0)) + 1
    if source == "news":
        bucket["news_count"] += 1
    elif source == "intl_news":
        bucket["news_count"] += 1
        bucket["intl_news_count"] += 1
    elif source == "domestic_news":
        bucket["news_count"] += 1
        bucket["domestic_news_count"] += 1
    elif source == "stock_news":
        bucket["stock_news_count"] += 1
    elif source == "chatroom":
        bucket["chatroom_count"] += 1
    if len(bucket["evidence"]) < 8:
        bucket["evidence"].append(evidence)


def map_importance_to_weight(level: str) -> float:
    return {
        "极高": 4.0,
        "高": 3.0,
        "中": 2.0,
        "低": 1.0,
        "极低": 0.5,
    }.get(str(level or "").strip(), 1.0)


def map_impact_to_direction(direction: str) -> str:
    d = str(direction or "").strip()
    if d in {"利多", "看多"}:
        return "看多"
    if d in {"利空", "看空"}:
        return "看空"
    return "中性"


def load_stock_name_map(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute("SELECT ts_code, name FROM stock_codes").fetchall()
    mapping: dict[str, str] = {}
    for ts_code, name in rows:
        mapping[str(ts_code)] = str(name or "")
    return mapping


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def classify_news_source(source: str) -> str:
    text = str(source or "").strip().lower()
    if text.startswith("cn_"):
        return "domestic_news"
    return "intl_news"


def iter_structured_impacts(raw_impacts) -> list[dict]:
    impacts = parse_json_text(raw_impacts) or []
    if isinstance(impacts, list):
        out = []
        for item in impacts:
            if not isinstance(item, dict):
                continue
            subject_name = normalize_subject(
                item.get("item")
                or item.get("asset")
                or item.get("industry")
                or item.get("market")
                or item.get("macro")
                or item.get("sector")
            )
            if not subject_name:
                continue
            out.append(
                {
                    "subject_name": subject_name,
                    "group": item.get("group") or item.get("item") or item.get("asset") or item.get("industry") or item.get("market") or item.get("macro") or item.get("sector"),
                    "direction": item.get("direction"),
                }
            )
        return out
    if isinstance(impacts, dict):
        group_map = {
            "macro": "macro",
            "markets": "market",
            "market": "market",
            "sectors": "sector",
            "sector": "sector",
            "assets": "asset",
            "asset": "asset",
            "industries": "industry",
            "industry": "industry",
        }
        out = []
        for key, values in impacts.items():
            if not isinstance(values, list):
                continue
            for item in values:
                if not isinstance(item, dict):
                    continue
                subject_name = normalize_subject(item.get("item") or item.get("name") or item.get("asset") or item.get("industry") or item.get("market") or item.get("macro") or item.get("sector"))
                if not subject_name:
                    continue
                out.append(
                    {
                        "subject_name": subject_name,
                        "group": group_map.get(str(key).strip().lower(), str(key).strip().lower()),
                        "direction": item.get("direction"),
                    }
                )
        return out
    return []


def aggregate_explicit_direction(items: list[dict]) -> str:
    bullish = 0
    bearish = 0
    weak_bullish = 0
    weak_bearish = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        bias = classify_direction_bias(item.get("direction"))
        if bias == "bullish":
            bullish += 1
        elif bias == "bearish":
            bearish += 1
        elif bias == "weak_bullish":
            weak_bullish += 1
        elif bias == "weak_bearish":
            weak_bearish += 1
    if bullish > bearish and bullish > 0:
        return "看多"
    if bearish > bullish and bearish > 0:
        return "看空"
    if weak_bullish > weak_bearish and weak_bullish > 0:
        return "弱看多"
    if weak_bearish > weak_bullish and weak_bearish > 0:
        return "弱看空"
    return "中性"


def determine_stock_news_direction(company_name: str, impacts: list[dict]) -> str:
    company_name = normalize_subject(company_name)
    focused = []
    for item in impacts:
        subject_name = normalize_subject(item.get("subject_name"))
        if not subject_name:
            continue
        if company_name and company_name in subject_name:
            focused.append(item)
            continue
        if "股价" in subject_name or subject_name in {"公司股价", "个股", "个股股价"}:
            focused.append(item)
    if focused:
        return aggregate_explicit_direction(focused)
    return aggregate_explicit_direction(impacts)


def normalize_sentiment_label(raw: str) -> str:
    text = str(raw or "").strip()
    if text in {"偏多", "看多", "利多"}:
        return "偏多"
    if text in {"偏空", "看空", "利空"}:
        return "偏空"
    return "中性"


def sentiment_to_signal_direction(label: str, score) -> str:
    norm = normalize_sentiment_label(label)
    try:
        val = float(score or 0)
    except Exception:
        val = 0.0
    if norm == "偏多" or val >= 20:
        return "弱看多" if val < 45 else "看多"
    if norm == "偏空" or val <= -20:
        return "弱看空" if val > -45 else "看空"
    return "中性"


def sentiment_weight_factor(score) -> float:
    try:
        val = abs(float(score or 0))
    except Exception:
        val = 0.0
    return 1.0 + min(val, 100.0) / 200.0


def row_sentiment_snapshot(row: dict | None) -> dict:
    if not row:
        return {"label": "", "score": 0.0}
    evidences = parse_json_text((row.get("evidence_json") or "")) or []
    for ev in evidences:
        if not isinstance(ev, dict):
            continue
        label = normalize_sentiment_label(ev.get("sentiment_label"))
        try:
            score = float(ev.get("sentiment_score") or 0)
        except Exception:
            score = 0.0
        if label != "中性" or abs(score) > 0:
            return {"label": label, "score": round(score, 2)}
    return {"label": "", "score": 0.0}


def merge_news_signals(conn: sqlite3.Connection, buckets: dict[str, dict], lookback_days: int, limit_news: int) -> None:
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='news_feed_items'"
    ).fetchone()[0]
    if not table_exists:
        return
    cols = get_table_columns(conn, "news_feed_items")
    has_related_codes = "related_ts_codes_json" in cols
    has_related_names = "related_stock_names_json" in cols
    has_sentiment_score = "llm_sentiment_score" in cols
    has_sentiment_label = "llm_sentiment_label" in cols
    select_related_codes = ", related_ts_codes_json" if has_related_codes else ", '' AS related_ts_codes_json"
    select_related_names = ", related_stock_names_json" if has_related_names else ", '' AS related_stock_names_json"
    select_sentiment_score = ", llm_sentiment_score" if has_sentiment_score else ", 0 AS llm_sentiment_score"
    select_sentiment_label = ", llm_sentiment_label" if has_sentiment_label else ", '中性' AS llm_sentiment_label"
    sql = f"""
    SELECT id, source, title, pub_date, llm_finance_importance, llm_impacts_json
           {select_related_codes}
           {select_related_names}
           {select_sentiment_score}
           {select_sentiment_label}
    FROM news_feed_items
    WHERE COALESCE(pub_date, '') >= ?
    ORDER BY COALESCE(pub_date, '') DESC, id DESC
    LIMIT ?
    """
    stock_name_map = load_stock_name_map(conn)
    for row in conn.execute(sql, (cutoff_news_time(lookback_days), limit_news)).fetchall():
        source_type = classify_news_source(row[1])
        impacts = iter_structured_impacts(row[5])
        overall_direction = aggregate_explicit_direction(impacts)
        sentiment_direction = sentiment_to_signal_direction(row[9], row[8])
        weight_factor = sentiment_weight_factor(row[8])
        for item in impacts[:12]:
            subject_name = item.get("subject_name") or ""
            if not subject_name:
                continue
            signal_type = subject_to_type(subject_name)
            bucket = buckets.setdefault(build_signal_key(signal_type, subject_name, ""), _signal_bucket(subject_name, signal_type))
            register_evidence(
                bucket,
                source=source_type,
                direction=map_impact_to_direction(item.get("direction")),
                weight=map_importance_to_weight(row[4]) * weight_factor,
                latest_date=str(row[3] or ""),
                evidence={
                    "source": row[1],
                    "title": row[2],
                    "date": row[3],
                    "direction": map_impact_to_direction(item.get("direction")),
                    "group": item.get("group"),
                    "sentiment_label": normalize_sentiment_label(row[9]),
                    "sentiment_score": row[8],
                },
            )
        related_codes = parse_json_text(row[6]) or []
        related_names = parse_json_text(row[7]) or []
        if not isinstance(related_codes, list):
            related_codes = []
        if not isinstance(related_names, list):
            related_names = []
        matched_names = {str(it.get("ts_code") or ""): str(it.get("name") or "") for it in related_names if isinstance(it, dict)}
        stock_direction = overall_direction if overall_direction in {"看多", "看空", "弱看多", "弱看空"} else sentiment_direction
        if stock_direction in {"看多", "看空", "弱看多", "弱看空"}:
            for ts_code in related_codes[:5]:
                ts_code = str(ts_code or "").strip().upper()
                if not ts_code:
                    continue
                stock_name = matched_names.get(ts_code) or stock_name_map.get(ts_code) or ts_code
                bucket = buckets.setdefault(
                    build_signal_key("stock", stock_name, ts_code),
                    _signal_bucket(stock_name, "stock", ts_code),
                )
                register_evidence(
                    bucket,
                    source=source_type,
                    direction=stock_direction,
                    weight=map_importance_to_weight(row[4]) * weight_factor,
                    latest_date=str(row[3] or ""),
                    evidence={
                        "source": row[1],
                        "title": row[2],
                        "date": row[3],
                        "direction": stock_direction,
                        "group": "stock",
                        "sentiment_label": normalize_sentiment_label(row[9]),
                        "sentiment_score": row[8],
                    },
                )


def merge_stock_news_signals(conn: sqlite3.Connection, buckets: dict[str, dict], lookback_days: int, limit_stock_news: int) -> None:
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_news_items'"
    ).fetchone()[0]
    if not table_exists:
        return
    sql = """
    SELECT id, ts_code, company_name, title, pub_time, llm_finance_importance, llm_impacts_json,
           COALESCE(llm_sentiment_score, 0) AS llm_sentiment_score,
           COALESCE(llm_sentiment_label, '中性') AS llm_sentiment_label
    FROM stock_news_items
    WHERE COALESCE(pub_time, '') >= ?
    ORDER BY COALESCE(pub_time, '') DESC, id DESC
    LIMIT ?
    """
    for row in conn.execute(sql, (cutoff_stock_news_time(lookback_days), limit_stock_news)).fetchall():
        ts_code = str(row[1] or "").strip()
        subject_name = normalize_subject(row[2] or ts_code)
        impacts = iter_structured_impacts(row[6])
        sentiment_direction = sentiment_to_signal_direction(row[8], row[7])
        weight_factor = sentiment_weight_factor(row[7])
        if subject_name:
            bucket = buckets.setdefault(build_signal_key("stock", subject_name, ts_code), _signal_bucket(subject_name, "stock", ts_code))
            direction = determine_stock_news_direction(subject_name, impacts)
            if direction == "中性":
                direction = sentiment_direction
            register_evidence(
                bucket,
                source="stock_news",
                direction=direction,
                weight=(map_importance_to_weight(row[5]) + 1.0) * weight_factor,
                latest_date=str(row[4] or ""),
                evidence={
                    "source": "stock_news",
                    "title": row[3],
                    "date": row[4],
                    "direction": direction,
                    "sentiment_label": normalize_sentiment_label(row[8]),
                    "sentiment_score": row[7],
                },
            )
        for item in impacts[:8]:
            related = normalize_subject(item.get("subject_name"))
            if not related or related == subject_name:
                continue
            signal_type = subject_to_type(related)
            bucket = buckets.setdefault(build_signal_key(signal_type, related, ""), _signal_bucket(related, signal_type))
            register_evidence(
                bucket,
                source="stock_news",
                direction=map_impact_to_direction(item.get("direction")),
                weight=map_importance_to_weight(row[5]) * weight_factor,
                latest_date=str(row[4] or ""),
                evidence={
                    "source": "stock_news",
                    "title": row[3],
                    "date": row[4],
                    "direction": map_impact_to_direction(item.get("direction")),
                    "group": item.get("group"),
                    "sentiment_label": normalize_sentiment_label(row[8]),
                    "sentiment_score": row[7],
                },
            )


def merge_chatroom_signals(conn: sqlite3.Connection, buckets: dict[str, dict]) -> None:
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chatroom_stock_candidate_pool'"
    ).fetchone()[0]
    if not table_exists:
        return
    stock_name_map = load_stock_name_map(conn)
    cols = get_table_columns(conn, "chatroom_stock_candidate_pool")
    select_ts_code = ", ts_code" if "ts_code" in cols else ", '' AS ts_code"
    sql = f"""
    SELECT candidate_name, candidate_type, dominant_bias, bullish_room_count, bearish_room_count,
           room_count, mention_count, latest_analysis_date
           {select_ts_code}
    FROM chatroom_stock_candidate_pool
    ORDER BY ABS(net_score) DESC, room_count DESC, mention_count DESC
    """
    for row in conn.execute(sql).fetchall():
        candidate_name = normalize_subject(row[0])
        if not candidate_name:
            continue
        ts_code = str(row[8] or "").strip().upper()
        if not ts_code:
            for code, name in stock_name_map.items():
                if normalize_subject(name) == candidate_name or normalize_subject(code) == candidate_name:
                    ts_code = code
                    break
        signal_type = "stock" if row[1] in {"股票", "标的"} and ts_code else subject_to_type(candidate_name)
        bucket = buckets.setdefault(build_signal_key(signal_type, candidate_name, ts_code), _signal_bucket(candidate_name, signal_type, ts_code))
        weight = float(abs(int(row[3] or 0) - int(row[4] or 0)) + int(row[5] or 0) * 0.5 + int(row[6] or 0) * 0.2)
        register_evidence(
            bucket,
            source="chatroom",
            direction=str(row[2] or "中性"),
            weight=max(weight, 1.0),
            latest_date=str(row[7] or ""),
            evidence={
                "source": "chatroom",
                "direction": str(row[2] or ""),
                "room_count": int(row[5] or 0),
                "mention_count": int(row[6] or 0),
                "date": row[7],
            },
        )
        bucket["chatroom_room_count"] = max(int(bucket.get("chatroom_room_count") or 0), int(row[5] or 0))
        bucket["chatroom_mention_count"] = max(int(bucket.get("chatroom_mention_count") or 0), int(row[6] or 0))


def explicit_bucket_direction(bucket: dict) -> str:
    bullish = float(bucket.get("bullish") or 0.0)
    bearish = float(bucket.get("bearish") or 0.0)
    weak_bullish = float(bucket.get("weak_bullish") or 0.0)
    weak_bearish = float(bucket.get("weak_bearish") or 0.0)
    if bullish > bearish and bullish >= 1.0:
        return "看多"
    if bearish > bullish and bearish >= 1.0:
        return "看空"
    if weak_bullish > weak_bearish and weak_bullish >= 1.0:
        return "弱看多"
    if weak_bearish > weak_bullish and weak_bearish >= 1.0:
        return "弱看空"
    return "中性"


def explicit_bucket_strength(bucket: dict) -> float:
    bullish = float(bucket.get("bullish") or 0.0)
    bearish = float(bucket.get("bearish") or 0.0)
    weak_bullish = float(bucket.get("weak_bullish") or 0.0)
    weak_bearish = float(bucket.get("weak_bearish") or 0.0)
    return abs(bullish - bearish) + abs(weak_bullish - weak_bearish) * 0.35


def merge_theme_mapped_stock_signals(conn: sqlite3.Connection, buckets: dict[str, dict], rules: dict[str, object]) -> None:
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='theme_stock_mapping'"
    ).fetchone()[0]
    if not table_exists:
        return
    stock_name_map = load_stock_name_map(conn)
    mapping_rows = conn.execute(
        """
        SELECT theme_name, ts_code, stock_name, relation_type, weight, source
        FROM theme_stock_mapping
        WHERE COALESCE(theme_name, '') <> '' AND COALESCE(ts_code, '') <> ''
        ORDER BY theme_name, COALESCE(weight, 1.0) DESC, ts_code
        """
    ).fetchall()
    if not mapping_rows:
        return

    by_theme: dict[str, list[dict]] = {}
    for row in mapping_rows:
        theme_name = normalize_subject(row[0])
        if not theme_name:
            continue
        by_theme.setdefault(theme_name, []).append(
            {
                "ts_code": str(row[1] or "").strip().upper(),
                "stock_name": str(row[2] or "").strip(),
                "relation_type": str(row[3] or "").strip(),
                "weight": float(row[4] or 1.0),
                "source": str(row[5] or "").strip() or "theme_stock_mapping",
            }
        )

    for bucket in list(buckets.values()):
        if str(bucket.get("signal_type") or "") == "stock":
            continue
        theme_name = normalize_subject(bucket.get("subject_name") or "")
        mappings = by_theme.get(theme_name)
        if not mappings:
            continue
        direction = explicit_bucket_direction(bucket)
        if direction not in {"看多", "看空"}:
            continue
        theme_strength = explicit_bucket_strength(bucket)
        if theme_strength < 2.0:
            continue
        for item in mappings:
            ts_code = item["ts_code"]
            if not ts_code:
                continue
            stock_name = item["stock_name"] or stock_name_map.get(ts_code) or ts_code
            propagated_weight = min(
                max((theme_strength ** 0.5) * 0.35 * float(item["weight"] or 1.0), 0.4),
                float(rules.get("theme_mapping_weight_cap", 5.0) or 5.0),
            )
            target = buckets.setdefault(
                build_signal_key("stock", stock_name, ts_code),
                _signal_bucket(stock_name, "stock", ts_code),
            )
            register_evidence(
                target,
                source="theme_mapping",
                direction=direction,
                weight=propagated_weight,
                latest_date=str(bucket.get("latest_signal_date") or ""),
                evidence={
                    "source": "theme_mapping",
                    "theme_name": theme_name,
                    "relation_type": item["relation_type"],
                    "mapping_source": item["source"],
                    "direction": direction,
                    "weight": round(propagated_weight, 2),
                },
            )


def finalize_rows(
    buckets: dict[str, dict],
    min_strength: float,
    rules: dict[str, object],
    stock_blocklist: dict[str, set[str]],
) -> list[dict]:
    rows: list[dict] = []
    for bucket in buckets.values():
        if bucket["signal_type"] == "stock" and is_blocked_stock_subject(bucket["subject_name"], stock_blocklist):
            continue
        bullish = float(bucket["bullish"])
        bearish = float(bucket["bearish"])
        weak_bullish = float(bucket["weak_bullish"])
        weak_bearish = float(bucket["weak_bearish"])
        neutral = float(bucket["neutral"])
        net = bullish - bearish
        weak_net = weak_bullish - weak_bearish
        strength = abs(net) + abs(weak_net) * 0.35 + neutral * 0.05 + bucket["chatroom_count"] * 0.5
        effective_min_strength = float(min_strength)
        has_direct_source = float(bucket.get("news_count") or 0) > 0 or float(bucket.get("stock_news_count") or 0) > 0 or float(bucket.get("chatroom_count") or 0) > 0
        if bucket["signal_type"] == "stock":
            if has_direct_source:
                effective_min_strength = min(effective_min_strength, float(rules.get("stock_min_strength_with_direct_source", 2.0) or 2.0))
            else:
                effective_min_strength = max(effective_min_strength, float(rules.get("stock_min_strength_without_direct_source", 10.0) or 10.0))
            theme_only = (not has_direct_source) and float(bucket["sources"].get("theme_mapping", 0) or 0) > 0
            if theme_only and not bool(rules.get("theme_only_stock_enabled", False)):
                continue
            if float(bucket.get("chatroom_count") or 0) > 0:
                min_room_count = int(float(rules.get("chatroom_min_room_count", 2.0) or 2.0))
                min_mention_count = int(float(rules.get("chatroom_min_mention_count", 3.0) or 3.0))
                room_count = int(bucket.get("chatroom_room_count") or 0)
                mention_count = int(bucket.get("chatroom_mention_count") or 0)
                if room_count < min_room_count and mention_count < min_mention_count and not (float(bucket.get("news_count") or 0) > 0 or float(bucket.get("stock_news_count") or 0) > 0):
                    effective_min_strength = max(effective_min_strength, float(rules.get("chatroom_weak_strength_threshold", 3.0) or 3.0))
        if strength < effective_min_strength:
            continue
        total = bullish + bearish + weak_bullish + weak_bearish + neutral
        confidence = 0.0 if total <= 0 else round(min(abs(net) / max(total, 1.0), 1.0), 4)
        if net > 0:
            direction = "看多"
        elif net < 0:
            direction = "看空"
        else:
            direction = "中性"
        active_min_confidence = float(rules.get("active_min_confidence_pct", 40.0) or 40.0) / 100.0
        if strength >= 25 and confidence >= active_min_confidence and direction in {"看多", "看空"}:
            status = "活跃"
        elif direction == "中性":
            status = "待证伪"
        else:
            status = "观察"
        rows.append(
            {
                "signal_key": bucket["signal_key"],
                "signal_type": bucket["signal_type"],
                "subject_name": bucket["subject_name"],
                "ts_code": bucket["ts_code"],
                "direction": direction,
                "signal_strength": round(strength, 2),
                "confidence": round(confidence * 100, 2),
                "evidence_count": int(bucket["news_count"] + bucket["stock_news_count"] + bucket["chatroom_count"]),
                "news_count": int(bucket["news_count"]),
                "stock_news_count": int(bucket["stock_news_count"]),
                "chatroom_count": int(bucket["chatroom_count"]),
                "signal_status": status,
                "latest_signal_date": bucket["latest_signal_date"],
                "evidence_json": json.dumps(bucket["evidence"], ensure_ascii=False),
                "source_summary_json": json.dumps(bucket["sources"], ensure_ascii=False),
            }
        )
    rows.sort(key=lambda x: (-float(x["signal_strength"]), -float(x["confidence"]), x["subject_name"]))
    return rows


def replace_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict]) -> int:
    now = now_utc_str()
    conn.execute(f"DELETE FROM {table_name}")
    sql = f"""
    INSERT INTO {table_name} (
        signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
        evidence_count, news_count, stock_news_count, chatroom_count, signal_status,
        latest_signal_date, evidence_json, source_summary_json, created_at, update_time
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for row in rows:
        conn.execute(
            sql,
            (
                row["signal_key"],
                row["signal_type"],
                row["subject_name"],
                row["ts_code"] or None,
                row["direction"],
                row["signal_strength"],
                row["confidence"],
                row["evidence_count"],
                row["news_count"],
                row["stock_news_count"],
                row["chatroom_count"],
                row["signal_status"],
                row["latest_signal_date"],
                row["evidence_json"],
                row["source_summary_json"],
                now,
                now,
            ),
        )
    conn.commit()
    return len(rows)


def load_previous_tracker(conn: sqlite3.Connection, table_name: str) -> dict[str, dict]:
    table_exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()[0]
    if not table_exists:
        return {}
    rows = conn.execute(
        f"""
        SELECT signal_key, signal_type, subject_name, ts_code, direction, signal_strength, confidence,
               evidence_count, news_count, stock_news_count, chatroom_count, signal_status,
               latest_signal_date, evidence_json, source_summary_json
        FROM {table_name}
        """
    ).fetchall()
    out: dict[str, dict] = {}
    for row in rows:
        item = {
            "signal_key": row[0],
            "signal_type": row[1],
            "subject_name": row[2],
            "ts_code": row[3] or "",
            "direction": row[4] or "",
            "signal_strength": float(row[5] or 0.0),
            "confidence": float(row[6] or 0.0),
            "evidence_count": int(row[7] or 0),
            "news_count": int(row[8] or 0),
            "stock_news_count": int(row[9] or 0),
            "chatroom_count": int(row[10] or 0),
            "signal_status": row[11] or "",
            "latest_signal_date": row[12] or "",
            "evidence_json": row[13] or "[]",
            "source_summary_json": row[14] or "{}",
        }
        out[item["signal_key"]] = item
    return out


def snapshot_signal_rows(conn: sqlite3.Connection, rows: list[dict], snapshot_at: str) -> int:
    snapshot_date = snapshot_at[:10]
    conn.executemany(
        f"""
        INSERT INTO {SNAPSHOT_TABLE} (
            snapshot_at, snapshot_date, signal_key, signal_type, subject_name, ts_code, direction,
            signal_strength, confidence, evidence_count, news_count, stock_news_count, chatroom_count,
            signal_status, latest_signal_date, evidence_json, source_summary_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(snapshot_at, signal_key) DO NOTHING
        """,
        [
            (
                snapshot_at,
                snapshot_date,
                row["signal_key"],
                row["signal_type"],
                row["subject_name"],
                row["ts_code"] or None,
                row["direction"],
                row["signal_strength"],
                row["confidence"],
                row["evidence_count"],
                row["news_count"],
                row["stock_news_count"],
                row["chatroom_count"],
                row["signal_status"],
                row["latest_signal_date"],
                row["evidence_json"],
                row["source_summary_json"],
                snapshot_at,
            )
            for row in rows
        ],
    )
    conn.commit()
    return len(rows)


def event_table_is_empty(conn: sqlite3.Connection) -> bool:
    count = conn.execute(f"SELECT COUNT(*) FROM {EVENT_TABLE}").fetchone()[0]
    return int(count or 0) <= 0


def determine_driver_type(old_row: dict | None, new_row: dict | None) -> str:
    old_sources = parse_json_text((old_row or {}).get("source_summary_json") or "") or {}
    new_sources = parse_json_text((new_row or {}).get("source_summary_json") or "") or {}
    deltas = {}
    for key in {"news", "stock_news", "chatroom"}:
        deltas[key] = int(new_sources.get(key, 0)) - int(old_sources.get(key, 0))
    winner = max(deltas.items(), key=lambda kv: kv[1])[0] if deltas else "mixed"
    if deltas.get(winner, 0) <= 0:
        return "mixed"
    return winner


def determine_event_type(old_row: dict | None, new_row: dict | None) -> str | None:
    if old_row is None and new_row is not None:
        return "new_signal"
    if old_row is not None and new_row is None:
        return "expire"
    if old_row is None or new_row is None:
        return None
    old_dir = str(old_row.get("direction") or "")
    new_dir = str(new_row.get("direction") or "")
    old_strength = float(old_row.get("signal_strength") or 0.0)
    new_strength = float(new_row.get("signal_strength") or 0.0)
    delta = new_strength - old_strength
    if old_dir == "看多" and new_dir == "看空":
        return "falsify"
    if old_dir == "看空" and new_dir == "看多":
        return "revive"
    if old_dir and new_dir and old_dir != new_dir and "中性" not in {old_dir, new_dir}:
        return "flip"
    if delta >= 5:
        return "strengthen"
    if delta <= -5:
        return "weaken"
    if old_row.get("signal_status") != new_row.get("signal_status"):
        return "status_change"
    return None


def event_level(delta_strength: float, delta_confidence: float, event_type: str) -> str:
    score = abs(delta_strength) + abs(delta_confidence) * 0.2
    if event_type in {"flip", "falsify", "revive"}:
        return "关键"
    if score >= 15:
        return "显著"
    if score >= 6:
        return "中等"
    return "轻微"


def event_summary(event_type: str, old_row: dict | None, new_row: dict | None) -> str:
    subject = (new_row or old_row or {}).get("subject_name") or "未知信号"
    sentiment = row_sentiment_snapshot(new_row or old_row)
    sentiment_text = ""
    if sentiment.get("label"):
        sentiment_text = f" 市场情绪{sentiment['label']}"
        if abs(float(sentiment.get("score") or 0)) > 0:
            sentiment_text += f"({sentiment['score']:+.0f})"
        sentiment_text += "。"
    if event_type == "new_signal":
        return f"{subject} 首次进入信号池。{sentiment_text}".strip()
    if event_type == "expire":
        return f"{subject} 本轮未再出现，信号暂时失效。{sentiment_text}".strip()
    if event_type == "strengthen":
        return f"{subject} 信号强度明显增强。{sentiment_text}".strip()
    if event_type == "weaken":
        return f"{subject} 信号强度明显减弱。{sentiment_text}".strip()
    if event_type == "flip":
        return f"{subject} 信号方向发生反转。{sentiment_text}".strip()
    if event_type == "falsify":
        return f"{subject} 原有判断被反向信号挑战。{sentiment_text}".strip()
    if event_type == "revive":
        return f"{subject} 反向信号后再次恢复。{sentiment_text}".strip()
    return f"{subject} 信号状态发生变化。{sentiment_text}".strip()


def generate_events(conn: sqlite3.Connection, previous: dict[str, dict], current_rows: list[dict], snapshot_at: str) -> int:
    current = {row["signal_key"]: row for row in current_rows}
    signal_keys = sorted(set(previous.keys()) | set(current.keys()))
    events = []
    for signal_key in signal_keys:
        old_row = previous.get(signal_key)
        new_row = current.get(signal_key)
        e_type = determine_event_type(old_row, new_row)
        if not e_type:
            continue
        old_strength = float((old_row or {}).get("signal_strength") or 0.0)
        new_strength = float((new_row or {}).get("signal_strength") or 0.0)
        old_conf = float((old_row or {}).get("confidence") or 0.0)
        new_conf = float((new_row or {}).get("confidence") or 0.0)
        delta_strength = round(new_strength - old_strength, 2)
        delta_conf = round(new_conf - old_conf, 2)
        after = new_row or old_row or {}
        evidence_list = parse_json_text((after.get("evidence_json") or "")) or []
        driver_evidence = evidence_list[0] if isinstance(evidence_list, list) and evidence_list else {}
        events.append(
            (
                signal_key,
                snapshot_at,
                snapshot_at[:10],
                e_type,
                (old_row or {}).get("direction") or None,
                (new_row or {}).get("direction") or None,
                old_strength,
                new_strength,
                delta_strength,
                old_conf,
                new_conf,
                delta_conf,
                event_level(delta_strength, delta_conf, e_type),
                determine_driver_type(old_row, new_row),
                driver_evidence.get("source") if isinstance(driver_evidence, dict) else None,
                None,
                driver_evidence.get("title") if isinstance(driver_evidence, dict) else None,
                after.get("signal_status") or None,
                event_summary(e_type, old_row, new_row),
                json.dumps(evidence_list[:5], ensure_ascii=False),
                json.dumps(old_row or {}, ensure_ascii=False),
                json.dumps(new_row or {}, ensure_ascii=False),
                snapshot_at,
            )
        )
    if not events:
        return 0
    sql = f"""
    INSERT INTO {EVENT_TABLE} (
        signal_key, event_time, event_date, event_type, old_direction, new_direction,
        old_strength, new_strength, delta_strength, old_confidence, new_confidence, delta_confidence,
        event_level, driver_type, driver_source, driver_ref_id, driver_title, status_after_event,
        event_summary, evidence_json, snapshot_before_json, snapshot_after_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(signal_key, event_time, event_type) DO NOTHING
    """
    for event in events:
        conn.execute(sql, event)
    conn.commit()
    return len(events)


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    try:
        ensure_table(conn, args.target_table)
        ensure_quality_tables(conn)
        previous = {}
        bootstrap_events = False
        if not args.skip_history:
            ensure_snapshot_and_event_tables(conn)
            previous = load_previous_tracker(conn, args.target_table)
            bootstrap_events = event_table_is_empty(conn)
        rules = load_quality_rules(conn)
        stock_blocklist = load_signal_blocklist(conn, "stock")
        buckets: dict[str, dict] = {}
        merge_news_signals(conn, buckets, args.lookback_days, args.limit_news)
        merge_stock_news_signals(conn, buckets, args.lookback_days, args.limit_stock_news)
        merge_chatroom_signals(conn, buckets)
        merge_theme_mapped_stock_signals(conn, buckets, rules)
        rows = finalize_rows(buckets, args.min_strength, rules, stock_blocklist)
        snapshot_at = now_utc_str()
        event_count = 0
        if not args.skip_history:
            event_previous = {} if bootstrap_events else previous
            event_count = generate_events(conn, event_previous, rows, snapshot_at)
        affected = replace_rows(conn, args.target_table, rows)
        if not args.skip_history:
            snapshot_signal_rows(conn, rows, snapshot_at)
        publish_app_event(
            event="investment_signal_tracker_update",
            payload={
                "table": args.target_table,
                "rows": affected,
                "events": event_count,
                "lookback_days": args.lookback_days,
                "skip_history": bool(args.skip_history),
            },
            producer="build_investment_signal_tracker.py",
        )
        print(f"完成: rows={affected}, events={event_count}, table={args.target_table}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
