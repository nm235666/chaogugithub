#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import db_compat as sqlite3
from realtime_streams import publish_app_event

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"
DEFAULT_TRACKER_TABLE = "theme_hotspot_tracker"
DEFAULT_SNAPSHOT_TABLE = "theme_daily_snapshots"
DEFAULT_EVIDENCE_TABLE = "theme_evidence_items"
THEME_DEFINITION_TABLE = "theme_definitions"
THEME_ALIAS_TABLE = "theme_aliases"


DEFAULT_THEMES: list[dict] = [
    {"theme_name": "黄金", "theme_group": "大宗", "description": "黄金与避险链条", "priority": 100, "aliases": ["黄金", "金价", "贵金属", "避险资产"]},
    {"theme_name": "原油", "theme_group": "大宗", "description": "原油与油价链条", "priority": 100, "aliases": ["原油", "油价", "布油", "WTI"]},
    {"theme_name": "能源", "theme_group": "行业", "description": "传统能源与能源安全", "priority": 95, "aliases": ["能源", "油气", "天然气", "煤炭", "电力能源"]},
    {"theme_name": "航运", "theme_group": "行业", "description": "航运、港口与运价", "priority": 90, "aliases": ["航运", "海运", "港口", "集运", "油运"]},
    {"theme_name": "军工", "theme_group": "行业", "description": "军工与国防安全", "priority": 95, "aliases": ["军工", "国防", "航空航天", "兵器"]},
    {"theme_name": "AI", "theme_group": "科技", "description": "人工智能与算力", "priority": 100, "aliases": ["AI", "人工智能", "大模型", "算力", "AIGC", "智算"]},
    {"theme_name": "半导体", "theme_group": "科技", "description": "芯片、半导体设备与材料", "priority": 95, "aliases": ["半导体", "芯片", "晶圆", "先进制程", "存储"]},
    {"theme_name": "科技", "theme_group": "科技", "description": "泛科技成长", "priority": 85, "aliases": ["科技", "互联网", "软件", "数字经济"]},
    {"theme_name": "消费", "theme_group": "行业", "description": "消费与内需", "priority": 85, "aliases": ["消费", "内需", "零售", "白酒", "家电"]},
    {"theme_name": "医药", "theme_group": "行业", "description": "医药医疗健康", "priority": 85, "aliases": ["医药", "医疗", "创新药", "器械", "生物科技"]},
    {"theme_name": "地产链", "theme_group": "行业", "description": "地产、建材、家居链条", "priority": 90, "aliases": ["地产链", "房地产", "地产", "建材", "家居", "物业"]},
    {"theme_name": "银行", "theme_group": "金融", "description": "银行板块", "priority": 85, "aliases": ["银行", "大行", "城商行", "农商行"]},
    {"theme_name": "券商", "theme_group": "金融", "description": "券商与资本市场", "priority": 85, "aliases": ["券商", "证券", "投行"]},
    {"theme_name": "保险", "theme_group": "金融", "description": "保险板块", "priority": 80, "aliases": ["保险", "寿险", "财险"]},
    {"theme_name": "新能源", "theme_group": "行业", "description": "新能源主链", "priority": 95, "aliases": ["新能源", "锂电", "电池", "新能车", "新能源车"]},
    {"theme_name": "储能", "theme_group": "行业", "description": "储能链条", "priority": 90, "aliases": ["储能", "储能电池", "储能系统"]},
    {"theme_name": "光伏", "theme_group": "行业", "description": "光伏链条", "priority": 90, "aliases": ["光伏", "组件", "硅料", "逆变器"]},
    {"theme_name": "风电", "theme_group": "行业", "description": "风电链条", "priority": 85, "aliases": ["风电", "风能", "海风", "陆风"]},
    {"theme_name": "有色", "theme_group": "大宗", "description": "有色金属链条", "priority": 85, "aliases": ["有色", "有色金属", "工业金属"]},
    {"theme_name": "铜", "theme_group": "大宗", "description": "铜价与铜产业链", "priority": 80, "aliases": ["铜", "铜价", "电解铜"]},
    {"theme_name": "出海", "theme_group": "主题", "description": "中资企业出海", "priority": 80, "aliases": ["出海", "海外扩张", "海外市场", "海外产能"]},
    {"theme_name": "汇率", "theme_group": "宏观", "description": "汇率与外汇", "priority": 100, "aliases": ["汇率", "外汇", "人民币汇率", "美元指数", "DXY", "USDCNY", "USDJPY", "EURUSD"]},
    {"theme_name": "利率", "theme_group": "宏观", "description": "利率、收益率曲线与货币定价", "priority": 95, "aliases": ["利率", "收益率", "国债收益率", "降息", "加息"]},
    {"theme_name": "通胀", "theme_group": "宏观", "description": "通胀与物价", "priority": 90, "aliases": ["通胀", "CPI", "PPI", "物价"]},
    {"theme_name": "流动性", "theme_group": "宏观", "description": "流动性与信用扩张", "priority": 90, "aliases": ["流动性", "信用扩张", "信贷", "社融"]},
    {"theme_name": "风险偏好", "theme_group": "宏观", "description": "市场风险偏好与避险情绪", "priority": 100, "aliases": ["风险偏好", "避险情绪", "风险厌恶", "risk-on", "risk-off"]},
    {"theme_name": "A股", "theme_group": "市场", "description": "A股市场", "priority": 75, "aliases": ["A股", "沪深股市", "内地股市"]},
    {"theme_name": "港股", "theme_group": "市场", "description": "港股市场", "priority": 75, "aliases": ["港股", "香港股市", "恒生"]},
    {"theme_name": "美股", "theme_group": "市场", "description": "美股市场", "priority": 75, "aliases": ["美股", "美股市场", "纳指", "标普", "道指"]},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="统一热点主题引擎：归并国际/国内/个股新闻与群聊到主题")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--lookback-days", type=int, default=7, help="主题聚合回看天数")
    parser.add_argument("--limit-news", type=int, default=1000, help="纳入聚合的新闻条数上限")
    parser.add_argument("--limit-stock-news", type=int, default=1500, help="纳入聚合的个股新闻条数上限")
    parser.add_argument("--limit-chatroom", type=int, default=1000, help="纳入聚合的群聊候选条数上限")
    parser.add_argument("--target-table", default=DEFAULT_TRACKER_TABLE, help="主题当前表")
    parser.add_argument("--snapshot-table", default=DEFAULT_SNAPSHOT_TABLE, help="主题快照表")
    parser.add_argument("--evidence-table", default=DEFAULT_EVIDENCE_TABLE, help="主题证据表")
    parser.add_argument("--min-strength", type=float, default=6.0, help="最低主题热度")
    parser.add_argument("--skip-snapshot", action="store_true", help="只刷新当前表，不写每日快照")
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


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def canonicalize(text: str) -> str:
    value = normalize_text(text).lower()
    value = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def parse_json_text(raw: str):
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def ensure_tables(conn: sqlite3.Connection, tracker_table: str, snapshot_table: str, evidence_table: str) -> None:
    if not table_exists(conn, THEME_DEFINITION_TABLE):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {THEME_DEFINITION_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                theme_name TEXT NOT NULL,
                theme_group TEXT,
                description TEXT,
                keywords_json TEXT,
                priority INTEGER DEFAULT 50,
                enabled INTEGER DEFAULT 1,
                created_at TEXT,
                update_time TEXT,
                UNIQUE(theme_name)
            )
            """
        )
    if not table_exists(conn, THEME_ALIAS_TABLE):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {THEME_ALIAS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                theme_name TEXT NOT NULL,
                alias TEXT NOT NULL,
                alias_type TEXT DEFAULT 'exact',
                confidence REAL DEFAULT 1.0,
                source TEXT,
                created_at TEXT,
                update_time TEXT,
                UNIQUE(theme_name, alias),
                FOREIGN KEY (theme_name) REFERENCES {THEME_DEFINITION_TABLE}(theme_name)
            )
            """
        )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{THEME_ALIAS_TABLE}_alias ON {THEME_ALIAS_TABLE}(alias)")
    if not table_exists(conn, evidence_table):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {evidence_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme_name TEXT NOT NULL,
            theme_group TEXT,
            source_type TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_id TEXT NOT NULL,
            source_name TEXT,
            evidence_time TEXT,
            evidence_date TEXT,
            original_term TEXT NOT NULL DEFAULT '',
            title TEXT,
            summary TEXT,
            direction TEXT NOT NULL DEFAULT '',
            weight REAL DEFAULT 1.0,
            ts_code TEXT,
            stock_name TEXT,
            sentiment_label TEXT,
            sentiment_score REAL,
            meta_json TEXT,
            created_at TEXT,
            UNIQUE(theme_name, source_table, source_id, original_term, direction)
        )
            """
        )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{evidence_table}_theme_time ON {evidence_table}(theme_name, evidence_time DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{evidence_table}_source ON {evidence_table}(source_table, source_id)"
    )
    if not table_exists(conn, tracker_table):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {tracker_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme_name TEXT NOT NULL,
            theme_group TEXT,
            direction TEXT,
            theme_strength REAL,
            confidence REAL,
            evidence_count INTEGER DEFAULT 0,
            intl_news_count INTEGER DEFAULT 0,
            domestic_news_count INTEGER DEFAULT 0,
            stock_news_count INTEGER DEFAULT 0,
            chatroom_count INTEGER DEFAULT 0,
            stock_link_count INTEGER DEFAULT 0,
            latest_evidence_time TEXT,
            heat_level TEXT,
            top_terms_json TEXT,
            top_stocks_json TEXT,
            source_summary_json TEXT,
            evidence_json TEXT,
            created_at TEXT,
            update_time TEXT,
            UNIQUE(theme_name)
        )
            """
        )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{tracker_table}_strength ON {tracker_table}(theme_strength DESC)")
    if not table_exists(conn, snapshot_table):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {snapshot_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL,
            lookback_days INTEGER NOT NULL,
            theme_name TEXT NOT NULL,
            theme_group TEXT,
            direction TEXT,
            theme_strength REAL,
            confidence REAL,
            evidence_count INTEGER DEFAULT 0,
            intl_news_count INTEGER DEFAULT 0,
            domestic_news_count INTEGER DEFAULT 0,
            stock_news_count INTEGER DEFAULT 0,
            chatroom_count INTEGER DEFAULT 0,
            stock_link_count INTEGER DEFAULT 0,
            latest_evidence_time TEXT,
            heat_level TEXT,
            top_terms_json TEXT,
            top_stocks_json TEXT,
            source_summary_json TEXT,
            evidence_json TEXT,
            created_at TEXT,
            UNIQUE(snapshot_date, lookback_days, theme_name)
        )
            """
        )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{snapshot_table}_date ON {snapshot_table}(snapshot_date, lookback_days)"
    )
    conn.commit()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()[0]
    )


def seed_defaults(conn: sqlite3.Connection) -> None:
    now = now_utc_str()
    existing = conn.execute(f"SELECT COUNT(*) FROM {THEME_DEFINITION_TABLE}").fetchone()[0]
    if int(existing or 0) > 0:
        return
    for row in DEFAULT_THEMES:
        conn.execute(
            f"""
            INSERT INTO {THEME_DEFINITION_TABLE} (
                theme_name, theme_group, description, keywords_json, priority, enabled, created_at, update_time
            ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                row["theme_name"],
                row["theme_group"],
                row["description"],
                json.dumps(row["aliases"], ensure_ascii=False),
                int(row["priority"]),
                now,
                now,
            ),
        )
        aliases = [row["theme_name"], *list(row["aliases"] or [])]
        inserted: set[str] = set()
        for alias in aliases:
            alias = normalize_text(alias)
            if not alias or alias in inserted:
                continue
            inserted.add(alias)
            conn.execute(
                f"""
                INSERT INTO {THEME_ALIAS_TABLE} (
                    theme_name, alias, alias_type, confidence, source, created_at, update_time
                ) VALUES (?, ?, 'exact', 1.0, 'default_seed', ?, ?)
                """,
                (row["theme_name"], alias, now, now),
            )
    conn.commit()


def load_theme_index(conn: sqlite3.Connection) -> tuple[dict[str, dict], list[dict]]:
    definitions = {}
    for row in conn.execute(
        f"""
        SELECT theme_name, theme_group, description, COALESCE(priority, 50), keywords_json
        FROM {THEME_DEFINITION_TABLE}
        WHERE COALESCE(enabled, 1) = 1
        ORDER BY COALESCE(priority, 50) DESC, theme_name
        """
    ).fetchall():
        theme_name = normalize_text(row[0])
        definitions[theme_name] = {
            "theme_name": theme_name,
            "theme_group": normalize_text(row[1]),
            "description": normalize_text(row[2]),
            "priority": int(row[3] or 50),
            "keywords": parse_json_text(row[4]) or [],
        }
    aliases = []
    for row in conn.execute(
        f"""
        SELECT theme_name, alias, alias_type, COALESCE(confidence, 1.0)
        FROM {THEME_ALIAS_TABLE}
        ORDER BY LENGTH(COALESCE(alias, '')) DESC, COALESCE(confidence, 1.0) DESC
        """
    ).fetchall():
        theme_name = normalize_text(row[0])
        alias = normalize_text(row[1])
        if not theme_name or not alias or theme_name not in definitions:
            continue
        aliases.append(
            {
                "theme_name": theme_name,
                "alias": alias,
                "alias_norm": canonicalize(alias),
                "alias_type": normalize_text(row[2]) or "exact",
                "confidence": float(row[3] or 1.0),
                "theme_group": definitions[theme_name]["theme_group"],
                "priority": definitions[theme_name]["priority"],
            }
        )
    return definitions, aliases


def resolve_themes(texts: list[str], aliases: list[dict]) -> list[dict]:
    joined = " ".join([normalize_text(x) for x in texts if normalize_text(x)])
    joined_norm = canonicalize(joined)
    hits: dict[str, dict] = {}
    if not joined_norm:
        return []
    for item in aliases:
        alias_norm = item["alias_norm"]
        if not alias_norm:
            continue
        exact_hit = any(canonicalize(text) == alias_norm for text in texts if normalize_text(text))
        contains_hit = alias_norm in joined_norm
        if not (exact_hit or contains_hit):
            continue
        theme_name = item["theme_name"]
        prev = hits.get(theme_name)
        score = item["confidence"] + (0.35 if exact_hit else 0.0) + (item["priority"] / 1000.0)
        if (not prev) or score > prev["score"]:
            hits[theme_name] = {
                "theme_name": theme_name,
                "theme_group": item["theme_group"],
                "matched_alias": item["alias"],
                "score": score,
            }
    return sorted(hits.values(), key=lambda x: (-x["score"], x["theme_name"]))


def iter_structured_impacts(raw_impacts) -> list[dict]:
    impacts = parse_json_text(raw_impacts) or []
    if isinstance(impacts, list):
        out = []
        for item in impacts:
            if not isinstance(item, dict):
                continue
            name = normalize_text(
                item.get("item")
                or item.get("asset")
                or item.get("industry")
                or item.get("market")
                or item.get("macro")
                or item.get("sector")
                or item.get("name")
            )
            if not name:
                continue
            out.append({"subject_name": name, "direction": normalize_text(item.get("direction")), "group": normalize_text(item.get("group"))})
        return out
    if isinstance(impacts, dict):
        out = []
        for key, values in impacts.items():
            if not isinstance(values, list):
                continue
            for item in values:
                if not isinstance(item, dict):
                    continue
                name = normalize_text(
                    item.get("item")
                    or item.get("asset")
                    or item.get("industry")
                    or item.get("market")
                    or item.get("macro")
                    or item.get("sector")
                    or item.get("name")
                )
                if not name:
                    continue
                out.append({"subject_name": name, "direction": normalize_text(item.get("direction")), "group": normalize_text(key)})
        return out
    return []


def direction_bucket(direction: str) -> str:
    value = normalize_text(direction)
    if value in {"看多", "利多", "偏多", "弱看多", "中性偏多"}:
        return "bullish"
    if value in {"看空", "利空", "偏空", "弱看空", "中性偏空"}:
        return "bearish"
    return "neutral"


def score_from_importance(level: str) -> float:
    return {"极高": 4.5, "高": 3.5, "中": 2.5, "低": 1.5, "极低": 0.8}.get(normalize_text(level), 1.0)


def score_from_sentiment(score) -> float:
    try:
        value = abs(float(score or 0.0))
    except Exception:
        value = 0.0
    return 1.0 + min(value, 100.0) / 200.0


def classify_news_scope(source: str) -> str:
    return "domestic_news" if normalize_text(source).lower().startswith("cn_") else "intl_news"


def stock_names_by_codes(conn: sqlite3.Connection, ts_codes: list[str]) -> dict[str, str]:
    if not ts_codes:
        return {}
    placeholders = ",".join(["?"] * len(ts_codes))
    rows = conn.execute(
        f"SELECT ts_code, name FROM stock_codes WHERE ts_code IN ({placeholders})",
        ts_codes,
    ).fetchall()
    return {str(r[0]): normalize_text(r[1]) for r in rows}


def empty_bucket(theme_name: str, theme_group: str) -> dict:
    return {
        "theme_name": theme_name,
        "theme_group": theme_group,
        "bullish": 0.0,
        "bearish": 0.0,
        "neutral": 0.0,
        "evidence_count": 0,
        "latest_evidence_time": "",
        "source_counts": {"intl_news": 0, "domestic_news": 0, "stock_news": 0, "chatroom": 0},
        "term_weights": {},
        "stock_weights": {},
        "evidence": [],
    }


def register_bucket_evidence(bucket: dict, *, source_type: str, direction: str, weight: float, evidence_time: str, term: str, stocks: list[dict], evidence: dict) -> None:
    bucket[direction_bucket(direction)] += float(weight or 0.0)
    bucket["evidence_count"] += 1
    bucket["source_counts"][source_type] = int(bucket["source_counts"].get(source_type, 0)) + 1
    if evidence_time and evidence_time > bucket["latest_evidence_time"]:
        bucket["latest_evidence_time"] = evidence_time
    if term:
        bucket["term_weights"][term] = round(float(bucket["term_weights"].get(term, 0.0)) + float(weight or 0.0), 4)
    for stock in stocks[:5]:
        ts_code = normalize_text(stock.get("ts_code"))
        stock_name = normalize_text(stock.get("stock_name"))
        key = ts_code or stock_name
        if not key:
            continue
        payload = bucket["stock_weights"].setdefault(
            key,
            {"ts_code": ts_code, "stock_name": stock_name, "weight": 0.0},
        )
        payload["weight"] = round(float(payload["weight"]) + float(weight or 0.0), 4)
    if len(bucket["evidence"]) < 8:
        bucket["evidence"].append(evidence)


def replace_tracker_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict]) -> int:
    now = now_utc_str()
    conn.execute(f"DELETE FROM {table_name}")
    for row in rows:
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                theme_name, theme_group, direction, theme_strength, confidence, evidence_count,
                intl_news_count, domestic_news_count, stock_news_count, chatroom_count, stock_link_count,
                latest_evidence_time, heat_level, top_terms_json, top_stocks_json, source_summary_json,
                evidence_json, created_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["theme_name"],
                row["theme_group"],
                row["direction"],
                row["theme_strength"],
                row["confidence"],
                row["evidence_count"],
                row["intl_news_count"],
                row["domestic_news_count"],
                row["stock_news_count"],
                row["chatroom_count"],
                row["stock_link_count"],
                row["latest_evidence_time"],
                row["heat_level"],
                row["top_terms_json"],
                row["top_stocks_json"],
                row["source_summary_json"],
                row["evidence_json"],
                now,
                now,
            ),
        )
    conn.commit()
    return len(rows)


def snapshot_rows(conn: sqlite3.Connection, table_name: str, rows: list[dict], lookback_days: int) -> int:
    snapshot_date = today_utc_str()
    now = now_utc_str()
    for row in rows:
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                snapshot_date, lookback_days, theme_name, theme_group, direction, theme_strength, confidence,
                evidence_count, intl_news_count, domestic_news_count, stock_news_count, chatroom_count,
                stock_link_count, latest_evidence_time, heat_level, top_terms_json, top_stocks_json,
                source_summary_json, evidence_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_date, lookback_days, theme_name) DO UPDATE SET
                direction=excluded.direction,
                theme_strength=excluded.theme_strength,
                confidence=excluded.confidence,
                evidence_count=excluded.evidence_count,
                intl_news_count=excluded.intl_news_count,
                domestic_news_count=excluded.domestic_news_count,
                stock_news_count=excluded.stock_news_count,
                chatroom_count=excluded.chatroom_count,
                stock_link_count=excluded.stock_link_count,
                latest_evidence_time=excluded.latest_evidence_time,
                heat_level=excluded.heat_level,
                top_terms_json=excluded.top_terms_json,
                top_stocks_json=excluded.top_stocks_json,
                source_summary_json=excluded.source_summary_json,
                evidence_json=excluded.evidence_json,
                created_at=excluded.created_at
            """,
            (
                snapshot_date,
                int(lookback_days),
                row["theme_name"],
                row["theme_group"],
                row["direction"],
                row["theme_strength"],
                row["confidence"],
                row["evidence_count"],
                row["intl_news_count"],
                row["domestic_news_count"],
                row["stock_news_count"],
                row["chatroom_count"],
                row["stock_link_count"],
                row["latest_evidence_time"],
                row["heat_level"],
                row["top_terms_json"],
                row["top_stocks_json"],
                row["source_summary_json"],
                row["evidence_json"],
                now,
            ),
        )
    conn.commit()
    return len(rows)


def replace_evidence_rows(conn: sqlite3.Connection, table_name: str, items: list[dict], lookback_days: int) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max(lookback_days + 2, 3))).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(f"DELETE FROM {table_name} WHERE COALESCE(evidence_time, '') >= ?", (cutoff,))
    now = now_utc_str()
    for item in items:
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                theme_name, theme_group, source_type, source_table, source_id, source_name,
                evidence_time, evidence_date, original_term, title, summary, direction, weight,
                ts_code, stock_name, sentiment_label, sentiment_score, meta_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(theme_name, source_table, source_id, original_term, direction) DO NOTHING
            """,
            (
                item["theme_name"],
                item["theme_group"],
                item["source_type"],
                item["source_table"],
                item["source_id"],
                item["source_name"],
                item["evidence_time"],
                item["evidence_date"],
                item["original_term"],
                item["title"],
                item["summary"],
                item["direction"],
                item["weight"],
                item["ts_code"],
                item["stock_name"],
                item["sentiment_label"],
                item["sentiment_score"],
                item["meta_json"],
                now,
            ),
        )
    conn.commit()
    return len(items)


def merge_news(conn: sqlite3.Connection, aliases: list[dict], lookback_days: int, limit_news: int, buckets: dict[str, dict], evidence_rows: list[dict]) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(news_feed_items)").fetchall()}
    if not table_exists(conn, "news_feed_items"):
        return
    select_codes = ", related_ts_codes_json" if "related_ts_codes_json" in cols else ", '' AS related_ts_codes_json"
    select_names = ", related_stock_names_json" if "related_stock_names_json" in cols else ", '' AS related_stock_names_json"
    select_sent_score = ", llm_sentiment_score" if "llm_sentiment_score" in cols else ", 0 AS llm_sentiment_score"
    select_sent_label = ", llm_sentiment_label" if "llm_sentiment_label" in cols else ", '' AS llm_sentiment_label"
    rows = conn.execute(
        f"""
        SELECT id, source, title, summary, pub_date, llm_finance_importance, llm_impacts_json
               {select_codes}
               {select_names}
               {select_sent_score}
               {select_sent_label}
        FROM news_feed_items
        WHERE COALESCE(pub_date, '') >= ?
        ORDER BY COALESCE(pub_date, '') DESC, id DESC
        LIMIT ?
        """,
        (cutoff_news_time(lookback_days), int(limit_news)),
    ).fetchall()
    for row in rows:
        source_type = classify_news_scope(row[1])
        title = normalize_text(row[2])
        summary = normalize_text(row[3])
        impacts = iter_structured_impacts(row[6])
        base_weight = score_from_importance(row[5]) * score_from_sentiment(row[9])
        code_list = parse_json_text(row[7]) or []
        if not isinstance(code_list, list):
            code_list = []
        name_list = parse_json_text(row[8]) or []
        stock_name_map = {}
        if isinstance(name_list, list):
            for item in name_list:
                if isinstance(item, dict):
                    code = normalize_text(item.get("ts_code"))
                    if code:
                        stock_name_map[code] = normalize_text(item.get("name"))
        db_name_map = stock_names_by_codes(conn, [normalize_text(x).upper() for x in code_list if normalize_text(x)])
        stocks = []
        for code in code_list[:5]:
            ts_code = normalize_text(code).upper()
            if not ts_code:
                continue
            stocks.append({"ts_code": ts_code, "stock_name": stock_name_map.get(ts_code) or db_name_map.get(ts_code) or ""})
        matched_terms: set[tuple[str, str]] = set()
        all_hits = []
        for item in impacts[:12]:
            term = normalize_text(item.get("subject_name"))
            if not term:
                continue
            for hit in resolve_themes([term], aliases):
                all_hits.append((hit, term, normalize_text(item.get("direction")) or "中性"))
        for hit in resolve_themes([title, summary], aliases)[:6]:
            all_hits.append((hit, hit.get("matched_alias") or "", normalize_text(row[10]) or "中性"))
        for hit, original_term, direction in all_hits:
            key = (hit["theme_name"], original_term)
            if key in matched_terms:
                continue
            matched_terms.add(key)
            bucket = buckets.setdefault(hit["theme_name"], empty_bucket(hit["theme_name"], hit["theme_group"]))
            weight = round(base_weight * max(hit.get("score", 1.0), 0.8), 3)
            evidence = {
                "source": row[1],
                "title": title,
                "date": row[4],
                "direction": direction,
                "original_term": original_term,
                "sentiment_label": normalize_text(row[10]),
                "sentiment_score": row[9],
            }
            register_bucket_evidence(
                bucket,
                source_type=source_type,
                direction=direction,
                weight=weight,
                evidence_time=normalize_text(row[4]),
                term=original_term or hit["theme_name"],
                stocks=stocks,
                evidence=evidence,
            )
            evidence_rows.append(
                {
                    "theme_name": hit["theme_name"],
                    "theme_group": hit["theme_group"],
                    "source_type": source_type,
                    "source_table": "news_feed_items",
                    "source_id": str(row[0]),
                    "source_name": normalize_text(row[1]),
                    "evidence_time": normalize_text(row[4]),
                    "evidence_date": normalize_text(row[4])[:10],
                    "original_term": original_term or hit["theme_name"],
                    "title": title,
                    "summary": summary,
                    "direction": direction,
                    "weight": weight,
                    "ts_code": stocks[0]["ts_code"] if stocks else "",
                    "stock_name": stocks[0]["stock_name"] if stocks else "",
                    "sentiment_label": normalize_text(row[10]),
                    "sentiment_score": float(row[9] or 0.0),
                    "meta_json": json.dumps({"stocks": stocks[:5], "scope": source_type}, ensure_ascii=False),
                }
            )


def merge_stock_news(conn: sqlite3.Connection, aliases: list[dict], lookback_days: int, limit_stock_news: int, buckets: dict[str, dict], evidence_rows: list[dict]) -> None:
    if not table_exists(conn, "stock_news_items"):
        return
    rows = conn.execute(
        """
        SELECT id, ts_code, company_name, source, title, summary, pub_time,
               llm_finance_importance, llm_impacts_json,
               COALESCE(llm_sentiment_score, 0), COALESCE(llm_sentiment_label, '')
        FROM stock_news_items
        WHERE COALESCE(pub_time, '') >= ?
        ORDER BY COALESCE(pub_time, '') DESC, id DESC
        LIMIT ?
        """,
        (cutoff_stock_news_time(lookback_days), int(limit_stock_news)),
    ).fetchall()
    for row in rows:
        ts_code = normalize_text(row[1]).upper()
        company_name = normalize_text(row[2])
        title = normalize_text(row[4])
        summary = normalize_text(row[5])
        impacts = iter_structured_impacts(row[8])
        stocks = [{"ts_code": ts_code, "stock_name": company_name}] if ts_code or company_name else []
        base_weight = (score_from_importance(row[7]) + 0.6) * score_from_sentiment(row[9])
        matched_terms: set[tuple[str, str]] = set()
        all_hits = []
        for item in impacts[:10]:
            term = normalize_text(item.get("subject_name"))
            if not term:
                continue
            for hit in resolve_themes([term], aliases):
                all_hits.append((hit, term, normalize_text(item.get("direction")) or "中性"))
        for hit in resolve_themes([title, summary], aliases)[:6]:
            all_hits.append((hit, hit.get("matched_alias") or "", normalize_text(row[10]) or "中性"))
        for hit, original_term, direction in all_hits:
            key = (hit["theme_name"], original_term)
            if key in matched_terms:
                continue
            matched_terms.add(key)
            bucket = buckets.setdefault(hit["theme_name"], empty_bucket(hit["theme_name"], hit["theme_group"]))
            weight = round(base_weight * max(hit.get("score", 1.0), 0.8), 3)
            evidence = {
                "source": "stock_news",
                "title": title,
                "date": row[6],
                "direction": direction,
                "original_term": original_term,
                "ts_code": ts_code,
                "stock_name": company_name,
                "sentiment_label": normalize_text(row[10]),
                "sentiment_score": row[9],
            }
            register_bucket_evidence(
                bucket,
                source_type="stock_news",
                direction=direction,
                weight=weight,
                evidence_time=normalize_text(row[6]).replace(" ", "T") + ("Z" if "T" in normalize_text(row[6]) and not normalize_text(row[6]).endswith("Z") else ""),
                term=original_term or hit["theme_name"],
                stocks=stocks,
                evidence=evidence,
            )
            evidence_rows.append(
                {
                    "theme_name": hit["theme_name"],
                    "theme_group": hit["theme_group"],
                    "source_type": "stock_news",
                    "source_table": "stock_news_items",
                    "source_id": str(row[0]),
                    "source_name": normalize_text(row[3]) or "stock_news",
                    "evidence_time": normalize_text(row[6]).replace(" ", "T") + ("Z" if "T" in normalize_text(row[6]) and not normalize_text(row[6]).endswith("Z") else ""),
                    "evidence_date": normalize_text(row[6])[:10],
                    "original_term": original_term or hit["theme_name"],
                    "title": title,
                    "summary": summary,
                    "direction": direction,
                    "weight": weight,
                    "ts_code": ts_code,
                    "stock_name": company_name,
                    "sentiment_label": normalize_text(row[10]),
                    "sentiment_score": float(row[9] or 0.0),
                    "meta_json": json.dumps({"source": row[3]}, ensure_ascii=False),
                }
            )


def merge_chatroom(conn: sqlite3.Connection, aliases: list[dict], limit_chatroom: int, buckets: dict[str, dict], evidence_rows: list[dict]) -> None:
    if not table_exists(conn, "chatroom_stock_candidate_pool"):
        return
    rows = conn.execute(
        """
        SELECT id, candidate_name, candidate_type, dominant_bias, mention_count, room_count,
               latest_analysis_date, sample_reasons_json, ts_code
        FROM chatroom_stock_candidate_pool
        ORDER BY COALESCE(latest_analysis_date, '') DESC, COALESCE(room_count, 0) DESC, COALESCE(mention_count, 0) DESC
        LIMIT ?
        """,
        (int(limit_chatroom),),
    ).fetchall()
    for row in rows:
        name = normalize_text(row[1])
        reasons = parse_json_text(row[7]) or []
        reason_texts = []
        for item in reasons[:3]:
            if isinstance(item, dict):
                reason_texts.append(normalize_text(item.get("reason")))
        hits = resolve_themes([name, *reason_texts], aliases)[:5]
        if not hits:
            continue
        direction = normalize_text(row[3]) or "中性"
        weight = max(1.0, float(row[4] or 0) * 0.4 + float(row[5] or 0) * 0.8)
        ts_code = normalize_text(row[8]).upper()
        stocks = [{"ts_code": ts_code, "stock_name": name}] if ts_code else []
        evidence_time = normalize_text(row[6])
        if evidence_time and len(evidence_time) == 10:
            evidence_time = evidence_time + "T00:00:00Z"
        for hit in hits:
            bucket = buckets.setdefault(hit["theme_name"], empty_bucket(hit["theme_name"], hit["theme_group"]))
            evidence = {
                "source": "chatroom",
                "title": name,
                "date": row[6],
                "direction": direction,
                "reason": reason_texts[0] if reason_texts else "",
                "candidate_type": normalize_text(row[2]),
                "mention_count": int(row[4] or 0),
                "room_count": int(row[5] or 0),
            }
            register_bucket_evidence(
                bucket,
                source_type="chatroom",
                direction=direction,
                weight=round(weight * max(hit.get("score", 1.0), 0.8), 3),
                evidence_time=evidence_time,
                term=name,
                stocks=stocks,
                evidence=evidence,
            )
            evidence_rows.append(
                {
                    "theme_name": hit["theme_name"],
                    "theme_group": hit["theme_group"],
                    "source_type": "chatroom",
                    "source_table": "chatroom_stock_candidate_pool",
                    "source_id": str(row[0]),
                    "source_name": "chatroom_stock_candidate_pool",
                    "evidence_time": evidence_time,
                    "evidence_date": normalize_text(row[6])[:10],
                    "original_term": name,
                    "title": name,
                    "summary": reason_texts[0] if reason_texts else "",
                    "direction": direction,
                    "weight": round(weight * max(hit.get("score", 1.0), 0.8), 3),
                    "ts_code": ts_code,
                    "stock_name": name if ts_code else "",
                    "sentiment_label": "",
                    "sentiment_score": 0.0,
                    "meta_json": json.dumps({"candidate_type": normalize_text(row[2]), "mention_count": int(row[4] or 0), "room_count": int(row[5] or 0)}, ensure_ascii=False),
                }
            )


def finalize_rows(buckets: dict[str, dict], min_strength: float) -> list[dict]:
    rows = []
    for bucket in buckets.values():
        bullish = float(bucket["bullish"])
        bearish = float(bucket["bearish"])
        neutral = float(bucket["neutral"])
        strength = bullish + bearish + neutral * 0.2
        if strength < float(min_strength):
            continue
        total = bullish + bearish + neutral
        if bullish > bearish:
            direction = "看多"
            confidence = 0.0 if total <= 0 else bullish / total
        elif bearish > bullish:
            direction = "看空"
            confidence = 0.0 if total <= 0 else bearish / total
        else:
            direction = "中性"
            confidence = 0.0
        if strength >= 24:
            heat_level = "极高"
        elif strength >= 16:
            heat_level = "高"
        elif strength >= 10:
            heat_level = "中"
        else:
            heat_level = "低"
        top_terms = sorted(bucket["term_weights"].items(), key=lambda kv: (-float(kv[1]), kv[0]))[:8]
        top_stocks = sorted(bucket["stock_weights"].values(), key=lambda x: (-float(x["weight"]), x["ts_code"] or x["stock_name"]))[:8]
        rows.append(
            {
                "theme_name": bucket["theme_name"],
                "theme_group": bucket["theme_group"],
                "direction": direction,
                "theme_strength": round(strength, 2),
                "confidence": round(confidence * 100, 2),
                "evidence_count": int(bucket["evidence_count"]),
                "intl_news_count": int(bucket["source_counts"].get("intl_news", 0)),
                "domestic_news_count": int(bucket["source_counts"].get("domestic_news", 0)),
                "stock_news_count": int(bucket["source_counts"].get("stock_news", 0)),
                "chatroom_count": int(bucket["source_counts"].get("chatroom", 0)),
                "stock_link_count": len(top_stocks),
                "latest_evidence_time": bucket["latest_evidence_time"],
                "heat_level": heat_level,
                "top_terms_json": json.dumps([{"term": k, "weight": round(v, 2)} for k, v in top_terms], ensure_ascii=False),
                "top_stocks_json": json.dumps(top_stocks, ensure_ascii=False),
                "source_summary_json": json.dumps(bucket["source_counts"], ensure_ascii=False),
                "evidence_json": json.dumps(bucket["evidence"], ensure_ascii=False),
            }
        )
    rows.sort(key=lambda x: (-float(x["theme_strength"]), -float(x["confidence"]), x["theme_name"]))
    return rows


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    try:
        ensure_tables(conn, args.target_table, args.snapshot_table, args.evidence_table)
        seed_defaults(conn)
        _, aliases = load_theme_index(conn)
        buckets: dict[str, dict] = {}
        evidence_rows: list[dict] = []
        merge_news(conn, aliases, args.lookback_days, args.limit_news, buckets, evidence_rows)
        merge_stock_news(conn, aliases, args.lookback_days, args.limit_stock_news, buckets, evidence_rows)
        merge_chatroom(conn, aliases, args.limit_chatroom, buckets, evidence_rows)
        rows = finalize_rows(buckets, args.min_strength)
        evidence_count = replace_evidence_rows(conn, args.evidence_table, evidence_rows, args.lookback_days)
        tracker_count = replace_tracker_rows(conn, args.target_table, rows)
        if not args.skip_snapshot:
            snapshot_rows(conn, args.snapshot_table, rows, args.lookback_days)
        publish_app_event(
            event="theme_hotspot_engine_update",
            payload={
                "table": args.target_table,
                "rows": tracker_count,
                "evidence_rows": evidence_count,
                "lookback_days": int(args.lookback_days),
            },
            producer="build_theme_hotspot_engine.py",
        )
        print(f"完成: theme_rows={tracker_count}, evidence_rows={evidence_count}, lookback_days={args.lookback_days}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
