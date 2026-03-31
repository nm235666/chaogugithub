#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3
from llm_gateway import chat_completion_with_fallback, normalize_model_name

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"

THEME_LIKE_KEYWORDS = [
    "黄金", "原油", "能源", "军工", "航运", "AI", "算力", "科技", "机器人", "芯片",
    "半导体", "消费", "地产", "银行", "有色", "医药", "汽车", "新能源", "大盘", "指数",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="用大模型补齐群聊里的股票别名，并沉淀到别名字典")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--model", default="auto", help="模型名；默认 auto 自动路由并降级")
    parser.add_argument("--limit", type=int, default=20, help="每轮最多识别多少个未归一候选")
    parser.add_argument("--min-confidence", type=float, default=0.86, help="低于该置信度不入库")
    parser.add_argument("--retry", type=int, default=2, help="单条失败重试次数")
    parser.add_argument("--sleep", type=float, default=0.3, help="每条间隔秒数")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_alias_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_alias_dictionary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alias TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            stock_name TEXT,
            alias_type TEXT,
            confidence REAL DEFAULT 1.0,
            source TEXT,
            notes TEXT,
            used_count INTEGER DEFAULT 0,
            last_used_at TEXT,
            created_at TEXT,
            update_time TEXT,
            UNIQUE(alias)
        )
        """
    )
    conn.commit()


def candidate_is_theme_like(name: str) -> bool:
    text = str(name or "").strip()
    if not text:
        return True
    if text.endswith(("ETF", "指数", "板块", "概念", "主线")):
        return True
    return any(keyword in text for keyword in THEME_LIKE_KEYWORDS)


def fetch_unresolved_candidates(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(chatroom_stock_candidate_pool)").fetchall()}
    has_ts_code = "ts_code" in cols
    sql = f"""
    SELECT candidate_name, candidate_type, sample_reasons_json, mention_count, room_count, latest_analysis_date
           {", ts_code" if has_ts_code else ", '' AS ts_code"}
    FROM chatroom_stock_candidate_pool
    WHERE candidate_type IN ('股票', '标的')
      AND COALESCE(candidate_name, '') <> ''
      AND COALESCE({('ts_code' if has_ts_code else "''")}, '') = ''
      AND candidate_name NOT IN (SELECT alias FROM stock_alias_dictionary)
    ORDER BY room_count DESC, mention_count DESC, latest_analysis_date DESC, candidate_name
    LIMIT ?
    """
    return conn.execute(sql, (max(limit, 1),)).fetchall()


def build_prompt(alias_name: str, reasons: list[dict]) -> str:
    return (
        "你是一个A股股票简称/别名识别器。你的任务是判断一个群聊中出现的简称、昵称、缩写，是否明确对应某一只A股上市公司。\n\n"
        "严格要求：\n"
        "1. 只能识别 A 股上市公司，返回 ts_code 格式必须是 6 位代码 + .SZ/.SH/.BJ。\n"
        "2. 如果是行业、主题、ETF、宏观词、商品名、模糊简称、多义词，必须返回 skip。\n"
        "3. 只有在高把握时才返回 match，不能猜。\n"
        "4. 请结合提供的群聊理由判断，但不要被情绪词误导。\n\n"
        "输出只允许 JSON：\n"
        "{\n"
        '  "decision": "match 或 skip",\n'
        '  "alias": "原别名",\n'
        '  "stock_name": "股票名称",\n'
        '  "ts_code": "000001.SZ",\n'
        '  "confidence": 0.0,\n'
        '  "reason": "一句话原因"\n'
        "}\n\n"
        "示例：\n"
        '- alias="宁王" -> 宁德时代 300750.SZ\n'
        '- alias="茅台" -> 贵州茅台 600519.SH\n'
        '- alias="东土" -> 东土科技 300353.SZ\n'
        '- alias="黄金" -> skip\n'
        '- alias="AI" -> skip\n\n'
        f"待识别别名：{alias_name}\n"
        f"群聊上下文：{json.dumps(reasons[:5], ensure_ascii=False)}"
    )


def call_llm(model: str, prompt: str):
    return chat_completion_with_fallback(
        model=model,
        temperature=0.1,
        timeout_s=120,
        max_retries=3,
        messages=[
            {"role": "system", "content": "你是严谨的 A 股简称识别引擎。"},
            {"role": "user", "content": prompt},
        ],
    )


def parse_llm_output(raw: str) -> dict:
    text = str(raw or "").strip()
    try:
        obj = json.loads(text)
    except Exception:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if not match:
            raise ValueError("LLM 输出不是 JSON")
        obj = json.loads(match.group(0))
    return {
        "decision": str(obj.get("decision") or "").strip().lower(),
        "alias": str(obj.get("alias") or "").strip(),
        "stock_name": str(obj.get("stock_name") or "").strip(),
        "ts_code": str(obj.get("ts_code") or "").strip().upper(),
        "confidence": float(obj.get("confidence") or 0.0),
        "reason": str(obj.get("reason") or "").strip(),
    }


def upsert_alias(conn: sqlite3.Connection, *, alias: str, ts_code: str, stock_name: str, confidence: float, notes: str) -> None:
    now = now_utc_str()
    updated = conn.execute(
        """
        UPDATE stock_alias_dictionary
        SET ts_code = ?, stock_name = ?, alias_type = ?, confidence = ?, source = ?, notes = ?, update_time = ?
        WHERE alias = ?
        """,
        (ts_code, stock_name, "llm", confidence, "llm_resolve_stock_aliases.py", notes, now, alias),
    ).rowcount
    if not updated:
        conn.execute(
            """
            INSERT INTO stock_alias_dictionary (
                alias, ts_code, stock_name, alias_type, confidence, source, notes, created_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (alias, ts_code, stock_name, "llm", confidence, "llm_resolve_stock_aliases.py", notes, now, now),
        )
    conn.commit()


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    resolved = 0
    skipped = 0
    failed = 0
    try:
        ensure_alias_table(conn)
        candidates = fetch_unresolved_candidates(conn, args.limit)
        for row in candidates:
            alias_name = str(row["candidate_name"] or "").strip()
            if not alias_name or candidate_is_theme_like(alias_name):
                skipped += 1
                continue
            reasons = []
            try:
                reasons = json.loads(row["sample_reasons_json"] or "[]")
            except Exception:
                reasons = []

            prompt = build_prompt(alias_name, reasons if isinstance(reasons, list) else [])
            attempt = 0
            while True:
                attempt += 1
                try:
                    raw = call_llm(args.model, prompt)
                    parsed = parse_llm_output(raw.text)
                    if (
                        parsed["decision"] == "match"
                        and re.fullmatch(r"\d{6}\.(SZ|SH|BJ)", parsed["ts_code"])
                        and parsed["stock_name"]
                        and parsed["confidence"] >= float(args.min_confidence)
                    ):
                        upsert_alias(
                            conn,
                            alias=alias_name,
                            ts_code=parsed["ts_code"],
                            stock_name=parsed["stock_name"],
                            confidence=parsed["confidence"],
                            notes=parsed["reason"],
                        )
                        resolved += 1
                        print(f"{alias_name}: match -> {parsed['stock_name']} {parsed['ts_code']} conf={parsed['confidence']:.2f} actual_model={raw.used_model}")
                    else:
                        skipped += 1
                        print(f"{alias_name}: skip")
                    break
                except Exception as exc:
                    if attempt > max(args.retry, 0):
                        failed += 1
                        print(f"{alias_name}: failed -> {exc}")
                        break
                    time.sleep(1.0)
            if args.sleep > 0:
                time.sleep(args.sleep)
    finally:
        conn.close()
    print(f"resolved={resolved} skipped={skipped} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
