#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3
from llm_gateway import chat_completion_text, normalize_model_name, normalize_temperature_for_model, resolve_provider
from realtime_streams import publish_app_event

DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
DEEPSEEK_API_KEY = "sk-374806b2f1744b1aa84a6b27758b0bb6"

SENTIMENT_LABELS = {"偏多", "中性", "偏空"}


def parse_args():
    parser = argparse.ArgumentParser(description="为新闻和个股新闻补统一情绪层")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--target", default="all", choices=["all", "news", "stock_news"], help="目标表")
    parser.add_argument("--model", default="GPT-5.4", help="模型名")
    parser.add_argument("--base-url", default=DEEPSEEK_BASE_URL, help="LLM Base URL")
    parser.add_argument("--api-key", default=DEEPSEEK_API_KEY, help="LLM API Key")
    parser.add_argument("--temperature", type=float, default=0.1, help="采样温度")
    parser.add_argument("--limit", type=int, default=30, help="每类表最多处理条数")
    parser.add_argument("--force", action="store_true", help="强制重评")
    parser.add_argument("--retry", type=int, default=1, help="失败重试次数")
    parser.add_argument("--sleep", type=float, default=0.1, help="每条间隔秒数")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_columns(conn: sqlite3.Connection, table_name: str):
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    need = [
        ("llm_sentiment_score", "REAL"),
        ("llm_sentiment_label", "TEXT"),
        ("llm_sentiment_reason", "TEXT"),
        ("llm_sentiment_confidence", "REAL"),
        ("llm_sentiment_model", "TEXT"),
        ("llm_sentiment_scored_at", "TEXT"),
    ]
    for name, typ in need:
        if name not in cols:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {name} {typ}")
    conn.commit()


def fetch_rows(conn: sqlite3.Connection, table_name: str, limit: int, force: bool):
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    base_cols = ["id", "title", "summary"]
    if "source" in cols:
        base_cols.append("source")
    if "pub_date" in cols:
        base_cols.append("pub_date")
    if "pub_time" in cols:
        base_cols.append("pub_time")
    if "ts_code" in cols:
        base_cols.append("ts_code")
    if "company_name" in cols:
        base_cols.append("company_name")
    if "llm_finance_importance" in cols:
        base_cols.append("llm_finance_importance")
    if "llm_finance_impact_score" in cols:
        base_cols.append("llm_finance_impact_score")
    if "llm_impacts_json" in cols:
        base_cols.append("llm_impacts_json")
    where = []
    if not force:
        where.append("(llm_sentiment_score IS NULL OR COALESCE(llm_sentiment_label,'')='' OR COALESCE(llm_sentiment_reason,'')='')")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    order_col = "pub_date" if "pub_date" in cols else ("pub_time" if "pub_time" in cols else "id")
    sql = f"""
    SELECT {", ".join(base_cols)}
    FROM {table_name}
    {where_sql}
    ORDER BY {order_col} DESC, id DESC
    LIMIT ?
    """
    return [dict(r) for r in conn.execute(sql, (max(limit, 1),)).fetchall()]


def build_prompt(item: dict, table_name: str) -> str:
    return (
        "你是专业的财经情绪判断系统。请仅基于输入新闻内容，判断这条信息对金融市场和相关资产的整体情绪倾向。\n\n"
        "输出要求：只输出 JSON，不要输出任何额外说明。\n"
        "JSON 格式：\n"
        "{\n"
        '  "sentiment_score": 0,\n'
        '  "sentiment_label": "中性",\n'
        '  "sentiment_reason": "不超过60字，说明偏多/偏空/中性的核心原因",\n'
        '  "sentiment_confidence": 0\n'
        "}\n\n"
        "规则：\n"
        "- sentiment_score 范围 -100 到 100\n"
        "- sentiment_label 只能是：偏多 / 中性 / 偏空\n"
        "- sentiment_confidence 范围 0 到 100\n"
        "- 若新闻对市场偏正面，给偏多；偏负面给偏空；信息中性或多空不明给中性\n"
        "- 不要分析个股涨跌目标价，不要扩展到未提供事实\n\n"
        f"数据表类型：{table_name}\n"
        f"输入：{json.dumps(item, ensure_ascii=False)}"
    )


def call_llm(base_url: str, api_key: str, model: str, temperature: float, prompt: str) -> str:
    return chat_completion_text(
        model=model,
        base_url=base_url,
        api_key=api_key,
        temperature=temperature,
        timeout_s=120,
        max_retries=3,
        messages=[
            {"role": "system", "content": "你是严谨、克制、结构化的财经情绪评分引擎。"},
            {"role": "user", "content": prompt},
        ],
    )


def parse_output(raw: str) -> dict:
    obj = json.loads((raw or "").strip())
    try:
        score = float(obj.get("sentiment_score", 0))
    except Exception:
        score = 0.0
    score = max(-100.0, min(100.0, score))
    label = str(obj.get("sentiment_label", "")).strip()
    if label not in SENTIMENT_LABELS:
        if score >= 20:
            label = "偏多"
        elif score <= -20:
            label = "偏空"
        else:
            label = "中性"
    try:
        conf = float(obj.get("sentiment_confidence", 0))
    except Exception:
        conf = 0.0
    conf = max(0.0, min(100.0, conf))
    reason = str(obj.get("sentiment_reason", "")).strip()[:120]
    return {
        "score": round(score, 2),
        "label": label,
        "reason": reason,
        "confidence": round(conf, 2),
    }


def update_row(conn: sqlite3.Connection, table_name: str, row_id: int, parsed: dict, model: str):
    now = now_utc_str()
    conn.execute(
        f"""
        UPDATE {table_name}
        SET
            llm_sentiment_score = ?,
            llm_sentiment_label = ?,
            llm_sentiment_reason = ?,
            llm_sentiment_confidence = ?,
            llm_sentiment_model = ?,
            llm_sentiment_scored_at = ?
        WHERE id = ?
        """,
        (
            parsed["score"],
            parsed["label"],
            parsed["reason"],
            parsed["confidence"],
            model,
            now,
            row_id,
        ),
    )


def score_table(conn, table_name: str, model: str, base_url: str, api_key: str, temperature: float, limit: int, force: bool, retry: int, sleep_s: float):
    ensure_columns(conn, table_name)
    rows = fetch_rows(conn, table_name, limit=limit, force=force)
    ok = 0
    failed = 0
    for row in rows:
        prompt = build_prompt(row, table_name)
        last_error = None
        for _ in range(max(retry, 0) + 1):
            try:
                raw = call_llm(base_url, api_key, model, temperature, prompt)
                parsed = parse_output(raw)
                update_row(conn, table_name, int(row["id"]), parsed, model)
                conn.commit()
                ok += 1
                print(f"{table_name} #{row['id']}: {parsed['label']} score={parsed['score']}")
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                time.sleep(1)
        if last_error is not None:
            failed += 1
            print(f"{table_name} #{row['id']}: FAIL {last_error}")
        if sleep_s > 0:
            time.sleep(sleep_s)
    return {"ok": ok, "failed": failed, "total": len(rows)}


def main():
    args = parse_args()
    model = normalize_model_name(args.model)
    temperature = normalize_temperature_for_model(model, args.temperature)
    base_url, api_key = resolve_provider(model, args.base_url, args.api_key)
    publish_app_event(
        event="llm_sentiment_update",
        payload={"status": "running", "target": args.target, "model": model, "limit": int(args.limit)},
        producer="llm_score_sentiment.py",
    )
    conn = sqlite3.connect(args.db_path)
    try:
        conn.row_factory = sqlite3.Row
        targets = ["news_feed_items", "stock_news_items"] if args.target == "all" else (
            ["news_feed_items"] if args.target == "news" else ["stock_news_items"]
        )
        summary = {}
        for table_name in targets:
            result = score_table(
                conn,
                table_name=table_name,
                model=model,
                base_url=base_url,
                api_key=api_key,
                temperature=temperature,
                limit=args.limit,
                force=args.force,
                retry=args.retry,
                sleep_s=args.sleep,
            )
            summary[table_name] = result
            print(f"{table_name}: ok={result['ok']} failed={result['failed']} total={result['total']}")
        publish_app_event(
            event="llm_sentiment_update",
            payload={"status": "done", "target": args.target, "model": model, "summary": summary},
            producer="llm_score_sentiment.py",
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        publish_app_event(
            event="llm_sentiment_update",
            payload={"status": "error", "error": str(exc)},
            producer="llm_score_sentiment.py",
        )
        raise
