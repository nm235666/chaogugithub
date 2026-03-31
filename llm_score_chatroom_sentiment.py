#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3
from llm_gateway import chat_completion_with_fallback, normalize_model_name, normalize_temperature_for_model
from realtime_streams import publish_app_event


def parse_args():
    parser = argparse.ArgumentParser(description="为群聊投资总结补统一情绪字段")
    parser.add_argument("--db-path", default=str(Path(__file__).resolve().parent / "stock_codes.db"), help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）")
    parser.add_argument("--table-name", default="chatroom_investment_analysis", help="目标表名")
    parser.add_argument("--model", default="auto", help="模型名；默认 auto 自动路由并降级")
    parser.add_argument("--temperature", type=float, default=0.1, help="采样温度")
    parser.add_argument("--limit", type=int, default=20, help="最多处理多少个群")
    parser.add_argument("--force", action="store_true", help="强制重评")
    parser.add_argument("--retry", type=int, default=1, help="失败重试次数")
    parser.add_argument("--sleep", type=float, default=0.1, help="每条间隔秒数")
    return parser.parse_args()


def now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_columns(conn, table_name: str):
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


def fetch_rows(conn, table_name: str, limit: int, force: bool):
    latest_sql = f"""
    SELECT a.*
    FROM {table_name} a
    JOIN (
      SELECT room_id, MAX(update_time) AS max_update_time
      FROM {table_name}
      GROUP BY room_id
    ) latest
      ON latest.room_id = a.room_id AND latest.max_update_time = a.update_time
    """
    where = []
    if not force:
        where.append("(a.llm_sentiment_score IS NULL OR COALESCE(a.llm_sentiment_label,'')='' OR COALESCE(a.llm_sentiment_reason,'')='')")
    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
    SELECT *
    FROM ({latest_sql}) a
    {where_sql}
    ORDER BY COALESCE(a.latest_message_date, '') DESC, a.id DESC
    LIMIT ?
    """
    return [dict(r) for r in conn.execute(sql, (max(limit, 1),)).fetchall()]


def build_prompt(item: dict) -> str:
    return (
        "你是专业的群聊投资情绪分析系统。请基于该群最近一次投资总结，判断群整体情绪倾向。\n\n"
        "只输出 JSON：\n"
        "{\n"
        '  "sentiment_score": 0,\n'
        '  "sentiment_label": "中性",\n'
        '  "sentiment_reason": "不超过60字，说明群整体为什么偏多/偏空/中性",\n'
        '  "sentiment_confidence": 0\n'
        "}\n\n"
        "规则：\n"
        "- sentiment_score 范围 -100 到 100\n"
        "- sentiment_label 只能是：偏多 / 中性 / 偏空\n"
        "- sentiment_confidence 范围 0 到 100\n"
        "- final_bias=看多 通常偏多，final_bias=看空 通常偏空，但仍需结合 room_summary 与 targets_json 判断\n"
        "- 不要扩展未给出的事实\n\n"
        f"输入：{json.dumps(item, ensure_ascii=False)}"
    )


def call_llm(model: str, temperature: float, prompt: str):
    return chat_completion_with_fallback(
        model=model,
        temperature=temperature,
        timeout_s=120,
        max_retries=3,
        messages=[
            {"role": "system", "content": "你是严谨、克制、结构化的群聊投资情绪评分引擎。"},
            {"role": "user", "content": prompt},
        ],
    )


def parse_output(raw: str) -> dict:
    obj = json.loads((raw or "").strip())
    score = max(-100.0, min(100.0, float(obj.get("sentiment_score", 0) or 0)))
    label = str(obj.get("sentiment_label", "")).strip()
    if label not in {"偏多", "中性", "偏空"}:
        label = "偏多" if score >= 20 else ("偏空" if score <= -20 else "中性")
    conf = max(0.0, min(100.0, float(obj.get("sentiment_confidence", 0) or 0)))
    reason = str(obj.get("sentiment_reason", "")).strip()[:120]
    return {"score": round(score, 2), "label": label, "reason": reason, "confidence": round(conf, 2)}


def update_row(conn, table_name: str, row_id: int, parsed: dict, model: str):
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
        (parsed["score"], parsed["label"], parsed["reason"], parsed["confidence"], model, now, row_id),
    )


def main():
    args = parse_args()
    model = normalize_model_name(args.model)
    temperature = normalize_temperature_for_model(model, args.temperature)
    publish_app_event(
        event="chatroom_sentiment_update",
        payload={"status": "running", "model": model, "limit": int(args.limit)},
        producer="llm_score_chatroom_sentiment.py",
    )
    conn = sqlite3.connect(args.db_path)
    try:
        conn.row_factory = sqlite3.Row
        ensure_columns(conn, args.table_name)
        rows = fetch_rows(conn, args.table_name, args.limit, args.force)
        ok = 0
        failed = 0
        for row in rows:
            prompt = build_prompt(row)
            last_error = None
            for _ in range(max(args.retry, 0) + 1):
                try:
                    raw = call_llm(model, temperature, prompt)
                    parsed = parse_output(raw.text)
                    update_row(conn, args.table_name, int(row["id"]), parsed, raw.used_model)
                    conn.commit()
                    ok += 1
                    print(f"{row['talker']}: {parsed['label']} score={parsed['score']} actual_model={raw.used_model}")
                    last_error = None
                    break
                except Exception as exc:
                    last_error = exc
                    time.sleep(1)
            if last_error is not None:
                failed += 1
                print(f"{row['talker']}: FAIL {last_error}")
            if args.sleep > 0:
                time.sleep(args.sleep)
        print(f"chatroom sentiment: ok={ok} failed={failed} total={len(rows)}")
        publish_app_event(
            event="chatroom_sentiment_update",
            payload={"status": "done", "model": model, "ok": ok, "failed": failed, "total": len(rows)},
            producer="llm_score_chatroom_sentiment.py",
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        publish_app_event(
            event="chatroom_sentiment_update",
            payload={"status": "error", "error": str(exc)},
            producer="llm_score_chatroom_sentiment.py",
        )
        raise
