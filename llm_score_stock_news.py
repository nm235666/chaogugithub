#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import db_compat as sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from llm_gateway import chat_completion_text, normalize_model_name, normalize_temperature_for_model, resolve_provider
from realtime_streams import publish_app_event

DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
DEEPSEEK_API_KEY = "sk-374806b2f1744b1aa84a6b27758b0bb6"

IMPORTANCE_LEVELS = {"极高", "高", "中", "低", "极低"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="对个股新闻进行LLM评分和摘要并入库")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--model", default="GPT-5.4", help="模型名")
    parser.add_argument("--base-url", default=DEEPSEEK_BASE_URL, help="LLM Base URL")
    parser.add_argument("--api-key", default=DEEPSEEK_API_KEY, help="LLM API Key")
    parser.add_argument("--temperature", type=float, default=0.1, help="采样温度")
    parser.add_argument("--limit", type=int, default=20, help="本次最多评分条数")
    parser.add_argument("--ts-code", default="", help="仅评分指定股票代码")
    parser.add_argument("--force", action="store_true", help="强制重评")
    parser.add_argument("--retry", type=int, default=1, help="单条失败重试次数")
    parser.add_argument("--sleep", type=float, default=0.1, help="每条间隔秒数")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")



def ensure_columns(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_news_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT NOT NULL,
            company_name TEXT NOT NULL,
            source TEXT NOT NULL,
            news_code TEXT,
            title TEXT NOT NULL,
            summary TEXT,
            link TEXT,
            pub_time TEXT,
            comment_num INTEGER,
            relation_stock_tags_json TEXT,
            llm_system_score INTEGER,
            llm_finance_impact_score INTEGER,
            llm_finance_importance TEXT,
            llm_impacts_json TEXT,
            llm_summary TEXT,
            llm_model TEXT,
            llm_scored_at TEXT,
            llm_prompt_version TEXT,
            llm_raw_output TEXT,
            content_hash TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            update_time TEXT,
            UNIQUE(ts_code, source, content_hash)
        )
        """
    )
    cols = {r[1] for r in conn.execute("PRAGMA table_info(stock_news_items)").fetchall()}
    need = [
        ("llm_system_score", "INTEGER"),
        ("llm_finance_impact_score", "INTEGER"),
        ("llm_finance_importance", "TEXT"),
        ("llm_impacts_json", "TEXT"),
        ("llm_summary", "TEXT"),
        ("llm_model", "TEXT"),
        ("llm_scored_at", "TEXT"),
        ("llm_prompt_version", "TEXT"),
        ("llm_raw_output", "TEXT"),
    ]
    for name, typ in need:
        if name not in cols:
            conn.execute(f"ALTER TABLE stock_news_items ADD COLUMN {name} {typ}")
    conn.commit()


def fetch_rows(conn: sqlite3.Connection, limit: int, ts_code: str, force: bool) -> list[sqlite3.Row]:
    where = []
    params: list[object] = []
    if ts_code.strip():
        where.append("ts_code = ?")
        params.append(ts_code.strip().upper())
    if not force:
        where.append("(llm_system_score IS NULL OR llm_finance_impact_score IS NULL OR llm_finance_importance IS NULL OR llm_summary IS NULL)")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
    SELECT id, ts_code, company_name, source, news_code, title, summary, link, pub_time, comment_num
    FROM stock_news_items
    {where_sql}
    ORDER BY pub_time DESC, id DESC
    LIMIT ?
    """
    params.append(max(limit, 1))
    return conn.execute(sql, params).fetchall()


def build_prompt(news: dict) -> str:
    return (
        "你是一个专业的个股新闻事件分析系统。请对输入的单条个股新闻做结构化评分与摘要。\n\n"
        "输出要求：只输出 JSON，不要输出任何额外解释。\n"
        "字段如下：\n"
        "{\n"
        '  "system_score": 0,\n'
        '  "finance_impact_score": 0,\n'
        '  "finance_importance": "中",\n'
        '  "summary": "不超过120字，概括事件及潜在影响",\n'
        '  "impacts": {\n'
        '    "macro": [{"item":"风险偏好","direction":"中性"}],\n'
        '    "markets": [{"item":"A股","direction":"利空"}],\n'
        '    "sectors": [{"item":"机械设备","direction":"利空"}]\n'
        "  }\n"
        "}\n\n"
        "评分规则与财经新闻评分一致：\n"
        "- 系统评分: 事件本身重要性\n"
        "- 财经影响评分: 对市场与资产价格影响强弱\n"
        "- 财经重要程度: 极高/高/中/低/极低\n"
        "- 若信息不足仍需审慎作答\n\n"
        f"输入新闻：\n{json.dumps(news, ensure_ascii=False)}"
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
            {"role": "system", "content": "你是严谨、克制、结构化的个股新闻评分引擎。"},
            {"role": "user", "content": prompt},
        ],
    )


def to_score(v) -> int:
    try:
        n = int(round(float(v)))
    except Exception:
        n = 0
    return max(0, min(100, n))


def fallback_importance(system_score: int, impact_score: int) -> str:
    x = max(system_score, impact_score)
    if x >= 90:
        return "极高"
    if x >= 75:
        return "高"
    if x >= 50:
        return "中"
    if x >= 20:
        return "低"
    return "极低"


def parse_llm_output(raw: str) -> dict:
    txt = (raw or "").strip()
    obj = json.loads(txt)
    ss = to_score(obj.get("system_score"))
    fi = to_score(obj.get("finance_impact_score"))
    imp = str(obj.get("finance_importance", "")).strip()
    if imp not in IMPORTANCE_LEVELS:
        imp = fallback_importance(ss, fi)
    impacts = obj.get("impacts", {})
    if not isinstance(impacts, dict):
        impacts = {}
    summary = str(obj.get("summary", "")).strip()[:240]
    return {
        "system_score": ss,
        "finance_impact_score": fi,
        "finance_importance": imp,
        "impacts_json": json.dumps(impacts, ensure_ascii=False),
        "summary": summary,
    }


def update_row(conn: sqlite3.Connection, row_id: int, parsed: dict, model: str, prompt_version: str, raw_output: str):
    conn.execute(
        """
        UPDATE stock_news_items
        SET
            llm_system_score = ?,
            llm_finance_impact_score = ?,
            llm_finance_importance = ?,
            llm_impacts_json = ?,
            llm_summary = ?,
            llm_model = ?,
            llm_scored_at = ?,
            llm_prompt_version = ?,
            llm_raw_output = ?,
            update_time = ?
        WHERE id = ?
        """,
        (
            parsed["system_score"],
            parsed["finance_impact_score"],
            parsed["finance_importance"],
            parsed["impacts_json"],
            parsed["summary"],
            model,
            now_utc_str(),
            prompt_version,
            raw_output,
            now_utc_str(),
            row_id,
        ),
    )


def main() -> int:
    args = parse_args()
    args.model = normalize_model_name(args.model)
    args.temperature = normalize_temperature_for_model(args.model, args.temperature)
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"数据库不存在: {db_path}")
        return 1

    base_url, api_key = resolve_provider(args.model, args.base_url, args.api_key)
    prompt_version = "stock_news_score_v1"
    publish_app_event(
        event="stock_news_score_update",
        payload={"status": "running", "ts_code": args.ts_code.strip().upper(), "limit": int(args.limit), "model": args.model},
        producer="llm_score_stock_news.py",
    )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_columns(conn)
        rows = fetch_rows(conn, limit=args.limit, ts_code=args.ts_code, force=args.force)
        if not rows:
            print("没有待评分个股新闻。")
            return 0
        ok = 0
        fail = 0
        for i, r in enumerate(rows, start=1):
            news = {
                "id": r["id"],
                "ts_code": r["ts_code"],
                "company_name": r["company_name"],
                "source": r["source"],
                "title": r["title"] or "",
                "summary": r["summary"] or "",
                "pub_time": r["pub_time"] or "",
                "link": r["link"] or "",
                "comment_num": r["comment_num"],
            }
            print(f"[{i}/{len(rows)}] scoring stock_news id={r['id']} ts_code={r['ts_code']}")
            prompt = build_prompt(news)
            last_err = None
            for attempt in range(args.retry + 1):
                try:
                    raw = call_llm(base_url, api_key, args.model, args.temperature, prompt)
                    parsed = parse_llm_output(raw)
                    update_row(conn, r["id"], parsed, args.model, prompt_version, raw)
                    conn.commit()
                    ok += 1
                    print(f"  -> 系统评分={parsed['system_score']}, 财经影响评分={parsed['finance_impact_score']}, 重要程度={parsed['finance_importance']}")
                    last_err = None
                    break
                except Exception as exc:
                    last_err = exc
                    if attempt < args.retry:
                        wait_s = 1.5 * (2**attempt)
                        print(f"  -> 重试 {attempt + 1}/{args.retry}: {exc}")
                        time.sleep(wait_s)
            if last_err is not None:
                fail += 1
                print(f"  -> 失败: {last_err}")
            time.sleep(max(args.sleep, 0.0))
        print(f"完成: success={ok}, failed={fail}, total={len(rows)}")
        publish_app_event(
            event="stock_news_score_update",
            payload={
                "status": "done",
                "ts_code": args.ts_code.strip().upper(),
                "success": ok,
                "failed": fail,
                "total": len(rows),
                "model": args.model,
            },
            producer="llm_score_stock_news.py",
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        publish_app_event(
            event="stock_news_score_update",
            payload={"status": "error", "error": str(exc)},
            producer="llm_score_stock_news.py",
        )
        raise
