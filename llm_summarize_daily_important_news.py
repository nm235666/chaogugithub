#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import db_compat as sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from llm_gateway import chat_completion_text, normalize_model_name, normalize_temperature_for_model, resolve_provider
from realtime_streams import publish_app_event

DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
DEEPSEEK_API_KEY = "sk-374806b2f1744b1aa84a6b27758b0bb6"
DEFAULT_IMPORTANCE = ("极高", "高", "中")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="汇总当日高重要度新闻并生成大模型分析报告")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--date", default="", help="日期，格式 YYYY-MM-DD；默认UTC今天")
    parser.add_argument(
        "--importance",
        default=",".join(DEFAULT_IMPORTANCE),
        help="重要程度筛选，逗号分隔（默认：极高,高,中）",
    )
    parser.add_argument("--source", default="", help="仅筛选指定来源")
    parser.add_argument("--exclude-sources", default="", help="排除来源，逗号分隔")
    parser.add_argument("--exclude-source-prefixes", default="", help="排除来源前缀，逗号分隔")
    parser.add_argument("--max-news", type=int, default=30, help="最多送入模型的新闻条数")
    parser.add_argument("--min-news", type=int, default=8, help="超时回退时最少保留新闻条数")
    parser.add_argument("--max-prompt-chars", type=int, default=18000, help="新闻JSON在prompt中的最大字符预算")
    parser.add_argument("--title-max-len", type=int, default=120, help="单条新闻标题最大长度")
    parser.add_argument("--summary-max-len", type=int, default=160, help="单条新闻摘要最大长度")
    parser.add_argument("--model", default="GPT-5.4", help="模型名，如 deepseek-chat / GPT-5.4")
    parser.add_argument("--base-url", default=DEEPSEEK_BASE_URL, help="LLM Base URL")
    parser.add_argument("--api-key", default=DEEPSEEK_API_KEY, help="LLM API Key")
    parser.add_argument("--temperature", type=float, default=0.2, help="采样温度")
    parser.add_argument("--request-timeout", type=int, default=180, help="单次请求超时秒数")
    parser.add_argument("--max-retries", type=int, default=4, help="请求失败最大重试次数")
    parser.add_argument("--retry-backoff", type=float, default=2.0, help="重试退避基数秒")
    parser.add_argument("--dry-run", action="store_true", help="仅打印入参和新闻列表，不调用模型")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def today_utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def ensure_summary_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_daily_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            summary_date TEXT NOT NULL,
            filter_importance TEXT NOT NULL,
            source_filter TEXT,
            news_count INTEGER NOT NULL,
            model TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            summary_markdown TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_news_daily_summaries_date ON news_daily_summaries(summary_date)"
    )
    conn.commit()


def fetch_news_rows(
    conn: sqlite3.Connection,
    date_ymd: str,
    importance: list[str],
    source: str,
    exclude_sources: list[str],
    exclude_prefixes: list[str],
    max_news: int,
) -> list[sqlite3.Row]:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(news_feed_items)").fetchall()}
    if "llm_finance_importance" not in cols:
        raise RuntimeError("news_feed_items 缺少 llm_finance_importance 字段，请先执行 llm_score_news.py")

    where = ["substr(pub_date, 1, 10) = ?"]
    params: list[object] = [date_ymd]

    valid_imp = [x for x in importance if x in {"极高", "高", "中", "低", "极低"}]
    if valid_imp:
        placeholders = ",".join(["?"] * len(valid_imp))
        where.append(f"llm_finance_importance IN ({placeholders})")
        params.extend(valid_imp)
    if source.strip():
        where.append("source = ?")
        params.append(source.strip().lower())
    if exclude_sources:
        placeholders = ",".join(["?"] * len(exclude_sources))
        where.append(f"source NOT IN ({placeholders})")
        params.extend([x.lower() for x in exclude_sources])
    for p in exclude_prefixes:
        where.append("source NOT LIKE ?")
        params.append(f"{p.lower()}%")

    sql = f"""
    SELECT
      id,
      source,
      title,
      summary,
      pub_date,
      llm_system_score,
      llm_finance_impact_score,
      llm_finance_importance,
      llm_impacts_json
    FROM news_feed_items
    WHERE {" AND ".join(where)}
    ORDER BY
      CASE llm_finance_importance
        WHEN '极高' THEN 5
        WHEN '高' THEN 4
        WHEN '中' THEN 3
        WHEN '低' THEN 2
        ELSE 1
      END DESC,
      COALESCE(llm_finance_impact_score, 0) DESC,
      pub_date DESC,
      id DESC
    LIMIT ?
    """
    params.append(max(max_news, 1))
    return conn.execute(sql, params).fetchall()


def build_prompt(date_ymd: str, cleaned_rows: list[dict]) -> str:
    # 保留用户目标结构，但压缩为更稳定的执行指令
    prompt = (
        "你是一名资深金融市场分析师。请基于输入新闻，输出一份可用于投资参考的结构化日报总结。\n\n"
        "任务范围：仅基于输入新闻，不得虚构事实；不做收益承诺；若信息不足需明确指出。\n"
        "分析维度：\n"
        "1) 新闻内容精炼（主体/性质/时间状态/范围/核心变量）\n"
        "2) 市场影响分析（短期1天-1月、中长期1月-1年、传导链条、确定性分层）\n"
        "3) 重要程度评估（★-★★★★★，并说明偏情绪或偏基本面、偏短期或中长期）\n"
        "4) 关联投资标的（直接受益/直接受损/间接受影响，含逻辑与强度）\n"
        "5) 关联大宗商品（方向、逻辑、持续性）\n"
        "6) 投资建议与风险提示（审慎可执行）\n"
        "7) 综合影响总结（核心结论、关注方向、警惕风险）\n\n"
        "输出要求：\n"
        "- 使用中文Markdown；结构清晰，标题固定为：\n"
        "  ### 1. 新闻内容精炼\n"
        "  ### 2. 市场影响分析\n"
        "  ### 3. 重要程度评估\n"
        "  ### 4. 关联投资标的识别\n"
        "  ### 5. 关联大宗商品识别\n"
        "  ### 6. 投资建议与风险提示\n"
        "  ### 7. 综合影响总结\n"
        "- 多条新闻需逐条分析，最后给综合结论。\n"
        "- 对“高确定性影响/中确定性推演/低确定性猜测”做显式标注。\n"
        "注意：若新闻无法直接推出明确受益标的或商品，请明确写“暂无高相关标的/商品”，不要为了完整性强行列举弱相关对象。\n\n"
        f"新闻日期（UTC）：{date_ymd}\n"
        f"新闻列表JSON：\n{json.dumps(cleaned_rows, ensure_ascii=False)}"
    )
    return prompt


def _trim_text(s: str, max_len: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max(max_len - 1, 1)] + "…"


def _extract_impacts(raw: str) -> dict:
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except Exception:
        return {}
    out: dict[str, list[dict]] = {}
    for k in ("macro", "markets", "sectors"):
        arr = obj.get(k)
        if not isinstance(arr, list):
            continue
        compact = []
        for x in arr[:2]:
            if isinstance(x, dict):
                compact.append(
                    {
                        "item": _trim_text(str(x.get("item", "")), 24),
                        "direction": _trim_text(str(x.get("direction", "")), 6),
                    }
                )
        if compact:
            out[k] = compact
    return out


def clean_news_for_prompt(
    rows: list[dict],
    max_prompt_chars: int,
    title_max_len: int,
    summary_max_len: int,
) -> list[dict]:
    cleaned: list[dict] = []
    for r in rows:
        cleaned.append(
            {
                "id": r.get("id"),
                "source": r.get("source", ""),
                "time": r.get("pub_date", ""),
                "title": _trim_text(str(r.get("title", "")), max(title_max_len, 20)),
                "summary": _trim_text(str(r.get("summary", "")), max(summary_max_len, 40)),
                "importance": r.get("llm_finance_importance", ""),
                "system_score": r.get("llm_system_score"),
                "finance_impact_score": r.get("llm_finance_impact_score"),
                "impacts": _extract_impacts(str(r.get("llm_impacts_json", ""))),
            }
        )

    budget = max(max_prompt_chars, 2000)
    while cleaned:
        blob = json.dumps(cleaned, ensure_ascii=False)
        if len(blob) <= budget:
            break
        cleaned = cleaned[:-1]
    return cleaned


def call_llm(
    base_url: str,
    api_key: str,
    model: str,
    temperature: float,
    prompt: str,
    request_timeout: int,
    max_retries: int,
    retry_backoff: float,
) -> str:
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            return chat_completion_text(
                model=model,
                base_url=base_url,
                api_key=api_key,
                temperature=temperature,
                timeout_s=max(request_timeout, 30),
                max_retries=1,
                messages=[
                    {"role": "system", "content": "你是专业、克制、面向交易与风控的金融分析助手。"},
                    {"role": "user", "content": prompt},
                ],
            )
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                sleep_s = max(retry_backoff, 0.1) * (2**attempt)
                time.sleep(sleep_s)
                continue
            break
    raise RuntimeError(f"调用LLM失败: {last_error}")


def build_fallback_sizes(total: int, min_news: int) -> list[int]:
    total = max(total, 1)
    min_news = max(min_news, 1)
    min_news = min(min_news, total)
    sizes: list[int] = []
    cur = total
    while True:
        if cur not in sizes:
            sizes.append(cur)
        if cur <= min_news:
            break
        nxt = max(min_news, cur // 2)
        if nxt == cur:
            break
        cur = nxt
    return sizes


def save_summary(
    conn: sqlite3.Connection,
    date_ymd: str,
    importance_filter: str,
    source_filter: str,
    news_count: int,
    model: str,
    prompt_version: str,
    summary_markdown: str,
) -> int:
    # 同一天仅保留最后一条总结
    conn.execute("DELETE FROM news_daily_summaries WHERE summary_date = ?", (date_ymd,))
    cur = conn.execute(
        """
        INSERT INTO news_daily_summaries (
            summary_date, filter_importance, source_filter, news_count, model, prompt_version, summary_markdown, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            date_ymd,
            importance_filter,
            source_filter,
            news_count,
            model,
            prompt_version,
            summary_markdown,
            now_utc_str(),
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM news_daily_summaries WHERE summary_date = ? ORDER BY id DESC LIMIT 1",
        (date_ymd,),
    ).fetchone()
    return int(row[0]) if row else 0


def main() -> int:
    args = parse_args()
    args.model = normalize_model_name(args.model)
    args.temperature = normalize_temperature_for_model(args.model, args.temperature)
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        print(f"数据库不存在: {db_path}")
        return 1

    date_ymd = args.date.strip() or today_utc_date()
    importance = [x.strip() for x in args.importance.split(",") if x.strip()]
    exclude_sources = [x.strip() for x in args.exclude_sources.split(",") if x.strip()]
    exclude_prefixes = [x.strip() for x in args.exclude_source_prefixes.split(",") if x.strip()]

    base_url, api_key = resolve_provider(args.model, args.base_url, args.api_key)
    prompt_version = "daily_news_summary_v1"
    publish_app_event(
        event="news_daily_summary_update",
        payload={"status": "running", "summary_date": date_ymd, "model": args.model},
        producer="llm_summarize_daily_important_news.py",
    )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_summary_table(conn)
        rows = fetch_news_rows(
            conn=conn,
            date_ymd=date_ymd,
            importance=importance,
            source=args.source,
            exclude_sources=exclude_sources,
            exclude_prefixes=exclude_prefixes,
            max_news=args.max_news,
        )
        if not rows:
            print(f"没有匹配新闻：date={date_ymd}, importance={importance}")
            return 0

        rows_dict = [dict(r) for r in rows]
        print(f"待汇总新闻数: {len(rows_dict)}")
        compact_rows = clean_news_for_prompt(
            rows=rows_dict,
            max_prompt_chars=args.max_prompt_chars,
            title_max_len=args.title_max_len,
            summary_max_len=args.summary_max_len,
        )
        if not compact_rows:
            print("可用于模型输入的新闻为空（被预算裁剪），请调大 --max-prompt-chars")
            return 2
        print(f"清洗后用于提示词的新闻数: {len(compact_rows)}")
        prompt = build_prompt(date_ymd=date_ymd, cleaned_rows=compact_rows)

        if args.dry_run:
            print("dry-run 模式，不调用模型。")
            print("新闻标题预览：")
            for i, r in enumerate(compact_rows[:10], start=1):
                print(f"{i}. [{r['importance']}] {r['title']}")
            return 0

        fallback_sizes = build_fallback_sizes(total=len(compact_rows), min_news=args.min_news)
        summary = None
        used_rows = compact_rows
        last_exc = None
        for n in fallback_sizes:
            used_rows = compact_rows[:n]
            prompt = build_prompt(date_ymd=date_ymd, cleaned_rows=used_rows)
            try:
                summary = call_llm(
                    base_url=base_url,
                    api_key=api_key,
                    model=args.model,
                    temperature=args.temperature,
                    prompt=prompt,
                    request_timeout=args.request_timeout,
                    max_retries=args.max_retries,
                    retry_backoff=args.retry_backoff,
                )
                break
            except Exception as exc:  # pragma: no cover
                last_exc = exc
                print(f"使用 {n} 条新闻生成失败，准备降级重试: {exc}")
                continue

        if summary is None:
            raise RuntimeError(f"日报总结生成失败: {last_exc}")

        sid = save_summary(
            conn=conn,
            date_ymd=date_ymd,
            importance_filter=",".join(importance),
            source_filter=args.source or "",
            news_count=len(used_rows),
            model=args.model,
            prompt_version=prompt_version,
            summary_markdown=summary,
        )
        print(f"已生成并入库 summary_id={sid}, date={date_ymd}, news_count={len(used_rows)}")
        publish_app_event(
            event="news_daily_summary_update",
            payload={
                "status": "done",
                "summary_date": date_ymd,
                "summary_id": sid,
                "news_count": len(used_rows),
                "model": args.model,
            },
            producer="llm_summarize_daily_important_news.py",
        )
        print("\n===== Summary =====\n")
        print(summary)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        publish_app_event(
            event="news_daily_summary_update",
            payload={"status": "error", "error": str(exc)},
            producer="llm_summarize_daily_important_news.py",
        )
        raise
