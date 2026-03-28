#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3
from backend.server import build_multi_role_context

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "stock_codes.db"
REPORT_TABLE = "research_reports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="标准投研报告生成器：输出结构化 Markdown 并入库")
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    parser.add_argument("--report-type", choices=["stock", "theme", "market"], required=True, help="报告类型")
    parser.add_argument("--subject-key", required=True, help="股票填 ts_code；主题填主题名；市场填 market_overview")
    parser.add_argument("--subject-name", default="", help="可选：展示名称")
    parser.add_argument("--lookback-days", type=int, default=20, help="上下文回看天数")
    parser.add_argument("--report-date", default="", help="报告日期，默认 UTC 今日 YYYY-MM-DD")
    parser.add_argument("--model", default="system_template_v1", help="报告生成方式标记")
    return parser.parse_args()


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def today_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def parse_json_text(raw: str):
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT NOT NULL,
            report_type TEXT NOT NULL,
            subject_key TEXT NOT NULL,
            subject_name TEXT,
            model TEXT,
            markdown_content TEXT,
            context_json TEXT,
            created_at TEXT,
            update_time TEXT,
            UNIQUE(report_date, report_type, subject_key)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{REPORT_TABLE}_date ON {REPORT_TABLE}(report_date)")
    conn.commit()


def fmt(value) -> str:
    if value is None or value == "":
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def build_stock_report(conn: sqlite3.Connection, ts_code: str, report_date: str, lookback_days: int) -> tuple[str, str, dict]:
    context = build_multi_role_context(ts_code, lookback_days)
    profile = context.get("company_profile") or {}
    subject_name = str(profile.get("name") or ts_code)
    score_row = conn.execute(
        """
        SELECT score_date, total_score, score_grade, industry_score_grade, industry_rank, industry_count, score_payload_json
        FROM stock_scores_daily
        WHERE ts_code = ?
        ORDER BY score_date DESC
        LIMIT 1
        """,
        (ts_code,),
    ).fetchone()
    state_row = conn.execute(
        """
        SELECT direction, signal_strength, confidence, current_state, driver_type, driver_title
        FROM signal_state_tracker
        WHERE signal_scope = 'stock' AND signal_key = ?
        ORDER BY update_time DESC
        LIMIT 1
        """,
        (f"stock:{ts_code}",),
    ).fetchone()
    theme_rows = conn.execute(
        """
        SELECT theme_name, COUNT(*) AS c
        FROM theme_evidence_items
        WHERE ts_code = ?
        GROUP BY theme_name
        ORDER BY c DESC, theme_name
        LIMIT 8
        """,
        (ts_code,),
    ).fetchall()
    event_rows = conn.execute(
        """
        SELECT event_type, event_summary, event_time
        FROM signal_state_events
        WHERE signal_scope = 'stock' AND signal_key = ?
        ORDER BY event_time DESC
        LIMIT 5
        """,
        (f"stock:{ts_code}",),
    ).fetchall()
    news_rows = conn.execute(
        """
        SELECT title, pub_time, llm_finance_importance
        FROM stock_news_items
        WHERE ts_code = ?
        ORDER BY COALESCE(pub_time, '') DESC, id DESC
        LIMIT 5
        """,
        (ts_code,),
    ).fetchall()

    price_summary = context.get("price_summary") or {}
    latest = price_summary.get("latest") or {}
    metrics = price_summary.get("metrics") or {}
    financial = context.get("financial_summary") or {}
    valuation = context.get("valuation_summary") or {}
    capital_flow = context.get("capital_flow_summary") or {}
    event_summary = context.get("event_summary") or {}
    governance = context.get("governance_summary") or {}
    risk = context.get("risk_summary") or {}
    stock_news_summary = context.get("stock_news_summary") or {}

    theme_text = "、".join([str(r[0]) for r in theme_rows]) if theme_rows else "-"
    event_lines = "\n".join([f"- {r[2]}｜{r[0]}｜{r[1]}" for r in event_rows]) or "- 近期待处理事件较少"
    news_lines = "\n".join([f"- {r[1]}｜{r[2] or '-'}｜{r[0]}" for r in news_rows]) or "- 近期待处理新闻较少"

    markdown = f"""# {subject_name} 标准投研报告

## 数据摘要
- 股票代码：{ts_code}
- 行业/地区/市场：{fmt(profile.get('industry'))} / {fmt(profile.get('area'))} / {fmt(profile.get('market'))}
- 最新价格日期：{fmt(latest.get('trade_date'))}
- 最新收盘价：{fmt(latest.get('close'))}
- 近端趋势强弱：5日 {fmt(metrics.get('ret_5d_pct'))}% / 20日 {fmt(metrics.get('ret_20d_pct'))}%
- 综合评分：{fmt(score_row[1] if score_row else '')}（{fmt(score_row[2] if score_row else '')}）
- 行业内评分：{fmt(score_row[3] if score_row else '')}，行业排名 {fmt(score_row[4] if score_row else '')}/{fmt(score_row[5] if score_row else '')}
- 当前信号状态：{fmt(state_row[3] if state_row else '')}
- 当前信号方向：{fmt(state_row[0] if state_row else '')}，强度 {fmt(state_row[1] if state_row else '')}，置信度 {fmt(state_row[2] if state_row else '')}
- 当前关联主题：{theme_text}

## 事件链路
{event_lines}

## 多角色观点
- 宏观环境：{fmt((context.get('macro_context') or {}).get('summary'))}
- 汇率环境：{fmt((context.get('fx_context') or {}).get('summary'))}
- 利率利差：{fmt((context.get('rate_spread_context') or {}).get('summary'))}
- 财务概况：营收 {fmt(financial.get('revenue'))}，净利润 {fmt(financial.get('net_profit'))}，ROE {fmt(financial.get('roe'))}
- 估值概况：PE {fmt(valuation.get('pe_ttm'))}，PB {fmt(valuation.get('pb'))}，总市值 {fmt(valuation.get('total_mv'))}
- 资金流概况：近5日净流入 {fmt(capital_flow.get('net_inflow_5d'))}，近20日净流入 {fmt(capital_flow.get('net_inflow_20d'))}
- 公司事件：{fmt(event_summary.get('summary'))}
- 公司治理：{fmt(governance.get('summary'))}

## 风险提示
- 风险情景：{fmt(risk.get('summary'))}
- 信号驱动：{fmt(state_row[4] if state_row else '')} / {fmt(state_row[5] if state_row else '')}
- 数据限制：本报告为模板化拼装结果，若需更深层推演，可继续叠加 LLM 多角色分析。

## 相关标的与相关新闻
- 个股相关新闻摘要：{fmt(stock_news_summary.get('summary'))}
{news_lines}

## 结论
- 当前这只股票处于“{fmt(state_row[3] if state_row else '未入状态机')}”阶段，方向偏“{fmt(state_row[0] if state_row else '中性')}”。
- 如果后续主题链条继续强化、资金流改善、公司事件转正，则更适合进入“强化”阶段观察。
- 如果方向转中性或反向，且驱动主题开始退潮，应优先关注“证伪/反转”风险。
"""
    context_json = {
        "company_profile": profile,
        "price_summary": price_summary,
        "financial_summary": financial,
        "valuation_summary": valuation,
        "capital_flow_summary": capital_flow,
        "event_summary": event_summary,
        "governance_summary": governance,
        "risk_summary": risk,
        "state_row": list(state_row) if state_row else None,
        "themes": [list(r) for r in theme_rows],
        "events": [list(r) for r in event_rows],
        "news": [list(r) for r in news_rows],
    }
    return subject_name, markdown, context_json


def build_theme_report(conn: sqlite3.Connection, theme_name: str, report_date: str) -> tuple[str, str, dict]:
    row = conn.execute(
        """
        SELECT theme_group, direction, theme_strength, confidence, evidence_count,
               intl_news_count, domestic_news_count, stock_news_count, chatroom_count,
               latest_evidence_time, heat_level, top_terms_json, top_stocks_json, evidence_json
        FROM theme_hotspot_tracker
        WHERE theme_name = ?
        """,
        (theme_name,),
    ).fetchone()
    if not row:
        raise ValueError(f"未找到主题: {theme_name}")
    state_row = conn.execute(
        """
        SELECT current_state, driver_type, driver_title
        FROM signal_state_tracker
        WHERE signal_scope = 'theme' AND signal_key = ?
        ORDER BY update_time DESC
        LIMIT 1
        """,
        (f"theme:{theme_name}",),
    ).fetchone()
    event_rows = conn.execute(
        """
        SELECT event_type, event_summary, event_time
        FROM signal_state_events
        WHERE signal_scope = 'theme' AND signal_key = ?
        ORDER BY event_time DESC
        LIMIT 5
        """,
        (f"theme:{theme_name}",),
    ).fetchall()
    top_terms = parse_json_text(row[11]) or []
    top_stocks = parse_json_text(row[12]) or []
    evidence = parse_json_text(row[13]) or []
    event_lines = "\n".join([f"- {r[2]}｜{r[0]}｜{r[1]}" for r in event_rows]) or "- 近期待处理事件较少"
    top_stock_lines = "\n".join([f"- {it.get('stock_name') or it.get('ts_code')}｜权重 {it.get('weight')}" for it in top_stocks[:8]]) or "- 暂无"
    top_term_lines = "\n".join([f"- {it.get('term')}｜权重 {it.get('weight')}" for it in top_terms[:8]]) or "- 暂无"
    evidence_lines = "\n".join([f"- {it.get('date') or '-'}｜{it.get('source') or '-'}｜{it.get('title') or it.get('theme_name') or '-'}" for it in evidence[:6]]) or "- 暂无"

    markdown = f"""# {theme_name} 标准投研报告

## 数据摘要
- 主题名称：{theme_name}
- 主题分组：{fmt(row[0])}
- 当前方向：{fmt(row[1])}
- 主题热度：{fmt(row[2])}
- 置信度：{fmt(row[3])}
- 证据数量：{fmt(row[4])}
- 来源构成：国际新闻 {fmt(row[5])} / 国内新闻 {fmt(row[6])} / 个股新闻 {fmt(row[7])} / 群聊 {fmt(row[8])}
- 最新证据时间：{fmt(row[9])}
- 热度等级：{fmt(row[10])}
- 当前状态：{fmt(state_row[0] if state_row else '')}

## 事件链路
{event_lines}

## 多角色观点
- 宏观视角：该主题当前方向偏 {fmt(row[1])}，其热度更多来自 {fmt(state_row[1] if state_row else '多源混合')}。
- 交易视角：若热度继续抬升且状态从“初始/观察”进入“强化”，适合重点跟踪。
- 风险视角：若主导方向转中性或来源集中度快速下降，需要警惕“证伪/反转”。

## 风险提示
- 主题驱动标题：{fmt(state_row[2] if state_row else '')}
- 本主题可能存在“高热度但弱兑现”的情况，需要配合个股与财务验证。

## 相关标的
{top_stock_lines}

## 关键术语
{top_term_lines}

## 近期证据样本
{evidence_lines}

## 结论
- 当前主题处于“{fmt(state_row[0] if state_row else '未入状态机')}”阶段，方向偏“{fmt(row[1])}”。
- 若后续跨源证据继续累积、关联标的开始同步强化，则更值得进入主题主线观察名单。
"""
    context_json = {
        "theme_name": theme_name,
        "tracker_row": list(row),
        "state_row": list(state_row) if state_row else None,
        "events": [list(r) for r in event_rows],
        "top_terms": top_terms,
        "top_stocks": top_stocks,
        "evidence": evidence,
    }
    return theme_name, markdown, context_json


def build_market_report(conn: sqlite3.Connection, subject_key: str, report_date: str) -> tuple[str, str, dict]:
    summary_row = conn.execute(
        """
        SELECT summary_markdown, model, news_count, created_at
        FROM news_daily_summaries
        WHERE summary_date = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (report_date,),
    ).fetchone()
    theme_rows = conn.execute(
        """
        SELECT t.theme_name, t.direction, t.theme_strength, s.current_state
        FROM theme_hotspot_tracker t
        LEFT JOIN signal_state_tracker s
          ON s.signal_scope = 'theme' AND s.signal_key = ('theme:' || t.theme_name)
        ORDER BY theme_strength DESC
        LIMIT 10
        """
    ).fetchall()
    signal_rows = conn.execute(
        """
        SELECT subject_name, ts_code, direction, signal_strength, current_state
        FROM signal_state_tracker
        WHERE signal_scope = 'stock'
        ORDER BY signal_strength DESC
        LIMIT 10
        """
    ).fetchall()
    theme_lines = "\n".join([f"- {r[0]}｜{r[1]}｜热度 {r[2]}｜状态 {r[3] or '-'}" for r in theme_rows]) or "- 暂无"
    stock_lines = "\n".join([f"- {r[0]}（{r[1]}）｜{r[2]}｜强度 {r[3]}｜状态 {r[4] or '-'}" for r in signal_rows]) or "- 暂无"
    summary_head = str(summary_row[0] or "")[:1500] if summary_row else "暂无市场日报总结。"
    markdown = f"""# 市场总览标准投研报告

## 数据摘要
- 报告日期：{report_date}
- 日报总结模型：{fmt(summary_row[1] if summary_row else '')}
- 参与新闻数量：{fmt(summary_row[2] if summary_row else '')}
- 日报生成时间：{fmt(summary_row[3] if summary_row else '')}

## 事件链路
{summary_head}

## 多角色观点
- 宏观主线：当前市场更适合围绕“主题热度 + 状态机”进行分层跟踪。
- 交易主线：先看最热主题，再看主题对应股票状态是否同步强化。
- 风险主线：若主题热度很高但个股状态没有跟进，需要防止误判为伪主线。

## 风险提示
- 市场总览报告是聚合结果，不能替代单只股票或单个主题的深度验证。

## 热门主题
{theme_lines}

## 强信号股票
{stock_lines}

## 结论
- 今日市场更适合按“主题 -> 股票 -> 状态变化”三层结构观察，而不是只看静态榜单。
"""
    context_json = {
        "summary_row": list(summary_row) if summary_row else None,
        "theme_rows": [list(r) for r in theme_rows],
        "signal_rows": [list(r) for r in signal_rows],
    }
    return "市场总览", markdown, context_json


def save_report(conn: sqlite3.Connection, report_date: str, report_type: str, subject_key: str, subject_name: str, model: str, markdown: str, context_json: dict) -> int:
    now = now_utc_str()
    conn.execute(
        f"""
        INSERT INTO {REPORT_TABLE} (
            report_date, report_type, subject_key, subject_name, model, markdown_content, context_json, created_at, update_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(report_date, report_type, subject_key) DO UPDATE SET
            subject_name=excluded.subject_name,
            model=excluded.model,
            markdown_content=excluded.markdown_content,
            context_json=excluded.context_json,
            update_time=excluded.update_time
        """,
        (
            report_date,
            report_type,
            subject_key,
            subject_name,
            model,
            markdown,
            json.dumps(context_json, ensure_ascii=False),
            now,
            now,
        ),
    )
    conn.commit()
    row = conn.execute(
        f"SELECT id FROM {REPORT_TABLE} WHERE report_date = ? AND report_type = ? AND subject_key = ?",
        (report_date, report_type, subject_key),
    ).fetchone()
    return int(row[0])


def main() -> int:
    args = parse_args()
    report_date = args.report_date or today_utc_str()
    conn = sqlite3.connect(args.db_path)
    try:
        ensure_table(conn)
        if args.report_type == "stock":
            subject_name, markdown, context_json = build_stock_report(conn, args.subject_key.strip().upper(), report_date, args.lookback_days)
        elif args.report_type == "theme":
            subject_name, markdown, context_json = build_theme_report(conn, args.subject_key.strip(), report_date)
        else:
            subject_name, markdown, context_json = build_market_report(conn, args.subject_key.strip(), report_date)
        report_id = save_report(
            conn,
            report_date=report_date,
            report_type=args.report_type,
            subject_key=args.subject_key.strip(),
            subject_name=args.subject_name.strip() or subject_name,
            model=args.model,
            markdown=markdown,
            context_json=context_json,
        )
        print(f"完成: report_id={report_id}, report_type={args.report_type}, subject={subject_name}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
