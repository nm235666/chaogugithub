#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import db_compat as sqlite3
from realtime_streams import publish_app_event


ROOT = Path(__file__).resolve().parent

THEME_MAP = {
    "stock_codes": "股票基础链",
    "stock_daily_prices": "股票基础链",
    "stock_minline": "股票基础链",
    "stock_valuation_daily": "股票基础链",
    "stock_financials": "股票基础链",
    "stock_events": "股票基础链",
    "company_governance": "股票基础链",
    "capital_flow_stock": "股票基础链",
    "stock_scores_daily": "股票基础链",
    "stock_news_items": "新闻链",
    "news_feed_items": "新闻链",
    "news_feed_items_archive": "新闻链",
    "news_daily_summaries": "新闻链",
    "macro_series": "宏观链",
    "fx_daily": "宏观链",
    "rate_curve_points": "宏观链",
    "spread_daily": "宏观链",
    "capital_flow_market": "宏观链",
    "risk_scenarios": "宏观链",
    "chatroom_list_items": "群聊链",
    "wechat_chatlog_clean_items": "群聊链",
    "chatroom_investment_analysis": "群聊链",
    "chatroom_stock_candidate_pool": "群聊链",
}

KEY_DATE_SQL = {
    "stock_daily_prices": "SELECT MAX(trade_date) FROM stock_daily_prices",
    "stock_minline": "SELECT MAX(trade_date) FROM stock_minline",
    "stock_valuation_daily": "SELECT MAX(trade_date) FROM stock_valuation_daily",
    "stock_financials": "SELECT MAX(report_period) FROM stock_financials",
    "stock_events": "SELECT MAX(COALESCE(event_date, ann_date)) FROM stock_events",
    "company_governance": "SELECT MAX(asof_date) FROM company_governance",
    "capital_flow_stock": "SELECT MAX(trade_date) FROM capital_flow_stock",
    "capital_flow_market": "SELECT MAX(trade_date) FROM capital_flow_market",
    "stock_scores_daily": "SELECT MAX(score_date) FROM stock_scores_daily",
    "stock_news_items": "SELECT MAX(pub_time) FROM stock_news_items",
    "news_feed_items": "SELECT MAX(pub_date) FROM news_feed_items",
    "news_feed_items_archive": "SELECT MAX(pub_date) FROM news_feed_items_archive",
    "news_daily_summaries": "SELECT MAX(summary_date) FROM news_daily_summaries",
    "macro_series": "SELECT MAX(period) FROM macro_series",
    "fx_daily": "SELECT MAX(trade_date) FROM fx_daily",
    "rate_curve_points": "SELECT MAX(trade_date) FROM rate_curve_points",
    "spread_daily": "SELECT MAX(trade_date) FROM spread_daily",
    "risk_scenarios": "SELECT MAX(scenario_date) FROM risk_scenarios",
    "chatroom_list_items": "SELECT MAX(update_time) FROM chatroom_list_items",
    "wechat_chatlog_clean_items": "SELECT MAX(message_date) FROM wechat_chatlog_clean_items",
    "chatroom_investment_analysis": "SELECT MAX(analysis_date) FROM chatroom_investment_analysis",
    "chatroom_stock_candidate_pool": "SELECT MAX(latest_analysis_date) FROM chatroom_stock_candidate_pool",
}

SAMPLE_SQL = {
    "stock_codes": "SELECT ts_code, name, industry, market, list_status FROM stock_codes ORDER BY ts_code LIMIT 3",
    "stock_daily_prices": "SELECT ts_code, trade_date, open, close, pct_chg FROM stock_daily_prices ORDER BY trade_date DESC LIMIT 3",
    "stock_minline": "SELECT ts_code, trade_date, minute_time, price, volume FROM stock_minline ORDER BY trade_date DESC, minute_time DESC LIMIT 3",
    "stock_valuation_daily": "SELECT ts_code, trade_date, pe_ttm, pb, total_mv FROM stock_valuation_daily ORDER BY trade_date DESC LIMIT 3",
    "stock_financials": "SELECT ts_code, report_period, revenue, net_profit, roe FROM stock_financials ORDER BY report_period DESC LIMIT 3",
    "stock_events": "SELECT ts_code, event_type, event_date, title FROM stock_events ORDER BY COALESCE(event_date, ann_date) DESC LIMIT 3",
    "company_governance": "SELECT ts_code, asof_date, governance_score, source FROM company_governance ORDER BY asof_date DESC LIMIT 3",
    "capital_flow_stock": "SELECT ts_code, trade_date, net_inflow, main_inflow FROM capital_flow_stock ORDER BY trade_date DESC LIMIT 3",
    "capital_flow_market": "SELECT trade_date, flow_type, net_inflow FROM capital_flow_market ORDER BY trade_date DESC LIMIT 3",
    "stock_scores_daily": "SELECT score_date, ts_code, name, total_score, score_grade FROM stock_scores_daily ORDER BY score_date DESC LIMIT 3",
    "stock_news_items": "SELECT ts_code, title, pub_time, llm_finance_importance FROM stock_news_items ORDER BY pub_time DESC LIMIT 3",
    "news_feed_items": "SELECT source, title, pub_date, llm_finance_importance FROM news_feed_items ORDER BY pub_date DESC LIMIT 3",
    "news_daily_summaries": "SELECT summary_date, model, news_count FROM news_daily_summaries ORDER BY summary_date DESC LIMIT 3",
    "macro_series": "SELECT indicator_code, period, value, source, publish_date FROM macro_series ORDER BY update_time DESC LIMIT 3",
    "fx_daily": "SELECT pair_code, trade_date, close, pct_chg, source FROM fx_daily ORDER BY trade_date DESC LIMIT 3",
    "rate_curve_points": "SELECT market, curve_code, trade_date, tenor, value FROM rate_curve_points ORDER BY trade_date DESC LIMIT 3",
    "spread_daily": "SELECT spread_code, trade_date, value FROM spread_daily ORDER BY trade_date DESC LIMIT 3",
    "risk_scenarios": "SELECT ts_code, scenario_date, scenario_name, var_95 FROM risk_scenarios ORDER BY scenario_date DESC LIMIT 3",
    "chatroom_list_items": "SELECT room_id, remark, user_count, llm_chatroom_primary_category FROM chatroom_list_items ORDER BY update_time DESC LIMIT 3",
    "wechat_chatlog_clean_items": "SELECT talker, message_date, sender_name, content_clean FROM wechat_chatlog_clean_items ORDER BY id DESC LIMIT 3",
    "chatroom_investment_analysis": "SELECT talker, analysis_date, final_bias, message_count FROM chatroom_investment_analysis ORDER BY update_time DESC LIMIT 3",
    "chatroom_stock_candidate_pool": "SELECT candidate_name, candidate_type, dominant_bias, room_count FROM chatroom_stock_candidate_pool ORDER BY update_time DESC LIMIT 3",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="输出数据库审核 Markdown 报告")
    parser.add_argument(
        "--output",
        default=str(ROOT / "docs" / "database_audit_report.md"),
        help="输出 Markdown 报告路径",
    )
    return parser.parse_args()


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fetch_scalar(conn: sqlite3.Connection, sql: str):
    return conn.execute(sql).fetchone()[0]


def fetch_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name"
    ).fetchall()
    return [str(r[0]) for r in rows]


def fetch_row_count(conn: sqlite3.Connection, table: str) -> int:
    return int(fetch_scalar(conn, f"SELECT COUNT(*) FROM {table}"))


def fetch_samples(conn: sqlite3.Connection, table: str) -> list[tuple]:
    sql = SAMPLE_SQL.get(table)
    if not sql:
        return []
    return list(conn.execute(sql).fetchall())


def detect_issues(conn: sqlite3.Connection, table: str) -> list[str]:
    issues: list[str] = []

    if table == "stock_daily_prices":
        if fetch_scalar(conn, "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' AND NOT EXISTS (SELECT 1 FROM stock_daily_prices p WHERE p.ts_code=s.ts_code)") > 0:
            issues.append("仍有上市股票没有日线数据")
    elif table == "stock_valuation_daily":
        miss = fetch_scalar(conn, "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' AND NOT EXISTS (SELECT 1 FROM stock_valuation_daily v WHERE v.ts_code=s.ts_code)")
        if miss > 0:
            issues.append(f"仍缺估值覆盖股票 {miss} 只")
    elif table == "stock_financials":
        miss = fetch_scalar(conn, "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' AND NOT EXISTS (SELECT 1 FROM stock_financials f WHERE f.ts_code=s.ts_code)")
        if miss > 0:
            issues.append(f"仍缺财务覆盖股票 {miss} 只")
    elif table == "stock_events":
        miss = fetch_scalar(conn, "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' AND NOT EXISTS (SELECT 1 FROM stock_events e WHERE e.ts_code=s.ts_code)")
        if miss > 0:
            issues.append(f"仍缺事件覆盖股票 {miss} 只")
        dup = fetch_scalar(conn, "SELECT COUNT(*) FROM (SELECT event_key, COUNT(*) c FROM stock_events GROUP BY event_key HAVING COUNT(*)>1) t")
        if dup > 0:
            issues.append(f"存在重复事件键 {dup} 组")
    elif table == "company_governance":
        miss = fetch_scalar(conn, "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' AND NOT EXISTS (SELECT 1 FROM company_governance g WHERE g.ts_code=s.ts_code)")
        if miss > 0:
            issues.append(f"仍缺治理画像股票 {miss} 只")
    elif table == "capital_flow_stock":
        miss = fetch_scalar(conn, "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' AND NOT EXISTS (SELECT 1 FROM capital_flow_stock c WHERE c.ts_code=s.ts_code)")
        if miss > 0:
            issues.append(f"仍缺个股资金流股票 {miss} 只")
    elif table == "stock_minline":
        miss = fetch_scalar(conn, "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' AND NOT EXISTS (SELECT 1 FROM stock_minline m WHERE m.ts_code=s.ts_code)")
        if miss > 0:
            issues.append(f"仍缺分钟线股票 {miss} 只")
        bad = fetch_scalar(conn, "SELECT COUNT(*) FROM stock_minline WHERE COALESCE(price,0)<=0")
        if bad > 0:
            issues.append(f"存在价格<=0 的分钟线记录 {bad} 条")
    elif table == "stock_news_items":
        miss = fetch_scalar(conn, "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' AND NOT EXISTS (SELECT 1 FROM stock_news_items n WHERE n.ts_code=s.ts_code)")
        if miss > 0:
            issues.append(f"仍缺个股新闻覆盖股票 {miss} 只")
        unscored = fetch_scalar(conn, "SELECT COUNT(*) FROM stock_news_items WHERE COALESCE(llm_finance_importance,'')=''")
        if unscored > 0:
            issues.append(f"仍有未评分个股新闻 {unscored} 条")
        dup = fetch_scalar(conn, "SELECT COUNT(*) FROM (SELECT ts_code, COALESCE(link,''), COUNT(*) c FROM stock_news_items GROUP BY ts_code, COALESCE(link,'') HAVING COALESCE(link,'')<>'' AND COUNT(*)>1) t")
        if dup > 0:
            issues.append(f"存在重复个股新闻链接 {dup} 组")
    elif table == "news_feed_items":
        unscored = fetch_scalar(conn, "SELECT COUNT(*) FROM news_feed_items WHERE COALESCE(llm_finance_importance,'')=''")
        if unscored > 0:
            issues.append(f"仍有未评分新闻 {unscored} 条")
        dup = fetch_scalar(conn, "SELECT COUNT(*) FROM (SELECT source, COALESCE(link,''), COUNT(*) c FROM news_feed_items GROUP BY source, COALESCE(link,'') HAVING COALESCE(link,'')<>'' AND COUNT(*)>1) t")
        if dup > 0:
            issues.append(f"存在重复新闻链接 {dup} 组")
    elif table == "macro_series":
        empty_publish = fetch_scalar(conn, "SELECT COUNT(*) FROM macro_series WHERE COALESCE(publish_date,'')=''")
        if empty_publish > 0:
            issues.append(f"发布日期为空的宏观记录仍有 {empty_publish} 条")
    elif table == "fx_daily":
        non_positive = fetch_scalar(conn, "SELECT COUNT(*) FROM fx_daily WHERE COALESCE(close,0)<=0")
        if non_positive > 0:
            issues.append(f"存在 close<=0 的汇率记录 {non_positive} 条")
    elif table == "risk_scenarios":
        miss = fetch_scalar(conn, "SELECT COUNT(*) FROM stock_codes s WHERE s.list_status='L' AND NOT EXISTS (SELECT 1 FROM risk_scenarios r WHERE r.ts_code=s.ts_code)")
        if miss > 0:
            issues.append(f"仍缺风险情景股票 {miss} 只")
    elif table == "wechat_chatlog_clean_items":
        dup = fetch_scalar(conn, "SELECT COUNT(*) FROM (SELECT message_key, COUNT(*) c FROM wechat_chatlog_clean_items GROUP BY message_key HAVING COUNT(*)>1) t")
        if dup > 0:
            issues.append(f"存在重复聊天 message_key {dup} 组")

    return issues


def issue_level(issues: list[str]) -> str:
    if not issues:
        return "正常"
    for issue in issues:
        if "仍缺" in issue or "未评分" in issue or "重复" in issue or "<=0" in issue:
            return "警告"
    return "提示"


def format_samples(rows: list[tuple]) -> str:
    if not rows:
        return "无样例配置"
    lines = []
    for row in rows:
        lines.append(f"- `{row}`")
    return "\n".join(lines)


def render_markdown(conn: sqlite3.Connection) -> str:
    tables = fetch_tables(conn)
    sections: list[str] = []
    sections.append("# 数据库审核报告")
    sections.append("")
    sections.append(f"- 生成时间：`{now_utc()}`")
    sections.append("- 数据库：`PostgreSQL 主库`")
    sections.append(f"- 审核表数量：`{len(tables)}`")
    sections.append("")
    sections.append("## 总览")
    sections.append("")
    sections.append("| 表名 | 主题 | 行数 | 最新关键日期 | 状态 | 主要问题 |")
    sections.append("| --- | --- | ---: | --- | --- | --- |")

    grouped: dict[str, list[str]] = {"股票基础链": [], "新闻链": [], "宏观链": [], "群聊链": [], "其他": []}

    for table in tables:
        row_count = fetch_row_count(conn, table)
        latest = fetch_scalar(conn, KEY_DATE_SQL[table]) if table in KEY_DATE_SQL else ""
        issues = detect_issues(conn, table)
        level = issue_level(issues)
        issue_text = "；".join(issues[:3]) if issues else "无明显异常"
        theme = THEME_MAP.get(table, "其他")
        sections.append(f"| `{table}` | {theme} | {row_count} | {latest or ''} | {level} | {issue_text} |")
        grouped.setdefault(theme, []).append(table)

    sections.append("")
    sections.append("## 分主题详查")
    sections.append("")

    for theme in ["股票基础链", "新闻链", "宏观链", "群聊链", "其他"]:
        theme_tables = grouped.get(theme) or []
        if not theme_tables:
            continue
        sections.append(f"### {theme}")
        sections.append("")
        for table in theme_tables:
            row_count = fetch_row_count(conn, table)
            latest = fetch_scalar(conn, KEY_DATE_SQL[table]) if table in KEY_DATE_SQL else ""
            issues = detect_issues(conn, table)
            samples = fetch_samples(conn, table)
            sections.append(f"#### `{table}`")
            sections.append("")
            sections.append(f"- 行数：`{row_count}`")
            sections.append(f"- 最新关键日期：`{latest or ''}`")
            sections.append(f"- 审核状态：`{issue_level(issues)}`")
            if issues:
                sections.append("- 发现问题：")
                for issue in issues:
                    sections.append(f"  - {issue}")
            else:
                sections.append("- 发现问题：无明显异常")
            sections.append("- 样例抽查：")
            sections.append(format_samples(samples))
            sections.append("")

    sections.append("## 建议动作")
    sections.append("")
    sections.append("1. 优先补齐仍有覆盖缺口的表：`company_governance`、`capital_flow_stock`、`stock_minline`、`stock_news_items`、`risk_scenarios`。")
    sections.append("2. 优先补评分：`stock_news_items` 当前未评分数量通常会很高，建议先清空未评分库存。")
    sections.append("3. 定期清理重复：重点关注 `news_feed_items` 与 `stock_news_items` 的重复链接。")
    sections.append("4. 提升宏观元数据质量：继续用 AKShare 回填 `macro_series.publish_date`。")
    sections.append("")
    return "\n".join(sections)


def main() -> int:
    args = parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    publish_app_event(
        event="database_audit_update",
        payload={"status": "running", "output": str(output)},
        producer="audit_database_report.py",
    )

    conn = sqlite3.connect(ROOT / "stocks.db")
    try:
        markdown = render_markdown(conn)
    finally:
        conn.close()

    output.write_text(markdown, encoding="utf-8")
    print(f"wrote {output}")
    publish_app_event(
        event="database_audit_update",
        payload={"status": "done", "output": str(output)},
        producer="audit_database_report.py",
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        publish_app_event(
            event="database_audit_update",
            payload={"status": "error", "error": str(exc)},
            producer="audit_database_report.py",
        )
        raise
