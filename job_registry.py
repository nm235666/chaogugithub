#!/usr/bin/env python3
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
PYTHON_BIN = sys.executable or "python3"


@dataclass(frozen=True)
class JobDefinition:
    job_key: str
    name: str
    category: str
    schedule_expr: str
    owner: str = "scheduler"
    description: str = ""
    commands: tuple[tuple[str, ...], ...] = field(default_factory=tuple)


def py_cmd(*parts: str) -> tuple[str, ...]:
    return (PYTHON_BIN, *parts)


def bash_cmd(script: str) -> tuple[str, ...]:
    return ("/bin/bash", "-lc", f"cd {ROOT_DIR} && . {ROOT_DIR}/runtime_env.sh && {script}")


DEFAULT_JOBS: tuple[JobDefinition, ...] = (
    JobDefinition(
        job_key="intl_news_pipeline",
        name="国际新闻采集评分映射",
        category="news",
        schedule_expr="*/5 * * * *",
        description="抓取国际新闻、补评分、补情绪、做股票映射",
        commands=(
            py_cmd(str(ROOT_DIR / "fetch_news_rss.py"), "--limit", "15", "--timeout", "30"),
            py_cmd(str(ROOT_DIR / "fetch_news_marketscreener.py"), "--limit", "20", "--timeout", "30"),
            py_cmd(str(ROOT_DIR / "fetch_news_marketscreener_live.py"), "--limit", "30", "--timeout", "30"),
            py_cmd(str(ROOT_DIR / "llm_score_news.py"), "--limit", "20", "--retry", "1", "--sleep", "0.1"),
            py_cmd(str(ROOT_DIR / "llm_score_sentiment.py"), "--target", "news", "--limit", "20", "--retry", "1", "--sleep", "0.1"),
            py_cmd(str(ROOT_DIR / "map_news_items_to_stocks.py"), "--limit", "200", "--days", "7"),
        ),
    ),
    JobDefinition(
        job_key="cn_news_pipeline",
        name="国内新闻采集评分映射",
        category="news",
        schedule_expr="*/2 * * * *",
        description="抓取新浪国内财经新闻并快速入库；评分与映射由独立任务补做",
        commands=(
            py_cmd(str(ROOT_DIR / "fetch_cn_news_sina_7x24.py"), "--limit", "60", "--timeout", "30"),
        ),
    ),
    JobDefinition(
        job_key="news_stock_map_refresh",
        name="新闻股票映射刷新",
        category="news",
        schedule_expr="*/5 * * * *",
        description="补做新闻到股票的映射",
        commands=(
            py_cmd(str(ROOT_DIR / "map_news_items_to_stocks.py"), "--limit", "300", "--days", "7"),
        ),
    ),
    JobDefinition(
        job_key="news_sentiment_refresh",
        name="新闻情绪刷新",
        category="news",
        schedule_expr="12 * * * *",
        description="为国际与国内新闻补统一情绪标签",
        commands=(
            py_cmd(str(ROOT_DIR / "llm_score_sentiment.py"), "--target", "news", "--limit", "60", "--retry", "1", "--sleep", "0.1"),
        ),
    ),
    JobDefinition(
        job_key="news_daily_summary_refresh",
        name="新闻日报总结",
        category="reports",
        schedule_expr="30 3,15 * * *",
        description="生成当日重要新闻总结，默认 GPT-5.4 优先，失败自动降级",
        commands=(
            bash_cmd(
                "timeout 900s python3 -u "
                f"{ROOT_DIR}/llm_summarize_daily_important_news.py "
                "--date __CN_DATE__ --importance '极高,高,中' --max-news 30 --min-news 8 "
                "--max-prompt-chars 9000 --request-timeout 180 --max-retries 2 --retry-backoff 2 "
                ""
            ),
        ),
    ),
    JobDefinition(
        job_key="database_audit_refresh",
        name="数据库审核报告刷新",
        category="audit",
        schedule_expr="50 16 * * *",
        description="生成数据库审核报告",
        commands=((PYTHON_BIN, str(ROOT_DIR / "audit_database_report.py")),),
    ),
    JobDefinition(
        job_key="investment_signal_tracker_refresh",
        name="投资信号跟踪器刷新",
        category="signals",
        schedule_expr="35 * * * *",
        description="刷新 30d/7d/1d 投资信号总表",
        commands=(
            py_cmd(str(ROOT_DIR / "build_investment_signal_tracker.py"), "--lookback-days", "30", "--target-table", "investment_signal_tracker"),
            py_cmd(str(ROOT_DIR / "build_investment_signal_tracker.py"), "--lookback-days", "7", "--target-table", "investment_signal_tracker_7d", "--skip-history"),
            py_cmd(str(ROOT_DIR / "build_investment_signal_tracker.py"), "--lookback-days", "1", "--target-table", "investment_signal_tracker_1d", "--skip-history"),
        ),
    ),
    JobDefinition(
        job_key="logic_view_cache_refresh",
        name="逻辑视图缓存回填",
        category="cache",
        schedule_expr="55 17 * * *",
        description="回填日报总结与信号逻辑视图缓存",
        commands=((PYTHON_BIN, str(ROOT_DIR / "backfill_logic_view_cache.py")),),
    ),
    JobDefinition(
        job_key="chatroom_analysis_pipeline",
        name="群聊分析流水线",
        category="chatrooms",
        schedule_expr="15 * * * *",
        description="群聊投资倾向、情绪、候选池与别名归一",
        commands=(
            py_cmd(str(ROOT_DIR / "llm_analyze_chatroom_investment_bias.py"), "--days", "7", "--limit", "20", "--primary-category", "投资交易", "--retry", "2", "--sleep", "0.5"),
            py_cmd(str(ROOT_DIR / "llm_score_chatroom_sentiment.py"), "--limit", "20", "--retry", "1", "--sleep", "0.1"),
            py_cmd(str(ROOT_DIR / "build_chatroom_candidate_pool.py"), "--min-room-count", "1"),
            py_cmd(str(ROOT_DIR / "llm_resolve_stock_aliases.py"), "--limit", "20", "--min-confidence", "0.88", "--retry", "2", "--sleep", "0.3"),
            py_cmd(str(ROOT_DIR / "build_chatroom_candidate_pool.py"), "--min-room-count", "1"),
        ),
    ),
    JobDefinition(
        job_key="chatroom_sentiment_refresh",
        name="群聊情绪刷新",
        category="chatrooms",
        schedule_expr="18 * * * *",
        description="为群聊投资总结补统一情绪",
        commands=(
            py_cmd(str(ROOT_DIR / "llm_score_chatroom_sentiment.py"), "--limit", "30", "--retry", "1", "--sleep", "0.1"),
        ),
    ),
    JobDefinition(
        job_key="monitored_chatlog_fetch",
        name="监控群聊天记录拉取",
        category="chatrooms",
        schedule_expr="*/3 * * * *",
        description="拉取处于监控中的群聊消息",
        commands=((PYTHON_BIN, str(ROOT_DIR / "fetch_monitored_chatlogs_once.py")),),
    ),
    JobDefinition(
        job_key="chatroom_list_refresh",
        name="群聊列表刷新",
        category="chatrooms",
        schedule_expr="5 0 * * *",
        description="每日同步群聊列表",
        commands=((PYTHON_BIN, str(ROOT_DIR / "fetch_chatroom_list_to_db.py"), "--once"),),
    ),
    JobDefinition(
        job_key="stock_news_score_refresh",
        name="个股新闻评分刷新",
        category="stock_news",
        schedule_expr="*/10 * * * *",
        description="为个股新闻补评分与摘要（GPT 批处理并发）",
        commands=(
            py_cmd(
                str(ROOT_DIR / "llm_score_stock_news.py"),
                "--model",
                "GPT-5.4",
                "--limit",
                "240",
                "--workers",
                "6",
                "--batch-size",
                "8",
                "--retry",
                "1",
                "--sleep",
                "0.02",
            ),
        ),
    ),
    JobDefinition(
        job_key="stock_news_backfill_missing",
        name="个股新闻缺口补抓",
        category="stock_news",
        schedule_expr="55 * * * *",
        description="补抓缺失个股新闻",
        commands=(
            py_cmd(str(ROOT_DIR / "backfill_stock_news_items.py"), "--missing-only", "--limit-stocks", "200", "--page-size", "20", "--max-pages", "2", "--retry", "2", "--pause", "0.2"),
        ),
    ),
    JobDefinition(
        job_key="stock_news_expand_focus",
        name="个股新闻重点扩抓",
        category="stock_news",
        schedule_expr="25 * * * *",
        description="围绕重点股票扩抓个股新闻并评分",
        commands=(
            py_cmd(str(ROOT_DIR / "run_stock_news_expand_focus.py"), "--limit-scores", "100", "--limit-candidates", "50", "--max-targets", "120", "--page-size", "20", "--score-after-fetch"),
        ),
    ),
    JobDefinition(
        job_key="macro_series_akshare_refresh",
        name="AKShare 宏观数据刷新",
        category="macro",
        schedule_expr="20 17 * * *",
        description="刷新宏观指标数据",
        commands=((PYTHON_BIN, str(ROOT_DIR / "backfill_macro_series_akshare.py")),),
    ),
    JobDefinition(
        job_key="news_archive_refresh",
        name="新闻归档",
        category="maintenance",
        schedule_expr="30 3,15 * * *",
        description="归档历史新闻并保留主表热数据",
        commands=(
            py_cmd(str(ROOT_DIR / "optimize_and_archive_news.py"), "--retain-days", "180", "--batch-size", "1000", "--max-batches", "50"),
        ),
    ),
    JobDefinition(
        job_key="news_dedupe_refresh",
        name="新闻语义去重",
        category="maintenance",
        schedule_expr="20 16 * * *",
        description="清理国际新闻、国内新闻、个股新闻近重复内容",
        commands=(
            py_cmd(str(ROOT_DIR / "cleanup_duplicate_items.py")),
        ),
    ),
    JobDefinition(
        job_key="db_health_check_refresh",
        name="数据库健康巡检",
        category="maintenance",
        schedule_expr="40 16 * * *",
        description="输出数据库健康检查结果",
        commands=((PYTHON_BIN, str(ROOT_DIR / "db_health_check.py")),),
    ),
    JobDefinition(
        job_key="daily_postclose_update",
        name="盘后更新流水线",
        category="market_data",
        schedule_expr="30 7 * * *",
        description="盘后刷新行情、估值、资金流、风险、财务与评分",
        commands=(
            bash_cmd(
                "python3 auto_update_stocks_and_prices.py --pause 0.02;"
                " python3 backfill_stock_valuation_daily.py --lookback-days 5 --pause 0.02;"
                " python3 backfill_capital_flow_stock.py --lookback-days 5 --pause 0.02;"
                " python3 backfill_capital_flow_stock_akshare.py --only-bj --missing-only --pause 0.05;"
                " python3 backfill_capital_flow_market.py --lookback-days 5 --pause 0.02;"
                " python3 backfill_fx_daily.py --lookback-days 10 --pause 0.02;"
                " python3 backfill_rate_curve_points.py --lookback-days 10 --pause 0.02;"
                " python3 backfill_spread_daily.py --lookback-days 10;"
                " python3 backfill_risk_scenarios.py --lookback-bars 120;"
                " python3 fast_backfill_stock_financials.py --recent-periods 4 --pause 0.02;"
                " python3 backfill_missing_stock_financials.py --recent-periods 4 --pause 0.05;"
                " python3 update_daily_stock_events.py;"
                " python3 backfill_stock_scores_daily.py --truncate-date;"
                " python3 build_stock_daily_price_rollups.py --window-days 30,90,365"
            ),
        ),
    ),
    JobDefinition(
        job_key="data_completion_nightly",
        name="夜间补数批次",
        category="maintenance",
        schedule_expr="0 17 * * *",
        description="夜间补治理、事件和评分缺口",
        commands=(
            py_cmd(
                str(ROOT_DIR / "run_data_completion_batches.py"),
                "--governance-batch",
                "50",
                "--events-batch",
                "100",
                "--rounds",
                "6",
                "--skip-flow",
                "--skip-scores",
            ),
            py_cmd(str(ROOT_DIR / "backfill_stock_scores_daily.py"), "--truncate-date"),
        ),
    ),
    JobDefinition(
        job_key="minline_backfill_recent",
        name="分钟线最近交易日补抓",
        category="market_data",
        schedule_expr="45 7 * * *",
        description="补抓最近两个交易日的分钟线",
        commands=(
            py_cmd(str(ROOT_DIR / "fetch_sina_minline_all_listed.py"), "--trade-date", "__TRADE_DATE_1__", "--workers", "6", "--min-workers", "2", "--max-workers", "10", "--retry", "2", "--batch-size", "300", "--max-rounds", "3", "--max-fail-per-stock", "5", "--stagnation-rounds", "2"),
            py_cmd(str(ROOT_DIR / "fetch_sina_minline_all_listed.py"), "--trade-date", "__TRADE_DATE_2__", "--workers", "6", "--min-workers", "2", "--max-workers", "10", "--retry", "2", "--batch-size", "300", "--max-rounds", "3", "--max-fail-per-stock", "5", "--stagnation-rounds", "2"),
        ),
    ),
    JobDefinition(
        job_key="minline_intraday_focus",
        name="分钟线盘中重点补抓",
        category="market_data",
        schedule_expr="*/10 1-3,5-7 * * 1-5",
        description="盘中围绕重点标的补抓分钟线",
        commands=(
            py_cmd(str(ROOT_DIR / "run_minline_focus_once.py"), "--limit-scores", "80", "--limit-candidates", "40", "--max-targets", "100"),
        ),
    ),
    JobDefinition(
        job_key="monitored_chatlog_backfill_midnight",
        name="监控群跨天补抓",
        category="chatrooms",
        schedule_expr="10 0 * * *",
        description="凌晨补抓昨天和今天的群聊消息",
        commands=((PYTHON_BIN, str(ROOT_DIR / "fetch_monitored_chatlogs_once.py"), "--yesterday-and-today"),),
    ),
    JobDefinition(
        job_key="market_expectations_refresh",
        name="市场预期刷新",
        category="signals",
        schedule_expr="42 * * * *",
        description="拉取 Polymarket 市场预期数据并入库",
        commands=(
            py_cmd(str(ROOT_DIR / "fetch_market_expectations_polymarket.py"), "--limit", "150", "--min-volume", "1000"),
        ),
    ),
    JobDefinition(
        job_key="theme_hotspot_refresh",
        name="主题热点引擎刷新",
        category="signals",
        schedule_expr="32 * * * *",
        description="刷新 7d 与 1d 主题热点引擎结果",
        commands=(
            py_cmd(str(ROOT_DIR / "build_theme_hotspot_engine.py"), "--lookback-days", "7", "--min-strength", "6"),
            py_cmd(
                str(ROOT_DIR / "build_theme_hotspot_engine.py"),
                "--lookback-days",
                "1",
                "--target-table",
                "theme_hotspot_tracker_1d",
                "--snapshot-table",
                "theme_daily_snapshots",
                "--evidence-table",
                "theme_evidence_items",
                "--skip-snapshot",
                "--min-strength",
                "4",
            ),
        ),
    ),
    JobDefinition(
        job_key="signal_state_refresh",
        name="信号状态机刷新",
        category="signals",
        schedule_expr="38 * * * *",
        description="刷新主题/股票状态机及事件",
        commands=((PYTHON_BIN, str(ROOT_DIR / "build_signal_state_machine.py")),),
    ),
    JobDefinition(
        job_key="fx_daily_dual_source",
        name="汇率双源补齐",
        category="macro",
        schedule_expr="35 7 * * *",
        description="使用 AKShare 对关键 FX 品种做双源补齐",
        commands=(
            py_cmd(
                str(ROOT_DIR / "backfill_fx_daily_akshare.py"),
                "--pairs",
                "USDJPY,EURUSD,DXY,USDCNY",
                "--lookback-days",
                "365",
            ),
        ),
    ),
    JobDefinition(
        job_key="minline_akshare_patch",
        name="分钟线 AKShare 补洞",
        category="market_data",
        schedule_expr="5 8 * * *",
        description="使用 AKShare 对最近交易日分钟线补洞",
        commands=(
            py_cmd(str(ROOT_DIR / "backfill_stock_minline_akshare.py"), "--trade-date", "__TRADE_DATE_1__", "--skip-existing"),
            py_cmd(str(ROOT_DIR / "backfill_stock_minline_akshare.py"), "--trade-date", "__TRADE_DATE_2__", "--skip-existing"),
        ),
    ),
    JobDefinition(
        job_key="research_reports_refresh",
        name="标准投研报告刷新",
        category="reports",
        schedule_expr="30 7,15 * * *",
        description="每日盘后与夜间补算：自动生成市场/主题/高分股票标准报告",
        commands=((PYTHON_BIN, str(ROOT_DIR / "generate_standard_research_report.py"), "--report-type", "market", "--subject-key", "market_overview", "--report-date", "__CN_DATE__", "--model", "auto_market_snapshot_v1"),),
    ),
    JobDefinition(
        job_key="chatroom_tagging_safe",
        name="群聊低风险标签刷新",
        category="chatrooms",
        schedule_expr="12 0 */3 * *",
        description="只更新监控群，并通过历史多数票稳定标签",
        commands=(
            py_cmd(
                str(ROOT_DIR / "llm_tag_chatrooms.py"),
                "--days",
                "30",
                "--limit",
                "20",
                "--monitored-only",
                "--retag-days",
                "3",
                "--min-confidence",
                "78",
                "--history-window",
                "7",
                "--majority-threshold",
                "0.62",
                "--min-majority-count",
                "3",
                "--retry",
                "2",
                "--sleep",
                "0.3",
            ),
        ),
    ),
)


def get_default_jobs() -> list[JobDefinition]:
    return list(DEFAULT_JOBS)
