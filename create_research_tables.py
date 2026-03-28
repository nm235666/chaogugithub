#!/usr/bin/env python3
from __future__ import annotations

import argparse
import db_compat as sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="创建投研扩展数据表（仅建表）")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "stock_codes.db"),
        help="PostgreSQL 主库兼容参数（默认走 PostgreSQL；仅兼容保留旧 db-path 传参）",
    )
    return parser.parse_args()


def create_tables(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON")

    # 1) 宏观指标时间序列（按指标编码+频率）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS macro_series (
            indicator_code TEXT NOT NULL,
            indicator_name TEXT,
            freq TEXT NOT NULL,                -- D/W/M/Q/Y
            period TEXT NOT NULL,              -- YYYYMMDD / YYYYMM / YYYYQn / YYYY
            value REAL,
            unit TEXT,
            source TEXT,
            publish_date TEXT,
            update_time TEXT,
            PRIMARY KEY (indicator_code, freq, period)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_macro_series_period ON macro_series(period)")

    # 2) 利率曲线/关键利率（中美等）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS rate_curve_points (
            market TEXT NOT NULL,              -- CN / US / ...
            curve_code TEXT NOT NULL,          -- gov_bond / policy_rate / shibor / ...
            trade_date TEXT NOT NULL,          -- YYYYMMDD
            tenor TEXT NOT NULL,               -- 1M / 3M / 1Y / 10Y ...
            value REAL,
            unit TEXT DEFAULT 'pct',
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (market, curve_code, trade_date, tenor)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rate_curve_date ON rate_curve_points(trade_date)"
    )

    # 3) 股票估值快照（日频）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_valuation_daily (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            pe REAL,
            pe_ttm REAL,
            pb REAL,
            ps REAL,
            ps_ttm REAL,
            dv_ratio REAL,
            dv_ttm REAL,
            total_mv REAL,
            circ_mv REAL,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (ts_code, trade_date),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_valuation_date ON stock_valuation_daily(trade_date)"
    )

    # 4) 财务报表核心指标（按公告期）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_financials (
            ts_code TEXT NOT NULL,
            report_period TEXT NOT NULL,       -- YYYYMMDD, 期末日
            report_type TEXT,                  -- annual / q1 / h1 / q3
            ann_date TEXT,
            revenue REAL,
            op_profit REAL,
            net_profit REAL,
            net_profit_excl_nr REAL,
            roe REAL,
            gross_margin REAL,
            debt_to_assets REAL,
            operating_cf REAL,
            free_cf REAL,
            eps REAL,
            bps REAL,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (ts_code, report_period),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_financials_period ON stock_financials(report_period)"
    )

    # 5) 事件数据（分红/回购/解禁/业绩预告等）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT NOT NULL,
            event_type TEXT NOT NULL,          -- dividend / buyback / unlock / guidance / ...
            event_date TEXT,
            ann_date TEXT,
            title TEXT,
            detail_json TEXT,                  -- 结构化内容JSON字符串
            source TEXT,
            update_time TEXT,
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_events_code_date ON stock_events(ts_code, event_date)"
    )

    # 6) 北向/南向资金（市场级别）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS capital_flow_market (
            trade_date TEXT NOT NULL,
            flow_type TEXT NOT NULL,           -- northbound / southbound
            net_inflow REAL,
            buy_amount REAL,
            sell_amount REAL,
            unit TEXT,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (trade_date, flow_type)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_capital_flow_date ON capital_flow_market(trade_date)"
    )

    # 7) 个股资金流（可选）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS capital_flow_stock (
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            net_inflow REAL,
            main_inflow REAL,
            super_large_inflow REAL,
            large_inflow REAL,
            medium_inflow REAL,
            small_inflow REAL,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (ts_code, trade_date),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_capital_flow_stock_date ON capital_flow_stock(trade_date)"
    )

    # 8) 汇率与美元指数
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fx_daily (
            pair_code TEXT NOT NULL,           -- USDCNY / USDJPY / DXY / CFETS / ...
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            pct_chg REAL,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (pair_code, trade_date)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fx_date ON fx_daily(trade_date)")

    # 9) 利差衍生指标（如中美10Y利差）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spread_daily (
            spread_code TEXT NOT NULL,         -- CN10Y_US10Y / CN2Y_US2Y / ...
            trade_date TEXT NOT NULL,
            value REAL,
            unit TEXT DEFAULT 'bp',
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (spread_code, trade_date)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spread_date ON spread_daily(trade_date)")

    # 10) 股票综合评分日快照
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_scores_daily (
            score_date TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            name TEXT,
            symbol TEXT,
            market TEXT,
            area TEXT,
            industry TEXT,
            industry_rank INTEGER,
            industry_count INTEGER,
            score_grade TEXT,
            industry_score_grade TEXT,
            total_score REAL,
            industry_total_score REAL,
            trend_score REAL,
            industry_trend_score REAL,
            financial_score REAL,
            industry_financial_score REAL,
            valuation_score REAL,
            industry_valuation_score REAL,
            capital_flow_score REAL,
            industry_capital_flow_score REAL,
            event_score REAL,
            industry_event_score REAL,
            news_score REAL,
            industry_news_score REAL,
            risk_score REAL,
            industry_risk_score REAL,
            latest_trade_date TEXT,
            latest_report_period TEXT,
            latest_valuation_date TEXT,
            latest_flow_date TEXT,
            latest_event_date TEXT,
            latest_news_time TEXT,
            latest_risk_date TEXT,
            score_payload_json TEXT,
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (score_date, ts_code),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_scores_daily_date ON stock_scores_daily(score_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_scores_daily_total ON stock_scores_daily(score_date, total_score DESC)"
    )

    # 11) 公司治理信息（慢频）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_governance (
            ts_code TEXT NOT NULL,
            asof_date TEXT NOT NULL,           -- YYYYMMDD
            holder_structure_json TEXT,        -- 股东结构
            board_structure_json TEXT,         -- 董监高结构
            mgmt_change_json TEXT,             -- 重大人事变动
            incentive_plan_json TEXT,          -- 激励计划
            governance_score REAL,             -- 可选治理评分
            source TEXT,
            update_time TEXT,
            PRIMARY KEY (ts_code, asof_date),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_governance_date ON company_governance(asof_date)"
    )

    # 11) 个股新闻（东方财富搜索等）
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
            UNIQUE(ts_code, source, content_hash),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_news_code_time ON stock_news_items(ts_code, pub_time)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_news_source_time ON stock_news_items(source, pub_time)"
    )

    # 12) 风控场景结果（可回填压力测试结果）
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_scenarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT,
            scenario_date TEXT NOT NULL,
            scenario_name TEXT NOT NULL,
            horizon TEXT,                      -- 5d / 20d / 3m ...
            pnl_impact REAL,
            max_drawdown REAL,
            var_95 REAL,
            cvar_95 REAL,
            assumptions_json TEXT,
            source TEXT,
            update_time TEXT,
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_risk_scenarios_date ON risk_scenarios(scenario_date)"
    )

    # 13) 投资信号追踪表
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS investment_signal_tracker (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL,
            signal_type TEXT NOT NULL,        -- stock / theme / macro / commodity / fx
            subject_name TEXT NOT NULL,
            ts_code TEXT,
            direction TEXT,                   -- 看多 / 看空 / 中性
            signal_strength REAL,
            confidence REAL,
            evidence_count INTEGER DEFAULT 0,
            news_count INTEGER DEFAULT 0,
            stock_news_count INTEGER DEFAULT 0,
            chatroom_count INTEGER DEFAULT 0,
            signal_status TEXT,               -- 活跃 / 观察 / 待证伪
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
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signal_tracker_subject ON investment_signal_tracker(subject_name)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signal_tracker_type_dir ON investment_signal_tracker(signal_type, direction)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signal_tracker_strength ON investment_signal_tracker(signal_strength DESC)"
    )

    # 14) 投资信号每日/每轮快照
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS investment_signal_daily_snapshots (
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
        "CREATE INDEX IF NOT EXISTS idx_signal_snapshots_signal_time ON investment_signal_daily_snapshots(signal_key, snapshot_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signal_snapshots_date ON investment_signal_daily_snapshots(snapshot_date)"
    )

    # 15) 投资信号演化事件
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS investment_signal_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL,
            event_time TEXT NOT NULL,
            event_date TEXT NOT NULL,
            event_type TEXT NOT NULL,         -- new_signal / strengthen / weaken / flip / falsify / revive / expire
            old_direction TEXT,
            new_direction TEXT,
            old_strength REAL,
            new_strength REAL,
            delta_strength REAL,
            old_confidence REAL,
            new_confidence REAL,
            delta_confidence REAL,
            event_level TEXT,                 -- 轻微 / 中等 / 显著 / 关键
            driver_type TEXT,                 -- news / stock_news / chatroom / price / mixed
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
        "CREATE INDEX IF NOT EXISTS idx_signal_events_signal_time ON investment_signal_events(signal_key, event_time DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signal_events_date ON investment_signal_events(event_date)"
    )

    # 16) 主题 -> 股票池映射
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_stock_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme_name TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            stock_name TEXT,
            relation_type TEXT,
            weight REAL DEFAULT 1.0,
            source TEXT,
            notes TEXT,
            created_at TEXT,
            update_time TEXT,
            UNIQUE(theme_name, ts_code),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_theme_stock_mapping_theme ON theme_stock_mapping(theme_name)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_theme_stock_mapping_code ON theme_stock_mapping(ts_code)"
    )

    # 17) 股票别名词典（群聊/新闻/LLM 归一使用）
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
            UNIQUE(alias),
            FOREIGN KEY (ts_code) REFERENCES stock_codes(ts_code)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_alias_dict_code ON stock_alias_dictionary(ts_code)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_alias_dict_alias_type ON stock_alias_dictionary(alias_type)"
    )

    # 18) 信号映射黑名单
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_mapping_blocklist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            term TEXT NOT NULL,
            target_type TEXT NOT NULL DEFAULT 'stock',
            match_type TEXT NOT NULL DEFAULT 'exact',   -- exact / contains
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
        "CREATE INDEX IF NOT EXISTS idx_signal_mapping_blocklist_target ON signal_mapping_blocklist(target_type, enabled)"
    )

    # 19) 信号质量规则
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_quality_rules (
            rule_key TEXT PRIMARY KEY,
            rule_value TEXT,
            value_type TEXT DEFAULT 'number',          -- number / bool / text
            category TEXT,
            description TEXT,
            enabled INTEGER DEFAULT 1,
            created_at TEXT,
            update_time TEXT
        )
        """
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signal_quality_rules_category ON signal_quality_rules(category, enabled)"
    )

    conn.commit()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).resolve()
    if (not sqlite3.using_postgres()) and not db_path.exists():
        raise SystemExit(f"数据库不存在: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        create_tables(conn)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        print(f"建表完成，数据库: {db_path}")
        print("当前表:")
        for (t,) in tables:
            print(f"- {t}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
