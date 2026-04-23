# 数据库数据字典

- 生成时间：`2026-04-10 12:24:57 UTC`
- 数据库：`PostgreSQL 主库`
- 表数量：`72`

## 总览

| 表名 | 中文用途 | 当前行数 | 归类 |
| --- | --- | ---: | --- |
| `stock_codes` | 股票基础信息主表，是所有股票相关数据的主索引表。 | 5824 | 股票与公司数据 |
| `stock_daily_prices` | 股票日线行情表，保存历史日线 OHLCV 数据。 | 1377367 | 股票与公司数据 |
| `stock_minline` | 股票分钟线表，保存分时/分钟级价格与成交量数据。 | 15215281 | 股票与公司数据 |
| `stock_valuation_daily` | 股票估值日频表，保存 PE/PB/PS/股息率/市值等估值快照。 | 180837 | 股票与公司数据 |
| `stock_financials` | 股票财务核心指标表，按报告期保存营收、利润、ROE、现金流等数据。 | 18132 | 股票与公司数据 |
| `stock_events` | 股票事件表，保存分红、回购、解禁、业绩预告等事件。 | 210720 | 股票与公司数据 |
| `company_governance` | 研究用治理画像表，把股东结构、董事高管结构、增减持、质押等治理数据拼成公司治理快照。 | 5501 | 股票与公司数据 |
| `capital_flow_stock` | 个股级资金流数据，用于观察单只股票主力/大单/中单/小单资金流向。 | 1342536 | 宏观、资金流与风险 |
| `capital_flow_market` | 市场级资金流数据，主要记录北向/南向等整体资金净流入情况。 | 496 | 宏观、资金流与风险 |
| `stock_scores_daily` | 股票综合评分日快照表，包含总分、分项得分和行业内相对评分。 | 54958 | 股票与公司数据 |
| `stock_news_items` | 个股相关新闻表，保存与某只股票关联的新闻、评分和摘要。 | 138052 | 股票与公司数据 |
| `news_feed_items` | 国际/国内财经快讯主表，保存抓取到的新闻原文信息及 LLM 评分结果。 | 44289 | 新闻与 LLM 新闻分析 |
| `news_feed_items_archive` | 新闻归档表，保存从主新闻表归档出去的历史新闻。 | 51 | 新闻与 LLM 新闻分析 |
| `news_daily_summaries` | 新闻日报总结表，保存对当日重要新闻的 LLM 总结 Markdown。 | 17 | 新闻与 LLM 新闻分析 |
| `macro_series` | 宏观指标时间序列表，存储 CPI、PPI、社融、PMI 等指标的历史序列。 | 513797 | 宏观、资金流与风险 |
| `fx_daily` | 汇率日线表，记录主要汇率对或指数的日频行情。 | 2875 | 宏观、资金流与风险 |
| `rate_curve_points` | 利率曲线关键点表，保存不同市场、不同期限的收益率或政策利率点位。 | 380 | 宏观、资金流与风险 |
| `spread_daily` | 利差日频表，如中美 10Y 利差等衍生指标。 | 95 | 宏观、资金流与风险 |
| `risk_scenarios` | 风险情景表，记录针对个股或资产做出的情景压力测试结果。 | 109630 | 宏观、资金流与风险 |
| `chatroom_list_items` | 群聊基础信息表，记录群 ID、备注名、成员数，以及是否继续纳入监控等状态。 | 201 | 群聊与社群分析 |
| `wechat_chatlog_clean_items` | 清洗后的群聊消息表，过滤图片/链接等无效内容后，保留可分析文本及引用结构。 | 150149 | 群聊与社群分析 |
| `chatroom_investment_analysis` | 群聊投资倾向分析结果表，存储大模型对单个群的总结、投资标的提炼和最终看多/看空结论。 | 329 | 群聊与社群分析 |
| `chatroom_stock_candidate_pool` | 由群聊投资倾向分析聚合出来的候选池表，汇总哪些股票/主题被更多群看多或看空。 | 455 | 群聊与社群分析 |
| `ai_retrieval_audit_logs` | 待补充说明 | 35 | 其他 |
| `ai_retrieval_chunks` | 待补充说明 | 11677 | 其他 |
| `ai_retrieval_documents` | 待补充说明 | 11527 | 其他 |
| `ai_retrieval_sync_state` | 待补充说明 | 3 | 其他 |
| `app_auth_audit_logs` | 待补充说明 | 810 | 其他 |
| `app_auth_email_verifications` | 待补充说明 | 1 | 其他 |
| `app_auth_invites` | 待补充说明 | 10 | 其他 |
| `app_auth_password_resets` | 待补充说明 | 1 | 其他 |
| `app_auth_role_policies` | 待补充说明 | 3 | 其他 |
| `app_auth_sessions` | 待补充说明 | 52 | 其他 |
| `app_auth_usage_daily` | 待补充说明 | 10 | 其他 |
| `app_auth_users` | 待补充说明 | 12 | 其他 |
| `chatroom_tag_history` | 待补充说明 | 100 | 群聊与社群分析 |
| `decision_actions` | 待补充说明 | 0 | 其他 |
| `decision_controls` | 待补充说明 | 1 | 其他 |
| `decision_snapshots` | 待补充说明 | 3 | 其他 |
| `decision_strategy_candidates` | 待补充说明 | 0 | 其他 |
| `decision_strategy_runs` | 待补充说明 | 1 | 其他 |
| `investment_signal_daily_snapshots` | 待补充说明 | 101381 | 其他 |
| `investment_signal_events` | 待补充说明 | 15908 | 其他 |
| `investment_signal_tracker` | 待补充说明 | 404 | 其他 |
| `investment_signal_tracker_1d` | 待补充说明 | 160 | 其他 |
| `investment_signal_tracker_7d` | 待补充说明 | 183 | 其他 |
| `job_alerts` | 待补充说明 | 282 | 其他 |
| `job_definitions` | 待补充说明 | 44 | 其他 |
| `job_runs` | 待补充说明 | 34398 | 其他 |
| `logic_view_cache` | 待补充说明 | 16534 | 其他 |
| `market_expectation_items` | 待补充说明 | 149 | 其他 |
| `market_expectation_snapshots` | 待补充说明 | 1366 | 其他 |
| `multi_role_analysis_history` | 待补充说明 | 23 | 其他 |
| `multi_role_v3_events` | 待补充说明 | 798 | 其他 |
| `multi_role_v3_jobs` | 待补充说明 | 39 | 其他 |
| `quantaalpha_backtest_results` | 待补充说明 | 24 | 其他 |
| `quantaalpha_factor_results` | 待补充说明 | 89 | 其他 |
| `quantaalpha_runs` | 待补充说明 | 81 | 其他 |
| `research_reports` | 待补充说明 | 31 | 其他 |
| `signal_mapping_blocklist` | 待补充说明 | 18 | 其他 |
| `signal_quality_rules` | 待补充说明 | 8 | 其他 |
| `signal_state_events` | 待补充说明 | 8490 | 其他 |
| `signal_state_tracker` | 待补充说明 | 182 | 其他 |
| `stock_alias_dictionary` | 待补充说明 | 7 | 股票与公司数据 |
| `stock_daily_price_rollups` | 待补充说明 | 32947 | 股票与公司数据 |
| `theme_aliases` | 待补充说明 | 126 | 其他 |
| `theme_daily_snapshots` | 待补充说明 | 406 | 其他 |
| `theme_definitions` | 待补充说明 | 29 | 其他 |
| `theme_evidence_items` | 待补充说明 | 3340 | 其他 |
| `theme_hotspot_tracker` | 待补充说明 | 28 | 其他 |
| `theme_hotspot_tracker_1d` | 待补充说明 | 28 | 其他 |
| `theme_stock_mapping` | 待补充说明 | 33 | 其他 |

## 第二层数据资产目录（四层架构）

| 数据资产域 | 核心数据表 | 资产说明 | 主要消费 API |
| --- | --- | --- | --- |
| 股票原始资产 | `stock_codes`、`stock_daily_prices`、`stock_minline` | 股票基础与行情底座 | `/api/stocks/*`、`/api/stock-scores` |
| 股票加工资产 | `stock_scores_daily`、`stock_daily_price_rollups` | 评分、分层、回溯聚合 | `/api/stock-scores`、`/api/decision/scores` |
| 新闻资产 | `news_feed_items`、`stock_news_items`、`news_daily_summaries` | 新闻主数据与结构化总结 | `/api/intelligence/*` |
| 群聊资产 | `chatroom_list_items`、`wechat_chatlog_clean_items`、`chatroom_stock_candidate_pool` | 群聊文本、候选与倾向沉淀 | `/api/chatrooms/*` |
| 信号资产 | `investment_signal_tracker*`、`signal_state_*`、`theme_*` | 主题/信号主表与状态事件 | `/api/signals/*` |
| 决策资产 | `decision_snapshots`、`decision_actions`、`funnel_candidates`、`portfolio_orders` | 决策、漏斗、执行闭环资产 | `/api/decision/*`、`/api/funnel/*`、`/api/portfolio/*` |
| 研究验证资产 | `quantaalpha_runs`、`quantaalpha_factor_results`、`quantaalpha_backtest_results` | 因子与策略验证沉淀 | `/api/quant-factors/*` |
| 治理审计资产 | `job_*`、`app_auth_*`、`signal_quality_rules` | 调度、权限、审计、治理规则 | `/api/system/*`、`/api/admin/*` |

## 展示字段到真源映射（关键口径）

| 展示字段（页面） | 真源字段 | 数据表 | 主接口 |
| --- | --- | --- | --- |
| 股票总分（评分总览） | `total_score` | `stock_scores_daily` | `/api/decision/scores` |
| 行业分（评分总览） | `industry_total_score` | `stock_scores_daily` | `/api/decision/scores` |
| 决策动作记录（决策板） | `action_type`、`action_payload_json` | `decision_actions` | `/api/decision/actions` |
| 漏斗状态（候选漏斗） | `state`、`state_version` | `funnel_candidates` | `/api/funnel/metrics`、`/api/funnel/candidates` |
| 执行状态（执行页） | `status`、`action_type` | `portfolio_orders` | `/api/portfolio/orders` |
| 方向验证命中（决策验证） | `return_5d`、`hit_5d` | `stock_daily_prices` + `decision_actions` | `/api/decision/calibration` |
| 系统运营汇总（后台） | daily 指标聚合字段 | `docs/metrics/daily/*` + 运行表 | `/api/metrics/summary` |

## 各表详解

### `stock_codes`

- 用途：股票基础信息主表，是所有股票相关数据的主索引表。
- 当前行数：`5824`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `ts_code` | 文本 | Tushare 股票代码，主键。 |
| `symbol` | 文本 | 纯数字股票代码。 |
| `name` | 文本 | 股票简称。 |
| `area` | 文本 | 所属地区。 |
| `industry` | 文本 | 所属行业。 |
| `market` | 文本 | 所属市场/板块。 |
| `list_date` | 文本 | 上市日期。 |
| `delist_date` | 文本 | 退市日期。 |
| `list_status` | 文本 | 上市状态，L=上市，D=退市，P=暂停上市。 |

### `stock_daily_prices`

- 用途：股票日线行情表，保存历史日线 OHLCV 数据。
- 当前行数：`1377367`
- 字段数：`11`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `ts_code` | 文本 | Tushare 股票代码。 |
| `trade_date` | 文本 | 交易日期。 |
| `open` | 双精度浮点 | 开盘价。 |
| `high` | 双精度浮点 | 最高价。 |
| `low` | 双精度浮点 | 最低价。 |
| `close` | 双精度浮点 | 收盘价。 |
| `pre_close` | 双精度浮点 | 前收盘价。 |
| `change` | 双精度浮点 | 涨跌额。 |
| `pct_chg` | 双精度浮点 | 涨跌幅。 |
| `vol` | 双精度浮点 | 成交量。 |
| `amount` | 双精度浮点 | 成交额。 |

### `stock_minline`

- 用途：股票分钟线表，保存分时/分钟级价格与成交量数据。
- 当前行数：`15215281`
- 字段数：`8`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `ts_code` | 文本 | Tushare 股票代码。 |
| `trade_date` | 文本 | 交易日。 |
| `minute_time` | 文本 | 分钟时间点。 |
| `price` | 双精度浮点 | 该分钟成交价/最新价。 |
| `avg_price` | 双精度浮点 | 均价。 |
| `volume` | 双精度浮点 | 该分钟成交量。 |
| `total_volume` | 双精度浮点 | 截至该分钟的累计成交量。 |
| `source` | 文本 | 数据来源标识。 |

### `stock_valuation_daily`

- 用途：股票估值日频表，保存 PE/PB/PS/股息率/市值等估值快照。
- 当前行数：`180837`
- 字段数：`13`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `ts_code` | 文本 | 股票代码。 |
| `trade_date` | 文本 | 交易日期。 |
| `pe` | 双精度浮点 | 市盈率。 |
| `pe_ttm` | 双精度浮点 | 滚动市盈率。 |
| `pb` | 双精度浮点 | 市净率。 |
| `ps` | 双精度浮点 | 市销率。 |
| `ps_ttm` | 双精度浮点 | 滚动市销率。 |
| `dv_ratio` | 双精度浮点 | 股息率。 |
| `dv_ttm` | 双精度浮点 | 滚动股息率。 |
| `total_mv` | 双精度浮点 | 总市值。 |
| `circ_mv` | 双精度浮点 | 流通市值。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 更新时间。 |

### `stock_financials`

- 用途：股票财务核心指标表，按报告期保存营收、利润、ROE、现金流等数据。
- 当前行数：`18132`
- 字段数：`17`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `ts_code` | 文本 | Tushare 股票代码。 |
| `report_period` | 文本 | 报告期末日期。 |
| `report_type` | 文本 | 报告类型，如年报、一季报、半年报、三季报。 |
| `ann_date` | 文本 | 公告日期。 |
| `revenue` | 双精度浮点 | 营业收入。 |
| `op_profit` | 双精度浮点 | 营业利润。 |
| `net_profit` | 双精度浮点 | 归母净利润。 |
| `net_profit_excl_nr` | 双精度浮点 | 扣非净利润。 |
| `roe` | 双精度浮点 | 净资产收益率。 |
| `gross_margin` | 双精度浮点 | 毛利率。 |
| `debt_to_assets` | 双精度浮点 | 资产负债率。 |
| `operating_cf` | 双精度浮点 | 经营现金流。 |
| `free_cf` | 双精度浮点 | 自由现金流。 |
| `eps` | 双精度浮点 | 每股收益。 |
| `bps` | 双精度浮点 | 每股净资产。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 更新时间。 |

### `stock_events`

- 用途：股票事件表，保存分红、回购、解禁、业绩预告等事件。
- 当前行数：`210720`
- 字段数：`10`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 主键 ID。 |
| `ts_code` | 文本 | Tushare 股票代码。 |
| `event_type` | 文本 | 事件类型，如分红、回购、解禁、业绩预告。 |
| `event_date` | 文本 | 事件生效或对应日期。 |
| `ann_date` | 文本 | 公告日期。 |
| `title` | 文本 | 事件标题。 |
| `detail_json` | 文本 | 事件详细结构化 JSON。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 更新时间。 |
| `event_key` | 文本 | 事件唯一键，用于幂等去重。 |

### `company_governance`

- 用途：研究用治理画像表，把股东结构、董事高管结构、增减持、质押等治理数据拼成公司治理快照。
- 当前行数：`5501`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `ts_code` | 文本 | Tushare 股票代码。 |
| `asof_date` | 文本 | 治理画像对应日期。 |
| `holder_structure_json` | 文本 | 股东结构画像 JSON，包括前十大股东、股东户数、质押等。 |
| `board_structure_json` | 文本 | 董事会/高管结构画像 JSON。 |
| `mgmt_change_json` | 文本 | 高管或重要股东近期增减持等变化 JSON。 |
| `incentive_plan_json` | 文本 | 股权激励计划或占位信息 JSON。 |
| `governance_score` | 双精度浮点 | 治理综合评分。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 更新时间。 |

### `capital_flow_stock`

- 用途：个股级资金流数据，用于观察单只股票主力/大单/中单/小单资金流向。
- 当前行数：`1342536`
- 字段数：`10`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `ts_code` | 文本 | Tushare 股票代码。 |
| `trade_date` | 文本 | 交易日期。 |
| `net_inflow` | 双精度浮点 | 个股总净流入金额。 |
| `main_inflow` | 双精度浮点 | 主力资金净流入金额。 |
| `super_large_inflow` | 双精度浮点 | 超大单净流入金额。 |
| `large_inflow` | 双精度浮点 | 大单净流入金额。 |
| `medium_inflow` | 双精度浮点 | 中单净流入金额。 |
| `small_inflow` | 双精度浮点 | 小单净流入金额。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 数据更新时间。 |

### `capital_flow_market`

- 用途：市场级资金流数据，主要记录北向/南向等整体资金净流入情况。
- 当前行数：`496`
- 字段数：`8`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `trade_date` | 文本 | 交易日期，格式通常为 YYYYMMDD。 |
| `flow_type` | 文本 | 资金流类型，例如北向、南向。 |
| `net_inflow` | 双精度浮点 | 净流入金额。 |
| `buy_amount` | 双精度浮点 | 买入总金额。 |
| `sell_amount` | 双精度浮点 | 卖出总金额。 |
| `unit` | 文本 | 金额单位。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 数据更新时间。 |

### `stock_scores_daily`

- 用途：股票综合评分日快照表，包含总分、分项得分和行业内相对评分。
- 当前行数：`54958`
- 字段数：`37`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `score_date` | 文本 | 评分日期。 |
| `ts_code` | 文本 | 股票代码。 |
| `name` | 文本 | 股票简称。 |
| `symbol` | 文本 | 数字代码。 |
| `market` | 文本 | 市场。 |
| `area` | 文本 | 地区。 |
| `industry` | 文本 | 行业。 |
| `score_grade` | 文本 | 综合评分等级。 |
| `total_score` | 双精度浮点 | 综合总分。 |
| `trend_score` | 双精度浮点 | 趋势分。 |
| `financial_score` | 双精度浮点 | 财务分。 |
| `valuation_score` | 双精度浮点 | 估值分。 |
| `capital_flow_score` | 双精度浮点 | 资金流分。 |
| `event_score` | 双精度浮点 | 事件分。 |
| `news_score` | 双精度浮点 | 新闻分。 |
| `risk_score` | 双精度浮点 | 风险分。 |
| `latest_trade_date` | 文本 | 使用到的最新行情日期。 |
| `latest_report_period` | 文本 | 使用到的最新财报期。 |
| `latest_valuation_date` | 文本 | 使用到的最新估值日期。 |
| `latest_flow_date` | 文本 | 使用到的最新资金流日期。 |
| `latest_event_date` | 文本 | 使用到的最新事件日期。 |
| `latest_news_time` | 文本 | 使用到的最新新闻时间。 |
| `latest_risk_date` | 文本 | 使用到的最新风险日期。 |
| `score_payload_json` | 文本 | 评分细项明细 JSON。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 更新时间。 |
| `industry_score_grade` | 文本 | 行业内相对评分等级。 |
| `industry_total_score` | 双精度浮点 | 行业内相对综合总分。 |
| `industry_trend_score` | 双精度浮点 | 行业内相对趋势分。 |
| `industry_financial_score` | 双精度浮点 | 行业内相对财务分。 |
| `industry_valuation_score` | 双精度浮点 | 行业内相对估值分。 |
| `industry_capital_flow_score` | 双精度浮点 | 行业内相对资金流分。 |
| `industry_event_score` | 双精度浮点 | 行业内相对事件分。 |
| `industry_news_score` | 双精度浮点 | 行业内相对新闻分。 |
| `industry_risk_score` | 双精度浮点 | 行业内相对风险分。 |
| `industry_rank` | 长整数 | 行业内排名。 |
| `industry_count` | 长整数 | 行业样本数。 |

### `stock_news_items`

- 用途：个股相关新闻表，保存与某只股票关联的新闻、评分和摘要。
- 当前行数：`138052`
- 字段数：`29`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 主键 ID。 |
| `ts_code` | 文本 | 关联股票代码。 |
| `company_name` | 文本 | 公司名称。 |
| `source` | 文本 | 新闻来源。 |
| `news_code` | 文本 | 新闻源内部编号。 |
| `title` | 文本 | 新闻标题。 |
| `summary` | 文本 | 新闻摘要。 |
| `link` | 文本 | 原文链接。 |
| `pub_time` | 文本 | 发布时间。 |
| `comment_num` | 长整数 | 评论数。 |
| `relation_stock_tags_json` | 文本 | 关联股票标签 JSON。 |
| `content_hash` | 文本 | 内容哈希，用于去重。 |
| `fetched_at` | 文本 | 抓取时间。 |
| `update_time` | 文本 | 更新时间。 |
| `llm_system_score` | 长整数 | 系统重要性评分。 |
| `llm_finance_impact_score` | 长整数 | 财经影响评分。 |
| `llm_finance_importance` | 文本 | 财经重要程度。 |
| `llm_impacts_json` | 文本 | 结构化影响 JSON。 |
| `llm_summary` | 文本 | 个股新闻 LLM 摘要。 |
| `llm_model` | 文本 | 评分/摘要模型。 |
| `llm_scored_at` | 文本 | 评分时间。 |
| `llm_prompt_version` | 文本 | 提示词版本。 |
| `llm_raw_output` | 文本 | 模型原始输出。 |
| `llm_sentiment_score` | 浮点 | 待补充说明 |
| `llm_sentiment_label` | 文本 | 待补充说明 |
| `llm_sentiment_reason` | 文本 | 待补充说明 |
| `llm_sentiment_confidence` | 浮点 | 待补充说明 |
| `llm_sentiment_model` | 文本 | 待补充说明 |
| `llm_sentiment_scored_at` | 文本 | 待补充说明 |

### `news_feed_items`

- 用途：国际/国内财经快讯主表，保存抓取到的新闻原文信息及 LLM 评分结果。
- 当前行数：`44289`
- 字段数：`31`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 主键 ID。 |
| `source` | 文本 | 新闻来源。 |
| `title` | 文本 | 新闻标题。 |
| `link` | 文本 | 新闻链接。 |
| `guid` | 文本 | 源站唯一标识。 |
| `summary` | 文本 | 原始摘要。 |
| `category` | 文本 | 新闻分类。 |
| `author` | 文本 | 作者。 |
| `pub_date` | 文本 | 发布时间。 |
| `fetched_at` | 文本 | 抓取时间。 |
| `content_hash` | 文本 | 内容哈希，用于去重。 |
| `llm_system_score` | 长整数 | LLM 评出的系统重要性评分。 |
| `llm_finance_impact_score` | 长整数 | LLM 评出的财经影响评分。 |
| `llm_finance_importance` | 文本 | LLM 评出的财经重要程度等级。 |
| `llm_impacts_json` | 文本 | 影响方向、资产、板块等结构化 JSON。 |
| `llm_model` | 文本 | 评分所用模型。 |
| `llm_scored_at` | 文本 | 评分时间。 |
| `llm_prompt_version` | 文本 | 新闻评分提示词版本。 |
| `llm_raw_output` | 文本 | 模型原始输出。 |
| `related_ts_codes_json` | 文本 | 待补充说明 |
| `related_stock_names_json` | 文本 | 待补充说明 |
| `stock_match_version` | 文本 | 待补充说明 |
| `stock_mapped_at` | 文本 | 待补充说明 |
| `llm_direct_related_ts_codes_json` | 文本 | 待补充说明 |
| `llm_direct_related_stock_names_json` | 文本 | 待补充说明 |
| `llm_sentiment_score` | 浮点 | 待补充说明 |
| `llm_sentiment_label` | 文本 | 待补充说明 |
| `llm_sentiment_reason` | 文本 | 待补充说明 |
| `llm_sentiment_confidence` | 浮点 | 待补充说明 |
| `llm_sentiment_model` | 文本 | 待补充说明 |
| `llm_sentiment_scored_at` | 文本 | 待补充说明 |

### `news_feed_items_archive`

- 用途：新闻归档表，保存从主新闻表归档出去的历史新闻。
- 当前行数：`51`
- 字段数：`32`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 原新闻主键 ID。 |
| `source` | 文本 | 新闻来源。 |
| `title` | 文本 | 新闻标题。 |
| `link` | 文本 | 新闻链接。 |
| `guid` | 文本 | 源站唯一标识。 |
| `summary` | 文本 | 原始摘要。 |
| `category` | 文本 | 新闻分类。 |
| `author` | 文本 | 作者。 |
| `pub_date` | 文本 | 发布时间。 |
| `fetched_at` | 文本 | 抓取时间。 |
| `content_hash` | 文本 | 内容哈希。 |
| `llm_system_score` | 长整数 | 系统重要性评分。 |
| `llm_finance_impact_score` | 长整数 | 财经影响评分。 |
| `llm_finance_importance` | 文本 | 财经重要程度。 |
| `llm_impacts_json` | 文本 | 结构化影响 JSON。 |
| `llm_model` | 文本 | 评分模型。 |
| `llm_scored_at` | 文本 | 评分时间。 |
| `llm_prompt_version` | 文本 | 提示词版本。 |
| `llm_raw_output` | 文本 | 模型原始输出。 |
| `archived_at` | 文本 | 归档时间。 |
| `related_ts_codes_json` | 文本 | 待补充说明 |
| `related_stock_names_json` | 文本 | 待补充说明 |
| `stock_match_version` | 文本 | 待补充说明 |
| `stock_mapped_at` | 文本 | 待补充说明 |
| `llm_direct_related_ts_codes_json` | 文本 | 待补充说明 |
| `llm_direct_related_stock_names_json` | 文本 | 待补充说明 |
| `llm_sentiment_score` | 浮点 | 待补充说明 |
| `llm_sentiment_label` | 文本 | 待补充说明 |
| `llm_sentiment_reason` | 文本 | 待补充说明 |
| `llm_sentiment_confidence` | 浮点 | 待补充说明 |
| `llm_sentiment_model` | 文本 | 待补充说明 |
| `llm_sentiment_scored_at` | 文本 | 待补充说明 |

### `news_daily_summaries`

- 用途：新闻日报总结表，保存对当日重要新闻的 LLM 总结 Markdown。
- 当前行数：`17`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 主键 ID。 |
| `summary_date` | 文本 | 日报日期。 |
| `filter_importance` | 文本 | 汇总时采用的重要度筛选条件。 |
| `source_filter` | 文本 | 汇总时采用的数据源筛选条件。 |
| `news_count` | 长整数 | 参与总结的新闻条数。 |
| `model` | 文本 | 使用的总结模型。 |
| `prompt_version` | 文本 | 提示词版本。 |
| `summary_markdown` | 文本 | 新闻日报总结 Markdown 内容。 |
| `created_at` | 文本 | 创建时间。 |

### `macro_series`

- 用途：宏观指标时间序列表，存储 CPI、PPI、社融、PMI 等指标的历史序列。
- 当前行数：`513797`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `indicator_code` | 文本 | 指标代码。 |
| `indicator_name` | 文本 | 指标中文/展示名称。 |
| `freq` | 文本 | 频率，如日、周、月、季、年。 |
| `period` | 文本 | 统计周期。 |
| `value` | 双精度浮点 | 指标值。 |
| `unit` | 文本 | 单位。 |
| `source` | 文本 | 数据来源标识。 |
| `publish_date` | 文本 | 发布日期。 |
| `update_time` | 文本 | 更新时间。 |

### `fx_daily`

- 用途：汇率日线表，记录主要汇率对或指数的日频行情。
- 当前行数：`2875`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `pair_code` | 文本 | 汇率对或指数代码，如 USDCNY、DXY。 |
| `trade_date` | 文本 | 交易日期。 |
| `open` | 双精度浮点 | 开盘价。 |
| `high` | 双精度浮点 | 最高价。 |
| `low` | 双精度浮点 | 最低价。 |
| `close` | 双精度浮点 | 收盘价。 |
| `pct_chg` | 双精度浮点 | 涨跌幅。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 更新时间。 |

### `rate_curve_points`

- 用途：利率曲线关键点表，保存不同市场、不同期限的收益率或政策利率点位。
- 当前行数：`380`
- 字段数：`8`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `market` | 文本 | 市场标识，如 CN、US。 |
| `curve_code` | 文本 | 曲线代码，如国债收益率、政策利率、Shibor。 |
| `trade_date` | 文本 | 交易日期。 |
| `tenor` | 文本 | 期限，如 1M、3M、10Y。 |
| `value` | 双精度浮点 | 点位值。 |
| `unit` | 文本 | 单位，通常为百分比。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 更新时间。 |

### `spread_daily`

- 用途：利差日频表，如中美 10Y 利差等衍生指标。
- 当前行数：`95`
- 字段数：`6`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `spread_code` | 文本 | 利差代码，如 CN10Y_US10Y。 |
| `trade_date` | 文本 | 交易日期。 |
| `value` | 双精度浮点 | 利差值。 |
| `unit` | 文本 | 单位，通常为 bp。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 更新时间。 |

### `risk_scenarios`

- 用途：风险情景表，记录针对个股或资产做出的情景压力测试结果。
- 当前行数：`109630`
- 字段数：`12`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 主键 ID。 |
| `ts_code` | 文本 | Tushare 股票代码。 |
| `scenario_date` | 文本 | 情景日期。 |
| `scenario_name` | 文本 | 情景名称。 |
| `horizon` | 文本 | 情景观察期限。 |
| `pnl_impact` | 双精度浮点 | 预估盈亏影响。 |
| `max_drawdown` | 双精度浮点 | 最大回撤估计。 |
| `var_95` | 双精度浮点 | 95% VaR。 |
| `cvar_95` | 双精度浮点 | 95% 条件 VaR。 |
| `assumptions_json` | 文本 | 情景假设 JSON。 |
| `source` | 文本 | 数据来源标识。 |
| `update_time` | 文本 | 更新时间。 |

### `chatroom_list_items`

- 用途：群聊基础信息表，记录群 ID、备注名、成员数，以及是否继续纳入监控等状态。
- 当前行数：`201`
- 字段数：`31`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `room_id` | 文本 | 群聊唯一 ID。 |
| `remark` | 文本 | 群备注名。 |
| `nick_name` | 文本 | 群昵称。 |
| `owner` | 文本 | 群主或拥有者标识。 |
| `user_count` | 长整数 | 群成员数量。 |
| `source_url` | 文本 | 群列表来源接口。 |
| `raw_csv_row` | 文本 | 原始 CSV 行内容。 |
| `first_seen_at` | 文本 | 首次被抓到的时间。 |
| `last_seen_at` | 文本 | 最近一次在群列表中出现的时间。 |
| `update_time` | 文本 | 更新时间。 |
| `skip_realtime_monitor` | 长整数 | 是否跳过实时监控，1 表示跳过。 |
| `skip_realtime_reason` | 文本 | 跳过实时监控的原因。 |
| `skip_realtime_marked_at` | 文本 | 被标记为跳过监控的时间。 |
| `last_message_date` | 文本 | 最近一条聊天记录日期。 |
| `last_chatlog_backfill_at` | 文本 | 最近一次聊天记录补抓时间。 |
| `last_chatlog_backfill_status` | 文本 | 最近一次聊天记录补抓状态。 |
| `last_30d_raw_message_count` | 长整数 | 最近 30 天原始消息条数。 |
| `last_30d_clean_message_count` | 长整数 | 最近 30 天清洗后消息条数。 |
| `last_30d_fetch_fail_count` | 长整数 | 最近 30 天拉取失败次数。 |
| `silent_candidate_runs` | 长整数 | 连续被判定为沉默群的次数。 |
| `silent_candidate_since` | 文本 | 开始被判定为沉默候选群的时间。 |
| `llm_chatroom_summary` | 文本 | LLM 生成的群聊简介。 |
| `llm_chatroom_tags_json` | 文本 | LLM 打的分类标签 JSON。 |
| `llm_chatroom_primary_category` | 文本 | LLM 判断的主分类，如投资交易。 |
| `llm_chatroom_activity_level` | 文本 | 群活跃度等级。 |
| `llm_chatroom_risk_level` | 文本 | 群风险等级。 |
| `llm_chatroom_confidence` | 长整数 | LLM 分类置信度。 |
| `llm_chatroom_model` | 文本 | 用于打标签的模型。 |
| `llm_chatroom_tagged_at` | 文本 | 最近一次打标签时间。 |
| `llm_chatroom_prompt_version` | 文本 | 群聊标签提示词版本。 |
| `llm_chatroom_raw_output` | 文本 | 群聊标签模型原始输出。 |

### `wechat_chatlog_clean_items`

- 用途：清洗后的群聊消息表，过滤图片/链接等无效内容后，保留可分析文本及引用结构。
- 当前行数：`150149`
- 字段数：`21`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 主键 ID。 |
| `talker` | 文本 | 群聊名称。 |
| `query_date_start` | 文本 | 本次抓取请求的起始日期。 |
| `query_date_end` | 文本 | 本次抓取请求的结束日期。 |
| `message_date` | 文本 | 消息日期。 |
| `message_time` | 文本 | 消息时间。 |
| `sender_name` | 文本 | 发送人昵称。 |
| `sender_id` | 文本 | 发送人 ID。 |
| `message_type` | 文本 | 消息类型，如文本、引用、系统消息。 |
| `content` | 文本 | 原始消息内容。 |
| `content_clean` | 文本 | 清洗后的消息正文。 |
| `is_quote` | 长整数 | 是否为引用消息，1 表示是。 |
| `quote_sender_name` | 文本 | 被引用消息发送人。 |
| `quote_sender_id` | 文本 | 被引用消息发送人 ID。 |
| `quote_time_text` | 文本 | 被引用消息时间文本。 |
| `quote_content` | 文本 | 被引用的消息内容。 |
| `raw_block` | 文本 | 抓取回来的原始消息块。 |
| `source_url` | 文本 | 聊天记录来源接口。 |
| `fetched_at` | 文本 | 抓取时间。 |
| `update_time` | 文本 | 更新时间。 |
| `message_key` | 文本 | 消息唯一键，用于去重。 |

### `chatroom_investment_analysis`

- 用途：群聊投资倾向分析结果表，存储大模型对单个群的总结、投资标的提炼和最终看多/看空结论。
- 当前行数：`329`
- 字段数：`22`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 主键 ID。 |
| `room_id` | 文本 | 群聊唯一 ID。 |
| `talker` | 文本 | 群聊展示名/查询名。 |
| `analysis_date` | 文本 | 本次分析对应的消息最新日期。 |
| `analysis_window_days` | 长整数 | 分析窗口天数。 |
| `message_count` | 长整数 | 本次分析使用的消息条数。 |
| `sender_count` | 长整数 | 参与发言人数。 |
| `latest_message_date` | 文本 | 本次分析覆盖到的最新消息日期。 |
| `room_summary` | 文本 | 群聊内容摘要。 |
| `targets_json` | 文本 | 提炼出的投资标的及其看多/看空 JSON。 |
| `final_bias` | 文本 | 群整体最终投资倾向，仅看多或看空。 |
| `model` | 文本 | 使用的大模型名称。 |
| `prompt_version` | 文本 | 提示词版本。 |
| `raw_output` | 文本 | 模型原始输出。 |
| `created_at` | 文本 | 创建时间。 |
| `update_time` | 文本 | 更新时间。 |
| `llm_sentiment_score` | 浮点 | 待补充说明 |
| `llm_sentiment_label` | 文本 | 待补充说明 |
| `llm_sentiment_reason` | 文本 | 待补充说明 |
| `llm_sentiment_confidence` | 浮点 | 待补充说明 |
| `llm_sentiment_model` | 文本 | 待补充说明 |
| `llm_sentiment_scored_at` | 文本 | 待补充说明 |

### `chatroom_stock_candidate_pool`

- 用途：由群聊投资倾向分析聚合出来的候选池表，汇总哪些股票/主题被更多群看多或看空。
- 当前行数：`455`
- 字段数：`19`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 主键 ID。 |
| `candidate_name` | 文本 | 候选标的名称，可能是股票、主题或资产。 |
| `candidate_type` | 文本 | 候选类型，如股票、主题、标的。 |
| `bullish_room_count` | 长整数 | 看多该标的的群数量。 |
| `bearish_room_count` | 长整数 | 看空该标的的群数量。 |
| `net_score` | 长整数 | 净分值，通常为看多群数减看空群数。 |
| `dominant_bias` | 文本 | 主导方向，看多或看空。 |
| `mention_count` | 长整数 | 总提及次数。 |
| `room_count` | 长整数 | 涉及该标的的群数量。 |
| `latest_analysis_date` | 文本 | 最近一次相关分析日期。 |
| `sample_reasons_json` | 文本 | 样例理由 JSON。 |
| `source_room_ids_json` | 文本 | 来源群 ID 列表 JSON。 |
| `source_talkers_json` | 文本 | 来源群名称列表 JSON。 |
| `created_at` | 文本 | 创建时间。 |
| `update_time` | 文本 | 更新时间。 |
| `ts_code` | 文本 | 待补充说明 |
| `alias_hit_name` | 文本 | 待补充说明 |
| `alias_source` | 文本 | 待补充说明 |
| `alias_confidence` | 浮点 | 待补充说明 |

### `ai_retrieval_audit_logs`

- 用途：待补充说明
- 当前行数：`35`
- 字段数：`12`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `request_type` | 文本 | 待补充说明 |
| `scene` | 文本 | 待补充说明 |
| `query_text` | 文本 | 待补充说明 |
| `top_k` | 整数 | 待补充说明 |
| `hit_count` | 整数 | 待补充说明 |
| `empty_recall` | 整数 | 待补充说明 |
| `latency_ms` | 整数 | 待补充说明 |
| `used_model` | 文本 | 待补充说明 |
| `attempts_json` | 文本 | 待补充说明 |
| `trace_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `ai_retrieval_chunks`

- 用途：待补充说明
- 当前行数：`11677`
- 字段数：`12`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `doc_id` | 整数 | 待补充说明 |
| `scene` | 文本 | 待补充说明 |
| `source_type` | 文本 | 待补充说明 |
| `source_id` | 文本 | 待补充说明 |
| `chunk_index` | 整数 | 待补充说明 |
| `chunk_text` | 文本 | 待补充说明 |
| `embedding_model` | 文本 | 待补充说明 |
| `embedding_dims` | 整数 | 待补充说明 |
| `embedding_vector` | USER-DEFINED | 待补充说明 |
| `metadata_json` | 文本 | 待补充说明 |
| `updated_at` | 文本 | 待补充说明 |

### `ai_retrieval_documents`

- 用途：待补充说明
- 当前行数：`11527`
- 字段数：`10`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `source_type` | 文本 | 待补充说明 |
| `source_id` | 文本 | 待补充说明 |
| `scene` | 文本 | 待补充说明 |
| `title` | 文本 | 待补充说明 |
| `content` | 文本 | 待补充说明 |
| `published_at` | 文本 | 待补充说明 |
| `metadata_json` | 文本 | 待补充说明 |
| `content_hash` | 文本 | 待补充说明 |
| `updated_at` | 文本 | 待补充说明 |

### `ai_retrieval_sync_state`

- 用途：待补充说明
- 当前行数：`3`
- 字段数：`5`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `scene` | 文本 | 待补充说明 |
| `source_type` | 文本 | 待补充说明 |
| `last_success_at` | 文本 | 待补充说明 |
| `last_error` | 文本 | 待补充说明 |
| `updated_at` | 文本 | 待补充说明 |

### `app_auth_audit_logs`

- 用途：待补充说明
- 当前行数：`810`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `event_type` | 文本 | 待补充说明 |
| `username` | 文本 | 待补充说明 |
| `user_id` | 整数 | 待补充说明 |
| `result` | 文本 | 待补充说明 |
| `detail` | 文本 | 待补充说明 |
| `ip` | 文本 | 待补充说明 |
| `user_agent` | 文本 | 待补充说明 |
| `created_at` | timestamp without time zone | 待补充说明 |

### `app_auth_email_verifications`

- 用途：待补充说明
- 当前行数：`1`
- 字段数：`7`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `user_id` | 整数 | 待补充说明 |
| `email` | 文本 | 待补充说明 |
| `verify_code` | 文本 | 待补充说明 |
| `expires_at` | timestamp without time zone | 待补充说明 |
| `used_at` | timestamp without time zone | 待补充说明 |
| `created_at` | timestamp without time zone | 待补充说明 |

### `app_auth_invites`

- 用途：待补充说明
- 当前行数：`10`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `invite_code` | 文本 | 待补充说明 |
| `max_uses` | 整数 | 待补充说明 |
| `used_count` | 整数 | 待补充说明 |
| `expires_at` | timestamp without time zone | 待补充说明 |
| `is_active` | 整数 | 待补充说明 |
| `created_by` | 文本 | 待补充说明 |
| `created_at` | timestamp without time zone | 待补充说明 |
| `updated_at` | timestamp without time zone | 待补充说明 |

### `app_auth_password_resets`

- 用途：待补充说明
- 当前行数：`1`
- 字段数：`7`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `user_id` | 整数 | 待补充说明 |
| `username` | 文本 | 待补充说明 |
| `reset_code` | 文本 | 待补充说明 |
| `expires_at` | timestamp without time zone | 待补充说明 |
| `used_at` | timestamp without time zone | 待补充说明 |
| `created_at` | timestamp without time zone | 待补充说明 |

### `app_auth_role_policies`

- 用途：待补充说明
- 当前行数：`3`
- 字段数：`7`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `role` | 文本 | 待补充说明 |
| `permissions_json` | 文本 | 待补充说明 |
| `trend_daily_limit` | 整数 | 待补充说明 |
| `multi_role_daily_limit` | 整数 | 待补充说明 |
| `created_at` | timestamp without time zone | 待补充说明 |
| `updated_at` | timestamp without time zone | 待补充说明 |

### `app_auth_sessions`

- 用途：待补充说明
- 当前行数：`52`
- 字段数：`6`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `user_id` | 整数 | 待补充说明 |
| `session_token_hash` | 文本 | 待补充说明 |
| `expires_at` | timestamp without time zone | 待补充说明 |
| `created_at` | timestamp without time zone | 待补充说明 |
| `last_seen_at` | timestamp without time zone | 待补充说明 |

### `app_auth_usage_daily`

- 用途：待补充说明
- 当前行数：`10`
- 字段数：`7`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `user_id` | 整数 | 待补充说明 |
| `usage_date` | 文本 | 待补充说明 |
| `trend_count` | 整数 | 待补充说明 |
| `created_at` | timestamp without time zone | 待补充说明 |
| `updated_at` | timestamp without time zone | 待补充说明 |
| `multi_role_count` | 整数 | 待补充说明 |

### `app_auth_users`

- 用途：待补充说明
- 当前行数：`12`
- 字段数：`15`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `username` | 文本 | 待补充说明 |
| `password_hash` | 文本 | 待补充说明 |
| `display_name` | 文本 | 待补充说明 |
| `is_active` | 整数 | 待补充说明 |
| `created_at` | timestamp without time zone | 待补充说明 |
| `updated_at` | timestamp without time zone | 待补充说明 |
| `email` | 文本 | 待补充说明 |
| `email_verified` | 整数 | 待补充说明 |
| `role` | 文本 | 待补充说明 |
| `tier` | 文本 | 待补充说明 |
| `invite_code_used` | 文本 | 待补充说明 |
| `failed_login_count` | 整数 | 待补充说明 |
| `locked_until` | timestamp without time zone | 待补充说明 |
| `last_login_at` | timestamp without time zone | 待补充说明 |

### `chatroom_tag_history`

- 用途：待补充说明
- 当前行数：`100`
- 字段数：`19`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `room_id` | 文本 | 待补充说明 |
| `talker` | 文本 | 待补充说明 |
| `sample_start_date` | 文本 | 待补充说明 |
| `sample_end_date` | 文本 | 待补充说明 |
| `sample_message_count` | 整数 | 待补充说明 |
| `proposed_primary_category` | 文本 | 待补充说明 |
| `proposed_activity_level` | 文本 | 待补充说明 |
| `proposed_risk_level` | 文本 | 待补充说明 |
| `proposed_confidence` | 整数 | 待补充说明 |
| `proposed_summary` | 文本 | 待补充说明 |
| `proposed_tags_json` | 文本 | 待补充说明 |
| `all_tags_text` | 文本 | 待补充说明 |
| `model` | 文本 | 待补充说明 |
| `prompt_version` | 文本 | 待补充说明 |
| `raw_output` | 文本 | 待补充说明 |
| `is_applied` | 整数 | 待补充说明 |
| `apply_reason` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `decision_actions`

- 用途：待补充说明
- 当前行数：`0`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `action_type` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `stock_name` | 文本 | 待补充说明 |
| `note` | 文本 | 待补充说明 |
| `actor` | 文本 | 待补充说明 |
| `snapshot_date` | 文本 | 待补充说明 |
| `action_payload_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `decision_controls`

- 用途：待补充说明
- 当前行数：`1`
- 字段数：`4`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `control_key` | 文本 | 待补充说明 |
| `allow_trading` | 整数 | 待补充说明 |
| `reason` | 文本 | 待补充说明 |
| `updated_at` | 文本 | 待补充说明 |

### `decision_snapshots`

- 用途：待补充说明
- 当前行数：`3`
- 字段数：`6`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `snapshot_date` | 文本 | 待补充说明 |
| `snapshot_type` | 文本 | 待补充说明 |
| `payload_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `updated_at` | 文本 | 待补充说明 |

### `decision_strategy_candidates`

- 用途：待补充说明
- 当前行数：`0`
- 字段数：`28`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `run_id` | 整数 | 待补充说明 |
| `run_key` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `keyword` | 文本 | 待补充说明 |
| `rank` | 整数 | 待补充说明 |
| `priority` | 整数 | 待补充说明 |
| `name` | 文本 | 待补充说明 |
| `mode` | 文本 | 待补充说明 |
| `status` | 文本 | 待补充说明 |
| `fit_score` | 浮点 | 待补充说明 |
| `llm_feasibility_score` | 浮点 | 待补充说明 |
| `llm_feasibility_label` | 文本 | 待补充说明 |
| `llm_explanation` | 文本 | 待补充说明 |
| `llm_risk_note` | 文本 | 待补充说明 |
| `llm_name_hint` | 文本 | 待补充说明 |
| `summary` | 文本 | 待补充说明 |
| `entry_rule` | 文本 | 待补充说明 |
| `exit_rule` | 文本 | 待补充说明 |
| `position_bias` | 文本 | 待补充说明 |
| `universe` | 文本 | 待补充说明 |
| `rationale` | 文本 | 待补充说明 |
| `risk_control` | 文本 | 待补充说明 |
| `linked_industries_json` | 文本 | 待补充说明 |
| `linked_stocks_json` | 文本 | 待补充说明 |
| `candidate_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `updated_at` | 文本 | 待补充说明 |

### `decision_strategy_runs`

- 用途：待补充说明
- 当前行数：`1`
- 字段数：`18`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `run_key` | 文本 | 待补充说明 |
| `run_version` | 整数 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `keyword` | 文本 | 待补充说明 |
| `title` | 文本 | 待补充说明 |
| `status` | 文本 | 待补充说明 |
| `source_mode` | 文本 | 待补充说明 |
| `generator_mode` | 文本 | 待补充说明 |
| `llm_model` | 文本 | 待补充说明 |
| `llm_enabled` | 整数 | 待补充说明 |
| `market_mode` | 文本 | 待补充说明 |
| `approval_state` | 文本 | 待补充说明 |
| `summary_json` | 文本 | 待补充说明 |
| `board_snapshot_json` | 文本 | 待补充说明 |
| `generator_rules_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `updated_at` | 文本 | 待补充说明 |

### `investment_signal_daily_snapshots`

- 用途：待补充说明
- 当前行数：`101381`
- 字段数：`19`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `snapshot_at` | 文本 | 待补充说明 |
| `snapshot_date` | 文本 | 待补充说明 |
| `signal_key` | 文本 | 待补充说明 |
| `signal_type` | 文本 | 待补充说明 |
| `subject_name` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `direction` | 文本 | 待补充说明 |
| `signal_strength` | 浮点 | 待补充说明 |
| `confidence` | 浮点 | 待补充说明 |
| `evidence_count` | 整数 | 待补充说明 |
| `news_count` | 整数 | 待补充说明 |
| `stock_news_count` | 整数 | 待补充说明 |
| `chatroom_count` | 整数 | 待补充说明 |
| `signal_status` | 文本 | 待补充说明 |
| `latest_signal_date` | 文本 | 待补充说明 |
| `evidence_json` | 文本 | 待补充说明 |
| `source_summary_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `investment_signal_events`

- 用途：待补充说明
- 当前行数：`15908`
- 字段数：`24`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `signal_key` | 文本 | 待补充说明 |
| `event_time` | 文本 | 待补充说明 |
| `event_date` | 文本 | 待补充说明 |
| `event_type` | 文本 | 待补充说明 |
| `old_direction` | 文本 | 待补充说明 |
| `new_direction` | 文本 | 待补充说明 |
| `old_strength` | 浮点 | 待补充说明 |
| `new_strength` | 浮点 | 待补充说明 |
| `delta_strength` | 浮点 | 待补充说明 |
| `old_confidence` | 浮点 | 待补充说明 |
| `new_confidence` | 浮点 | 待补充说明 |
| `delta_confidence` | 浮点 | 待补充说明 |
| `event_level` | 文本 | 待补充说明 |
| `driver_type` | 文本 | 待补充说明 |
| `driver_source` | 文本 | 待补充说明 |
| `driver_ref_id` | 文本 | 待补充说明 |
| `driver_title` | 文本 | 待补充说明 |
| `status_after_event` | 文本 | 待补充说明 |
| `event_summary` | 文本 | 待补充说明 |
| `evidence_json` | 文本 | 待补充说明 |
| `snapshot_before_json` | 文本 | 待补充说明 |
| `snapshot_after_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `investment_signal_tracker`

- 用途：待补充说明
- 当前行数：`404`
- 字段数：`18`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `signal_key` | 文本 | 待补充说明 |
| `signal_type` | 文本 | 待补充说明 |
| `subject_name` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `direction` | 文本 | 待补充说明 |
| `signal_strength` | 浮点 | 待补充说明 |
| `confidence` | 浮点 | 待补充说明 |
| `evidence_count` | 整数 | 待补充说明 |
| `news_count` | 整数 | 待补充说明 |
| `stock_news_count` | 整数 | 待补充说明 |
| `chatroom_count` | 整数 | 待补充说明 |
| `signal_status` | 文本 | 待补充说明 |
| `latest_signal_date` | 文本 | 待补充说明 |
| `evidence_json` | 文本 | 待补充说明 |
| `source_summary_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `investment_signal_tracker_1d`

- 用途：待补充说明
- 当前行数：`160`
- 字段数：`18`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `signal_key` | 文本 | 待补充说明 |
| `signal_type` | 文本 | 待补充说明 |
| `subject_name` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `direction` | 文本 | 待补充说明 |
| `signal_strength` | 浮点 | 待补充说明 |
| `confidence` | 浮点 | 待补充说明 |
| `evidence_count` | 整数 | 待补充说明 |
| `news_count` | 整数 | 待补充说明 |
| `stock_news_count` | 整数 | 待补充说明 |
| `chatroom_count` | 整数 | 待补充说明 |
| `signal_status` | 文本 | 待补充说明 |
| `latest_signal_date` | 文本 | 待补充说明 |
| `evidence_json` | 文本 | 待补充说明 |
| `source_summary_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `investment_signal_tracker_7d`

- 用途：待补充说明
- 当前行数：`183`
- 字段数：`18`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `signal_key` | 文本 | 待补充说明 |
| `signal_type` | 文本 | 待补充说明 |
| `subject_name` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `direction` | 文本 | 待补充说明 |
| `signal_strength` | 浮点 | 待补充说明 |
| `confidence` | 浮点 | 待补充说明 |
| `evidence_count` | 整数 | 待补充说明 |
| `news_count` | 整数 | 待补充说明 |
| `stock_news_count` | 整数 | 待补充说明 |
| `chatroom_count` | 整数 | 待补充说明 |
| `signal_status` | 文本 | 待补充说明 |
| `latest_signal_date` | 文本 | 待补充说明 |
| `evidence_json` | 文本 | 待补充说明 |
| `source_summary_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `job_alerts`

- 用途：待补充说明
- 当前行数：`282`
- 字段数：`10`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `job_key` | 文本 | 待补充说明 |
| `run_id` | 整数 | 待补充说明 |
| `severity` | 文本 | 待补充说明 |
| `message` | 文本 | 待补充说明 |
| `detail_text` | 文本 | 待补充说明 |
| `acknowledged` | 整数 | 待补充说明 |
| `acknowledged_at` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `job_definitions`

- 用途：待补充说明
- 当前行数：`44`
- 字段数：`10`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `job_key` | 文本 | 待补充说明 |
| `name` | 文本 | 待补充说明 |
| `category` | 文本 | 待补充说明 |
| `owner` | 文本 | 待补充说明 |
| `schedule_expr` | 文本 | 待补充说明 |
| `description` | 文本 | 待补充说明 |
| `enabled` | 整数 | 待补充说明 |
| `commands_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `job_runs`

- 用途：待补充说明
- 当前行数：`34398`
- 字段数：`17`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `job_key` | 文本 | 待补充说明 |
| `trigger_mode` | 文本 | 待补充说明 |
| `status` | 文本 | 待补充说明 |
| `started_at` | 文本 | 待补充说明 |
| `finished_at` | 文本 | 待补充说明 |
| `duration_seconds` | 浮点 | 待补充说明 |
| `host_name` | 文本 | 待补充说明 |
| `process_id` | 整数 | 待补充说明 |
| `exit_code` | 整数 | 待补充说明 |
| `command_json` | 文本 | 待补充说明 |
| `stdout_text` | 文本 | 待补充说明 |
| `stderr_text` | 文本 | 待补充说明 |
| `error_text` | 文本 | 待补充说明 |
| `metadata_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `logic_view_cache`

- 用途：待补充说明
- 当前行数：`16534`
- 字段数：`6`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `entity_type` | 文本 | 待补充说明 |
| `entity_key` | 文本 | 待补充说明 |
| `content_hash` | 文本 | 待补充说明 |
| `logic_view_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `market_expectation_items`

- 用途：待补充说明
- 当前行数：`149`
- 字段数：`21`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `source` | 文本 | 待补充说明 |
| `market_id` | 文本 | 待补充说明 |
| `question` | 文本 | 待补充说明 |
| `slug` | 文本 | 待补充说明 |
| `active` | 整数 | 待补充说明 |
| `closed` | 整数 | 待补充说明 |
| `end_date` | 文本 | 待补充说明 |
| `liquidity` | 浮点 | 待补充说明 |
| `volume` | 浮点 | 待补充说明 |
| `volume_24h` | 浮点 | 待补充说明 |
| `best_bid` | 浮点 | 待补充说明 |
| `best_ask` | 浮点 | 待补充说明 |
| `last_trade_price` | 浮点 | 待补充说明 |
| `outcomes_json` | 文本 | 待补充说明 |
| `outcome_prices_json` | 文本 | 待补充说明 |
| `related_theme_names_json` | 文本 | 待补充说明 |
| `source_url` | 文本 | 待补充说明 |
| `raw_json` | 文本 | 待补充说明 |
| `fetched_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `market_expectation_snapshots`

- 用途：待补充说明
- 当前行数：`1366`
- 字段数：`12`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `snapshot_at` | 文本 | 待补充说明 |
| `market_id` | 文本 | 待补充说明 |
| `question` | 文本 | 待补充说明 |
| `active` | 整数 | 待补充说明 |
| `closed` | 整数 | 待补充说明 |
| `volume` | 浮点 | 待补充说明 |
| `liquidity` | 浮点 | 待补充说明 |
| `last_trade_price` | 浮点 | 待补充说明 |
| `outcome_prices_json` | 文本 | 待补充说明 |
| `related_theme_names_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `multi_role_analysis_history`

- 用途：待补充说明
- 当前行数：`23`
- 字段数：`21`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `job_id` | 文本 | 待补充说明 |
| `version` | 文本 | 待补充说明 |
| `status` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `name` | 文本 | 待补充说明 |
| `lookback` | 整数 | 待补充说明 |
| `roles_json` | 文本 | 待补充说明 |
| `accept_auto_degrade` | 整数 | 待补充说明 |
| `requested_model` | 文本 | 待补充说明 |
| `used_model` | 文本 | 待补充说明 |
| `attempts_json` | 文本 | 待补充说明 |
| `role_runs_json` | 文本 | 待补充说明 |
| `aggregator_run_json` | 文本 | 待补充说明 |
| `decision_state_json` | 文本 | 待补充说明 |
| `warnings_json` | 文本 | 待补充说明 |
| `error` | 文本 | 待补充说明 |
| `analysis_markdown` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `updated_at` | 文本 | 待补充说明 |
| `finished_at` | 文本 | 待补充说明 |

### `multi_role_v3_events`

- 用途：待补充说明
- 当前行数：`798`
- 字段数：`6`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `job_id` | 文本 | 待补充说明 |
| `stage` | 文本 | 待补充说明 |
| `event_type` | 文本 | 待补充说明 |
| `payload_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `multi_role_v3_jobs`

- 用途：待补充说明
- 当前行数：`39`
- 字段数：`17`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `job_id` | 文本 | 待补充说明 |
| `status` | 文本 | 待补充说明 |
| `stage` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `lookback` | 整数 | 待补充说明 |
| `config_json` | 文本 | 待补充说明 |
| `state_json` | 文本 | 待补充说明 |
| `result_json` | 文本 | 待补充说明 |
| `decision_state_json` | 文本 | 待补充说明 |
| `metrics_json` | 文本 | 待补充说明 |
| `error` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `updated_at` | 文本 | 待补充说明 |
| `finished_at` | 文本 | 待补充说明 |
| `worker_id` | 文本 | 待补充说明 |
| `lease_until` | 文本 | 待补充说明 |

### `quantaalpha_backtest_results`

- 用途：待补充说明
- 当前行数：`24`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `task_id` | 文本 | 待补充说明 |
| `strategy_name` | 文本 | 待补充说明 |
| `arr` | 浮点 | 待补充说明 |
| `mdd` | 浮点 | 待补充说明 |
| `calmar` | 浮点 | 待补充说明 |
| `params_json` | 文本 | 待补充说明 |
| `artifact_path` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `quantaalpha_factor_results`

- 用途：待补充说明
- 当前行数：`89`
- 字段数：`8`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `task_id` | 文本 | 待补充说明 |
| `factor_name` | 文本 | 待补充说明 |
| `ic` | 浮点 | 待补充说明 |
| `rank_ic` | 浮点 | 待补充说明 |
| `effective_window` | 文本 | 待补充说明 |
| `source_version` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `quantaalpha_runs`

- 用途：待补充说明
- 当前行数：`81`
- 字段数：`16`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `task_id` | 文本 | 待补充说明 |
| `job_key` | 文本 | 待补充说明 |
| `task_type` | 文本 | 待补充说明 |
| `status` | 文本 | 待补充说明 |
| `error_code` | 文本 | 待补充说明 |
| `error_message` | 文本 | 待补充说明 |
| `input_json` | 文本 | 待补充说明 |
| `output_json` | 文本 | 待补充说明 |
| `artifacts_json` | 文本 | 待补充说明 |
| `metrics_json` | 文本 | 待补充说明 |
| `started_at` | 文本 | 待补充说明 |
| `finished_at` | 文本 | 待补充说明 |
| `duration_seconds` | 浮点 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `research_reports`

- 用途：待补充说明
- 当前行数：`31`
- 字段数：`10`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `report_date` | 文本 | 待补充说明 |
| `report_type` | 文本 | 待补充说明 |
| `subject_key` | 文本 | 待补充说明 |
| `subject_name` | 文本 | 待补充说明 |
| `model` | 文本 | 待补充说明 |
| `markdown_content` | 文本 | 待补充说明 |
| `context_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `signal_mapping_blocklist`

- 用途：待补充说明
- 当前行数：`18`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `term` | 文本 | 待补充说明 |
| `target_type` | 文本 | 待补充说明 |
| `match_type` | 文本 | 待补充说明 |
| `source` | 文本 | 待补充说明 |
| `reason` | 文本 | 待补充说明 |
| `enabled` | 整数 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `signal_quality_rules`

- 用途：待补充说明
- 当前行数：`8`
- 字段数：`8`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `rule_key` | 文本 | 待补充说明 |
| `rule_value` | 文本 | 待补充说明 |
| `value_type` | 文本 | 待补充说明 |
| `category` | 文本 | 待补充说明 |
| `description` | 文本 | 待补充说明 |
| `enabled` | 整数 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `signal_state_events`

- 用途：待补充说明
- 当前行数：`8490`
- 字段数：`26`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `signal_scope` | 文本 | 待补充说明 |
| `signal_key` | 文本 | 待补充说明 |
| `signal_type` | 文本 | 待补充说明 |
| `subject_name` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `event_time` | 文本 | 待补充说明 |
| `event_date` | 文本 | 待补充说明 |
| `event_type` | 文本 | 待补充说明 |
| `old_state` | 文本 | 待补充说明 |
| `new_state` | 文本 | 待补充说明 |
| `old_direction` | 文本 | 待补充说明 |
| `new_direction` | 文本 | 待补充说明 |
| `old_strength` | 浮点 | 待补充说明 |
| `new_strength` | 浮点 | 待补充说明 |
| `delta_strength` | 浮点 | 待补充说明 |
| `old_confidence` | 浮点 | 待补充说明 |
| `new_confidence` | 浮点 | 待补充说明 |
| `delta_confidence` | 浮点 | 待补充说明 |
| `source_table` | 文本 | 待补充说明 |
| `driver_type` | 文本 | 待补充说明 |
| `driver_title` | 文本 | 待补充说明 |
| `event_summary` | 文本 | 待补充说明 |
| `snapshot_before_json` | 文本 | 待补充说明 |
| `snapshot_after_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `signal_state_tracker`

- 用途：待补充说明
- 当前行数：`182`
- 字段数：`20`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `signal_scope` | 文本 | 待补充说明 |
| `signal_key` | 文本 | 待补充说明 |
| `signal_type` | 文本 | 待补充说明 |
| `subject_name` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `direction` | 文本 | 待补充说明 |
| `signal_strength` | 浮点 | 待补充说明 |
| `confidence` | 浮点 | 待补充说明 |
| `current_state` | 文本 | 待补充说明 |
| `prev_state` | 文本 | 待补充说明 |
| `source_table` | 文本 | 待补充说明 |
| `latest_signal_date` | 文本 | 待补充说明 |
| `evidence_count` | 整数 | 待补充说明 |
| `driver_type` | 文本 | 待补充说明 |
| `driver_title` | 文本 | 待补充说明 |
| `source_summary_json` | 文本 | 待补充说明 |
| `snapshot_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `stock_alias_dictionary`

- 用途：待补充说明
- 当前行数：`7`
- 字段数：`12`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `alias` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `stock_name` | 文本 | 待补充说明 |
| `alias_type` | 文本 | 待补充说明 |
| `confidence` | 浮点 | 待补充说明 |
| `source` | 文本 | 待补充说明 |
| `notes` | 文本 | 待补充说明 |
| `used_count` | 整数 | 待补充说明 |
| `last_used_at` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `stock_daily_price_rollups`

- 用途：待补充说明
- 当前行数：`32947`
- 字段数：`13`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `ts_code` | 文本 | 待补充说明 |
| `window_days` | 整数 | 待补充说明 |
| `start_date` | 文本 | 待补充说明 |
| `end_date` | 文本 | 待补充说明 |
| `rows_count` | 整数 | 待补充说明 |
| `close_first` | 浮点 | 待补充说明 |
| `close_last` | 浮点 | 待补充说明 |
| `close_change_pct` | 浮点 | 待补充说明 |
| `high_max` | 浮点 | 待补充说明 |
| `low_min` | 浮点 | 待补充说明 |
| `vol_avg` | 浮点 | 待补充说明 |
| `amount_avg` | 浮点 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `theme_aliases`

- 用途：待补充说明
- 当前行数：`126`
- 字段数：`8`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `theme_name` | 文本 | 待补充说明 |
| `alias` | 文本 | 待补充说明 |
| `alias_type` | 文本 | 待补充说明 |
| `confidence` | 浮点 | 待补充说明 |
| `source` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `theme_daily_snapshots`

- 用途：待补充说明
- 当前行数：`406`
- 字段数：`21`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `snapshot_date` | 文本 | 待补充说明 |
| `lookback_days` | 整数 | 待补充说明 |
| `theme_name` | 文本 | 待补充说明 |
| `theme_group` | 文本 | 待补充说明 |
| `direction` | 文本 | 待补充说明 |
| `theme_strength` | 浮点 | 待补充说明 |
| `confidence` | 浮点 | 待补充说明 |
| `evidence_count` | 整数 | 待补充说明 |
| `intl_news_count` | 整数 | 待补充说明 |
| `domestic_news_count` | 整数 | 待补充说明 |
| `stock_news_count` | 整数 | 待补充说明 |
| `chatroom_count` | 整数 | 待补充说明 |
| `stock_link_count` | 整数 | 待补充说明 |
| `latest_evidence_time` | 文本 | 待补充说明 |
| `heat_level` | 文本 | 待补充说明 |
| `top_terms_json` | 文本 | 待补充说明 |
| `top_stocks_json` | 文本 | 待补充说明 |
| `source_summary_json` | 文本 | 待补充说明 |
| `evidence_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `theme_definitions`

- 用途：待补充说明
- 当前行数：`29`
- 字段数：`9`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `theme_name` | 文本 | 待补充说明 |
| `theme_group` | 文本 | 待补充说明 |
| `description` | 文本 | 待补充说明 |
| `keywords_json` | 文本 | 待补充说明 |
| `priority` | 整数 | 待补充说明 |
| `enabled` | 整数 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `theme_evidence_items`

- 用途：待补充说明
- 当前行数：`3340`
- 字段数：`20`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `theme_name` | 文本 | 待补充说明 |
| `theme_group` | 文本 | 待补充说明 |
| `source_type` | 文本 | 待补充说明 |
| `source_table` | 文本 | 待补充说明 |
| `source_id` | 文本 | 待补充说明 |
| `source_name` | 文本 | 待补充说明 |
| `evidence_time` | 文本 | 待补充说明 |
| `evidence_date` | 文本 | 待补充说明 |
| `original_term` | 文本 | 待补充说明 |
| `title` | 文本 | 待补充说明 |
| `summary` | 文本 | 待补充说明 |
| `direction` | 文本 | 待补充说明 |
| `weight` | 浮点 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `stock_name` | 文本 | 待补充说明 |
| `sentiment_label` | 文本 | 待补充说明 |
| `sentiment_score` | 浮点 | 待补充说明 |
| `meta_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |

### `theme_hotspot_tracker`

- 用途：待补充说明
- 当前行数：`28`
- 字段数：`20`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `theme_name` | 文本 | 待补充说明 |
| `theme_group` | 文本 | 待补充说明 |
| `direction` | 文本 | 待补充说明 |
| `theme_strength` | 浮点 | 待补充说明 |
| `confidence` | 浮点 | 待补充说明 |
| `evidence_count` | 整数 | 待补充说明 |
| `intl_news_count` | 整数 | 待补充说明 |
| `domestic_news_count` | 整数 | 待补充说明 |
| `stock_news_count` | 整数 | 待补充说明 |
| `chatroom_count` | 整数 | 待补充说明 |
| `stock_link_count` | 整数 | 待补充说明 |
| `latest_evidence_time` | 文本 | 待补充说明 |
| `heat_level` | 文本 | 待补充说明 |
| `top_terms_json` | 文本 | 待补充说明 |
| `top_stocks_json` | 文本 | 待补充说明 |
| `source_summary_json` | 文本 | 待补充说明 |
| `evidence_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `theme_hotspot_tracker_1d`

- 用途：待补充说明
- 当前行数：`28`
- 字段数：`20`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `theme_name` | 文本 | 待补充说明 |
| `theme_group` | 文本 | 待补充说明 |
| `direction` | 文本 | 待补充说明 |
| `theme_strength` | 浮点 | 待补充说明 |
| `confidence` | 浮点 | 待补充说明 |
| `evidence_count` | 整数 | 待补充说明 |
| `intl_news_count` | 整数 | 待补充说明 |
| `domestic_news_count` | 整数 | 待补充说明 |
| `stock_news_count` | 整数 | 待补充说明 |
| `chatroom_count` | 整数 | 待补充说明 |
| `stock_link_count` | 整数 | 待补充说明 |
| `latest_evidence_time` | 文本 | 待补充说明 |
| `heat_level` | 文本 | 待补充说明 |
| `top_terms_json` | 文本 | 待补充说明 |
| `top_stocks_json` | 文本 | 待补充说明 |
| `source_summary_json` | 文本 | 待补充说明 |
| `evidence_json` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

### `theme_stock_mapping`

- 用途：待补充说明
- 当前行数：`33`
- 字段数：`10`

| 字段名 | 类型 | 中文解读 |
| --- | --- | --- |
| `id` | 长整数 | 待补充说明 |
| `theme_name` | 文本 | 待补充说明 |
| `ts_code` | 文本 | 待补充说明 |
| `stock_name` | 文本 | 待补充说明 |
| `relation_type` | 文本 | 待补充说明 |
| `weight` | 浮点 | 待补充说明 |
| `source` | 文本 | 待补充说明 |
| `notes` | 文本 | 待补充说明 |
| `created_at` | 文本 | 待补充说明 |
| `update_time` | 文本 | 待补充说明 |

## 说明

- 本文档基于当前 PostgreSQL 主库实时导出，行数会随采集和归档变化。
- 大量 `*_json` 字段保存的是结构化 JSON 文本，适合前端展示和 LLM 上下文拼装。
- 日期字段目前以文本形式存储较多，常见格式为 `YYYYMMDD`、`YYYY-MM-DD` 或 ISO 时间字符串。
- 若后续新增表或字段，可重新运行导出脚本自动更新本文档。
