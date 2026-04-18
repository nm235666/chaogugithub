# 项目命令行命令总表

更新时间：2026-04-10

> 文档治理说明：2026-04-10 起，阶段性全量测试/分析报告统一迁移到 `docs/archive/`，本文件仅维护当前可执行命令。

本文件汇总当前项目主要命令行入口，包括：

- 顶层 `python3 *.py` 脚本
- 顶层 `bash *.sh` 运行/安装/启动/守护脚本
- 任务编排器 `job_orchestrator.py`

默认约定：

- 项目根目录：`/home/zanbo/zanbotest`
- 数据库：默认走 PostgreSQL 主库
- 大多数 Python 脚本支持 `-h` 查看完整参数
- 大多数 cron 包装脚本实际都会先加载 `runtime_env.sh`

## 通用用法

查看某个 Python 脚本帮助：

```bash
python3 /home/zanbo/zanbotest/<script>.py -h
```

运行某个 shell 包装脚本：

```bash
bash /home/zanbo/zanbotest/<script>.sh
```

## 启动与服务

| 命令 | 作用 |
| --- | --- |
| `bash /home/zanbo/zanbotest/start_all.sh` | 一次性启动后端 + 新版前端（Vue dist） |
| `bash /home/zanbo/zanbotest/start_backend.sh` | 启动主后端 `8002` |
| `bash /home/zanbo/zanbotest/start_backend_llm.sh` | 启动后端 `8003` |
| `bash /home/zanbo/zanbotest/start_backend_llm2.sh` | 启动后端 `8004` |
| `bash /home/zanbo/zanbotest/start_backend_macro.sh` | 启动后端 `8005` |
| `bash /home/zanbo/zanbotest/start_backend_multi_role.sh` | 启动后端 `8006` |
| `bash /home/zanbo/zanbotest/start_frontend.sh` | 启动新版前端构建产物与主后端同源入口 `8002` |
| `python3 /home/zanbo/zanbotest/serve_spa.py --host 0.0.0.0 --port 8080 --root /home/zanbo/zanbotest/apps/web/dist` | 旧版静态托管入口（历史兼容，不作为正式部署推荐） |
| `bash /home/zanbo/zanbotest/start_frontend_nextgen_dev.sh` | 启动主前端开发环境（兼容旧脚本名） |
| `bash /home/zanbo/zanbotest/start_frontend_nextgen_preview.sh` | 启动主前端预览环境（兼容旧脚本名） |
| `bash /home/zanbo/zanbotest/start_nginx_8077.sh` | 启动 Nginx 统一入口 `8077` |
| `bash /home/zanbo/zanbotest/start_stream_news_worker.sh` | 启动 Redis Stream 新闻 Worker |
| `bash /home/zanbo/zanbotest/start_ws_realtime.sh` | 启动 WebSocket 实时广播服务 `8010` |
| `bash /home/zanbo/zanbotest/start_cn_news_eastmoney_10s.sh` | 启动东方财富 10 秒循环抓取守护主进程 |
| `bash /home/zanbo/zanbotest/start_auto_update.sh 3600` | 启动定时自动更新循环，默认每 3600 秒一轮 |
| `bash /home/zanbo/zanbotest/watch_cn_news_eastmoney_10s.sh` | 检查东方财富 10 秒抓取是否存活，不存活则拉起 |
| `bash /home/zanbo/zanbotest/watch_realtime_services.sh` | 检查 WebSocket / Stream Worker 等实时服务 |

常用排障（不依赖 `rg`）：

```bash
cd /home/zanbo/zanbotest
pkill -f "python3 backend/server.py" || true
sleep 1
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; PORT=8000 python3 backend/server.py' >/tmp/stock_backend_8000.log 2>&1 &
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; PORT=8002 python3 backend/server.py' >/tmp/stock_backend_8002.log 2>&1 &
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; PORT=8004 python3 backend/server.py' >/tmp/stock_backend_8004.log 2>&1 &
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; PORT=8005 python3 backend/server.py' >/tmp/stock_backend_8005.log 2>&1 &
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; PORT=8006 python3 backend/server.py' >/tmp/stock_backend_8006.log 2>&1 &
ss -ltnp | grep -E ':8000|:8002|:8004|:8005|:8006'
```

## 定时任务安装

| 命令 | 作用 |
| --- | --- |
| `bash /home/zanbo/zanbotest/install_all_crons.sh` | 按 `job_definitions(enabled=1)` 自动安装 cron（统一触发 `job_orchestrator`） |
| `bash /home/zanbo/zanbotest/install_chatroom_daily_cron.sh "5 0 * * *"` | 安装群聊列表每日同步 cron |
| `bash /home/zanbo/zanbotest/install_monitored_chatlog_cron.sh "*/3 * * * *"` | 安装监控群聊天记录增量抓取 cron |
| `bash /home/zanbo/zanbotest/install_monitored_chatlog_midnight_cron.sh "10 0 * * *"` | 安装监控群跨天补抓 cron |
| `python3 /home/zanbo/zanbotest/scripts/scheduler/check_cron_sync.py` | 校验 `job_definitions` 与 crontab 一致性，并输出下次触发 UTC/CST |
| `bash /home/zanbo/zanbotest/run_scheduler_consistency_check.sh` | 统一入口执行调度一致性巡检（输出 missing/extra/drift 摘要） |

## 任务编排器

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/job_orchestrator.py list` | 列出当前注册任务 |
| `python3 /home/zanbo/zanbotest/job_orchestrator.py sync` | 同步任务定义到数据库 |
| `python3 /home/zanbo/zanbotest/job_orchestrator.py run <job_key>` | 手动执行某个编排任务 |
| `python3 /home/zanbo/zanbotest/job_orchestrator.py skip <job_key> --reason skipped_non_trading_day` | 记录一次任务跳过（交易日门禁） |
| `python3 /home/zanbo/zanbotest/job_orchestrator.py dry-run <job_key>` | 展开任务命令但不执行（回归/排障首选） |
| `python3 /home/zanbo/zanbotest/job_orchestrator.py runs --job-key <job_key> --limit 20` | 查看任务运行记录 |
| `python3 /home/zanbo/zanbotest/job_orchestrator.py alerts --job-key <job_key> --limit 20` | 查看任务失败告警（默认仅未确认） |

## 回归与 Smoke

| 命令 | 作用 |
| --- | --- |
| `bash /home/zanbo/zanbotest/run_minimal_regression.sh` | 运行后端最小回归 + 前端 API smoke |
| `bash /home/zanbo/zanbotest/run_frontend_api_smoke.sh` | 运行前端 API/路由协议 smoke（无 UI 框架） |

常见 `job_key`：

- `intl_news_pipeline`
- `cn_news_pipeline`
- `cn_news_score_refresh`
- `intl_news_score_refresh`
- `news_stock_map_refresh`
- `news_sentiment_refresh`
- `news_daily_summary_refresh`
- `stock_news_score_refresh`
- `stock_news_expand_focus`
- `chatroom_analysis_pipeline`
- `chatroom_sentiment_refresh`
- `chatroom_tagging_safe`
- `investment_signal_tracker_refresh`
- `theme_hotspot_refresh`
- `signal_state_refresh`
- `research_reports_refresh`
- `daily_postclose_update`
- `decision_daily_snapshot`

## 一次性 cron 包装脚本

这些脚本通常供 cron 调用，也可以手工运行。

统一调度新增入口：

| 命令 | 作用 |
| --- | --- |
| `bash /home/zanbo/zanbotest/run_job_always.sh <job_key>` | 总是执行指定编排任务 |
| `bash /home/zanbo/zanbotest/run_job_if_trade_day.sh <job_key>` | 交易日才执行，非交易日写入 `skipped_non_trading_day` |
| `python3 /home/zanbo/zanbotest/jobs/run_decision_job.py --job-key decision_daily_snapshot` | 手工运行投研决策快照任务 |

| 命令 | 作用 |
| --- | --- |
| `bash /home/zanbo/zanbotest/run_news_fetch_once.sh` | 运行国际新闻采集评分映射流水线 |
| `bash /home/zanbo/zanbotest/run_cn_news_fetch_once.sh` | 运行国内新闻新浪 7x24 快速抓取流水线 |
| `bash /home/zanbo/zanbotest/run_cn_news_eastmoney_once.sh` | 运行东方财富快讯抓取 + 评分补做 |
| `bash /home/zanbo/zanbotest/run_news_stock_map_once.sh` | 运行新闻股票映射刷新 |
| `bash /home/zanbo/zanbotest/run_news_sentiment_once.sh` | 运行新闻情绪补评 |
| `bash /home/zanbo/zanbotest/run_news_daily_summary_once.sh` | 运行新闻日报总结 |
| `bash /home/zanbo/zanbotest/run_news_archive_once.sh` | 运行新闻归档 |
| `bash /home/zanbo/zanbotest/run_news_dedupe_once.sh` | 运行新闻/聊天去重 |
| `bash /home/zanbo/zanbotest/run_stock_news_score_once.sh` | 运行个股新闻评分刷新 |
| `bash /home/zanbo/zanbotest/run_stock_news_backfill_missing_once.sh` | 运行个股新闻缺口补抓 |
| `bash /home/zanbo/zanbotest/run_stock_news_expand_once.sh` | 运行重点股票个股新闻扩抓 |
| `bash /home/zanbo/zanbotest/run_chatroom_fetch_once.sh` | 运行群聊列表同步 |
| `bash /home/zanbo/zanbotest/run_monitored_chatlog_fetch_once.sh` | 运行监控群聊天记录抓取 |
| `bash /home/zanbo/zanbotest/run_monitored_chatlog_backfill_midnight.sh` | 运行监控群跨天补抓 |
| `bash /home/zanbo/zanbotest/run_chatroom_analysis_pipeline_once.sh` | 运行群聊分析全流水线 |
| `bash /home/zanbo/zanbotest/run_chatroom_sentiment_once.sh` | 运行群聊情绪刷新 |
| `bash /home/zanbo/zanbotest/run_chatroom_tagging_safe_once.sh` | 运行低风险群聊标签刷新 |
| `bash /home/zanbo/zanbotest/run_investment_signal_tracker_once.sh` | 运行投资信号总表刷新 |
| `bash /home/zanbo/zanbotest/run_theme_hotspot_engine_once.sh` | 运行主题热点引擎刷新 |
| `bash /home/zanbo/zanbotest/run_signal_state_machine_once.sh` | 运行信号状态机刷新 |
| `bash /home/zanbo/zanbotest/run_market_expectations_once.sh` | 运行市场预期层刷新 |
| `bash /home/zanbo/zanbotest/run_research_reports_once.sh` | 运行标准投研报告生成 |
| `bash /home/zanbo/zanbotest/run_macro_series_akshare_once.sh` | 运行 AKShare 宏观数据刷新 |
| `bash /home/zanbo/zanbotest/run_fx_daily_akshare_once.sh` | 运行 FX 双源补齐 |
| `bash /home/zanbo/zanbotest/run_minline_backfill_recent.sh` | 运行最近交易日分钟线补抓 |
| `bash /home/zanbo/zanbotest/run_minline_akshare_patch_once.sh` | 运行 AKShare 分钟线补洞 |
| `bash /home/zanbo/zanbotest/run_minline_intraday_focus_once.sh` | 运行盘中重点股票分钟线补抓 |
| `bash /home/zanbo/zanbotest/run_daily_postclose_update.sh` | 运行盘后数据更新流水线 |
| `bash /home/zanbo/zanbotest/run_data_completion_nightly.sh` | 运行夜间补数批次 |
| `bash /home/zanbo/zanbotest/run_data_completion_once.sh` | 手工跑一轮数据补全 |
| `bash /home/zanbo/zanbotest/run_database_audit_once.sh` | 运行数据库审计报告 |
| `bash /home/zanbo/zanbotest/run_db_health_check_once.sh` | 运行数据库健康巡检 |
| `bash /home/zanbo/zanbotest/run_logic_view_cache_once.sh` | 运行逻辑视图缓存回填 |
| `bash /home/zanbo/zanbotest/scripts/sync_strategy_repo.sh` | 同步 `WealthCodePro/Strategy` 仓库到 `external/strategy`（支持 SSH/Token） |
| `bash /home/zanbo/zanbotest/scripts/sync_and_import_strategy_reports.sh` | 一键执行“同步 Strategy 仓库 + 导入 `research_reports` 数据源” |

## 数据抓取与回填

## 本地归档与禁提交治理

| 命令 | 作用 |
| --- | --- |
| `git config core.hooksPath .githooks` | 启用仓库内置 hooks（pre-commit/pre-push） |
| `bash /home/zanbo/zanbotest/scripts/git_guard_no_archive_upload.sh` | 手工检查暂存区是否包含禁止提交路径 |
| `git check-ignore -v local_archive/test.txt` | 验证本地归档路径命中忽略规则 |

### 股票、行情、估值、资金流

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/fetch_all_stock_codes.py` | 获取全部股票代码 |
| `python3 /home/zanbo/zanbotest/auto_update_stocks_and_prices.py` | 自动更新股票列表和日线 |
| `python3 /home/zanbo/zanbotest/fast_backfill_listed_prices.py` | 按交易日快速回填上市股票日线 |
| `python3 /home/zanbo/zanbotest/backfill_listed_3y_prices.py` | 回填上市股票历史日线 |
| `python3 /home/zanbo/zanbotest/backfill_stock_valuation_daily.py` | 回填估值日频数据 |
| `python3 /home/zanbo/zanbotest/backfill_capital_flow_stock.py` | 回填个股资金流 |
| `python3 /home/zanbo/zanbotest/backfill_capital_flow_stock_akshare.py` | 使用 AKShare 回填个股资金流 |
| `python3 /home/zanbo/zanbotest/backfill_capital_flow_market.py` | 回填市场级资金流 |
| `python3 /home/zanbo/zanbotest/backfill_stock_scores_daily.py` | 生成股票综合评分日快照 |

### 分钟线

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/fetch_sina_minline_one.py` | 抓取单只股票新浪分时 |
| `python3 /home/zanbo/zanbotest/fetch_sina_minline_all_listed.py` | 稳健版全市场新浪分钟线抓取 |
| `python3 /home/zanbo/zanbotest/backfill_stock_minline_akshare.py` | AKShare 回填分钟线 |
| `python3 /home/zanbo/zanbotest/run_minline_focus_once.py` | 围绕重点股票刷新分钟线 |

### 财务、事件、治理、风险

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/backfill_stock_financials.py` | 回填股票财务核心指标 |
| `python3 /home/zanbo/zanbotest/fast_backfill_stock_financials.py` | 按报告期批量快速回填财务 |
| `python3 /home/zanbo/zanbotest/backfill_missing_stock_financials.py` | 补抓缺失财务记录 |
| `python3 /home/zanbo/zanbotest/backfill_stock_events.py` | 回填股票事件 |
| `python3 /home/zanbo/zanbotest/update_daily_stock_events.py` | 增量更新股票事件 |
| `python3 /home/zanbo/zanbotest/backfill_company_governance.py` | 回填公司治理画像 |
| `python3 /home/zanbo/zanbotest/clean_governance_json_nan.py` | 清洗治理 JSON 中的 NaN/Infinity |
| `python3 /home/zanbo/zanbotest/backfill_risk_scenarios.py` | 从日线派生风险情景 |

### 汇率、利率、宏观

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/backfill_fx_daily.py` | 回填汇率日线 |
| `python3 /home/zanbo/zanbotest/backfill_fx_daily_akshare.py` | AKShare + 现有源双源回填 FX |
| `python3 /home/zanbo/zanbotest/backfill_rate_curve_points.py` | 回填利率曲线点 |
| `python3 /home/zanbo/zanbotest/backfill_spread_daily.py` | 派生利差数据 |
| `python3 /home/zanbo/zanbotest/backfill_macro_series.py` | Tushare 回填宏观数据 |
| `python3 /home/zanbo/zanbotest/backfill_macro_series_akshare.py` | AKShare 回填宏观数据 |
| `python3 /home/zanbo/zanbotest/fetch_market_expectations_polymarket.py` | 抓取市场预期层数据 |
| `python3 /home/zanbo/zanbotest/market_calendar.py --token <TUSHARE_TOKEN> --count 2` | 获取最近交易日 |

## 新闻、个股新闻、映射

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/fetch_news_rss.py` | 抓取国际 RSS 新闻 |
| `python3 /home/zanbo/zanbotest/fetch_news_marketscreener.py` | 抓取 MarketScreener 新闻页 |
| `python3 /home/zanbo/zanbotest/fetch_news_marketscreener_live.py` | 抓取 MarketScreener Live |
| `python3 /home/zanbo/zanbotest/fetch_cn_news_sina_7x24.py` | 抓取新浪 7x24 国内新闻 |
| `python3 /home/zanbo/zanbotest/fetch_cn_news_eastmoney.py` | 抓取东方财富快讯 |
| `python3 /home/zanbo/zanbotest/fetch_stock_news_eastmoney_to_db.py --ts-code 601100.SH` | 抓取单只股票相关新闻 |
| `python3 /home/zanbo/zanbotest/query_stock_news_eastmoney.py` | 查询东方财富个股相关新闻 |
| `python3 /home/zanbo/zanbotest/backfill_stock_news_items.py` | 批量补齐个股新闻 |
| `python3 /home/zanbo/zanbotest/run_stock_news_expand_focus.py` | 围绕重点股票扩抓个股新闻 |
| `python3 /home/zanbo/zanbotest/map_news_items_to_stocks.py` | 新闻标题/摘要映射到股票 |
| `python3 /home/zanbo/zanbotest/cleanup_duplicate_items.py` | 清理新闻和聊天重复数据 |
| `python3 /home/zanbo/zanbotest/optimize_and_archive_news.py` | 新闻归档与优化 |

## LLM 评分、分析、总结

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/llm_score_news.py` | 国际/国内新闻结构化评分 |
| `python3 /home/zanbo/zanbotest/llm_score_stock_news.py` | 个股新闻评分与摘要 |
| `python3 /home/zanbo/zanbotest/llm_score_sentiment.py` | 新闻/个股新闻统一情绪评分 |
| `python3 /home/zanbo/zanbotest/llm_summarize_daily_important_news.py` | 重要新闻日报总结 |
| `python3 /home/zanbo/zanbotest/llm_analyze_stock_trend.py` | 股票走势分析 |
| `python3 /home/zanbo/zanbotest/llm_multi_role_company_review.py` | 多角色公司分析 |
| `python3 /home/zanbo/zanbotest/llm_tag_chatrooms.py` | 群聊标签与概要 |
| `python3 /home/zanbo/zanbotest/llm_analyze_chatroom_investment_bias.py` | 群聊投资倾向分析 |
| `python3 /home/zanbo/zanbotest/llm_score_chatroom_sentiment.py` | 群聊情绪分析 |
| `python3 /home/zanbo/zanbotest/llm_resolve_stock_aliases.py` | 群聊股票别名归一 |
| `python3 /home/zanbo/zanbotest/loop_score_unscored_news.py` | 循环补评分未评新闻 |
| `bash /home/zanbo/zanbotest/un_news_score_backlog_parallel.sh --mode cn --parallel 4 --limit-per-source 100` | 并行清理国内新闻未评分积压 |

常用示例：

```bash
python3 /home/zanbo/zanbotest/loop_score_unscored_news.py --model auto --batch-size 100 --retry 2 --sleep 0.2 --round-sleep 2
python3 /home/zanbo/zanbotest/llm_score_stock_news.py --model auto --limit 100 --retry 2 --sleep 0.2
python3 /home/zanbo/zanbotest/llm_analyze_stock_trend.py --ts-code 000001.SZ --lookback 120 --model auto
python3 /home/zanbo/zanbotest/llm_multi_role_company_review.py --ts-code 000001.SZ --lookback 120 --model auto
```

## 群聊、聊天记录

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/fetch_chatroom_list_to_db.py --once` | 同步群聊列表 |
| `python3 /home/zanbo/zanbotest/fetch_wechat_chatlog_clean_to_db.py --date 2026-03-02 --talker "<群名>"` | 抓取并清洗单日聊天记录 |
| `python3 /home/zanbo/zanbotest/fetch_wechat_chatlog_clean_to_db.py --start-date 2026-03-02 --end-date 2026-03-04 --talker "<群名>"` | 抓取并清洗区间聊天记录 |
| `python3 /home/zanbo/zanbotest/fetch_monitored_chatlogs_once.py` | 抓取监控中群聊消息 |
| `python3 /home/zanbo/zanbotest/backfill_wechat_chatlogs_30d.py` | 回填所有群聊最近 30 天聊天记录 |
| `python3 /home/zanbo/zanbotest/build_chatroom_candidate_pool.py` | 汇总群聊候选股票/主题池 |

## 信号、主题、报告

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/build_investment_signal_tracker.py` | 生成投资信号追踪表 |
| `python3 /home/zanbo/zanbotest/build_theme_hotspot_engine.py` | 生成主题热点引擎结果 |
| `python3 /home/zanbo/zanbotest/build_signal_state_machine.py` | 生成信号状态机和事件 |
| `python3 /home/zanbo/zanbotest/generate_standard_research_report.py --report-type market --subject-key market_overview --report-date 2026-03-30` | 生成标准投研报告 |
| `python3 /home/zanbo/zanbotest/create_research_tables.py` | 创建研究扩展表 |
| `python3 /home/zanbo/zanbotest/backfill_logic_view_cache.py` | 回填逻辑视图缓存 |

## 数据库、迁移、审计

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/init_postgres_schema.py` | 初始化 PostgreSQL 表结构 |
| `python3 /home/zanbo/zanbotest/migrate_sqlite_to_postgres.py` | 迁移 SQLite 数据到 PostgreSQL |
| `python3 /home/zanbo/zanbotest/db_health_check.py` | 数据库健康巡检 |
| `python3 /home/zanbo/zanbotest/audit_database_report.py` | 生成数据库审计报告 |
| `python3 /home/zanbo/zanbotest/export_db_dictionary_md.py` | 导出数据字典 Markdown |

## 种子与初始化

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/seed_signal_quality_rules.py` | 初始化信号质量规则 |
| `python3 /home/zanbo/zanbotest/seed_stock_alias_dictionary.py` | 初始化股票别名字典 |
| `python3 /home/zanbo/zanbotest/seed_theme_stock_mapping.py` | 初始化主题股票映射 |

## 实时链路

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/stream_news_worker.py` | 消费 Redis Stream 并广播新闻事件 |
| `python3 /home/zanbo/zanbotest/ws_realtime_server.py --host 0.0.0.0 --port 8010` | 启动 WebSocket 服务 |

## 数据补全与批处理

| 命令 | 作用 |
| --- | --- |
| `python3 /home/zanbo/zanbotest/run_data_completion_batches.py --token <TUSHARE_TOKEN>` | 分批补治理、事件、评分缺口 |

常用示例：

```bash
python3 /home/zanbo/zanbotest/run_data_completion_batches.py \
  --token "$TUSHARE_TOKEN" \
  --governance-batch 50 \
  --events-batch 100 \
  --rounds 20
```

## 说明

1. 若某个脚本未在本表中列出参数细节，请直接执行 `-h` 查看。
2. 若某个脚本是底层模块，例如 `db_compat.py`、`llm_gateway.py`、`llm_provider_config.py`、`realtime_streams.py`，通常不作为独立业务命令直接运行。
3. 若你希望，我可以继续把这份文档升级成：
   - “按功能页面反查命令”
   - “按定时任务反查命令”
   - “带实际常用参数示例的完整版手册”
