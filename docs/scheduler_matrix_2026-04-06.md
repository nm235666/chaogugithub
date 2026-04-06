# 定时任务调度矩阵（UTC/CST 双时区）

更新时间：2026-04-06

调度原则：

- cron 仅触发 `job_orchestrator` 的 `job_key`
- 调度真源：`job_definitions(enabled=1)`
- `market_data` 任务启用交易日门禁（非交易日写 `skipped_non_trading_day`）
- 其余任务按自然日运行

## 业务时段矩阵

### 实时链（自然日）

| job_key | cron(UTC) | 北京时间(CST) |
| --- | --- | --- |
| `cn_news_pipeline` | `*/2 * * * *` | 每 2 分钟 |
| `intl_news_pipeline` | `1-59/5 * * * *` | 每小时第 1,6,11... 分 |
| `news_stock_map_refresh` | `2-59/5 * * * *` | 每小时第 2,7,12... 分 |
| `news_sentiment_refresh` | `3-59/5 * * * *` | 每小时第 3,8,13... 分 |
| `stock_news_score_refresh` | `*/10 * * * *` | 每 10 分钟 |
| `stock_news_expand_focus` | `23 * * * *` | 每小时第 23 分 |
| `stock_news_backfill_missing` | `53 * * * *` | 每小时第 53 分 |

### 信号/主题（自然日）

| job_key | cron(UTC) | 北京时间(CST) |
| --- | --- | --- |
| `theme_hotspot_refresh` | `32 * * * *` | 每小时第 32 分 |
| `signal_state_refresh` | `38 * * * *` | 每小时第 38 分 |
| `market_expectations_refresh` | `42 * * * *` | 每小时第 42 分 |
| `investment_signal_tracker_refresh` | `35 * * * *` | 每小时第 35 分 |

### 群聊链（自然日）

| job_key | cron(UTC) | 北京时间(CST) |
| --- | --- | --- |
| `monitored_chatlog_fetch` | `*/3 * * * *` | 每 3 分钟 |
| `chatroom_analysis_pipeline` | `15 * * * *` | 每小时第 15 分 |
| `chatroom_sentiment_refresh` | `18 * * * *` | 每小时第 18 分 |
| `chatroom_list_refresh` | `5 0 * * *` | 每天 08:05 |
| `monitored_chatlog_backfill_midnight` | `10 0 * * *` | 每天 08:10 |

### 交易日行情链（交易日门禁）

| job_key | cron(UTC) | 北京时间(CST) |
| --- | --- | --- |
| `daily_postclose_update` | `40 7 * * 1-5` | 15:40（周一至周五） |
| `capital_flow_market_refresh` | `55 7 * * 1-5` | 15:55（周一至周五） |
| `risk_scenarios_refresh` | `10 8 * * 1-5` | 16:10（周一至周五） |
| `minline_backfill_recent` | `45 7 * * 1-5` | 15:45（周一至周五） |
| `minline_akshare_patch` | `5 8 * * 1-5` | 16:05（周一至周五） |
| `minline_intraday_focus` | `*/10 1-3,5-7 * * 1-5` | 09:00-11:59、13:00-15:59 每 10 分钟 |

### 报告与维护（自然日）

| job_key | cron(UTC) | 北京时间(CST) |
| --- | --- | --- |
| `news_daily_summary_refresh` | `35 3,15 * * *` | 11:35、23:35 |
| `research_reports_refresh` | `50 7,15 * * *` | 15:50、23:50 |
| `news_archive_refresh` | `30 3,15 * * *` | 11:30、23:30 |
| `news_dedupe_refresh` | `20 16 * * *` | 00:20 |
| `db_health_check_refresh` | `40 16 * * *` | 00:40 |
| `database_audit_refresh` | `50 16 * * *` | 00:50 |
| `logic_view_cache_refresh` | `55 17 * * *` | 01:55 |
| `macro_series_akshare_refresh` | `20 17 * * *` | 01:20 |
| `macro_context_refresh` | `10 17 * * *` | 01:10 |
| `llm_provider_nodes_probe` | `*/20 * * * *` | 每 20 分钟 |
| `multi_role_v3_stale_recovery` | `*/5 * * * *` | 每 5 分钟 |
| `multi_role_v3_worker_guard` | `* * * * *` | 每分钟 |

### QuantaAlpha（按需）

- `quantaalpha_health_check`
- `quantaalpha_mine_daily`
- `quantaalpha_backtest_daily`

上述三项默认 `enabled=0`，不进入常规 cron，按手动/API 触发。

## 验证命令

```bash
bash /home/zanbo/zanbotest/install_all_crons.sh
python3 /home/zanbo/zanbotest/scripts/scheduler/check_cron_sync.py
python3 /home/zanbo/zanbotest/job_orchestrator.py runs --limit 50
```
