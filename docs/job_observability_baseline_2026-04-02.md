# 调度观测基线（制定日期：2026-04-02）

## 目标
为 `market/macro/news` 三类任务建立可持续观测基线，支撑每周复盘。

## 覆盖任务
- `market_expectations_refresh`
- `market_news_refresh`
- `macro_context_refresh`
- `intl_news_pipeline`

## 每日检查命令
```bash
cd /home/zanbo/zanbotest
python3 job_orchestrator.py runs --limit 100
python3 job_orchestrator.py alerts --limit 50
python3 job_orchestrator.py dry-run market_expectations_refresh
python3 job_orchestrator.py dry-run market_news_refresh
python3 job_orchestrator.py dry-run macro_context_refresh
```

## 每周复盘指标
- 成功率（7天）
- 平均耗时 / P95 耗时
- 失败原因 TopN（按 stderr 关键词）
- 数据增量（每任务写入行数或事件数）

## 阈值建议
- 7天成功率 < 95%：进入整改
- P95 耗时较上周上升 > 30%：触发排查
- 连续两天告警：升级处理

## 输出模板
- 本周任务总览：成功率、平均耗时、失败Top3
- 异常案例：时间、任务、错误、修复动作
- 下周改进：1-3 条可执行项
