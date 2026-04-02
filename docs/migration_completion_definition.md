# 改造迁移完成定义（收口基线）

## 目标
确保新增能力默认走 `collectors + jobs + services`，而不是继续回流到根目录散脚本和 `server.py` 业务拼装。

## 完成判定

### 1. 目录落点
- 新抓取能力必须落在 `collectors/*`，按域拆分（news/stock_news/chatrooms/macro/market）。
- 新调度入口必须落在 `jobs/run_*_job.py`，`job_registry.py` 只登记 runner 命令。
- 新研究逻辑必须落在 `services/agent_service/*`，新报告逻辑必须落在 `services/reporting/*`。

### 2. 接口协议
- 研究接口统一主字段：`analysis_markdown`。
- 报告接口统一主字段：`analysis_markdown`；`markdown_content` 为兼容镜像字段，后续可退场。
- 研究结果可选携带 `pre_trade_check`（受配置开关控制）。

### 3. 兼容策略
- 现有 API 路径保持不变；旧字段在兼容窗口内可返回但不作为主路径。
- 根目录旧脚本仅保留兼容壳，不新增业务实现。
- `job_key` 尽量不改名；新增 `job_key` 需在评审中明确是否纳入默认调度。

### 4. 验收命令基线
- `python3 -m unittest`（现有全量 + 新增 market/integration 测试）
- `cd apps/web && npm run build`
- `python3 job_orchestrator.py dry-run` 覆盖 `news/stock_news/chatrooms/macro/market`

## 发布与回滚

### 发布建议
- 先启用新 runner 的 dry-run 与 describe 验证，再逐步打开 cron。
- `ENABLE_AGENT_NOTIFICATIONS`、`ENABLE_REPORTING_NOTIFICATIONS` 默认关闭，先用测试 webhook 验证。
- 评审时应同时参考 `docs/pr_review_checklist.md`。
- reporting 字段退场按 `docs/reporting_protocol_retirement_plan_2026-04-02.md` 执行。
- 调度观测按 `docs/job_observability_baseline_2026-04-02.md` 执行。
- 个人微信（ItChat）通知仅按 `docs/notifications_itchat_experimental.md` 做实验，不纳入生产主链。

### 回滚策略
- 任务层回滚：将对应 job 的 command 指回旧入口（或临时禁用新 job）。
- 接口层回滚：保留兼容字段读取优先级，前端回退到旧字段兜底。
- 通知/风控回滚：关闭环境变量开关，不影响研究主链。
