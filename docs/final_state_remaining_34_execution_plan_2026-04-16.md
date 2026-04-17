# 距离终局剩余 34% 的执行拆解（含验收标准）

> 对齐目标文档：`docs/project_final_state_projection_2026-04-15.md`
> 当前估算：完成度约 66%，剩余约 34%
> 本文目标：把剩余差距拆成可执行任务，并为每项提供明确验收标准。

---

## 1. 剩余 34% 构成

1. 运行稳态缺口（约 12%）
- 因子链路 worker 离线、pending 积压、UNKNOWN_ERROR 集中。

2. 回归门禁缺口（约 8%）
- smoke 未全绿（当前 58/60），质量门禁不闭环。

3. 决策闭环后半段缺口（约 8%）
- 跟踪/反馈/复盘链路未形成默认成立的稳定机制。

4. 模块动作化打通缺口（约 6%）
- news/chatrooms/signal 到 decision 的结构化动作沉淀不统一。

---

## 2. P0（必须先完成，预计回收 20%）

## P0-1 因子 worker 稳态恢复（owner: backend/runtime）

### 要做什么
1. 为 quantaalpha worker 增加常驻启动与守护（与 multi-role/roundtable 同等级）。
2. 确保 `EXECUTION_MODE=hybrid` 下 pending 队列可持续被消费。
3. 将 `UNKNOWN_ERROR` 细分为可行动错误码（至少拆出：THREAD_LOST/EXECUTION_EXCEPTION/EXTERNAL_TIMEOUT）。

### 验收标准
1. `/api/quant-factors/health` 连续 15 分钟采样，`worker.alive=true` 且 `heartbeat_age_seconds < 60`。
2. 构造 3 条 `mine` 任务后，10 分钟内 `pending=0`。
3. 最近 50 条错误中 `UNKNOWN_ERROR` 占比 < 30%。
4. 不出现 `WORKER_OFFLINE_BACKLOG` 告警。

### 回归命令
- `python3 jobs/run_quantaalpha_worker.py --once`
- `python3 -c "import db_compat as s; from services.quantaalpha_service import get_quantaalpha_runtime_health as h; import json; print(json.dumps(h(sqlite3_module=s,db_path='stock_codes.db'),ensure_ascii=False,indent=2))"`

---

## P0-2 质量门禁全绿（owner: frontend/test）

### 要做什么
1. 修复 smoke 剩余失败用例：
- `tests/e2e/intelligence.spec.ts` 新闻筛选功能
- `tests/e2e/research.spec.ts` 决策板页面加载
2. 固化失败场景对应的等待策略和页面稳定性处理（避免 flaky）。
3. 保持 `build + smoke` 作为提交前强制门禁。

### 验收标准
1. `npm run build` 连续 3 次通过。
2. `npm run smoke:e2e:all` 连续 2 轮全绿（60/60）。
3. 对上述两个历史失败 case，生成通过截图与 trace 归档。

### 回归命令
- `cd apps/web && npm run build`
- `cd apps/web && npm run smoke:e2e:all`

---

## P0-3 决策闭环后半段补齐（owner: research/backend）

### 要做什么
1. 强化“动作 -> 跟踪 -> 反馈”统一结构字段（状态、时间、标识、终态原因）。
2. 决策动作默认显示追踪可见字段：`action_id/run_id/snapshot_id`（缺失时显式标记“无标识”）。
3. 将“命中率/收益反馈”与动作记录关联到同一视图，避免分散。

### 验收标准
1. 随机抽 20 条动作，>=95% 能在 UI 追溯到完整状态链。
2. 决策页中每条动作均有“当前状态 + 最近更新时间 + 结果入口”。
3. 无“动作已提交但无后续状态”的孤儿记录。

### 回归命令
- `tests/test_decision_service.py`
- `cd apps/web && npm run smoke:e2e:all`（重点看 research/stocks/signals 场景）

---

## 3. P1（紧随其后，预计回收 14%）

## P1-1 news/chatrooms/signal 的结构化动作沉淀（owner: frontend+product）

### 要做什么
1. 统一“发送到决策板”动作协议：动作类型、理由、时限、验证指标、来源模块。
2. news 与 chatrooms 页面从“跳转链接”升级为“动作创建 + 上下文携带”。
3. signal 图谱节点支持一键生成观察/复核动作并回写。

### 验收标准
1. 三个模块均可一键生成结构化动作，且在决策板可见来源标签。
2. 采样 30 条跨模块动作，字段完整率 >= 95%。
3. 用户从入口到动作落地平均点击数 <= 2。

---

## P1-2 首页决策化收敛（owner: frontend）

### 要做什么
1. 增加“今日决策工作台”首屏块（候选、风险、待办、异常）。
2. admin/pro 默认落点保持一致的决策主线入口策略。
3. 将关键跨模块入口收敛到同一任务看板。

### 验收标准
1. admin/pro 登录后都能在首屏看到“决策待办 + 风险提示 + 最新动作”。
2. 默认落点相关 smoke 用例稳定通过。
3. 首页到执行动作链路可在 1 分钟内走通。

---

## P1-3 可观测统一（owner: backend/system）

### 要做什么
1. 后台写操作统一 `idle/pending/success/error` 状态机展示。
2. 所有关键写操作补“最近生效时间 + 有效值 + 失败原因”。
3. 告警面板聚合关键健康项（worker、queue、error concentration）。

### 验收标准
1. `system/*` 关键写页全部显示统一状态反馈。
2. 失败提示可行动（至少包含原因 + 建议下一步）。
3. 关键 worker 健康项可在一个页面完成巡检。

---

## 4. 里程碑与完成定义

## M1（P0 完成）
- 条件：P0-1/P0-2/P0-3 全部验收通过。
- 目标完成度：66% -> 86%。

## M2（P1 完成）
- 条件：P1-1/P1-2/P1-3 全部验收通过。
- 目标完成度：86% -> 95%+（进入终局收敛期）。

---

## 5. 风险与回滚点

1. 风险：worker 守护引入后若重复拉起，可能导致并发消费冲突。
- 回滚点：先关闭 guard，仅保留单 worker 手动运行。

2. 风险：为修 smoke 增加等待逻辑可能掩盖真实性能问题。
- 回滚点：保留 trace，对等待策略设上限并记录原因。

3. 风险：跨模块动作协议一次改动过大导致兼容问题。
- 回滚点：先加兼容字段，不移除旧字段，分阶段切换。

---

## 6. 一句话执行顺序

先修 P0 的运行与回归稳态，再做 P1 的动作化与首页收敛；
先保证“能稳定闭环”，再追求“更高产品完成度”。

