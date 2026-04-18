# 当前项目不足项审计（2026-04-18）

## 1. 审计结论

当前项目已从“分散功能页”进入“决策主线雏形”阶段，但仍存在关键缺口：  
主线可见，但规则硬约束不足；页面已连通，但业务闭环后半段未完全自动化；能力已有文档定义，但部分尚未落成可阻断的工程机制。

---

## 2. P0 不足项（不补会持续影响主线）

### P0-1 候选漏斗并发裁决规则未完整落地

- 表面现状：已实现状态机、`state_version` 乐观锁、`idempotency_key` 防重。  
- 实际缺口：未实现“多触发源冲突时的最终写权限优先级裁决”执行逻辑。  
- 影响：同一候选在并发写入下仍可能出现口径不一致。  
- 证据：
  - `services/funnel_service/service.py`
  - `backend/routes/funnel.py`
- 优先补齐：
  - 在服务层实现触发源优先级裁决与受保护终态覆盖限制。

### P0-2 市场结论页冲突裁决仍是文案级

- 表面现状：页面与接口已输出 `resolution_basis` 与 `conflict_note`。  
- 实际缺口：未执行“时效 > 风险 > 信号强度 > AI一致性”的真实计算裁决。  
- 影响：冲突场景下最终结论可能不稳定，用户信任受损。  
- 证据：
  - `backend/routes/market.py`
  - `apps/web/src/pages/market/MarketConclusionPage.vue`
- 优先补齐：
  - 引入冲突评分与优先级裁决函数，产出可解释的取舍结果。

### P0-3 Quick Insight 缺少硬 SLA 与边界守护

- 表面现状：已提供 `/api/llm/quick-insight` 快速结论接口。  
- 实际缺口：缺严格超时控制、必要证据集完整性校验、统一降级协议。  
- 影响：Quick 可能逐步退化为慢版 Deep，破坏主线效率。  
- 证据：
  - `backend/routes/llm_quick_insight.py`
- 优先补齐：
  - 增加 8s 超时硬截断、缺证据统一返回模型、与 Deep 解耦。

### P0-4 决策动作到执行任务自动承接不完整

- 表面现状：已存在 `portfolio` 页面与决策页“查看执行”入口。  
- 实际缺口：决策动作写入后，执行任务自动生成与状态回写链路不完整。  
- 影响：闭环卡在“有动作无后续状态”，复盘价值下降。  
- 证据：
  - `apps/web/src/pages/research/DecisionBoardPage.vue`
  - `backend/routes/portfolio.py`
  - `services/portfolio_service/service.py`
- 优先补齐：
  - 明确 `confirm/reject/defer/watch` 到执行任务的自动映射与回写规则。

---

## 3. P1 不足项（补齐后明显更顺手）

### P1-1 新主线页面 e2e 回归不足

- 表面现状：已有 smoke，覆盖 workbench 基础跳转。  
- 实际缺口：`market/funnel/portfolio/quick-insight` 缺专项回归。  
- 影响：主线新增能力回归风险高，易出现“上线后才发现断链”。  
- 证据：
  - `apps/web/tests/e2e/smoke.spec.ts`
  - `apps/web/tests/e2e/stocks.spec.ts`
- 优先补齐：
  - 新增主线专项 e2e 并接入阻断门禁。

### P1-2 Strategy 研报源已接入但未纳入统一调度与质量规范

- 表面现状：可手工执行“拉取 + 导入”并写入 `research_reports`。  
- 实际缺口：缺定时任务、内容质量规则、目录规范校验。  
- 影响：可用但不稳，长期维护成本高。  
- 证据：
  - `scripts/sync_strategy_repo.sh`
  - `scripts/sync_and_import_strategy_reports.sh`
  - `jobs/import_strategy_reports.py`
- 优先补齐：
  - 接入 `job_registry.py` + 导入质量报告（日期/重复/空内容/异常样本）。

### P1-3 外部仓库管理方式存在协作风险

- 表面现状：`external/strategy` 以嵌套仓库（gitlink）形式存在。  
- 实际缺口：团队 clone 主仓库后不会自动包含其内容。  
- 影响：跨机器结果不一致，排障成本上升。  
- 证据：
  - `external/strategy`（gitlink）
- 优先补齐：
  - 二选一：标准 submodule；或不入仓、统一通过同步脚本动态拉取。

---

## 4. P2 不足项（增强层）

### P2-1 指标门禁化未完全落地

- 表面现状：文档已定义对象页收口率、漏斗转化率、闭环完整率等指标。  
- 实际缺口：尚未全部变成“每日产出 + 阈值门禁 + 失败阻断”。  
- 影响：目标可描述但不可持续证明。  
- 证据：
  - `docs/decision_productization_batches_A_to_C_2026-04-18.md`
  - `jobs/collect_daily_metrics.py`
- 优先补齐：
  - 指标产出与回归门禁联动，形成“指标失败=不可放行”机制。

### P2-2 全链路错误可行动化仍需统一

- 表面现状：部分核心路径已有改进。  
- 实际缺口：跨模块错误语义与建议动作仍不统一。  
- 影响：用户感知为“同类错误不同说法”，学习成本高。  
- 证据：
  - `backend/server.py`
  - `backend/routes/system.py`
- 优先补齐：
  - 统一错误码词典 + 前端统一错误呈现组件。

---

## 5. 影响优先级排序（Top 5）

1. 候选漏斗并发裁决优先级落代码（P0-1）  
2. 决策动作自动承接执行任务（P0-4）  
3. 市场结论冲突裁决算法化（P0-2）  
4. Quick Insight 硬 SLA 与输入边界守护（P0-3）  
5. 主线新增页面 e2e 全覆盖并接门禁（P1-1）  

---

## 6. 与现有计划文档关系

- 本文是“当前不足项审计快照”，用于补充执行优先级。  
- 目标态参考：`docs/project_final_state_projection_2026-04-15.md`  
- 批次执行参考：`docs/decision_productization_batches_A_to_C_2026-04-18.md`  
- 终局能力参考：`docs/uzi_skill_reuse_final_architecture_2026-04-18.md`  
