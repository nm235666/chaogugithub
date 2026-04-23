# 决策产品化改造批次方案 A-C（2026-04-18）

## 1. 目标与原则

本文将“工程化投研控制台”升级为“顺手好用的决策产品”，聚焦三个批次：

- 批次 A：主线收敛（统一入口 + 角色首页 + 导航分层）
- 批次 B：业务闭环（决策动作 -> 执行任务 -> 结果复盘）
- 批次 C：体验升级（Quick Insight + Deep Workflow 双通道）

执行原则：
- 优先服务前端展示业务与测试可用性。
- 不破坏现有 API 与权限协议（仅兼容增强）。
- 每批次必须独立可交付、可回归、可回滚。

---

## 2. 批次 A：主线收敛

## 2.1 目标（最终效果）

1. 普通研究用户登录后有明确单一主入口，不再在多菜单中迷路。  
2. 管理后台与研究工作台分层展示，研究用户默认看不到运维噪声。  
3. 各业务页面都能回到同一个“决策工作台”，形成统一心智。  
4. 单只股票进入后不再依赖跨页拼装，而是对象级研究信息一次看全。  
5. 市场层信息不再分散在资讯/宏观/信号多页，而是压缩到单页“市场结论”。  
6. 候选不再是“又一个列表”，而是有状态流转的研究漏斗对象。  

## 2.2 范围

- 前端路由与导航结构
- 登录后落地页策略
- 统一入口页面（研究工作台）
- 股票对象主页面收口（`/stocks/detail/:tsCode?`）
- 市场结论页收口（聚合资讯 + 宏观 + 信号）
- 候选漏斗对象化（进入/增强/淘汰/晋级规则）

## 2.3 关键改动

1. 新增统一入口页：`/research/workbench`
- 首屏区：今日重点、待处理动作、风险预警、最近决策。
- 作为 `pro/limited` 的默认落地页。

2. 角色落地页分流
- `admin`：保持 `/dashboard`。
- `pro/limited`：落地到 `/research/workbench`。

3. 导航分层
- 一级导航仅保留主业务链：`市场/信号/研究/决策`。
- `system/*` 收拢到“系统控制台”（admin 可见）。
- 将非稳定能力（如按需量化）移到“高级工具”分组，并显示状态标签。

4. 主页面统一“进入决策工作台”动作
- `stocks/scores`、`stocks/detail`、`signals/overview`、`signals/graph`、`chatrooms/investment` 等页面加统一 CTA，携带上下文跳转。

5. `/stocks/detail/:tsCode?` 升级为“股票对象主页面”
- 固定 6 大区块（默认同屏可见或折叠一屏可达）：
  - 价格结构
  - 最新催化（个股/主题相关新闻摘要）
  - 主题/信号
  - 群聊热度
  - AI 结论（快读摘要）
  - 决策动作（进入决策板并保留上下文）
- 目标是“研究一只票不需要跨 4-6 个页面拼装”。

6. 新增“市场结论页”（建议路由：`/market/conclusion`）
- 将以下现有能力压成一个结论页：
  - 国际资讯（`/intelligence/global-news`）
  - 国内资讯（`/intelligence/cn-news`）
  - 宏观（`/macro`）
  - 主题信号（`/signals/themes`）
- 页面输出以决策导向组织：
  - 今日交易主线
  - 主要风险
  - 受益行业 / 受压行业
  - 值得跟踪的候选方向（可一键进入 workbench/对象页）
- 增加“冲突裁决口径”（固定优先级，避免结论话术漂移）：
  - `时效` > `风险等级` > `信号强度` > `AI 摘要一致性`
  - 当资讯结论与信号结论冲突时，按上述优先级生成最终结论，并显示“冲突说明 + 取舍依据”。

7. 新增“候选漏斗”产品对象（建议路由：`/research/funnel`）
- 把候选生命周期显式化为状态机：
  - `ingested`（进入池）
  - `amplified`（被催化增强）
  - `ai_screen_passed`（AI 初筛通过）
  - `shortlisted`（进入研究短名单）
  - `decision_ready`（进入决策板待动作）
  - `confirmed/rejected/deferred`（决策终态）
  - `executed/reviewed`（执行与复盘终态）
- 明确“为什么前进/为什么淘汰”：
  - 前进依据：催化强度、信号一致性、风险阈值、AI 置信度
  - 淘汰依据：证据冲突、催化衰减、风险超限、时效失效
- 每次状态流转都记录：`from_state`、`to_state`、`reason`、`evidence_ref`、`operator`、`timestamp`
- 明确“谁来驱动流转”（防止状态来源不一致）：
  - 自动流转：
    - `ingested -> amplified`：由信号增强/催化增强规则触发
    - `amplified -> ai_screen_passed`：由 AI 初筛阈值触发
    - `shortlisted -> decision_ready`：由前置条件满足触发（证据齐全、风险未超限）
  - 人工确认流转：
    - `ai_screen_passed -> shortlisted`：研究员确认纳入短名单
    - `decision_ready -> confirmed/rejected/deferred`：决策动作触发
    - `executed -> reviewed`：复盘确认触发
  - 混合触发（系统建议 + 人工确认）：
    - `amplified -> rejected`（高风险自动建议淘汰，人工最终确认）
    - `shortlisted -> deferred`（时效衰减或证据冲突时建议暂缓）
- 统一触发源枚举：`trigger_source = signal | ai_screen | researcher | decision_action | execution_feedback | system_rule`
- 同一候选在任一时刻仅允许一个当前状态，跨页读取统一以漏斗状态源为准。
- 异常流转保护（并发与幂等）：
  - 最终写权限优先级：`decision_action/researcher > execution_feedback > system_rule > ai_screen > signal`
  - 同时到达的多触发写入按“优先级 + 事件时间 + 版本号”裁决，低优先级写入不得覆盖高优先级终态。
  - 状态写入必须携带 `idempotency_key`（建议：`candidate_id + trigger_source + trigger_event_id`），重复提交只生效一次。
  - 使用乐观锁字段 `state_version` 防并发覆盖：仅当版本匹配时允许更新，不匹配则重读后重放裁决。
  - 对 `confirmed/rejected/executed/reviewed` 设为受保护终态：非高优先级触发不得直接回写到前置状态。
- 漏斗页要回答三个核心问题：
  - 现在池子里有多少票，分别卡在哪一层？
  - 为什么这只票晋级/淘汰？
  - 从候选到决策的转化率和耗时如何？

## 2.4 文件落点

- `apps/web/src/app/router.ts`
- `apps/web/src/shared/ui/AppShell.vue`
- `apps/web/src/app/permissions.ts`
- `apps/web/src/pages/research/ResearchWorkbenchPage.vue`（新增）
- `apps/web/src/pages/market/MarketConclusionPage.vue`（新增）
- `apps/web/src/pages/research/CandidateFunnelPage.vue`（新增）
- `apps/web/src/pages/stocks/StockScoresPage.vue`
- `apps/web/src/pages/stocks/StockDetailPage.vue`
- `apps/web/src/pages/signals/SignalsOverviewPage.vue`
- `apps/web/src/pages/signals/SignalChainGraphPage.vue`
- `apps/web/src/pages/chatrooms/ChatroomInvestmentPage.vue`
- `apps/web/src/services/api/market.ts`（新增，聚合市场结论数据）
- `apps/web/src/services/api/funnel.ts`（新增，候选漏斗读写与状态流转）
- `backend/routes/market.py`（新增，或在现有 intelligence/macro 路由增加聚合端点）
- `backend/routes/funnel.py`（新增，或在 decision/signal 路由增加漏斗聚合端点）
- `services/funnel_service/`（新增，维护候选漏斗状态机与流转规则）

## 2.5 验收标准

1. `limited/pro` 登录后 100% 落到 `/research/workbench`。  
2. 从核心页面进入决策工作台的上下文透传成功率 >= 99%。  
3. 研究角色导航中不再暴露 `system/*` 一级菜单。  
4. `/stocks/detail/:tsCode?` 对象页的 6 大区块可在单页完成阅读，不依赖跨页拼装。  
5. 市场问题“今天交易什么/风险是什么/行业受益受压”可在 `/market/conclusion` 单页回答。  
6. 候选漏斗链路可追踪：`市场结论 -> 候选 -> 对象页 -> AI -> 决策` 每步都可解释。  
7. 任一候选可回查“进入原因、晋级原因、淘汰原因、当前卡点”。  
8. 页面截图审查中，导航信息密度明显下降且主线可识别。  

## 2.6 回归验证

- `npm run build`
- `npm run smoke:e2e:all`
- 新增 e2e：
  - 角色落地页分流
  - 导航可见性（role-based）
  - 各入口跳 `/research/workbench` 的 query 上下文一致性
  - `/stocks/detail/:tsCode?` 六区块渲染完整性
  - `/market/conclusion` 单页结论完整性与跳转可用性
  - `/research/funnel` 状态流转正确性与原因可追溯性

## 2.7 风险与回滚

风险：
- 路由守卫和权限显示不一致导致“可见不可达”。
- 股票对象页聚合后首屏信息密度过高，导致可读性下降。
- 市场结论聚合口径不统一，可能出现“资讯结论与信号结论冲突”。
- 漏斗状态定义不清导致同一标的在不同页面状态不一致。

回滚点：
- `router.ts` 中 `resolveLanding` 恢复原逻辑；
- `AppShell.vue` 恢复原菜单结构；
- 入口 CTA 保留但不强制跳转到 workbench。
- 股票对象页可临时退回“分 Tab 聚合展示”，保留旧子入口链接。
- 市场结论页可降级为只读汇总卡，不阻断原资讯/宏观/信号入口。
- 漏斗页可先降级为只读漏斗看板，流转操作仍走原决策入口。

---

## 3. 批次 B：业务闭环

## 3.1 目标（最终效果）

1. 决策不止“建议 + 留痕”，而是进入“执行任务”并跟踪结果。  
2. 用户可以在系统内完成：判断 -> 动作 -> 执行状态 -> 复盘。  
3. 所有动作都有可追溯 ID 与证据链，不再只有日志感记录。  

## 3.2 范围

- 决策动作的后续承接模型
- 执行任务页面（先做纸面组合，不对接真实券商）
- 复盘字段与反馈环节

## 3.3 关键改动

1. 决策动作升级为“执行任务”
- `confirm/reject/defer/watch/review` 写入 `decision_actions` 同时生成 `execution_tasks`。
- `confirm` 默认进入“待执行”；`defer/watch` 默认进入“观察队列”。

2. 新增执行层页面
- `/portfolio/positions`：当前纸面持仓与状态。
- `/portfolio/orders`：计划单、执行单、取消单。
- `/portfolio/review`：执行偏差与复盘结论。

3. 决策页展示闭环状态
- 在 `research/decision` 给每条动作显示执行状态（未执行/执行中/已完成/已取消）。
- 提供“查看执行详情”跳转。

4. 复盘字段补齐
- 最低字段：`planned_price`、`executed_price`、`size`、`latency`、`slippage`、`review_tag`、`review_note`。
- 强制“执行后反馈”可见，不允许只有动作无结果。

## 3.4 文件落点

- `apps/web/src/pages/research/DecisionBoardPage.vue`
- `apps/web/src/services/api/decision.ts`
- `apps/web/src/services/api/portfolio.ts`（新增）
- `apps/web/src/pages/portfolio/PositionsPage.vue`（新增）
- `apps/web/src/pages/portfolio/OrdersPage.vue`（新增）
- `apps/web/src/pages/portfolio/ReviewPage.vue`（新增）
- `backend/routes/decision.py`
- `backend/routes/portfolio.py`（新增）
- `services/decision_service/service.py`
- `services/portfolio_service/`（新增）

## 3.5 验收标准

1. 决策动作到执行任务的自动关联率 100%。  
2. 任一动作可在 30 秒内定位到执行任务与复盘记录。  
3. 决策页“动作有状态、状态有解释、解释可追溯”覆盖率 100%。  
4. 用户可在不离开系统的情况下完成闭环全流程。  

## 3.6 回归验证

- `npm run build`
- `npm run smoke:e2e:all`
- 新增 e2e：
  - 决策动作生成执行任务
  - 执行状态流转（待执行 -> 执行中 -> 已完成/取消）
  - 复盘字段写入与展示

## 3.7 风险与回滚

风险：
- 新增执行层后，旧决策页面出现字段兼容问题。

回滚点：
- 保留原 `recordDecisionAction` 旧路径；
- 执行任务模块可降级为只读；
- 页面端隐藏执行入口，仅保留日志展示。

---

## 4. 批次 C：体验升级

## 4.1 目标（最终效果）

1. AI 分析既支持“秒级可读结论”，也支持“深度异步流程”。  
2. 用户不会因排队或 worker 慢而失去操作路径。  
3. 状态语义从“系统内部状态”转为“用户任务状态”。  

## 4.2 范围

- 多角色与趋势等研究页的交互模式
- 快速结论通道（同步）
- 深度任务通道（异步）
- 状态文案与异常反馈

## 4.3 关键改动

1. 双通道分析模型
- `Quick Insight`（<=8s）：返回结构化结论卡（观点、风险、建议动作）。
- `Deep Workflow`（异步）：完整多角色、取证、汇总、待审批链路。
- Quick 输入边界（硬约束）：
  - 仅消费“当前对象必要证据集”（价格快照、最新催化摘要、信号摘要、基础风险标签）。
  - 不等待全量深度取证、不触发多角色全链路聚合。
  - 若必要证据缺失，Quick 返回“受限结论 + 缺失项提示”，而不是阻塞等待 Deep 数据。

2. 页面交互改造
- 首屏先展示 Quick 结果并标注“快速版/置信度级别”。
- 同时后台拉起 Deep 任务，完成后增量替换或补充详情。

3. 状态词表重写
- 把 `queued/running/pending_user_decision/...` 翻译成用户可懂的话术：
  - 排队中：预计等待、当前位置
  - 分析中：当前阶段、已完成比例
  - 需你决定：给出三种建议动作
  - 已完成（含告警）：明确哪些部分降级

4. 失败兜底与可恢复
- Deep 失败时保留 Quick 结果可继续用。
- 提供“重试失败角色/降级汇总/仅用快速结论继续”的明确按钮。

## 4.4 文件落点

- `apps/web/src/pages/research/MultiRoleResearchPage.vue`
- `apps/web/src/pages/research/TrendAnalysisPage.vue`
- `apps/web/src/pages/research/ChiefRoundtablePage.vue`
- `apps/web/src/services/api/research.ts`
- `backend/routes/llm_multi_role.py`（或现有对应路由文件）
- `services/agent_service/multi_role_v3.py`
- `services/decision_service/service.py`

## 4.5 验收标准

1. 用户首次触发分析后 8 秒内拿到可读结论卡。  
2. Deep 任务失败时，Quick 通道仍可用且有清晰提示。  
3. 用户能够理解“当前发生了什么、为什么、下一步做什么”。  
4. 多角色页面的用户中断恢复成功率 >= 99%。  

## 4.6 回归验证

- `npm run build`
- `npm run smoke:e2e:all`
- 新增 e2e：
  - Quick 与 Deep 并行触发
  - Deep 失败降级路径
  - 用户决策中断后恢复
  - 状态文案映射正确性

## 4.7 风险与回滚

风险：
- 双通道并行导致状态竞争，出现“结果闪烁/覆盖顺序错乱”。

回滚点：
- Quick 作为只读提示卡，不覆盖 Deep 主结果；
- 关闭自动替换，仅允许手动“应用深度结果”。

---

## 5. 批次依赖关系

- A 是 B/C 的前置：没有统一入口，闭环和体验改造会继续分散。  
- B 与 C 可并行部分推进，但建议先完成 B 的数据闭环骨架，再做 C 的交互增强。  

推荐顺序：
1. A（主线收敛）
2. B（闭环成型）
3. C（体验提效）

---

## 6. 跨批次统一验收指标

1. 主线触达率：研究用户从首页到决策动作的平均点击步数 <= 3。  
2. 闭环完整率：有输入、有判断、有动作、有结果反馈的任务占比 >= 85%。  
3. 可追溯覆盖率：关键动作 `trace_id` 与证据链完整率 >= 99%。  
4. 降级可用率：异步失败场景仍有可执行路径覆盖率 100%。  
5. 回归阻断有效率：高风险失败必须阻断合入，拦截率 100%。  
6. 对象页收口率：研究单票时“无需离开 `/stocks/detail/:tsCode?` 即可完成主要判断”的会话占比 >= 80%。  

---

## 7. 本文与现有文档关系

- 对齐目标文档：`docs/project_final_state_projection_2026-04-15.md`
- 对齐差距文档：`docs/final_state_gap_report_2026-04-16.md`
- 对齐执行文档：`docs/final_state_remaining_34_execution_plan_2026-04-16.md`
- 能力终局参考：`docs/uzi_skill_reuse_final_architecture_2026-04-18.md`

本文用于把 A-C 三个批次直接落到执行层，不替代上述文档。

---

## 8. 需补充的缺口项（并入 P0/P1/P2）

以下为在原整改清单基础上必须补齐的系统性缺口，避免“页面变顺手，但底层机制脆弱”。

## 8.1 P0 补充项（不改会持续影响主线）

### P0-1 数据新鲜度与时效标识统一

目标：
- 所有主决策信息都带“数据时间戳 + 新鲜度状态（正常/延迟/过期）”。

验收：
- `stocks/signals/research/workbench` 关键卡片覆盖率 100%。
- 用户可在 1 屏内判断“这个结论是不是今天可用”。

落点：
- `apps/web/src/shared/ui/StatCard.vue`
- `apps/web/src/pages/research/ResearchWorkbenchPage.vue`
- `apps/web/src/pages/stocks/StockDetailPage.vue`
- `apps/web/src/pages/signals/SignalsOverviewPage.vue`
- `backend/routes/system.py`（统一 freshness 字段）

### P0-2 跨页上下文一致性契约

目标：
- 从 `stocks/signals/news/chatrooms` 进入决策链时，上下文字段统一、刷新不丢、分享可恢复。

验收：
- 跨页进入决策板上下文丢失率 < 1%。
- URL query 与 store 状态一致率 100%。

落点：
- `apps/web/src/pages/research/DecisionBoardPage.vue`
- `apps/web/src/stores/`（新增/收敛 decision context store）
- `apps/web/src/services/api/decision.ts`

### P0-3 异步任务可靠性闭环

目标：
- 针对 worker 离线、pending 积压、未知错误，形成自动恢复与人工接管双机制。

验收：
- stale pending 任务自动收敛成功率 >= 99%。
- 卡死任务可在系统页一键重试/终止，且状态可解释。

落点：
- `services/agent_service/multi_role_v3.py`
- `backend/server.py`（任务终态回收与错误映射）
- `apps/web/src/pages/system/JobsOpsPage.vue`

### P0-4 错误反馈可行动化

目标：
- 禁止直接暴露 `UNKNOWN_ERROR`；必须映射到用户可理解原因和下一步动作。

验收：
- 核心链路错误提示中“可行动建议”覆盖率 100%。
- 未分类错误占比连续下降。

落点：
- `backend/server.py`（错误码标准化）
- `apps/web/src/shared/ui/*`（统一错误提示组件）
- `apps/web/src/pages/research/MultiRoleResearchPage.vue`

### P0-5 决策证据链强制化

目标：
- 每条关键动作都带 `trace_id + evidence_sources + snapshot_id/run_id`（无则显式标注无标识）。

验收：
- 关键动作证据链完整率 >= 99%。
- 任一动作 30 秒内可定位到证据详情。

落点：
- `apps/web/src/pages/research/DecisionBoardPage.vue`
- `backend/routes/decision.py`
- `services/decision_service/service.py`

### P0-6 空态/弱数据态产品化

目标：
- 空态不只展示“暂无数据”，而是给出原因、替代入口、推荐下一步。

验收：
- 主链页面空态均提供至少 1 个替代动作。
- 弱数据场景下无“空白可用假象”。

落点：
- `apps/web/src/pages/signals/SignalChainGraphPage.vue`
- `apps/web/src/pages/research/ResearchWorkbenchPage.vue`
- `apps/web/src/pages/stocks/StockDetailPage.vue`

## 8.2 P1 补充项（改了显著更顺手）

### P1-1 统一任务收件箱

目标：
- 把待审批、待重试、待复盘汇总到一个任务收件箱，避免分散在多页。

落点：
- `apps/web/src/pages/research/TaskInboxPage.vue`（新增）
- `apps/web/src/app/router.ts`
- `backend/routes/decision.py`（聚合接口）

### P1-2 性能预算与交互 SLA

目标：
- 设定首屏、图谱、列表响应预算；超限自动降级，保证可用优先。

落点：
- `apps/web/src/pages/signals/SignalChainGraphPage.vue`
- `apps/web/src/pages/research/ResearchWorkbenchPage.vue`
- `tests/` 性能基线脚本

### P1-3 权限可见性一致化

目标：
- 不可访问即不可见，减少“点开才被拦截”的体验割裂。

落点：
- `apps/web/src/app/permissions.ts`
- `apps/web/src/shared/ui/AppShell.vue`

### P1-4 主线漏斗埋点

目标：
- 建立“市场结论 -> 候选 -> 对象页 -> 动作 -> 复盘”转化指标。

落点：
- `apps/web/src/services/api/metrics.ts`
- `jobs/collect_daily_metrics.py`
- `docs/metrics/`

### P1-5 写操作幂等

目标：
- 决策提交、配置保存、任务触发防重复，避免双击脏写。

落点：
- `apps/web/src/pages/research/DecisionBoardPage.vue`
- `apps/web/src/pages/system/LlmProvidersPage.vue`
- `backend/routes/system.py`

## 8.3 P2 补充项（增强层）

### P2-1 策略模板库

目标：
- 提供可复用研究动作模板，降低新用户决策成本。

### P2-2 角色化 onboarding

目标：
- 研究员/决策者/管理员首登路径分流，减少学习负担。

### P2-3 审美与可读性门禁常态化

目标：
- 截图回归加入 8 维评分阈值，不只检查能否渲染。

### P2-4 周度复盘自动报告

目标：
- 自动沉淀本周命中率、误判来源与规则优化建议。

---

## 9. 新增项与 A-C 批次映射

- 批次 A 主收敛：P0-1 / P0-2 / P1-3  
- 批次 A 主收敛（补充）：股票对象主页面收口 / 市场结论页收口  
- 批次 A 主收敛（新增核心）：候选漏斗对象化（状态机 + 进退场规则 + 可追溯流转）  
- 批次 B 主闭环：P0-5 / P1-1 / P1-5 / P2-4  
- 批次 C 主体验：P0-3 / P0-4 / P0-6 / P1-2  
- 指标与治理穿透：P1-4 / P2-3 / P2-2 / P2-1  
