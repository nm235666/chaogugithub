# 当前项目不足项审计（2026-04-18）

## 1. 审计结论

当前项目已从“分散功能页”进入“决策主线雏形”阶段，但仍存在关键缺口：  
主线可见，但**多数模块**的规则硬约束仍不足；**例外**：市场结论多源冲突已在 `backend/routes/market.py` 落地 **R30 加权数值裁决**（见 **P0-2** 修订），属已部分硬化的规则链，其剩余风险主要在**输入质量（P0-6）、证据灌满度（P0-5）、文档与产品口径是否与加权模型一致**。  
页面已连通，但业务闭环后半段未完全自动化；能力已有文档定义，但部分尚未落成可阻断的工程机制。

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

### P0-2 市场结论页冲突裁决（R30：已实现数值裁决；文档与产品口径需对齐）

> **2026-04-23 修订说明**：本条已对照 `backend/routes/market.py` 更新。此前「仍是文案级 / 未真实算分」的描述**与当前代码不一致**，易误导团队；现改为「已实现什么 + 仍剩什么风险」。

- **已实现（代码为准）**
  - `query_market_conclusion_from_conn` 在窗口内聚合多表行：主题热点、投资信号、新闻日报、个股新闻、宏观序列、风险情景等（各 `_fetch_*_rows`）。
  - **`_score_conflict_resolution`**（及 `_score_source_group`、`_group_source_rows`）对每个 `source` 分组计算子指标后，用 **加权综合分** 比较各来源，选出 `winning_source`，并写入接口字段：
    - **综合分公式**（与文件内注释及前端规则链副标题一致）：  
      `composite = 0.45 * signal_strength + 0.20 * recency_score + 0.20 * ai_consistency + 0.15 * risk_priority`  
      其中 `risk_priority` 主要来自 `risk_scenarios` 源；非风险源为 0。
    - **同分细化排序**（在 `composite` 相同或极接近时的次序）：  
      按 `(-composite, -risk_priority, data_age_hours, source)` 排序——即综合分更高者优先；再比风险优先项；再比数据龄（小时）；最后比来源名字符串。
    - **输出结构**：`conflict_resolution` 内含 `confidence`、`direction`、`winning_source`、`score_breakdown`（含各源 `composite` / 分项）、`needs_review`（如胜者 `composite < 0.5`）、`dissenting_sources`（与胜者分差 ≥ 0.15 的来源）、`resolution_basis`（人类可读胜出理由，如时效优势 / 风险优先 / 信号强度等分支）。
  - **前端**：`MarketConclusionPage.vue` 在 `conflictResolution.score_breakdown.sources` 非空时展示「查看规则链」与分解表（与上述权重文案一致）。

- **与旧审计表述的差异（避免产品/研发各说各话）**
  - 旧版写的是 **字典序**「时效 > 风险 > 信号强度 > AI一致性」的**纯层级裁决**；当前实现是 **四维加权求和成一个 composite**，再排序决胜，**不是**同一套数学定义。若产品规格书仍用旧句式，应二选一：**改文档为加权模型**，或 **改代码为规格中的层级模型**。

- **仍存在的缺口 / 风险（不否定已实现算分）**
  - **输入质量**：各源行若本身有误（见 **P0-6 信号主体错位**），算分再「真」也会得出**可信的错误结论**；需在信号构建层与 `candidate_directions` 出口治理。
  - **证据覆盖**：源行过少或聚合异常时，`score_breakdown.sources` 可能为空或仅单源，前端不展示规则链；`resolution_basis` 会退化为「无足够数据源参与裁决」等——属于**数据/任务侧**问题，而非「没写算分函数」。
  - **可信度门槛**：`needs_review` 仅与胜者 `composite` 阈值等挂钩；是否与业务上「必须人工复核」清单完全一致，仍需产品确认。
  - **口径统一**：`resolution_basis` 中「综合评分 …（信号强度>时效>…）」一句为**叙述性总结**，与实现上的加权公式应视为**同一套 R30 的口语化说明**，避免被理解成第二套独立排序规则。

- **证据（定位代码）**
  - `backend/routes/market.py`：`# R30 conflict arbitration helpers`、` _score_source_group`、` _score_conflict_resolution`、`query_market_conclusion_from_conn` 内组装 `conflict_resolution`。
  - `apps/web/src/pages/market/MarketConclusionPage.vue`：置信度徽章、`conclusion-rule-trace-toggle`、规则链 `PageSection` 副标题权重。

- **优先补齐（按优先级重排）**
  1. **产品/审计口径**：在全站文档中统一为「R30 加权综合裁决」，删除或更正「仅文案、未算分」类表述。  
  2. **数据与上游质量**：推进 P0-6，减少 `investment_signals` 等源对 composite 的污染。  
  3. **门禁**：对「多源参与且 `needs_review` 为 false」等场景增加接口或 E2E 契约测试，避免回归时退回无算分路径。  
  4. 若业务坚持「严格字典序而非加权」：再单独立项改 `_score_conflict_resolution` 与前端说明，**本条 P0-2 可降级为已关闭后另开需求**。

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

### P0-5 核心判断页存在“结构性空白页”

- 表面现状：页面框架、标题、卡片和 CTA 已存在，视觉上像是可用的结论页或动作页。  
- 实际缺口：多页依赖的聚合结果未真正灌满，导致页面长期大面积显示 `暂无数据 / 数据不足 / 暂无记录`，属于“页面有了、结论没形成”。  
- 影响：
  - 前端展示业务会出现“能打开但不能判断”的假完成态
  - smoke 通过也不能证明主链成立
  - 用户无法在 `3 分钟判断` 与 `5 分钟动作` 目标内完成任务
- 已验证高优先级页面：
  - `/app/market`
  - `/app/workbench`
  - `/app/research/scoreboard`
  - `/app/decision`
  - `/app/macro-regime`
  - `/app/allocation`
- 证据：
  - [structural_empty_pages_audit_2026-04-20.md](/home/zanbo/zanbotest/docs/structural_empty_pages_audit_2026-04-20.md)
  - `apps/web/src/pages/market/MarketConclusionPage.vue`
  - `apps/web/src/pages/research/ResearchWorkbenchPage.vue`
  - `apps/web/src/pages/research/ScoreboardPage.vue`
  - `apps/web/src/pages/research/DecisionBoardPage.vue`
  - `apps/web/src/pages/macro/MacroRegimePage.vue`
  - `apps/web/src/pages/portfolio/AllocationPage.vue`
- 优先补齐：
  - 先按“前端字段等待 -> 接口返回 -> 后端聚合根因”逐页排查
  - 优先修 `market/workbench/decision/macro-regime/allocation`
  - 当前进度：
    - `market` 已开始切到真实主表并补显式状态字段
    - `workbench` 已开始把“六问”状态化
    - `scoreboard/decision` 已开始把泛化空文案拆成根因型说明
    - `macro-regime/allocation` 已开始区分 `ready / insufficient_evidence / not_initialized`
  - 仍未完成：
    - P0 页的“核心区块有答案”覆盖率还没有门禁化
    - 决策到执行的真实承接、宏观到配置的自动生成质量仍需继续补强

### P0-6 候选方向存在“信号主体错位”（观点发布者被当作可交易标的）

- 表面现状：`/app/market` 的“候选方向”会出现如“中信证券”这类标的，且带有“看多、活跃、较高强度”。
- 实际缺口：当前信号构建链路会把“券商观点发布主体”直接归到 `stock` 信号桶，行业/主题观点在回写时被投射为该发布主体的个股看多信号，导致“观点来源主体”与“投资受益主体”混淆。
- 影响：
  - 候选池被伪机会污染，降低“今天买什么”可信度。
  - 决策板与执行链输入质量下降，放大误决策风险。
  - 3 分钟判断与 5 分钟动作 SLA 的有效性被削弱（页面有答案但答案主体偏移）。
- 证据：
  - `backend/routes/market.py`（候选方向取数：从 `investment_signal_tracker_*` 取看多 TopN）
  - `build_investment_signal_tracker.py`（`merge_stock_news_signals` / `determine_stock_news_direction`）
  - `investment_signal_tracker_7d` 实际样本：`600030.SH`（中信证券）由大量“中信证券：行业观点”新闻累积为 `direction=看多, signal_status=活跃`
  - `stock_news_items` 实际样本：多数标题为“中信证券：xxx行业/主题观点”，并非公司本体催化
- 优先补齐：
  - 在信号构建层拆分“发布主体”和“受益主体”：
    - 发布主体仅作为证据来源，不直接进入 `stock` 候选信号；
    - 行业/主题观点优先沉淀到 `theme/sector`，只有命中公司本体影响时才写入该 `ts_code`。
  - 在 `market candidate_directions` 出口增加主体校验，过滤“机构观点发布主体型”伪个股候选。

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
3. 结构性空白页逐页修实（P0-5）  
4. 市场结论**可信度与漂移治理**（P0-2：R30 算分已落地；持续推进 **P0-6** 信号主体、**P0-5** 证据覆盖、口径文档与契约门禁）  
5. Quick Insight 硬 SLA 与输入边界守护（P0-3）  

---

## 6. 与现有计划文档关系

- 本文是“当前不足项审计快照”，用于补充执行优先级。  
- 目标态参考：`docs/project_final_state_projection_2026-04-15.md`  
- 批次执行参考：`docs/decision_productization_batches_A_to_C_2026-04-18.md`  
- 终局能力参考：`docs/uzi_skill_reuse_final_architecture_2026-04-18.md`  
- 数据价值缺口参考：`docs/underutilized_data_audit_2026-04-20.md`
- 结构性空白页参考：`docs/structural_empty_pages_audit_2026-04-20.md`

---

## 7. 补充说明：部分缺口的根因是“已有数据未进入主链”

当前项目的部分功能缺口，并不只是“还没有某个模块”或“还没有某个页面”，而是系统里已经有一批高价值数据，但还没有被压缩成主链可执行能力。

典型表现包括：

- 有资金流数据，但还没有稳定转成买入确认、减仓触发和仓位压缩规则
- 有风险情景数据，但还没有稳定转成动作卡中的失效条件与风控约束
- 有公司事件数据，但还没有稳定转成事件驱动型候选晋级、观察或卖出理由
- 有日报总结、群聊元数据、信号状态机、主题映射等数据，但仍停留在展示、摘要或治理层

这类问题的特点是：

- 表面上看“能力已经有了”
- 实际上还没有进入 `workbench / market / funnel / decision / positions / allocation / review` 主链
- 因而没有真正转成“今天买什么、卖什么、仓位多少、为什么、何时失效、宏观怎么应对”的用户可见结果

详细的数据价值缺口、优先级和主链落点，统一以
`docs/underutilized_data_audit_2026-04-20.md`
为准。
