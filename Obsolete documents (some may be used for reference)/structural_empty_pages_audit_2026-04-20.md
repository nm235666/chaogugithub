# 结构性空白页审计（2026-04-20）

## 1. 文档目的

这份文档记录的是一类特殊问题：

- 不是正常业务空态
- 不是“用户没查到数据”
- 不是“当前对象本来就没有这类信息”

而是页面本身承诺要输出结论、判断、动作或主链摘要，但当前真实数据链路没有灌满，最终页面大面积显示“暂无数据 / 数据不足 / 暂无结论 / 暂无记录”。

这类问题会直接伤害两件事：

- 前端展示业务：页面看起来像完成了，实际没有形成可用结果
- 测试可用性：smoke 可能通过，但业务主链仍然是空心的

因此，这份文档不是一般空态盘点，而是“结构性空白页”台账。

---

## 2. 判断标准

本次按三层口径判断页面是否属于“结构性空白”：

### 2.1 高相似度

- 页面职责是给结论、给判断、给动作
- 不是单纯列表页或查询页
- 内部依赖多个聚合数据源
- 一旦后端聚合不完整，就会出现大面积占位文案

### 2.2 中等相似度

- 页面承载一定的研究对象或分析能力
- 空态部分合理，但空态数量明显偏多
- 更像“数据已接入一部分，但还没有稳定吃满”

### 2.3 低相似度或正常业务空态

- 页面本身就是查询、列表、治理或条件型结果页
- 当前为空更可能是正常筛选、对象稀疏或尚未录入
- 不应与“结构性空白页”混为一谈

---

## 3. 结论总览

- 高相似度页面：`6`
- 中等相似度页面：`7`
- 低相似度或正常业务空态页面：若干

当前至少有 `13` 个页面值得重点复查，但真正与“市场结论页空白”同类、会直接伤害主链判断能力的，是前 `6` 个。

### 3.1 当前治理进度（2026-04-20 本轮已落地）

本轮已经开始把“结构性空白页”从静态占位问题，推进到“显式状态 + 缺因解释 + 可执行下一步”：

- 已进入修复态的页面：
  - `/app/market`
  - `/app/workbench`
  - `/app/research/scoreboard`
  - `/app/decision`
  - `/app/macro-regime`
  - `/app/allocation`
- 已落地能力：
  - 核心聚合接口开始补充显式状态字段：
    - `status`
    - `status_reason`
    - `missing_inputs`
    - `generated_from`
  - 市场结论页开始按显式状态渲染，不再只靠空集合猜页面状态
  - 宏观三周期与长线配置页开始区分：
    - `ready`
    - `insufficient_evidence`
    - `not_initialized`
  - 工作台“今日 6 问”开始从“有无数据”升级为“已回答 / 证据不足 / 未初始化”
  - 评分总览与决策板开始把“暂无数据”拆成更接近根因的说明
- 仍未完成：
  - P1 七个页面的解释型空态治理
  - 结构性空白页专项 e2e 断言
  - 所有聚合接口的统一状态协议覆盖

这说明主链的 6 个核心页已经从“整块空心化”开始转向“有状态、可解释”，但距离“核心区块都稳定有答案”仍有明显差距。

---

## 4. 高相似度页面（P0 复查范围）

## 4.1 `/app/market`

- 对应文件：
  - [MarketConclusionPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/market/MarketConclusionPage.vue)
  - [market.ts](/home/zanbo/zanbotest/apps/web/src/services/api/market.ts)
  - [market.py](/home/zanbo/zanbotest/backend/routes/market.py)
- 页面承诺：
  - 今日交易主线
  - 主要风险
  - 行业影响
  - 候选方向
  - 数据来源
- 当前表现：
  - `暂无交易主线数据`
  - `暂无风险数据`
  - `暂无候选方向数据`
  - `暂无来源信息`
- 已验证根因：
  - 后端聚合逻辑仍依赖一批旧表或弱聚合来源，例如 `stock_news`、`macro_indicators`、`theme_hotspots`、`investment_signals`
  - `main_risks` 初始化后几乎未被有效填充
  - `sources` 字段协议不一致：前端按 `array | number` 读取，后端返回对象结构，导致来源统计显示为空
  - `trading_theme` 主要依赖摘要截断，缺乏稳定的主线生成逻辑
- 判断：
  - 这是当前最典型的结构性空白页
- 影响：
  - 用户无法在市场层形成单口径判断
  - “市场结论页”退化为占位页面
- 当前修复进度：
  - 已开始把后端聚合切到当前真实主表：
    - `news_daily_summaries`
    - `macro_series`
    - `theme_hotspots`
    - `investment_signal_tracker*`
    - `risk_scenarios`
    - `signal_state_tracker`
  - 已补 `status/status_reason/missing_inputs/generated_from/sources`
  - 前端已开始解释“为什么没有主线、为什么没有风险、缺哪类输入”
  - 仍待继续：
    - 主线生成逻辑仍偏摘要驱动
    - 冲突裁决虽已开始算法化，但还需要进一步校准真实有效性

## 4.2 `/app/workbench`

- 对应文件：
  - [ResearchWorkbenchPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/research/ResearchWorkbenchPage.vue)
- 页面承诺：
  - 研究主入口
  - 今日重点
  - 风险
  - 动作
  - 宏观结论
  - 候选与主题收口
- 当前表现证据：
  - `暂无主题数据`
  - `暂无候选`
  - `暂无高优先级风险`
  - `暂无现成动作`
  - `暂无卖出/减仓理由记录`
  - `暂无宏观三周期结论`
- 判断：
  - 这是“研究工作台有壳层、但聚合结果还没灌满”的典型
- 主要问题：
  - 下游依赖多，任何一个聚合链路没形成，就会把首页打空
  - 六问式判断没有真正产出答案时，页面会像过程描述而非结果页
- 影响：
  - 无法支撑“登录后 3 分钟形成判断”
- 当前修复进度：
  - 已把“今日 6 问”从二元 answered/not answered 升级为状态化表达
  - 已开始把市场结论、配置动作、宏观状态、候选漏斗等状态映射回首页
  - 仍待继续：
    - 六问仍未全部形成强答案门禁
    - 部分答案仍依赖下游聚合页继续补强

## 4.3 `/app/research/scoreboard`

- 对应文件：
  - [ScoreboardPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/research/ScoreboardPage.vue)
- 页面承诺：
  - 评分总览
  - 短名单
  - 理由包
  - 市场模式摘要
- 当前表现证据：
  - `当前还没有可展示的短名单`
  - `数据不足`
  - `暂无市场模式摘要`
  - `暂无行业评分数据`
  - `当前还没有理由包可展示`
- 判断：
  - 这页不是简单数据表，而是研究入口级总览页，因此大面积空态属于结构性问题
- 影响：
  - 用户看得到评分框架，但拿不到足够结论材料
- 当前修复进度：
  - 已开始把短名单为空、市场模式为空、理由包为空拆成更具体的根因提示
  - 已利用 `source_health` 显示缺失来源
  - 仍待继续：
    - 评分页还没有完全纳入统一状态模型
    - 行业评分、理由包、宏观模式之间的收口仍不够强

## 4.4 `/app/decision`

- 对应文件：
  - [DecisionBoardPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/research/DecisionBoardPage.vue)
- 页面承诺：
  - 动作确认
  - 裁决记录
  - 风险提示
  - 验证层
  - 行业与短名单辅助判断
- 当前表现证据：
  - `数据不足`
  - `暂无市场模式摘要`
  - `暂无验证数据`
  - `暂无行业评分数据`
  - `暂无裁决记录`
  - `暂无人工确认记录`
  - `暂无风险提示`
- 判断：
  - 决策板的主问题不是“页面打不开”，而是“页面打开了但决策流量没形成”
- 影响：
  - 5 分钟动作 SLA 很难成立
  - 决策闭环停在说明层，而不是动作层
- 当前修复进度：
  - 已开始补“暂无裁决记录 / 暂无人工确认记录 / 暂无验证数据”的机制级解释
  - 已强化 `decision_action_id -> /app/orders` 的回查语义
  - 仍待继续：
    - 动作自动承接执行任务仍不完整
    - 验证层与人工确认层的状态模型还需要继续收口

## 4.5 `/app/macro-regime`

- 对应文件：
  - [MacroRegimePage.vue](/home/zanbo/zanbotest/apps/web/src/pages/macro/MacroRegimePage.vue)
- 页面承诺：
  - 宏观短中长期状态
  - 状态变化
  - 历史沉淀
- 当前表现证据：
  - `暂无记录`
  - `暂无三周期状态记录，请通过下方表单录入首次评估`
  - `暂无历史记录`
- 判断：
  - 当前更像“手工录入页”，不是“计算 + 人工复核 + 沉淀”的宏观状态页
- 影响：
  - 长线链路的第一页就容易空心化
- 当前修复进度：
  - 已补页面级状态面板
  - 已开始区分“未初始化 / 证据不足 / 已就绪”
  - 已把历史空表解释成更接近根因的状态
  - 仍待继续：
    - 当前仍明显偏手工录入
    - 距离“计算 + 人工复核 + 沉淀”仍有差距

## 4.6 `/app/allocation`

- 对应文件：
  - [AllocationPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/portfolio/AllocationPage.vue)
- 页面承诺：
  - 长线组合动作建议
  - 历史配置记录
- 当前表现证据：
  - `暂无配置记录，请先在宏观三周期录入状态`
  - `暂无记录`
- 判断：
  - 这是宏观状态页空心化后的直接连带问题
- 影响：
  - 长线配置动作没有稳定产出
  - 宏观判断无法自动承接到组合动作
- 当前修复进度：
  - 已补页面级状态面板和历史空态解释
  - 已开始区分“没有宏观状态 / 有状态但证据不足 / 已能输出配置动作”
  - 仍待继续：
    - 配置动作自动生成仍高度依赖宏观状态沉淀质量
    - 冲突裁决与风险预算压缩仍需更稳定规则支撑

---

## 5. 中等相似度页面（P1 复查范围）

## 5.1 `/app/stocks/detail`

- 对应文件：
  - [StockDetailPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/stocks/StockDetailPage.vue)
- 典型空态：
  - `暂无概览数据`
  - `暂无日线数据`
  - `暂无分钟线数据`
  - `暂无评分数据`
  - `暂无财务或估值数据`
  - `暂无资金流或风险数据`
  - `暂无治理数据`
- 判断：
  - 对象页允许局部空，但当前空态分布过多，说明对象页能力还未稳定收口

## 5.2 `/app/stocks/prices`

- 对应文件：
  - [PricesPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/stocks/PricesPage.vue)
- 典型空态：
  - `暂无日线数据`
  - `暂无可展示的趋势数据，先执行一次查询`
  - `暂无分钟线数据`
  - `暂无分钟线明细`
- 判断：
  - 更偏工具页，空态部分合理，但默认体验仍偏弱

## 5.3 `/app/signals/themes`

- 对应文件：
  - [ThemesPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/signals/ThemesPage.vue)
- 典型空态：
  - `当前主题暂无股票池映射`
  - `当前主题暂无预期层数据`
  - `当前主题暂无证据链数据`
- 判断：
  - 主题页已有结果，但深层解释还没有稳定成熟

## 5.4 `/app/signals/graph`

- 对应文件：
  - [SignalChainGraphPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/signals/SignalChainGraphPage.vue)
- 典型空态：
  - `当前图谱暂无可筛选关系`
  - `当前图谱暂无可展示数据`
  - `当前没有可展示的图谱数据`
- 判断：
  - 图谱页并非结论中枢，但仍受数据适配命中率影响明显

## 5.5 `/app/signals/overview`

- 对应文件：
  - [SignalsOverviewPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/signals/SignalsOverviewPage.vue)
- 典型空态：
  - `当前筛选下没有信号结果`
  - `暂无信号结果`
- 判断：
  - 一部分是正常筛选空，一部分仍可能反映信号产出主链不稳

## 5.6 `/app/research/reports`

- 对应文件：
  - [ReportsPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/research/ReportsPage.vue)
- 典型空态：
  - `当前没有命中的报告`
  - `当前没有可导出的报告`
- 判断：
  - 当前更像内容库，若后续作为研报沉淀主链，仍需提升命中率与默认内容量

## 5.7 `/app/review`

- 对应文件：
  - [ReviewPage.vue](/home/zanbo/zanbotest/apps/web/src/pages/portfolio/ReviewPage.vue)
- 典型空态：
  - `暂无复盘记录，完成交易后添加复盘`
- 判断：
  - 更偏“闭环未打通”而非页面故障，但在完成态里仍属于有页无流量

---

## 6. 低相似度或更多属于正常空态的页面

以下页面也可能显示“暂无数据”，但优先级不应等同于前述 13 页：

- `macro/MacroPage.vue`
- `chatrooms/overview`
- `chatrooms/candidates`
- `signals/audit`
- `stocks/list`
- `UpgradePage.vue`

原因是这些页面更偏查询、列表、治理或条件型结果页，当前为空并不天然意味着主链能力缺失。

---

## 7. 根因分层

当前结构性空白页的根因，不是一种，而是至少有四类：

### 7.1 后端聚合未真正成型

- 页面需要结论
- 后端只提供原始片段或弱拼接
- 没有稳定的裁决、摘要或动作生成逻辑

典型页：

- `/app/market`
- `/app/workbench`
- `/app/decision`

### 7.2 使用旧表、错表或弱来源

- 查询逻辑仍依赖旧表名或弱语义表
- 与当前系统主数据表脱节

已验证典型页：

- `/app/market`

### 7.3 前后端协议未对齐

- 后端返回字段有值
- 但前端解析协议与后端实际结构不一致
- 页面最终仍显示为空

已验证典型页：

- `/app/market` 的 `sources`

### 7.4 页面依赖上游手工输入或稀疏链路

- 上游没有自动计算或自动沉淀
- 页面只能等待人工录入或稀疏数据填充

典型页：

- `/app/macro-regime`
- `/app/allocation`
- `/app/review`

---

## 8. 优先修复顺序

### P0：先查这 6 页

1. `/app/market`
2. `/app/workbench`
3. `/app/research/scoreboard`
4. `/app/decision`
5. `/app/macro-regime`
6. `/app/allocation`

这 6 页一旦空，不是“小问题”，而是主链判断页或动作页没有成立。

### P1：随后查这 7 页

1. `/app/stocks/detail`
2. `/app/stocks/prices`
3. `/app/signals/themes`
4. `/app/signals/graph`
5. `/app/signals/overview`
6. `/app/research/reports`
7. `/app/review`

---

## 9. 建议排查方法

逐页统一按三步排：

1. 查前端空态来源  
- 页面到底在等哪个字段
- 是空数组、空对象、空字符串，还是协议不匹配

2. 查接口返回是否真实为空  
- 确认接口是没数据、没聚合、还是字段命名不一致

3. 查后端根因属于哪类  
- 查错表
- 没聚合
- 返回协议不对
- 只有占位逻辑，没有真实结论生产

---

## 10. 与其他文档关系

- 主缺口入口：
  - [current_project_gap_audit_2026-04-18.md](/home/zanbo/zanbotest/docs/current_project_gap_audit_2026-04-18.md)
- 数据价值缺口：
  - [underutilized_data_audit_2026-04-20.md](/home/zanbo/zanbotest/docs/underutilized_data_audit_2026-04-20.md)
- 完成态目标：
  - [project_final_target_shape_dual_horizon_2026-04-19.md](/home/zanbo/zanbotest/docs/project_final_target_shape_dual_horizon_2026-04-19.md)

这三类文档的分工是：

- 本文回答：哪些页面“看起来有能力、实际是空心的”
- 数据审计回答：哪些数据已经存在但还没进入主链
- 完成态文档回答：最终这些页面应承载什么职责

---

## 11. 下一轮修复建议

1. 为 P0 页面补结构性空白页专项断言，禁止“整块占位文案”回归
2. 继续统一 `status/status_reason/missing_inputs/generated_from` 协议
3. 收 P1 七个页面的解释型空态
4. 把“有状态”进一步推进成“有答案、有动作、有回查”
