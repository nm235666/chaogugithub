# 系统全景总览

- 更新时间：`2026-04-10 UTC`
- 数据库：`PostgreSQL 主库`
- 文档定位：这是一份“项目全景图”，重点回答“我们现在有什么、这些数据怎么流转、系统每天自动在做什么”。
- 当前前端结构说明：当前页面仍以单一应用承载，但后续信息架构与验收将按“用户模式 / 后台管理模式 / 公共共享页层”组织；详细规则见 [前端双模式重构执行方案](/home/zanbo/zanbotest/docs/frontend_dual_mode_refactor_plan_2026-04-20.md)。

## 0. 重规划口径（四层架构重基线）

当前按四层能力划分产品边界，避免职责继续混叠：

| 层级 | 目标问题 | 代表页面（canonical） |
| --- | --- | --- |
| 第一层 用户决策层 | 今天该如何做投资动作 | `/app/workbench`、`/app/decision`、`/app/funnel`、`/app/orders`、`/app/review` |
| 第二层 数据资产层 | 数据口径与证据是否可靠 | `/app/research/scoreboard`、`/app/stocks/scores`、`/app/signals/overview` |
| 第三层 验证与研究层 | 策略与因子是否有效可复用 | `/app/research/quant-factors`、`/api/decision/calibration` |
| 第四层 后台治理层 | 系统是否稳定可管控 | `/admin/dashboard`、`/admin/system/*` |

说明：历史路径（如 `/research/decision`）保留重定向兼容，但文档与验收统一使用 canonical 路径。

## 1. 目前系统已经具备的能力

### 1.1 股票基础与行情

- 股票基础资料查询
  - 股票代码、简称、市场、地区、上市状态
- 股票日线行情查询
  - 支持历史区间查询
- 股票分钟线查询
  - 前端以 K 线图形式展示
- 股票综合评分
  - 已按数据库日快照方式落表，不是前端现算

### 1.2 新闻与事件

- 国际财经资讯
  - 国际 RSS 源抓取、去重、入库、展示
- 国内财经资讯
  - 新浪 7x24、东方财富快讯抓取、去重、入库、展示
- 个股新闻
  - 按股票查询相关新闻
  - 支持前端“立即采集”
- 新闻自动评分
  - 系统评分
  - 财经影响评分
  - 财经重要程度
  - 影响方向与板块识别
- 新闻日报总结
  - 支持自动生成
  - 支持主动触发生成
  - 支持 Markdown / 图片下载

### 1.3 LLM 分析

- LLM 股票走势分析
- LLM 多角色公司分析
  - 宏观经济分析师
  - 股票分析师
  - 国际资本分析师
  - 汇率分析师
  - 以及其他补充角色
- 群聊投资倾向分析
  - 自动提炼群内讨论的投资标的
  - 自动判断看多/看空
- 群聊标签分类
- 新闻评分与新闻总结
- 新闻映射到 A 股股票
  - 规则识别 + LLM 双重校对
- 群聊别名归一
  - 词典优先
  - LLM 补别名
  - 结果沉淀到别名字典

### 1.4 群聊与社群分析

- 群聊列表抓取
- 群聊聊天记录抓取与清洗
  - 过滤图片、动画表情、链接、视频等无效内容
  - 保留引用结构
- 群聊聊天记录查询页
- 群聊总览页
- 群聊投资倾向总览页
- 股票候选池
  - 目前已切换为“近 7 天累计”口径

### 1.5 信号系统

- 投资信号总览
  - 融合国际新闻、国内新闻、个股新闻、群聊候选池
- 投资信号时间线
  - 看信号如何增强、减弱、翻转
- 主题映射到股票池
  - 例如黄金、原油、能源、AI、航运、军工
- 股票别名字典
  - 用于群聊和新闻中简称归一
- 投资信号双口径
  - `近7天累计`
  - `最近1天`

### 1.6 宏观与研究扩展

- 宏观指标网页
- 汇率、利率曲线、利差
- 公司治理画像
- 公司事件
- 风险情景
- 市场/个股资金流
- 投研决策板
  - 宏观-行业-个股统一评分
  - 候选短名单
  - 执行提示与风险验证
  - 验证结果与人工 Kill Switch
  - 人工确认动作留痕
- 评分总览
  - 宏观评分卡
  - 行业评分榜
  - 自动短名单
  - 入选理由包（评分 / 新闻 / 信号 / 候选池）
- 产业链图谱
  - 主题 / 行业 / 股票关系浏览
  - 支持中心切换、节点下钻和关系筛选

## 2. 当前数据库里有哪些主要数据

下面按业务模块整理，而不是只按表名堆砌。

### 2.1 股票与公司数据

| 表名 | 中文作用 |
| --- | --- |
| `stock_codes` | 股票基础信息主表，是所有股票相关数据的主索引。 |
| `stock_daily_prices` | 股票日线行情。 |
| `stock_minline` | 股票分钟线。 |
| `stock_valuation_daily` | 股票估值日频快照。 |
| `stock_financials` | 股票财务核心指标。 |
| `stock_events` | 分红、回购、解禁、业绩预告等事件。 |
| `company_governance` | 公司治理画像。 |
| `stock_scores_daily` | 股票综合评分日快照。 |
| `stock_news_items` | 个股相关新闻与其 LLM 评分结果。 |

### 2.2 宏观、资金流与风险

| 表名 | 中文作用 |
| --- | --- |
| `macro_series` | 宏观指标时间序列。 |
| `fx_daily` | 汇率与指数日线。 |
| `rate_curve_points` | 利率曲线关键点。 |
| `spread_daily` | 利差衍生指标。 |
| `capital_flow_market` | 市场级资金流。 |
| `capital_flow_stock` | 个股级资金流。 |
| `risk_scenarios` | 风险情景分析结果。 |

### 2.3 新闻与新闻分析

| 表名 | 中文作用 |
| --- | --- |
| `news_feed_items` | 国际/国内财经资讯主表。 |
| `news_feed_items_archive` | 新闻归档表。 |
| `news_daily_summaries` | 新闻日报总结表。 |

### 2.4 群聊与社群数据

| 表名 | 中文作用 |
| --- | --- |
| `chatroom_list_items` | 群聊基础资料。 |
| `wechat_chatlog_clean_items` | 清洗后的群聊消息。 |
| `chatroom_investment_analysis` | 群聊投资倾向分析结果。 |
| `chatroom_stock_candidate_pool` | 群聊股票/主题候选池。 |
| `chatroom_stock_signal_predictions` | 群聊荐股方向信号与次日收盘校验结果。 |
| `chatroom_signal_accuracy_labels` | 群聊/荐股人滚动命中率标签（30 日）。 |

### 2.5 信号系统与映射

| 表名 | 中文作用 |
| --- | --- |
| `investment_signal_tracker` | 主信号表，偏综合全量视角。 |
| `investment_signal_tracker_7d` | 近 7 天累计信号表。 |
| `investment_signal_tracker_1d` | 最近 1 天信号表。 |
| `investment_signal_daily_snapshots` | 信号快照历史。 |
| `investment_signal_events` | 信号演化事件时间线。 |
| `theme_stock_mapping` | 主题 -> 股票池映射。 |
| `stock_alias_dictionary` | 股票别名字典。 |

## 3. 前端页面清单

> 说明：前端路由以 `apps/web/src/app/router.ts` 为准；下面列的是当前主入口页面。`/stocks/minline` 目前已重定向到 `/stocks/prices`，不再作为独立主入口维护。
> canonical 路径统一使用 `/app/*` 与 `/admin/*` 前缀（例如 `/app/decision`、`/app/funnel`、`/app/orders`、`/app/review`），历史无前缀路径仅保留重定向兼容。
> 当前页面清单仍按现有真实路径列出，并按“四层架构重基线”持续收敛：第一层用户决策、第二层数据资产、第三层验证与研究、第四层后台治理。研究、判断、决策、执行、复盘相关页面优先归第一层；原始与加工数据展示优先归第二层；因子/回测/策略验证优先归第三层；权限、任务、配置、监控、审计归第四层。

| 页面 | 模式归属 | 主要用途 |
| --- | --- | --- |
| `/login` | 公共共享页层 | 登录、注册、重置密码入口。 |
| `/upgrade` | 公共共享页层 | 权限不足时的升级/引导页。 |
| `/dashboard` | 后台管理模式 | Admin 总控台，聚合系统健康、业务波动摘要、关键健康摘要与治理页面快捷入口。 |
| `/stocks/list` | 用户模式 | 股票列表查询。 |
| `/stocks/scores` | 用户模式 | 股票综合评分列表。 |
| `/stocks/detail/:tsCode?` | 用户模式 | 单只股票详情页。 |
| `/stocks/prices` | 用户模式 | 日线价格查询。 |
| `/intelligence/stock-news` | 用户模式 | 个股新闻查询。 |
| `/intelligence/global-news` | 用户模式 | 国际财经资讯。 |
| `/intelligence/cn-news` | 用户模式 | 国内财经资讯。 |
| `/intelligence/daily-summaries` | 用户模式 | 新闻日报总结。 |
| `/research/workbench` | 用户模式 | 研究用户默认工作台与主入口。 |
| `/research/reports` | 用户模式 | 标准投研报告查询。 |
| `/research/scoreboard` | 用户模式 | 评分总览，聚合宏观模式、行业评分、自动短名单和入选理由。 |
| `/research/funnel` | 用户模式 | 候选漏斗与候选流转管理。 |
| `/research/decision` | 用户模式 | 投研决策板，聚合评分、短名单、执行提示、验证和开关。 |
| `/research/quant-factors` | 用户模式 | 自研双引擎 AI 因子挖掘与回测工作台（business/research，A 股日频）。 |
| `/research/trend` | 用户模式 | LLM 股票走势分析。 |
| `/research/multi-role` | 用户模式 | LLM 多角色公司分析。 |
| `/market/conclusion` | 用户模式 | 市场结论单口径输出页。 |
| `/portfolio/positions` | 用户模式 | 持仓结构与仓位查看。 |
| `/portfolio/orders` | 用户模式 | 执行单与执行状态跟踪。 |
| `/portfolio/review` | 用户模式 | 复盘与策略修正。 |
| `/portfolio/allocation` | 用户模式 | 长线配置动作与组合层应对。 |
| `/macro` | 用户模式 | 宏观数据展示。 |
| `/chatrooms/overview` | 用户模式 | 群聊总览。 |
| `/chatrooms/chatlog` | 用户模式 | 群聊聊天记录查询。 |
| `/chatrooms/investment` | 用户模式 | 群聊投资倾向总览（含荐股准确率标注）。 |
| `/chatrooms/investment/room` | 用户模式 | 单群聊荐股方向与校验详情。 |
| `/chatrooms/investment/sender` | 用户模式 | 单荐股人方向与校验详情。 |
| `/chatrooms/candidates` | 用户模式 | 股票候选池（近 7 天累计）。 |
| `/signals/overview` | 用户模式 | 投资信号总览。 |
| `/signals/themes` | 用户模式 | 主题热点与主题维度信号。 |
| `/signals/graph` | 用户模式 | 产业链图谱，浏览主题 / 行业 / 股票关系。 |
| `/signals/timeline` | 用户模式 | 信号时间线。 |
| `/signals/audit` | 后台管理模式 | 信号审计视图。 |
| `/signals/quality-config` | 后台管理模式 | 信号质量规则与屏蔽配置。 |
| `/signals/state-timeline` | 后台管理模式 | 信号状态机时间线。 |
| `/system/source-monitor` | 后台管理模式 | 数据源与运行进程监控。 |
| `/system/jobs-ops` | 后台管理模式 | 任务定义、运行记录与告警。 |
| `/system/llm-providers` | 后台管理模式 | LLM provider 管理。 |
| `/system/permissions` | 后台管理模式 | 角色权限策略与日配额配置。 |
| `/system/database-audit` | 后台管理模式 | 数据库质量监控与审计。 |
| `/system/invites` | 后台管理模式 | 邀请码管理。 |
| `/system/users` | 后台管理模式 | 用户、会话、配额与审计日志管理。 |

## 4. 现在系统的数据流转方式

### 4.1 股票线

1. 抓取股票基础信息
2. 抓取日线 / 分钟线 / 财务 / 估值 / 事件 / 治理 / 资金流
3. 写入 PostgreSQL 主库
4. 前端页面查询展示
5. LLM 分析页面调用这些数据形成解释与结论

### 4.2 新闻线

1. 抓取国际新闻 / 国内新闻 / 个股新闻
2. 做去重与清洗
3. 入库
4. 用 LLM 做评分、影响识别、摘要
5. 把新闻直接提到的 A 股公司映射到 `ts_code`
6. 新闻进入投资信号系统

### 4.3 群聊线

1. 抓取群列表
2. 抓取群聊天记录
3. 清洗无效消息
4. 用 LLM 生成群聊投资倾向分析
5. 提取群内讨论的投资标的
6. 做别名归一
7. 汇总成股票候选池
8. 再进入投资信号系统

### 4.4 信号线

1. 汇总新闻、个股新闻、群聊候选池
2. 叠加主题 -> 股票池映射
3. 生成股票 / 主题 / 宏观 / 商品 / 外汇信号
4. 写入：
   - 主表
   - `7d`
   - `1d`
5. 记录快照和信号事件
6. 前端展示“总览 + 时间线”

### 4.5 决策线

1. 复用 `stock_scores_daily`、股票详情和验证快照等现有数据
2. 聚合宏观模式、行业排序和个股短名单
3. 生成执行提示与风险说明
4. 读取 / 写入决策快照与 Kill Switch 状态
5. 记录人工确认、拒绝、暂缓等动作留痕
6. 在前端 `/app/decision` 和 `/app/stocks/detail/:tsCode?` 中展示可解释决策结果
7. 作为后续策略验证、淘汰和人工确认的统一入口

### 4.6 API 边界（评分与决策）

| API | 定位 | 主要用途 |
| --- | --- | --- |
| `/api/stock-scores` | 原始评分数据面 | 评分筛选、排序、分页、统计分析 |
| `/api/decision/scores` | 决策总览聚合面 | 短名单、reason packets、source health |
| `/api/decision/calibration` | 方向验证面 | confirm/reject 事后收益与命中率校验 |
| `/api/funnel/metrics` | 漏斗统计面 | 候选状态分布与阶段统计 |
| `/api/metrics/summary` | 系统级统计面 | 全局趋势、运营监控、汇总展示 |

### 4.7 指标真源矩阵（统计口径统一）

| 指标 | 真源 |
| --- | --- |
| 股票/行业评分统计 | `stock_scores_daily` + `/api/stock-scores` / `/api/decision/scores` |
| 决策短名单解释 | `/api/decision/scores.reason_packets` |
| 方向验证命中率 | `/api/decision/calibration` |
| 漏斗阶段数量 | `/api/funnel/metrics` |
| 系统汇总趋势 | `/api/metrics/summary` |

### 4.8 后台治理层映射（第四层）

| 治理能力 | 页面入口 | 主要治理对象 | 对齐指标 |
| --- | --- | --- |
| 用户与权限治理 | `/admin/system/users`、`/admin/system/permissions` | `app_auth_users`、角色策略、配额 | 登录失败率、配额耗尽率、异常审计数 |
| 任务与调度治理 | `/admin/system/jobs-ops` | `job_definitions`、`job_runs`、`job_alerts` | 任务成功率、平均耗时、失败告警数 |
| 数据质量治理 | `/admin/system/database-audit` | 数据缺口、重复、时效性、异常 | 缺口率、重复率、陈旧率 |
| 信号治理 | `/admin/system/signals-audit`、`/admin/system/signals-quality`、`/admin/system/signals-state-timeline` | 信号规则、状态机、映射质量 | 误映射率、弱信号占比、状态跳变异常数 |
| 基础设施治理 | `/admin/system/source-monitor`、`/admin/system/llm-providers` | 数据源进程、LLM provider/node 健康 | 节点可用率、请求延迟、错误码分布 |

### 4.9 页面 smoke 线

1. 保留 `tests/test_frontend_api_smoke.py` 与 `run_minimal_regression.sh` 做静态/接口回归
2. 新增 `apps/web/tests/e2e/smoke.spec.ts` 做浏览器级主流程 smoke
3. 覆盖登录、默认落点、升级页权限展示、评分总览和关键研究页首屏渲染
4. 默认通过 `cd apps/web && npm run smoke:e2e` 运行，目标地址由 `PLAYWRIGHT_BASE_URL` 指定
5. 分层 smoke：`smoke:e2e:core`（主链路）/ `smoke:e2e:write-boundary`（写操作与边界）/ `smoke:e2e:all`（全量）

### 4.10 LLM 分析线（走势 + 多角色）

1. 前端在 `/research/trend` 或 `/research/multi-role` 发起分析
2. 后端创建异步任务并轮询状态
3. 多角色沿用 v3 API 路径，执行内核已切换为 v4（LangGraph 图编排），仍是六阶段（Analyst -> Bull/Bear -> Research Manager -> Trader -> 风控三方 -> Portfolio Manager）
4. 独立 worker 进程消费任务队列，API 仅负责创建/查询/控制任务
5. 前端回显阶段时间线、辩论面板、审批面板与完整 Markdown

多角色 v3 当前接口：

- `POST /api/llm/multi-role/v3/jobs`
- `GET /api/llm/multi-role/v3/jobs/<job_id>`
- `GET /api/llm/multi-role/v3/jobs/<job_id>/stream`
- `POST /api/llm/multi-role/v3/jobs/<job_id>/decisions`
- `POST /api/llm/multi-role/v3/jobs/<job_id>/actions`

多角色 v3 当前关键状态：

- `queued`
- `running`
- `pending_user_decision`
- `approved`
- `rejected`
- `deferred`
- `done_with_warnings`
- `error`

多角色任务返回中会带 `engine_version`；当前默认应为 `v4`。如遇紧急回滚，可通过环境变量 `MULTI_ROLE_V4_ROLLBACK_TO_V3=1` 临时切回旧执行管线。
多角色 v3 模型路由支持“按角色 + 按阶段”双层策略，配置入口为 `config/llm_providers.json` 的 `multi_role_v3_policies`（可参考 quick/deep 思路分别配置证据节点与裁决节点）。

### 4.11 第三层验证与研究主路径

验证与研究层主路径统一为：

`策略生成/因子挖掘 -> 回测验证 -> 复盘修正 -> 反馈到用户决策层`

对应入口：
- 页面：`/app/research/quant-factors`
- 决策验证 API：`/api/decision/calibration`
- 量化验证 API：`/api/quant-factors/*`

说明：验证与研究层用于“证明方法有效”，不直接承担用户动作执行；动作执行仍在第一层用户决策层完成（`/app/decision`、`/app/orders`）。

## 5. 当前自动运行的定时任务

> 说明：当前任务系统以 `job_registry.py` 为准，不应再仅以历史 crontab 片段理解系统调度。下面列的是当前主链路中的代表性任务。

### 5.1 新闻

| 调度 | 任务 |
| --- | --- |
| `*/5 * * * *` | 国际新闻采集评分映射 |
| `*/2 * * * *` | 国内新闻采集评分映射 |
| `*/5 * * * *` | 新闻映射股票 |
| `*/5 * * * *` | 新闻情绪刷新 |
| `30 3,15 * * *` | 新闻日报总结 |
| `30 3,15 * * *` | 新闻归档 |
| `20 16 * * *` | 新闻语义去重 |

### 5.2 股票与研究数据

| 调度 | 任务 |
| --- | --- |
| `30 7 * * *` | 盘后更新流水线 |
| `45 7 * * *` | 分钟线最近交易日补抓 |
| `20 17 * * *` | AKShare 宏观数据刷新 |
| `10 17 * * *` | 宏观上下文刷新 |
| `0 17 * * *` | 夜间补数批次 |
| `30 7,15 * * *` | 标准投研报告刷新 |

### 5.3 群聊

| 调度 | 任务 |
| --- | --- |
| `5 0 * * *` | 群聊列表抓取 |
| `*/3 * * * *` | 监控群聊天记录拉取 |
| `10 0 * * *` | 跨天补抓昨天 + 今天 |
| `15 * * * *` | 群聊分析流水线 |
| `18 * * * *` | 群聊情绪刷新 |
| `12 0 */3 * *` | 群聊低风险标签刷新 |

### 5.4 个股新闻与信号

| 调度 | 任务 |
| --- | --- |
| `*/10 * * * *` | 个股新闻评分 |
| `55 * * * *` | 个股新闻缺口补抓 |
| `25 * * * *` | 个股新闻重点扩抓 |
| `35 * * * *` | 投资信号跟踪器刷新 |
| `32 * * * *` | 主题热点引擎刷新 |
| `38 * * * *` | 信号状态机刷新 |
| `15 1 * * 1-5` | 投研决策快照 |

### 5.5 其他旁路与治理

| 调度 | 任务 |
| --- | --- |
| `50 16 * * *` | 数据库审核报告刷新 |
| `40 16 * * *` | 数据库健康巡检 |
| `*/20 * * * *` | LLM 节点全模型巡检 |
| `*/5 * * * *` | 多角色僵尸任务回收 |
| `* * * * *` | 多角色 Worker 守护 |
| `*/30 * * * *` | 量化链路健康检查（按需启用） |
| `20 1 * * *` | AI 因子挖掘（按需启用） |
| `45 1 * * *` | AI 因子回测（按需启用） |

### 5.6 实时服务

| 调度 | 任务 |
| --- | --- |
| `@reboot` | WebSocket 服务启动 |
| `@reboot` | Stream Worker 启动 |
| `* * * * *` | 实时服务守护 |

## 6. 目前最有价值的几个入口

### 6.1 如果看“今天/最近一周哪些股票值得盯”

- `/chatrooms/candidates`
- `/signals/overview`

### 6.2 如果看“某只股票发生了什么”

- `/stocks/detail/:tsCode?`
- `/intelligence/stock-news`
- `/research/trend`
- `/research/multi-role`

### 6.3 如果看“市场发生了什么”

- `/intelligence/global-news`
- `/intelligence/cn-news`
- `/intelligence/daily-summaries`
- `/macro`

### 6.4 如果看“群里最近在讨论什么”

- `/chatrooms/overview`
- `/chatrooms/chatlog`
- `/chatrooms/investment`

## 7. 当前系统的几个关键特征

- 不是单纯的数据展示站，而是“数据 -> 分析 -> 映射 -> 信号”的链路
- 股票、新闻、群聊、宏观数据已经互相打通
- 已有定时刷新与自动补齐，不依赖手工逐条运行
- LLM 不是孤立调用，而是嵌入到了新闻评分、公司分析、群聊分析、别名归一、日报总结等多个环节
- 前后端已经分离，且可通过局域网访问

## 8. 一句话总结

我们现在已经有了一套围绕 A 股研究、新闻理解、群聊观点提炼与投资信号追踪的本地化投研系统，既能看数据，也能看解释，还能看信号如何演化。
