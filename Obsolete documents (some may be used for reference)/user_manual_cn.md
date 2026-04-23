# 项目使用说明书（当前版本）

更新时间：2026-04-23

本手册面向两类用户：
- 业务使用者：关注页面怎么用、从哪里进入、动作后会发生什么。
- 运维/开发同学：关注如何启动、如何检查服务是否正常、常见问题怎么排查。

## 1. 系统简介

这是一个围绕 A 股投研的本地化系统，核心链路是：
- 数据采集（行情/新闻/群聊/宏观）
- 智能分析（评分、信号、LLM 研究）
- 决策收口（短名单、动作记录、漏斗流转、执行任务）
- 复盘校验（命中率、历史快照、动作追踪）

统一访问入口（当前常用）：
- `http://192.168.5.52:8077/`

## 2. 启动前准备

在服务器上确认以下依赖已就绪：
- Python 3.10+
- Node.js 20+
- PostgreSQL（主库）
- Redis（缓存/流式链路）

关键环境变量由 `runtime_env.sh` 提供，至少需要：
- `DATABASE_URL`
- `REDIS_URL`
- `TUSHARE_TOKEN`
- `BACKEND_ADMIN_TOKEN`

## 3. 启动与停止

### 3.1 推荐启动方式（统一入口）

```bash
cd /home/zanbo/zanbotest
bash start_all.sh
```

### 3.2 分服务启动（常用于排障）

```bash
cd /home/zanbo/zanbotest
bash start_backend.sh
bash start_backend_llm2.sh
bash start_backend_macro.sh
bash start_backend_multi_role.sh
bash start_ws_realtime.sh
bash start_nginx_8077.sh
```

### 3.3 服务状态检查

```bash
ss -ltnp
```

重点端口：
- `8002` 主后端
- `8004` LLM 路由后端
- `8005` 宏观后端
- `8006` 多角色后端
- `8010` WebSocket
- `8077` Nginx 统一入口

### 3.4 停止服务（按端口）

```bash
fuser -k 8002/tcp 8004/tcp 8005/tcp 8006/tcp 8010/tcp 8077/tcp || true
```

## 4. 登录与权限

系统支持多角色访问（例如 admin/pro/limited）。

如本地调试时遇到受保护接口 401/403，可检查：
- 是否已登录
- 浏览器是否携带 `zanbo_admin_token`
- 后端 `BACKEND_ADMIN_TOKEN` 是否正确

前端会按以下优先级读取 token：
- `localStorage['zanbo_admin_token']`
- `sessionStorage['zanbo_admin_token']`
- `VITE_ADMIN_API_TOKEN`（开发时）

## 5. 页面使用导航（建议顺序）

### 5.0 四层信息架构（先分清你在做什么）

| 层级 | 你要解决的问题 | 主入口页面 | 备注 |
| --- | --- | --- | --- |
| 第一层 用户决策层 | 今天该怎么做（短线/长线动作） | `/app/workbench`、`/app/decision`、`/app/orders`、`/app/review` | 面向 PRO/limited 的执行闭环 |
| 第二层 数据资产层 | 数据是否完整、证据是否可靠 | `/app/research/scoreboard`、`/app/stocks/scores`、`/app/signals/overview` | 原始/加工数据的消费视图 |
| 第三层 验证与研究层 | 方向是否有效、策略是否可复用 | `/app/research/quant-factors`、`/app/decision`（calibration） | 验证、回测、复盘修正 |
| 第四层 后台治理层 | 系统是否稳定、权限与任务是否可控 | `/admin/dashboard`、`/admin/system/*` | 用户、权限、任务、监控、审计、LLM 节点治理 |

### 5.0.1 页面归属矩阵（首批）

| 页面 | 主职责 | 次职责（仅下钻） |
| --- | --- | --- |
| `/app/workbench` | 第一层 用户决策层 | 下钻到第二层证据与第三层验证 |
| `/app/market` | 第一层 用户决策层 | 下钻到评分总览 |
| `/app/research/scoreboard` | 第二层 数据资产层 | 下钻到 `/app/decision` 执行动作 |
| `/app/decision` | 第一层 用户决策层 | 内含第三层验证入口（calibration） |
| `/app/funnel` | 第一层 用户决策层 | 展示动作流转与状态校验 |
| `/app/research/quant-factors` | 第三层 验证与研究层 | 因子研究、回测验证与沉淀 |
| `/app/orders` | 第一层 用户决策层 | 执行状态与回执 |
| `/app/review` | 第一层 用户决策层 | 复盘与修正回路 |
| `/admin/system/jobs-ops` | 第四层 后台治理层 | 任务与调度治理 |
| `/admin/system/permissions` | 第四层 后台治理层 | 权限与配额治理 |

### 5.1 日常研究主链（用户视角）

1) `/app/workbench`：研究工作台入口  
2) `/app/research/scoreboard`：看宏观模式、行业排序、自动短名单  
3) `/app/decision`：做确认/暂缓/拒绝/观察，形成动作留痕  
4) `/app/funnel`：查看候选状态是否已进入漏斗流程  
5) `/app/orders`：确认执行任务是否创建并跟踪状态  
6) `/app/review`：复盘历史动作与结果

说明：系统已兼容旧路径（如 `/app/research/decision`），但建议优先使用上面的 canonical 路径。

### 5.1.1 五步闭环操作卡（canonical）

1. 看证据：`/app/research/scoreboard`  
2. 下动作：`/app/decision`  
3. 看流转：`/app/funnel`  
4. 看执行：`/app/orders`  
5. 做复盘：`/app/review`

### 5.2 关键信息页面

- `/app/market/conclusion`：市场结论总览（含可用性分级）
- `/app/stocks/list`：股票基础列表
- `/app/stocks/detail/<ts_code>`：单票详情
- `/app/intelligence/global-news`：国际新闻
- `/app/intelligence/cn-news`：国内新闻
- `/app/signals/overview`：信号总览
- `/app/signals/themes`：主题热点
- `/app/signals/graph`：产业链图谱

## 6. 决策动作说明（重点）

在 `/app/decision` 页面可执行：
- `confirm`
- `defer`
- `reject`
- `watch`

建议使用 canonical 页面：`/app/decision`（旧路径仍可跳转）。

当前行为已经打通：
- 写入 `decision_actions`
- 同步 `funnel_candidates / funnel_transitions`
- 按需创建 `portfolio_orders`

动作后页面会给出回执，通常包含：
- 动作记录成功（含 `action_id`）
- 漏斗同步状态（如“已确认入池/已暂缓”）
- 执行任务创建结果（如有）

## 7. 常见操作手册

### 7.0 评分相关 API 边界（避免混用）

- `/api/stock-scores`
  - 定位：纯评分数据面（原始评分筛选、排序、分页）。
  - 适用场景：做数据统计、分布观察、基础筛选。
- `/api/decision/scores`
  - 定位：决策总览面（短名单 + reason packets + source health）。
  - 适用场景：研究入口的证据聚合与候选解释。

页面对应建议：
- `评分总览`优先使用 `/api/decision/scores`。
- `综合评分`列表优先使用 `/api/stock-scores`。

### 7.1 生成一次决策快照

页面入口：`/app/decision` -> “生成快照”。

也可命令行触发：

```bash
python3 /home/zanbo/zanbotest/jobs/run_decision_job.py --job-key decision_daily_snapshot
```

### 7.2 运行前端构建检查

```bash
cd /home/zanbo/zanbotest/apps/web
npm run build
```

### 7.3 运行最小回归

```bash
cd /home/zanbo/zanbotest
bash run_minimal_regression.sh
```

### 7.4 运行浏览器 smoke

```bash
cd /home/zanbo/zanbotest/apps/web
npm run smoke:e2e:core
```

## 8. 常见问题与排障

### 8.1 页面显示“数据为空/置信度很低”

优先检查：
- 决策链路状态是否为 `ready`
- 当日评分数据是否已更新（`stock_scores_daily`）
- 决策快照是否生成成功
- 漏斗是否已有候选

### 8.2 操作成功了，但页面没变化

优先检查：
- 后端是否已重启到最新代码
- 浏览器是否命中旧缓存（强刷）
- 接口是否 401（token 失效）

### 8.3 端口占用导致启动失败

```bash
fuser -k 8002/tcp 8004/tcp 8005/tcp 8006/tcp 8010/tcp 8077/tcp || true
```

然后重新启动服务。

## 9. 建议的日常使用节奏

- 早盘前：看 `scoreboard` + `market conclusion`
- 盘中：在 `decision` 做动作记录，关注动作回执
- 收盘后：看 `funnel` + `orders` + `review`，确认“判断 -> 执行 -> 复盘”闭环

## 10. 指标真源矩阵（统计口径统一）

| 指标类型 | 主接口 / 主来源 | 说明 |
| --- | --- | --- |
| 评分统计（股票、行业、短名单） | `/api/decision/scores`、`/api/stock-scores` | 前者是聚合总览，后者是原始评分数据面 |
| 方向验证（命中率、收益窗口） | `/api/decision/calibration` | 专门用于“判断对不对”的事后验证 |
| 漏斗统计（状态分布、阶段数量） | `/api/funnel/metrics` | 只负责漏斗链路统计 |
| 系统运营汇总（全局趋势） | `/api/metrics/summary` | 用于系统级观察，不替代业务动作页面 |

## 11. 数据资产层真源目录（第二层）

| 数据资产类型 | 主数据表 | 主消费 API | 主要消费页面 |
| --- | --- | --- | --- |
| 股票原始行情/评分 | `stock_daily_prices`、`stock_scores_daily` | `/api/stock-scores` | `/app/stocks/scores`、`/app/research/scoreboard` |
| 决策聚合数据 | `decision_snapshots`、`decision_actions` | `/api/decision/scores`、`/api/decision/board` | `/app/research/scoreboard`、`/app/decision` |
| 漏斗状态数据 | `funnel_candidates`、`funnel_transitions` | `/api/funnel/metrics`、`/api/funnel/candidates` | `/app/funnel` |
| 新闻与总结数据 | `news_feed_items`、`stock_news_items`、`news_daily_summaries` | `/api/intelligence/*` | `/app/intelligence/*` |
| 信号与主题数据 | `investment_signal_tracker*`、`theme_*` | `/api/signals/*` | `/app/signals/*` |
| 研究验证数据 | `quantaalpha_runs`、`quantaalpha_factor_results`、`quantaalpha_backtest_results` | `/api/quant-factors/*` | `/app/research/quant-factors` |

## 12. 第三层验证与研究使用方式

目标：验证“这个方向是否真的有效”，并把结果反哺第一层用户决策。

推荐路径：
1. 在 `/app/research/quant-factors` 发起研究方向（因子挖掘/回测）  
2. 查看任务结果与相对基准表现  
3. 在 `/app/review` 记录复盘修正  
4. 回到 `/app/decision` 更新动作策略

关键接口：
- `/api/quant-factors/*`：因子与回测验证
- `/api/decision/calibration`：动作命中率与收益窗口验证

## 13. 第四层后台治理映射

| 治理对象 | 页面入口 | 关键观测指标 |
| --- | --- | --- |
| 任务调度与告警 | `/admin/system/jobs-ops` | 任务成功率、失败率、平均耗时、告警数量 |
| 权限与用户 | `/admin/system/permissions`、`/admin/system/users` | 角色覆盖、配额使用率、登录/审计异常 |
| 数据质量 | `/admin/system/database-audit` | 缺口率、重复率、陈旧率、未评分比率 |
| 信号质量 | `/admin/system/signals-audit`、`/admin/system/signals-quality` | 误映射率、弱信号占比、规则触发率 |
| 基础设施与模型 | `/admin/system/source-monitor`、`/admin/system/llm-providers` | 节点可用率、延迟、错误码分布 |

## 14. 术语统一（避免概念混淆）

- 用户决策层：告诉你“今天该怎么做”。
- 数据资产层：告诉你“证据从哪里来、口径是什么”。
- 验证与研究层：告诉你“方法是否有效、能否复用”。
- 后台治理层：保证“系统长期稳定、可审计、可管控”。
- 投研决策（`/api/decision/*`）：业务决策主链路。
- 任务控制 decision（多角色任务内部）：异步任务控制语义，不等同于投研决策。

## 15. 相关文档

- 系统全景：`docs/system_overview_cn.md`
- 命令参考：`docs/command_line_reference.md`
- 数据字典：`docs/database_dictionary.md`
- 调度矩阵：`docs/scheduler_matrix_2026-04-06.md`

