# Zanbo Quant 投研系统 - Agent 项目指南

> 本文件是面向 AI Coding Agent 的项目全景说明，目的是让陌生代理在零背景知识下快速定位代码、理解架构并安全地开展工作。
> 
> 注意：如果你要找的是「编码执行规则、任务优先级、不可破坏项」，请优先阅读同目录下的 `AGENTS.md`。

---

## 1. 项目概述

**Zanbo Quant** 是一套围绕 A 股市场的本地化投研系统，核心目标是把「数据采集 → 信号生成 → LLM 分析 → 前端展示」整条链路跑通并稳定运行。

系统当前已具备的能力包括：
- **股票基础与行情**：股票资料、日线/分钟线行情、综合评分。
- **新闻与情报**：国际国内财经新闻抓取、个股新闻、新闻自动评分与情绪标签、新闻到股票的映射、日报总结生成。
- **LLM 分析**：多角色公司分析（宏观/股票/国际资本/汇率等角色）、趋势分析、群聊投资倾向分析、新闻评分与总结。
- **信号系统**：投资信号总览、主题热点、信号时间线与审计、产业链图谱（`/signals/graph`）。
- **投研决策板**：宏观-行业-个股统一评分、候选短名单、策略实验台、Kill Switch 与人工确认留痕。
- **群聊与社群**：群聊抓取、聊天记录清洗、投资倾向总览、股票候选池。
- **量化因子挖掘**：内置自研 AI 因子挖掘执行器（`/api/quant-factors/*`）。
- **任务调度与可观测性**：统一任务编排器、定时任务 cron 管理、任务运行记录与告警。

**主力前端**：`apps/web/`（Vue 3 + TypeScript + Vite）。
**旧版前端**：`frontend/` 已完全退场，不再维护。
**主数据库**：PostgreSQL（`stockapp`）。SQLite 已退役，仅作为历史迁移源保留。
**缓存与消息队列**：Redis（Stream / PubSub / 缓存）。

---

## 2. 技术栈

### 2.1 后端
- **语言**：Python 3（项目环境多为 3.10/3.11）
- **数据库驱动**：`psycopg2-binary==2.9.2`
- **缓存/队列**：`redis==3.5.3`
- **HTTP 服务**：基于 `http.server.BaseHTTPRequestHandler` 的自研单文件服务（`backend/server.py`），非 FastAPI/Flask。
- **网络请求**：`requests==2.25.1`、`httpx==0.28.1`
- **LLM/Agent**：`langgraph==0.3.34`、`langchain-core==0.3.83`、`pydantic==2.12.5`
- **模板渲染**：`Jinja2==3.0.3`、`markdown-it-py==4.0.0`
- **数据科学/金融**：视模块需要引入 `pandas`、`numpy`、`akshare`、`tushare` 等（未锁死在 `requirements.txt`，由运行时环境提供）

### 2.2 前端
- **框架**：Vue 3.5+（Composition API + `<script setup>`）
- **语言**：TypeScript 5.9+
- **构建工具**：Vite 8+
- **样式**：Tailwind CSS 4.2+（通过 `@tailwindcss/vite` 插件）
- **状态管理**：Pinia 3+
- **数据获取**：TanStack Vue Query (`@tanstack/vue-query`)
- **工具库**：VueUse (`@vueuse/core`)、Axios、ECharts 6、Lightweight Charts 5
- **校验**：Zod 4+
- **E2E 测试**：Playwright 1.56+

### 2.3 基础设施
- **数据库**：PostgreSQL（主库），本地开发可回退到 SQLite（通过 `USE_POSTGRES=0`）
- **缓存/实时**：Redis
- **WebSocket 实时广播**：`ws_realtime_server.py`（端口 8010）
- **Nginx**：可选统一入口（8077），运行时配置在 `nginx_runtime/`

---

## 3. 代码组织

### 3.1 根目录关键约定
```
/home/zanbo/zanbotest
├── backend/server.py           # 主后端入口（8344 行，单体但按路由拆分处理函数）
├── backend/routes/             # 按业务域拆分的路由模块（被 server.py 导入使用）
├── apps/web/                   # 新版前端（Vue 3 + Vite）
├── services/                   # 业务服务层（被 backend/server.py 和 jobs/ 共用）
├── jobs/                       # 定时任务/异步任务定义与运行入口
├── collectors/                 # 数据采集器（新闻、行情、群聊等）
├── tests/                      # Python 单元测试与接口 smoke 测试
├── config/                     # 配置文件（LLM providers、RBAC、token 等）
├── docs/                       # 项目文档（主链 / 归档 / 提案）
├── scripts/                    # 辅助脚本（调度检查、配置对齐检查等）
├── db_compat.py                # 数据库兼容层：PostgreSQL/SQLite 统一访问接口
├── job_registry.py             # 任务定义真源（job_definitions 数据结构）
├── job_orchestrator.py         # 任务编排器（运行、记录、告警）
├── runtime_env.sh              # 统一环境变量加载脚本
├── start_all.sh                # 一键启动脚本（后端 + 前端构建 + WS + Stream Worker）
└── requirements.txt            # 最小 Python 依赖清单
```

### 3.2 后端路由拆分（`backend/routes/`）
后端 API 虽然集中在 `backend/server.py` 一个大文件中注册，但业务路由处理逻辑已拆分到 `backend/routes/` 下的模块：
- `stocks.py` — 股票基础/行情/评分
- `signals.py` — 信号系统
- `decision.py` — 投研决策板
- `quant_factors.py` — 量化因子
- `news.py` — 新闻与日报
- `chatrooms.py` — 群聊
- `system.py` — 系统管理（用户、权限、任务、LLM providers、数据库审计）
- `ai_retrieval.py` — AI 检索协议
- `roundtable.py` — 首席圆桌会

### 3.3 服务层（`services/`）
业务逻辑的核心实现，供 `backend/server.py` 和各类脚本/Worker 调用：
- `agent_service/` — LLM 多角色分析、趋势分析、Chief Roundtable
- `decision_service/` — 决策板评分与策略
- `signals_service/` — 信号查询与图谱
- `quantaalpha_service/` — 量化因子服务
- `chatrooms_service/` — 群聊数据服务
- `stock_news_service/` — 个股新闻服务
- `stock_detail_service/` — 股票详情页综合数据组装
- `reporting/` — 日报/报告生成
- `execution/` — 交易前检查
- `notifications/` — 通知推送（企业微信等）
- `ai_retrieval_service.py` — AI 检索服务

### 3.4 前端结构（`apps/web/src/`）
```
src/
├── app/
│   ├── router.ts               # Vue Router 定义
│   ├── permissions.ts          # 权限常量与校验函数
│   ├── layouts/                # 布局组件
│   └── providers/              # 全局 Provider
├── pages/                      # 页面级组件（按业务域分子目录）
│   ├── stocks/
│   ├── signals/
│   ├── research/
│   ├── intelligence/
│   ├── chatrooms/
│   ├── system/
│   ├── macro/
│   ├── dashboard/
│   └── auth/
├── services/api/               # 按业务域封装的 API 请求函数
├── stores/                     # Pinia stores（auth、realtime、ui）
├── shared/
│   ├── ui/                     # 通用 UI 组件（AppShell 等）
│   ├── charts/                 # 图表封装
│   ├── markdown/               # Markdown 渲染
│   ├── query/                  # Query 相关共享逻辑
│   ├── realtime/               # WebSocket / 实时事件总线
│   ├── types/                  # 共享 TS 类型
│   └── utils/                  # 工具函数
├── components/                 # 可复用原子/分子组件
└── assets/                     # 静态资源
```

### 3.5 任务系统（`jobs/`）
- `job_registry.py` + `job_orchestrator.py` 是任务调度的「单一真源」。
- `jobs/` 目录下按业务域定义 job 模块：`news_jobs.py`、`market_jobs.py`、`macro_jobs.py`、`chatroom_jobs.py`、`decision_jobs.py`、`llm_jobs.py`、`quantaalpha_jobs.py` 等。
- `run_*.py` 是各任务的独立运行入口，供 cron 或手动调用。
- 特殊 Worker：
  - `jobs/run_multi_role_v3_worker.py` — 多角色分析 v3/v4 Worker
  - `jobs/run_quantaalpha_worker.py` — 量化因子 Worker
  - `jobs/run_chief_roundtable_worker.py` — Chief Roundtable Worker

---

## 4. 构建与运行命令

### 4.1 环境准备
所有运行入口默认先加载 `runtime_env.sh`，其中定义了数据库、Redis、LLM 配置、Admin Token 等关键环境变量。
```bash
# 关键环境变量（默认值）
export USE_POSTGRES=1
export DATABASE_URL="postgresql://zanbo@/stockapp"
export REDIS_URL="redis://127.0.0.1:6379/0"
export BACKEND_ADMIN_TOKEN=""
export BACKEND_ALLOWED_ORIGINS="..."
export LLM_PROVIDER_CONFIG_FILE="/home/zanbo/zanbotest/config/llm_providers.json"
```

### 4.2 一键启动（生产/联调推荐）
```bash
cd /home/zanbo/zanbotest
./start_all.sh
```
这会同时启动：
- 主后端 `backend/server.py`（端口 8002）
- 前端构建 `npm run build`（产物由后端同源托管）
- WebSocket 实时服务 `ws_realtime_server.py`（端口 8010）
- Redis Stream 新闻 Worker `stream_news_worker.py`

### 4.3 分开启动
```bash
# 只启动后端（监听局域网）
./start_backend.sh          # 端口 8002
./start_backend_llm.sh      # 端口 8003
./start_backend_multi_role.sh # 端口 8006

# 只启动前端开发服务器
cd apps/web && npm run dev   # 端口 5173，代理 /api 到 8077

# 只启动前端预览（build 后预览）
cd apps/web && npm run preview # 端口 4173

# 实时服务
./start_ws_realtime.sh
./start_stream_news_worker.sh
```

### 4.4 前端构建
```bash
cd /home/zanbo/zanbotest/apps/web
npm install   # 或 npm ci
npm run build # 输出到 apps/web/dist，由 backend/server.py 托管
```

### 4.5 数据库初始化与迁移
```bash
# 初始化 PostgreSQL 表结构
python3 init_postgres_schema.py --database-url postgresql://zanbo@/stockapp

# SQLite -> PostgreSQL 全量迁移
python3 migrate_sqlite_to_postgres.py --database-url postgresql://zanbo@/stockapp --batch-size 5000
```

### 4.6 定时任务安装
```bash
bash install_all_crons.sh
```
安装后会根据 `job_registry.py` 中的 `DEFAULT_JOBS` 生成 cron 条目，统一由 `job_orchestrator.py` 触发。

---

## 5. 测试策略与命令

项目采用分层验证策略：Python 单元测试 + 接口 smoke + 前端 E2E smoke。

### 5.1 Python 层测试
```bash
cd /home/zanbo/zanbotest

# 最小回归测试（启动本地后端并验证核心接口可用）
python3 -m unittest tests/test_minimal_regression.py

# 全量 shell 回归入口
bash run_minimal_regression.sh

# 前端 API smoke
python3 -m unittest tests/test_frontend_api_smoke.py
bash run_frontend_api_smoke.sh

# 特定服务测试
python3 -m unittest tests/test_decision_service.py
python3 -m unittest tests/test_signals_service.py
python3 -m unittest tests/test_stock_detail_service.py
```

### 5.2 前端 E2E Smoke（Playwright）
```bash
cd /home/zanbo/zanbotest/apps/web

# 核心链路 smoke（auth / navigation / stocks / signals / intelligence / research / chatrooms / system / responsive / smoke）
npm run smoke:e2e:core

# 写操作 + 边界输入
npm run smoke:e2e:write-boundary

# 全量
npm run smoke:e2e:all
```
可选环境变量：
- `PLAYWRIGHT_BASE_URL`
- `SMOKE_ADMIN_USERNAME` / `SMOKE_ADMIN_PASSWORD`
- `SMOKE_PRO_USERNAME` / `SMOKE_PRO_PASSWORD`
- `SMOKE_LIMITED_USERNAME` / `SMOKE_LIMITED_PASSWORD`

### 5.3 CI / GitHub Actions
`.github/workflows/web-smoke-layered.yml` 定义了分层 CI：
1. `smoke-core`：安装依赖、启动后端（`USE_POSTGRES=0`）、运行核心 Playwright smoke。
2. `smoke-write-boundary`：依赖 core 通过后，运行写操作与边界测试。

---

## 6. 开发约定与代码风格

### 6.1 Python 侧
- **数据库访问**：统一使用 `import db_compat as sqlite3`，禁止直接 `import sqlite3`。
- **环境变量**：运行时统一先 source `runtime_env.sh`，不要在代码里硬编码数据库连接。
- **错误处理**：必须输出可行动信息（缺依赖/缺配置/缺数据/超时），避免仅返回抽象失败码。
- **日志**：保持可追踪、可定位问题的日志输出。
- **服务层依赖注入**：很多服务模块提供 `build_*_runtime_deps()` 函数用于组装依赖。
- **兼容优先**：新增能力默认非破坏增量，旧字段/旧路径保留至少一个发布周期。

### 6.2 前端侧
- **状态管理**：前端状态只能通过 `stores/` 管理，禁止绕过 Pinia 直接修改全局状态。
- **API 请求**：封装在 `services/api/` 下，按业务域拆分模块，禁止在页面组件里直接写裸 `fetch`/`axios`。
- **路由与权限**：路由定义在 `app/router.ts`，权限常量在 `app/permissions.ts`，导航结构在 `shared/ui/AppShell.vue`。
- **样式**：使用 Tailwind CSS 4 的 utility-first 方式；全局布局优先复用 `shared/ui/` 组件。
- **类型安全**：接口返回优先配合 Zod 做运行时校验。

### 6.3 通用原则
- **最小改动**：修 bug 和新功能都优先做局部修复，不做顺手重构。
- **不引入新依赖**：除非明确说明理由。
- **不改无关文件**：严禁一次性跨模块大改。
- **文档同步**：凡是影响业务行为、页面入口、接口、任务、部署方式、目录落点的改动，必须同步检查并更新对应文档（`README_WEB.md`、`docs/system_overview_cn.md`、`docs/command_line_reference.md` 等）。

---

## 7. 安全与权限

### 7.1 管理员令牌（BACKEND_ADMIN_TOKEN）
- 任务触发、立即抓取、评分、日报生成、配置保存等「受保护接口」要求请求头携带管理员令牌。
- 前端会从 `localStorage['zanbo_admin_token']` / `sessionStorage['zanbo_admin_token']` / 构建时环境变量 `VITE_ADMIN_API_TOKEN` 读取并发送。
- CI 中统一使用 `BACKEND_ADMIN_TOKEN=test-admin-token`。

### 7.2 CORS
- 后端 CORS 允许列表由 `BACKEND_ALLOWED_ORIGINS` 控制，开发模式包含 `5173`、`4173`、`8077`、`8080` 等本地端口。

### 7.3 权限模型
- 前端权限基于 `effective` 字符串数组（包含具体权限标识或 `*` 通配）。
- 关键权限值定义在 `apps/web/src/app/permissions.ts`：
  - `admin_system`、`admin_users`：系统与用户管理
  - `stocks_advanced`：股票高级功能
  - `signals_advanced`：信号系统
  - `research_advanced`：投研决策与量化
  - `multi_role_analyze`、`trend_analyze`：LLM 分析
  - `news_read`、`stock_news_read`、`daily_summary_read`：情报阅读
  - `chatrooms_advanced`：群聊
  - `macro_advanced`：宏观数据

### 7.4 数据库 Schema
- **不允许自动改动数据库 schema**，除非任务明确要求。
- Schema 变更需通过 `init_postgres_schema.py` 或手写迁移脚本完成，并同步更新 `docs/database_dictionary.md`。

---

## 8. 关键入口速查表

| 目的 | 文件/目录 |
|------|-----------|
| 主后端 API | `backend/server.py` |
| 后端路由拆分 | `backend/routes/*.py` |
| 前端主入口 | `apps/web/index.html` → `src/app/router.ts` |
| 前端 API 封装 | `apps/web/src/services/api/*.ts` |
| 前端状态管理 | `apps/web/src/stores/*.ts` |
| 任务定义真源 | `job_registry.py` |
| 任务编排器 | `job_orchestrator.py` |
| 数据库兼容层 | `db_compat.py` |
| 环境变量 | `runtime_env.sh` |
| 系统文档 | `docs/system_overview_cn.md` |
| 命令行参考 | `docs/command_line_reference.md` |
| 数据字典 | `docs/database_dictionary.md` |
| 前端构建配置 | `apps/web/vite.config.ts` |
| Python 依赖 | `requirements.txt` |
| 前端依赖 | `apps/web/package.json` |

---

## 9. 常见问题与排障入口

### 9.1 后端起不来
- 检查 PostgreSQL 和 Redis 是否已启动。
- 检查 `runtime_env.sh` 中的 `DATABASE_URL`、`REDIS_URL`、`TUSHARE_TOKEN` 是否正确。
- 查看日志：`/tmp/stock_backend_*.log`

### 9.2 前端页面空白或接口 404
- 开发模式：确认 `apps/web/vite.config.ts` 中的 `proxy` 目标端口（默认 `8077`）是否与后端实际端口一致。
- 生产模式：确认 `./start_all.sh` 已执行 `npm run build`，且 `backend/server.py` 正在托管 `apps/web/dist`。

### 9.3 任务没运行
- 检查 cron 是否安装：`python3 scripts/scheduler/check_cron_sync.py`
- 检查任务定义：`python3 job_orchestrator.py list`
- 检查运行记录：`python3 job_orchestrator.py runs --limit 50`

### 9.4 SQLite vs PostgreSQL 数据不一致
- 当前主链路强制走 PostgreSQL。若发现 SQLite 有数据而 PostgreSQL 没有，说明退役后仍有脚本绕过了 `db_compat.py` 或未正确设置 `USE_POSTGRES=1`。

---

## 10. 与其他文档的关系

- **`AGENTS.md`**：Agent 的「执行守则」，包含当前最高优先级、不可破坏项、任务 framing template、输出格式要求。执行任务前必读。
- **`README_WEB.md`**：运行与部署手册，包含更详细的启动命令、局域网访问方式、数据迁移步骤。
- **`docs/DOCS_INDEX.md`**：文档索引与生命周期说明，告诉你哪些文档是主链、哪些是归档、哪些是实验提案。
- **`docs/system_overview_cn.md`**：系统业务能力全景，适合理解数据流和页面清单。

---

*最后更新：基于项目实际代码状态生成（2026-04-15）*
