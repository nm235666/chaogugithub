# Zanbo Quant 投研系统 - 完整项目分析报告

---

## 一、项目整体目录概览

```
/home/zanbo/zanbotest/
├── AGENTS.md                          # Agent 开发指南（核心规范文件）
├── README_WEB.md                      # 前后端运行说明
├── docs/                              # 项目文档
│   ├── system_overview_cn.md          # 系统全景总览
│   ├── database_dictionary.md         # 数据库数据字典
│   ├── database_audit_report.md       # 数据库审计报告
│   └── ...                            # 其他文档
│
├── apps/web/                          # 前端应用 (Vue 3 + TypeScript + Vite)
│   ├── src/
│   │   ├── app/                       # 应用核心配置
│   │   │   ├── router.ts              # 路由配置
│   │   │   ├── permissions.ts         # 权限定义
│   │   │   └── navigation.ts          # 导航配置
│   │   ├── pages/                     # 页面组件
│   │   │   ├── auth/                  # 认证相关
│   │   │   ├── dashboard/             # 总控台
│   │   │   ├── stocks/                # 股票相关
│   │   │   ├── intelligence/          # 情报资讯
│   │   │   ├── signals/               # 信号系统
│   │   │   ├── research/              # 深度研究
│   │   │   ├── chatrooms/             # 群聊舆情
│   │   │   ├── macro/                 # 宏观数据
│   │   │   └── system/                # 系统管理
│   │   ├── services/api/              # API 服务
│   │   ├── stores/                    # Pinia 状态管理
│   │   └── shared/                    # 共享组件/工具
│   ├── package.json
│   └── vite.config.ts
│
├── backend/                           # 后端 API
│   ├── server.py                      # 主服务入口
│   └── routes/                        # 路由模块
│       ├── stocks.py
│       ├── news.py
│       ├── signals.py
│       ├── chatrooms.py
│       ├── quant_factors.py
│       └── system.py
│
├── services/                          # 业务服务层
│   ├── agent_service/                 # LLM Agent 服务
│   ├── chatrooms_service/             # 群聊服务
│   ├── stock_news_service/            # 个股新闻服务
│   ├── signals_service/               # 信号服务
│   ├── stock_detail_service/          # 股票详情服务
│   ├── quantaalpha_service/           # 因子挖掘服务
│   ├── reporting/                     # 报告服务
│   └── system/                        # 系统服务
│
├── jobs/                              # 任务执行模块
│   ├── registry_adapter.py
│   ├── run_news_job.py
│   ├── run_chatroom_job.py
│   ├── run_stock_news_job.py
│   ├── run_macro_job.py
│   ├── run_market_job.py
│   ├── run_llm_job.py
│   ├── run_quantaalpha_job.py
│   └── run_multi_role_v3_worker.py
│
├── collectors/                        # 数据采集管道
│   ├── news/                          # 新闻采集
│   ├── chatrooms/                     # 群聊采集
│   ├── market/                        # 市场数据采集
│   ├── macro/                         # 宏观数据采集
│   └── stock_news/                    # 个股新闻采集
│
├── config/                            # 配置文件
│   ├── llm_providers.json             # LLM Provider 配置
│   └── rbac_dynamic.config.json       # 动态 RBAC 配置
│
├── external/quantaalpha/              # 因子挖掘系统（旁路）
│
├── .codex/                            # Codex 技能与模板
├── .deps/                             # 本地依赖（akshare等）
├── runtime/                           # 运行时环境
├── tests/                             # 测试目录
│
# === 数据采集脚本（根目录）===
├── fetch_*.py                         # 实时/增量采集脚本
├── backfill_*.py                      # 历史数据补录脚本
├── build_*.py                         # 数据构建/聚合脚本
├── llm_*.py                           # LLM 分析脚本
├── run_*.sh                           # 一次性运行脚本
├── job_registry.py                    # 任务注册中心
└── job_orchestrator.py                # 任务编排器
```

---

## 二、文件级分析

### 2.1 核心配置文件

#### `/home/zanbo/zanbotest/AGENTS.md`
- **文件作用**：Agent 开发指南，定义项目开发规范
- **主要功能**：
  - 明确项目优先级：前端展示业务 > 测试可用性 > 其他
  - 定义架构约束：状态管理、API 兼容、数据库 schema 等
  - 规范工作流程：分析 → 计划 → 执行 → 验证
  - 定义输出格式和文档新鲜度原则
- **与其他文件关系**：引用 docs/、backend/、apps/web/ 等核心模块
- **是否核心文件**：**是**，开发行为的首要参考

#### `/home/zanbo/zanbotest/README_WEB.md`
- **文件作用**：前后端分离系统运行说明
- **主要功能**：
  - 启动命令说明
  - 环境变量配置（DATABASE_URL, REDIS_URL, TUSHARE_TOKEN 等）
  - 多角色分析 v3 API 说明
  - SQLite 退役流程
- **是否核心文件**：**是**，部署和运维的首要参考

### 2.2 后端核心文件

#### `/home/zanbo/zanbotest/backend/server.py`
- **文件作用**：后端主服务入口（Flask-like HTTP 服务）
- **主要功能**：
  - HTTP 请求路由与处理
  - 用户认证与权限控制（RBAC）
  - API 端点实现（股票、新闻、信号、LLM 等）
  - 多角色分析任务管理（v2/v3）
  - Redis 缓存集成
- **关键端点**：
  - `/api/health` - 健康检查
  - `/api/stocks` - 股票列表
  - `/api/news` - 新闻查询
  - `/api/llm/multi-role/v3/jobs` - 多角色分析
  - `/api/investment-signals` - 投资信号
  - `/api/chatrooms/*` - 群聊相关
- **是否核心文件**：**是**，系统的后端核心

#### `/home/zanbo/zanbotest/job_registry.py`
- **文件作用**：任务注册中心，定义所有定时任务
- **主要功能**：
  - 定义 JobDefinition 数据类
  - 注册所有定时任务（新闻采集、信号刷新、群聊分析等）
  - 调度表达式管理（crontab 格式）
- **关键任务**：
  - 国际/国内新闻采集（每5分钟/每2分钟）
  - 投资信号跟踪器刷新（每小时）
  - 群聊分析流水线（每小时）
  - 个股新闻评分（每10分钟）
  - 数据库审核（每日）
- **是否核心文件**：**是**，任务调度的核心配置

#### `/home/zanbo/zanbotest/job_orchestrator.py`
- **文件作用**：任务编排器，负责任务执行和监控
- **主要功能**：
  - 任务执行（run_job）
  - Dry-run 支持
  - 任务运行记录查询
  - 告警管理
- **是否核心文件**：**是**，任务执行的核心

#### `/home/zanbo/zanbotest/llm_gateway.py`
- **文件作用**：LLM 调用网关，提供统一的大模型访问接口
- **主要功能**：
  - 多 Provider 支持（DeepSeek, GPT, Kimi 等）
  - 自动降级与故障转移
  - 速率限制管理
  - 调用指标统计
- **是否核心文件**：**是**，所有 LLM 功能的入口

#### `/home/zanbo/zanbotest/db_compat.py`
- **文件作用**：数据库兼容层，支持 SQLite/PostgreSQL 双后端
- **主要功能**：
  - 兼容 SQLite 和 PostgreSQL 的访问接口
  - SQL 语法转换（? → %s）
  - Redis 客户端封装
  - Row 对象支持
- **是否核心文件**：**是**，数据访问的核心基础设施

### 2.3 前端核心文件

#### `/home/zanbo/zanbotest/apps/web/src/app/router.ts`
- **文件作用**：前端路由配置
- **主要功能**：
  - 定义所有页面路由
  - 权限控制集成（meta.permission）
  - 路由守卫（beforeEach）
- **关键路由**：
  - `/dashboard` - 总控台
  - `/stocks/*` - 股票相关
  - `/signals/*` - 信号系统
  - `/research/*` - 深度研究
  - `/chatrooms/*` - 群聊舆情
  - `/system/*` - 系统管理
- **是否核心文件**：**是**，前端导航的核心

#### `/home/zanbo/zanbotest/apps/web/src/services/api/stocks.ts`
- **文件作用**：股票相关 API 服务
- **主要功能**：
  - 封装 HTTP 请求
  - 股票列表、详情、价格查询
  - 多角色分析任务接口（v2/v3）
  - 流式数据支持
- **是否核心文件**：**是**，前端数据访问的核心

### 2.4 数据采集脚本（按功能分类）

#### 股票数据采集
| 文件 | 作用 | 核心功能 |
|------|------|----------|
| `fetch_all_stock_codes.py` | 获取股票基础信息 | 使用 Tushare API 获取全部股票代码 |
| `fetch_sina_minline_all_listed.py` | 分钟线采集 | 批量抓取股票分钟线数据 |
| `backfill_stock_financials.py` | 财务数据补录 | 补录股票财务核心指标 |
| `backfill_stock_valuation_daily.py` | 估值数据补录 | 补录 PE/PB/PS 等估值数据 |
| `backfill_stock_events.py` | 事件数据补录 | 分红、回购、解禁等事件 |
| `build_stock_daily_price_rollups.py` | 日线数据构建 | 构建日线行情汇总 |

#### 新闻数据采集
| 文件 | 作用 | 核心功能 |
|------|------|----------|
| `fetch_news_rss.py` | RSS 新闻采集 | 国际财经新闻抓取 |
| `fetch_cn_news_sina_7x24.py` | 国内新闻采集 | 新浪 7x24 快讯 |
| `fetch_cn_news_eastmoney.py` | 东方财富新闻 | 东财快讯抓取 |
| `fetch_stock_news_eastmoney_to_db.py` | 个股新闻采集 | 抓取与股票相关的新闻 |
| `llm_score_news.py` | 新闻评分 | 使用 LLM 对新闻进行评分和摘要 |
| `map_news_items_to_stocks.py` | 新闻映射 | 将新闻映射到相关股票 |

#### 群聊数据采集
| 文件 | 作用 | 核心功能 |
|------|------|----------|
| `fetch_chatroom_list_to_db.py` | 群聊列表抓取 | 获取群聊基础信息 |
| `fetch_wechat_chatlog_clean_to_db.py` | 聊天记录抓取 | 清洗并存储群聊消息 |
| `llm_analyze_chatroom_investment_bias.py` | 投资倾向分析 | LLM 分析群聊投资观点 |
| `build_chatroom_candidate_pool.py` | 候选池构建 | 汇总群聊提到的投资标的 |
| `llm_resolve_stock_aliases.py` | 别名归一 | 将群聊中的股票简称映射到标准代码 |

#### 信号与主题
| 文件 | 作用 | 核心功能 |
|------|------|----------|
| `build_investment_signal_tracker.py` | 信号跟踪器 | 综合新闻、群聊生成投资信号 |
| `build_theme_hotspot_engine.py` | 主题热点引擎 | 主题强度分析和映射 |
| `build_signal_state_machine.py` | 信号状态机 | 信号状态迁移管理 |
| `seed_theme_stock_mapping.py` | 主题映射初始化 | 主题到股票池的映射配置 |
| `seed_stock_alias_dictionary.py` | 别名字典初始化 | 股票简称标准化字典 |

#### LLM 分析
| 文件 | 作用 | 核心功能 |
|------|------|----------|
| `llm_analyze_stock_trend.py` | 走势分析 | LLM 股票技术分析 |
| `llm_multi_role_company_review.py` | 多角色分析 | 六角色协同公司分析 |
| `llm_summarize_daily_important_news.py` | 日报总结 | 生成每日新闻总结 |
| `llm_score_sentiment.py` | 情绪评分 | 文本情绪分析 |
| `llm_tag_chatrooms.py` | 群聊标签 | 自动为群聊打标签 |

### 2.5 业务服务层（services/）

#### `/home/zanbo/zanbotest/services/agent_service/`
- **作用**：LLM Agent 服务
- **关键模块**：
  - `multi_role_v3/` - 多角色分析 v3 实现（LangGraph 编排）
  - `outputs/` - 报告输出格式化
- **功能**：多角色分析、走势分析、异步任务管理

#### `/home/zanbo/zanbotest/services/chatrooms_service/`
- **作用**：群聊业务服务
- **功能**：群聊查询、投资分析查询、候选池查询、实时抓取

#### `/home/zanbo/zanbotest/services/stock_news_service/`
- **作用**：个股新闻服务
- **功能**：新闻查询、立即抓取、评分触发

#### `/home/zanbo/zanbotest/services/signals_service/`
- **作用**：投资信号服务
- **功能**：信号查询、时间线查询、审计查询

### 2.6 任务执行模块（jobs/）

| 文件 | 作用 |
|------|------|
| `registry_adapter.py` | 任务注册适配 |
| `run_news_job.py` | 新闻任务执行 |
| `run_chatroom_job.py` | 群聊任务执行 |
| `run_stock_news_job.py` | 个股新闻任务执行 |
| `run_macro_job.py` | 宏观任务执行 |
| `run_market_job.py` | 市场任务执行 |
| `run_llm_job.py` | LLM 相关任务执行 |
| `run_quantaalpha_job.py` | 因子挖掘任务执行 |
| `run_multi_role_v3_worker.py` | 多角色分析 Worker |

### 2.7 外部系统（external/quantaalpha/）

- **作用**：因子挖掘与回测系统（旁路能力）
- **技术栈**：Python + Qlib
- **功能**：因子自动生成、回测验证、超参数优化
- **与主系统关系**：通过 job 系统调用，结果回写主数据库

---

## 三、模块级归纳

### 3.1 前端模块（apps/web/）

**职责**：提供用户界面和数据可视化

**关键文件**：
- `src/app/router.ts` - 路由定义
- `src/services/api/` - API 客户端
- `src/stores/` - 状态管理（Pinia）
- `src/shared/ui/` - 共享 UI 组件
- `src/pages/` - 页面组件

**依赖关系**：
- 依赖后端 API（backend/server.py）
- 依赖 WebSocket 实时数据（ws_realtime_server.py）

### 3.2 后端 API 模块（backend/）

**职责**：提供 REST API 服务

**关键文件**：
- `server.py` - 主服务入口
- `routes/*.py` - 路由模块

**模块关系**：
```
backend/server.py
  ├── routes/stocks.py → services/stock_*
  ├── routes/news.py → collectors/news/
  ├── routes/signals.py → services/signals_service/
  ├── routes/chatrooms.py → services/chatrooms_service/
  └── routes/system.py → services/system/
```

### 3.3 业务服务层（services/）

**职责**：实现核心业务逻辑

**模块划分**：
| 模块 | 职责 | 调用方 |
|------|------|--------|
| agent_service | LLM 分析服务 | backend/server.py |
| chatrooms_service | 群聊业务 | backend/routes/chatrooms.py |
| stock_news_service | 个股新闻 | backend/routes/stocks.py |
| signals_service | 投资信号 | backend/routes/signals.py |
| stock_detail_service | 股票详情 | backend/server.py |
| quantaalpha_service | 因子挖掘 | jobs/run_quantaalpha_job.py |
| reporting | 报告服务 | backend/server.py |

### 3.4 数据采集模块（collectors/ + 根目录脚本）

**职责**：从外部源采集数据

**数据流**：
```
外部数据源
  ├── 新闻源（RSS/新浪/东财）→ collectors/news/ → PostgreSQL
  ├── 股票行情（Tushare/新浪）→ fetch_*/backfill_* → PostgreSQL
  ├── 群聊数据（微信）→ collectors/chatrooms/ → PostgreSQL
  └── 宏观数据（AKShare）→ collectors/macro/ → PostgreSQL
```

### 3.5 任务调度模块（job_registry.py + job_orchestrator.py + jobs/）

**职责**：定时任务管理和执行

**架构**：
```
job_registry.py (定义任务)
       ↓
job_orchestrator.py (调度执行)
       ↓
jobs/run_*.py (任务执行器)
       ↓
 collectors/ (数据采集)
 或
 根目录脚本 (数据处理)
```

### 3.6 数据库访问层（db_compat.py）

**职责**：统一数据库访问接口

**功能**：
- SQLite/PostgreSQL 兼容
- Redis 缓存集成
- SQL 语法自动转换

---

## 四、项目整体总结

### 4.1 项目的主要目标

Zanbo Quant 是一个围绕 **A 股市场**的**本地化投研系统**，目标是为投资者提供：

1. **数据聚合**：股票行情、财务数据、新闻资讯、群聊舆情、宏观指标
2. **智能分析**：基于 LLM 的新闻评分、公司分析、走势预测、情绪识别
3. **信号生成**：综合多源数据生成投资信号和主题热点
4. **研究支持**：多角色协同分析、标准投研报告、因子挖掘

### 4.2 核心功能

| 功能域 | 核心能力 |
|--------|----------|
| **股票数据** | 基础信息、日线/分钟线行情、财务指标、估值、事件、治理画像 |
| **新闻情报** | 国际/国内财经新闻、个股新闻、自动评分、股票映射、日报总结 |
| **群聊舆情** | 群聊抓取、投资倾向分析、候选池构建、别名归一 |
| **投资信号** | 信号跟踪器、主题热点、状态机、时间线审计 |
| **LLM 分析** | 走势分析、多角色公司分析（6角色）、情绪评分 |
| **宏观研究** | 宏观指标、汇率、利率曲线、利差、风险情景 |
| **系统管理** | 任务调度、LLM Provider 管理、权限控制、数据库审计 |

### 4.3 技术栈

| 层级 | 技术 |
|------|------|
| **前端** | Vue 3 + TypeScript + Vite + Tailwind CSS + Pinia |
| **后端** | Python 3.10 + 自定义 HTTP 服务（类 Flask）|
| **数据库** | PostgreSQL（主库）+ Redis（缓存/消息队列）|
| **数据采集** | Tushare + AKShare + 新浪接口 + RSS |
| **LLM** | DeepSeek + GPT + Kimi（多 Provider 支持）|
| **任务调度** | 自定义 Job Registry + Orchestrator |
| **实时通信** | WebSocket + Redis Pub/Sub |
| **因子挖掘** | QuantaAlpha（Qlib 基础）|

### 4.4 主要架构或设计模式

#### 1. 分层架构
```
前端 (Vue 3)
    ↓ HTTP/WebSocket
后端 API (backend/server.py)
    ↓
业务服务层 (services/)
    ↓
数据访问层 (db_compat.py)
    ↓
PostgreSQL / Redis
```

#### 2. 管道模式（数据采集）
外部源 → 采集器 → 清洗 → 存储 → 分析 → 展示

#### 3. 任务队列模式（LLM 分析）
API 创建任务 → 任务队列 → Worker 消费 → 结果存储 → 前端轮询

#### 4. 多角色协作模式（Research）
```
Analyst → Bull/Bear → Research Manager → Trader → 风控 → Portfolio Manager
```

#### 5. 兼容层模式（db_compat）
统一接口兼容 SQLite/PostgreSQL，便于迁移和测试

### 4.5 关键执行流程

#### 数据流转图
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  外部数据源  │    │  外部数据源  │    │  外部数据源  │
│  (新闻源)    │    │  (行情源)   │    │  (群聊源)   │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ collectors/ │    │ fetch_*/    │    │ collectors/ │
│   news/     │    │ backfill_*  │    │  chatrooms/ │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          ▼
                   ┌─────────────┐
                   │  PostgreSQL │
                   │   主数据库   │
                   └──────┬──────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
   ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
   │ build_*/    │ │   llm_*/    │ │   services/ │
   │ llm_score_* │ │ llm_analyze*│ │  业务查询    │
   └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
          │               │               │
          └───────────────┼───────────────┘
                          ▼
                   ┌─────────────┐
                   │  backend/   │
                   │  server.py  │
                   └──────┬──────┘
                          │
                          ▼
                   ┌─────────────┐
                   │  apps/web/  │
                   │   前端展示   │
                   └─────────────┘
```

### 4.6 重要入口文件

| 入口 | 路径 | 用途 |
|------|------|------|
| **主启动** | `./start_all.sh` | 一键启动前后端 |
| **后端服务** | `backend/server.py` | API 服务入口 |
| **前端入口** | `apps/web/src/main.ts` | Vue 应用入口 |
| **任务注册** | `job_registry.py` | 所有定时任务定义 |
| **数据库兼容** | `db_compat.py` | 数据库访问入口 |
| **LLM 网关** | `llm_gateway.py` | LLM 调用入口 |

### 4.7 潜在风险、问题或可优化点

#### 潜在风险

1. **数据库依赖**
   - 目前已迁移到 PostgreSQL，但仍存在 SQLite 兼容代码
   - 建议：逐步清理 SQLite 兼容代码，简化 db_compat

2. **LLM 调用稳定性**
   - 多 Provider 故障转移机制存在，但依赖外部服务
   - 建议：增加本地模型兜底或缓存策略

3. **任务调度单点**
   - 任务编排器是单点，无分布式支持
   - 建议：考虑引入 Celery 或 APScheduler 的分布式模式

4. **前端构建**
   - 使用 Vite 构建，但缺少完整的 CI/CD 流程
   - 建议：增加自动化测试和构建流水线

#### 可优化点

1. **代码组织**
   - 根目录脚本较多（80+ 个），可按功能分目录整理
   - 建议：将 fetch_*, backfill_*, build_*, llm_* 按域分目录

2. **配置管理**
   - 配置分散在多个文件（runtime_secrets.py, config/, 环境变量）
   - 建议：统一配置中心或配置文件

3. **测试覆盖**
   - 测试目录存在但覆盖度未知
   - 建议：增加单元测试和集成测试

4. **文档维护**
   - 文档较多但更新频率不一
   - 建议：建立文档更新自动化检查

5. **监控告警**
   - 已有基础监控，但可进一步完善
   - 建议：增加业务指标监控（信号准确率、LLM 响应时间等）

---

## 附录：核心数据表汇总

| 表名 | 用途 | 数据量级 |
|------|------|----------|
| `stock_codes` | 股票基础信息 | 5,814 |
| `stock_daily_prices` | 日线行情 | 1,327,993 |
| `stock_minline` | 分钟线 | 1,757,145 |
| `stock_financials` | 财务指标 | 17,631 |
| `stock_events` | 股票事件 | 128,529 |
| `news_feed_items` | 新闻资讯 | 6,740 |
| `wechat_chatlog_clean_items` | 群聊消息 | 320,945 |
| `investment_signal_tracker* ` | 投资信号 | 动态增长 |

---

**分析完成时间**：2026-04-06  
**分析范围**：项目根目录及以下所有核心源代码文件（排除依赖目录）
