# Zanbo Quant 投研系统 - Agent 指南

> 本文件供 AI 编程助手阅读，用于理解项目结构、技术栈和开发规范。

## 项目概述

这是一个围绕 A 股市场的本地化投研系统，集成了股票数据查询、新闻分析、群聊观点提炼、LLM 智能分析和投资信号追踪等功能。

### 核心能力

- **股票数据**：基础信息、日线/分钟线行情、财务指标、估值、公司事件
- **新闻情报**：国际/国内财经新闻抓取、LLM 评分、情绪分析、股票映射
- **群聊分析**：微信群聊监控、投资倾向分析、股票候选池生成
- **LLM 分析**：股票走势分析、多角色公司研究、日报总结生成
- **投资信号**：多源信号融合、主题热点追踪、信号时间线
- **宏观数据**：汇率、利率曲线、利差、市场资金流

## 技术栈

### 后端

| 组件 | 技术 | 说明 |
|------|------|------|
| 主服务 | Python 3 + `http.server` |  monolithic API 服务器 |
| 数据库 | PostgreSQL 主库 | 所有业务数据存储 |
| 缓存/消息 | Redis | 缓存、Pub/Sub、Stream |
| 数据库访问 | `db_compat.py` | PostgreSQL/SQLite 兼容层 |
| 任务调度 | `job_orchestrator.py` | 内置任务编排与执行跟踪 |
| LLM 网关 | `llm_gateway.py` | 多提供商 fallback 机制 |
| 实时通信 | WebSocket + Redis Stream | 新闻实时推送 |

### 前端

| 版本 | 技术栈 | 用途 |
|------|--------|------|
| 旧版 | Vanilla HTML/JS | 只读冻结，`frontend/` 目录（历史存档） |
| 新版 | Vue 3 + TypeScript + Vite | 主力开发，`apps/web/` 目录 |

**新版前端技术细节**：
- Vue 3 (Composition API)
- TypeScript 5.9
- Vite 8 (构建工具)
- Vue Router 4 (路由)
- Pinia 3 (状态管理)
- TanStack Query (服务端状态)
- Tailwind CSS v4 (样式)
- Axios (HTTP 客户端)
- Lightweight Charts / ECharts (图表)

### 外部依赖

- **数据源**：Tushare、AKShare、新浪财经、东方财富、RSS  feeds
- **LLM 提供商**：DeepSeek、GPT-5.4、Kimi K2.5

## 项目结构

```
/home/zanbo/zanbotest/
├── backend/
│   └── server.py              # 主 API 服务器 (6880 行，monolithic)
├── frontend/                  # 旧版前端（只读冻结存档）
├── apps/web/                  # 新版前端 (Vue 3 + TS)
│   ├── src/
│   │   ├── pages/            # 页面组件
│   │   ├── services/api/     # API 客户端
│   │   ├── shared/           # 共享组件/工具
│   │   ├── stores/           # Pinia stores
│   │   └── app/router.ts     # 路由配置
│   ├── package.json
│   └── vite.config.ts
├── docs/                      # 文档
│   ├── system_overview_cn.md  # 系统全景
│   ├── database_dictionary.md # 数据字典
│   └── database_audit_report.md
├── job_registry.py           # 任务定义注册表
├── job_orchestrator.py       # 任务调度执行
├── llm_gateway.py            # LLM 调用网关
├── llm_provider_config.py    # LLM 提供商配置
├── db_compat.py              # 数据库兼容层
├── realtime_streams.py       # Redis Stream 发布
├── ws_realtime_server.py     # WebSocket 服务器
├── stream_news_worker.py     # 新闻流消费者
├── migrate_sqlite_to_postgres.py  # 数据迁移
├── init_postgres_schema.py   # PG 表结构初始化
└── [80+ 数据抓取/处理脚本]   # fetch_*.py, backfill_*.py, llm_*.py 等
```

## 核心配置文件

### 数据库连接

```bash
# 环境变量 (runtime_env.sh)
export USE_POSTGRES=1
export DATABASE_URL=postgresql://zanbo@/stockapp
export REDIS_URL=redis://127.0.0.1:6379/0
```

### LLM 配置 (llm_provider_config.py)

- `LLM_DEFAULT_REQUEST_MODEL`: 默认模型 (auto)
- `LLM_FALLBACK_MODELS`: fallback 链 (GPT-5.4,kimi-k2.5,deepseek-chat)
- `*_BASE_URL`, `*_API_KEY`: 各提供商配置

## 启动命令

### 一键启动全部服务

```bash
cd /home/zanbo/zanbotest
./start_all.sh
```

启动的服务：
- 后端 API: `http://IP:8002`
- 新版前端: `http://IP:8080`
- WebSocket: `ws://IP:8010/ws/realtime`
- 新闻流 Worker

### 新版前端开发

```bash
cd /home/zanbo/zanbotest/apps/web
npm install
npm run dev          # http://0.0.0.0:5173
```

### 单独启动后端

```bash
cd /home/zanbo/zanbotest
./start_backend.sh   # PORT=8002
```

### Nginx 反向代理 (生产)

```bash
./start_nginx_8077.sh   # 端口 8077，代理到各后端服务
```

## 数据库架构

### 主表分类

| 分类 | 代表表 |
|------|--------|
| 股票与公司数据 | `stock_codes`, `stock_daily_prices`, `stock_minline`, `stock_financials`, `stock_valuation_daily`, `company_governance` |
| 新闻与 LLM 分析 | `news_feed_items`, `news_daily_summaries`, `stock_news_items` |
| 群聊与社群 | `chatroom_list_items`, `wechat_chatlog_clean_items`, `chatroom_investment_analysis`, `chatroom_stock_candidate_pool` |
| 宏观与资金流 | `macro_series`, `fx_daily`, `capital_flow_market`, `capital_flow_stock` |
| 投资信号 | `investment_signal_tracker`, `investment_signal_tracker_7d`, `investment_signal_tracker_1d`, `theme_stock_mapping` |

### 数据库初始化

```bash
# 从 SQLite 迁移到 PostgreSQL
python3 migrate_sqlite_to_postgres.py --database-url postgresql://zanbo@/stockapp

# 仅初始化表结构
python3 init_postgres_schema.py --database-url postgresql://zanbo@/stockapp
```

## 任务调度系统

### 查看任务定义

```bash
python3 job_orchestrator.py list
```

### 同步任务到数据库

```bash
python3 job_orchestrator.py sync
```

### 手动触发任务

```bash
python3 job_orchestrator.py run <job_key>
```

### 主要定时任务 (cron)

| 频率 | 任务 | 脚本 |
|------|------|------|
| */5 分钟 | 国际新闻采集 | `run_news_fetch_once.sh` |
| */2 分钟 | 国内新闻采集 | `run_cn_news_fetch_once.sh` |
| */3 分钟 | 监控群聊抓取 | `run_monitored_chatlog_fetch_once.sh` |
| 每小时 | 投资信号刷新 | `run_investment_signal_tracker_once.sh` |
| 每日 | 盘后数据更新 | `run_daily_postclose_update.sh` |

### 安装所有定时任务

```bash
./install_all_crons.sh
```

## API 端点概览

### 核心端点

| 端点 | 说明 |
|------|------|
| `GET /api/health` | 健康检查 |
| `GET /api/stocks` | 股票列表查询 |
| `GET /api/stock-detail?ts_code=xxx` | 股票详情 |
| `GET /api/news` | 新闻列表 |
| `GET /api/news/daily-summaries` | 日报总结 |
| `GET /api/investment-signals` | 投资信号 |
| `GET /api/chatrooms` | 群聊列表 |
| `GET /api/chatrooms/candidate-pool` | 股票候选池 |
| `POST /api/llm/trend` | LLM 走势分析 |
| `POST /api/llm/multi-role/start` | 多角色分析 (异步) |
| `GET /api/llm/multi-role/task?id=xxx` | 查询异步任务状态 |

## 代码组织规范

### Python 脚本分类

| 前缀 | 用途 | 示例 |
|------|------|------|
| `fetch_*.py` | 数据抓取 | `fetch_news_rss.py`, `fetch_cn_news_eastmoney.py` |
| `backfill_*.py` | 历史数据回填 | `backfill_stock_financials.py`, `backfill_macro_series.py` |
| `llm_*.py` | LLM 分析 | `llm_score_news.py`, `llm_analyze_stock_trend.py` |
| `build_*.py` | 数据构建/聚合 | `build_investment_signal_tracker.py`, `build_chatroom_candidate_pool.py` |
| `run_*.py` | 一次性运行脚本 | `run_news_fetch_once.sh` (包装器) |
| `seed_*.py` | 数据种子/初始化 | `seed_stock_alias_dictionary.py` |
| `cleanup_*.py` | 数据清理 | `cleanup_duplicate_items.py` |

### 新增脚本的规范

1. **Shebang**: `#!/usr/bin/env python3`
2. **Future imports**: `from __future__ import annotations`
3. **数据库访问**: 使用 `db_compat as sqlite3`，不要直接 import sqlite3
4. **环境变量**: 从环境读取 `DATABASE_URL`, `REDIS_URL`
5. **错误处理**: 网络请求需有重试机制
6. **日志**: 使用 `print()` 输出关键步骤，便于追踪

## LLM 使用规范

### 调用方式

```python
from llm_gateway import chat_completion_with_fallback, DEFAULT_LLM_MODEL

result = chat_completion_with_fallback(
    model="auto",  # 或指定 "GPT-5.4", "kimi-k2.5", "deepseek-chat"
    messages=[{"role": "user", "content": prompt}],
    temperature=0.2,
    max_retries=2,
)
```

### 多提供商 Fallback

系统自动按 `LLM_FALLBACK_MODELS` 顺序尝试，单提供商失败时自动切换。

## 开发注意事项

### 数据库兼容性

- 所有代码必须使用 `db_compat as sqlite3`，保持对 PostgreSQL 和 SQLite 的兼容
- 使用参数化查询: `cursor.execute("SELECT * FROM t WHERE id = ?", (id,))`
- 批量插入使用 `execute_values` (PostgreSQL) 或 `executemany` (兼容层)

### 实时数据流

- 新闻抓取后通过 `realtime_streams.publish_news_batch()` 发布
- `stream_news_worker.py` 消费 Redis Stream 并广播到 WebSocket
- 前端通过 `useRealtimeBus.ts` 连接 WebSocket

### 异步 LLM 任务

- 耗时 LLM 分析使用异步模式: `start` 返回任务 ID，`task` 端点轮询结果
- 任务状态存储在内存字典 (开发环境) 或 Redis (生产)

### 配置文件管理

- 敏感配置 (API Key) 通过环境变量传入
- 通用配置在 `llm_provider_config.py`
- 角色配置在 `roles_config.example.json` (复制为 `roles_config.json`)

## 调试技巧

### 查看日志

```bash
tail -f /tmp/stock_backend.log
tail -f /tmp/ws_realtime.log
tail -f /tmp/stream_news_worker.log
```

### 数据库检查

```bash
# 检查表行数
python3 -c "import db_compat as sqlite3; conn = sqlite3.connect(''); print(conn.execute('SELECT COUNT(*) FROM stock_codes').fetchone()[0])"

# 检查 Redis
redis-cli ping
redis-cli keys '*'
```

### API 测试

```bash
curl "http://localhost:8002/api/health"
curl "http://localhost:8002/api/stocks?page=1&page_size=5"
```

## 扩展指南

### 添加新数据表

1. 在 SQLite (开发环境) 创建表结构
2. 运行 `init_postgres_schema.py` 同步到 PostgreSQL
3. 在 `db_compat.py` 添加表名常量 (如果需要)
4. 编写 `fetch_xxx.py` 或 `backfill_xxx.py` 脚本
5. 在 `job_registry.py` 添加定时任务

### 添加新 API 端点

1. 在 `backend/server.py` 的 `do_GET`/`do_POST` 中添加路由
2. 使用 `self.send_json_response(data)` 返回 JSON
3. 数据库查询使用 `db_compat.get_connection()`
4. 大查询添加 Redis 缓存

### 添加新前端页面

1. 在 `apps/web/src/pages/` 创建页面目录和 `.vue` 文件
2. 在 `app/router.ts` 添加路由配置
3. 如需别名兼容旧版，添加 `alias` 配置
4. 在 `services/api/` 添加 API 客户端方法

## 相关文档

- `docs/system_overview_cn.md` - 系统全景总览
- `docs/database_dictionary.md` - 数据库数据字典
- `README_WEB.md` - 前后端分离部署说明
- `SQLITE_RETIRED.md` - SQLite 退役记录
