# 前后端分离投研系统运行说明

## 目录结构

- `backend/server.py`：后端 API（PostgreSQL + Redis 缓存 + REST）
- `apps/web/`：新版前端（Vue 3 + TS + Vite，默认入口）
- `stock_codes.db`：原 SQLite 数据库（迁移源/兼容保留）
- `migrate_sqlite_to_postgres.py`：SQLite 全量迁移到 PostgreSQL
- `init_postgres_schema.py`：仅初始化 PostgreSQL 表结构
- `db_compat.py`：SQLite/PostgreSQL 兼容访问层

## 启动方式

1. 一键启动（推荐，默认新版前端）：

```bash
cd /home/zanbo/zanbotest
./start_all.sh
```

2. 分开启动后端（监听局域网）：

```bash
cd /home/zanbo/zanbotest
./start_backend.sh
```

默认会使用：

- `DATABASE_URL=postgresql://zanbo@/stockapp`
- `REDIS_URL=redis://127.0.0.1:6379/0`
- `TUSHARE_TOKEN=<你的 Tushare token>`
- `BACKEND_ADMIN_TOKEN=<受保护接口令牌>`
- `BACKEND_ALLOWED_ORIGINS=http://127.0.0.1:8077,http://localhost:8077,...`

说明：

- 现在主后端、新闻脚本、聊天脚本、回填脚本、LLM 脚本默认都直接读写 PostgreSQL
- PostgreSQL 是当前唯一主运行库；仓库中若仍存在 SQLite 文件，视为迁移源、兼容保留或历史遗留，不应作为当前主库
- 只有迁移脚本 `migrate_sqlite_to_postgres.py` / `init_postgres_schema.py` 仍会直接读取 SQLite 源结构
- 任务触发、立即抓取、评分、日报生成、配置保存等受保护接口现在要求 `BACKEND_ADMIN_TOKEN`
- 新版前端会自动从 `localStorage['zanbo_admin_token']` / `sessionStorage['zanbo_admin_token']` / `VITE_ADMIN_API_TOKEN` 读取令牌
- `apps/web/` 是当前唯一主力前端；旧版 `frontend/` 已退场，不再作为开发或部署目标

### 多角色分析 v3（当前主链路，v4 引擎）

- 前端页面：`/research/multi-role`
- 主接口：
  - `POST /api/llm/multi-role/v3/jobs`
  - `GET /api/llm/multi-role/v3/jobs/<job_id>`
  - `GET /api/llm/multi-role/v3/jobs/<job_id>/stream`
  - `POST /api/llm/multi-role/v3/jobs/<job_id>/decisions`
  - `POST /api/llm/multi-role/v3/jobs/<job_id>/actions`
- 常见任务状态：`queued` / `running` / `pending_user_decision` / `approved` / `rejected` / `deferred` / `done_with_warnings` / `error`
- 执行内核：默认走 `engine_version=v4`（LangGraph 图编排）；接口路径保持 v3 不变
- 模型路由：v3 已支持“按角色 + 按阶段”双层策略（`config/llm_providers.json` 的 `multi_role_v3_policies`）
- 紧急回滚开关：设置环境变量 `MULTI_ROLE_V4_ROLLBACK_TO_V3=1` 可临时回退旧 v3 执行管线

独立 worker（建议长期运行）：

```bash
cd /home/zanbo/zanbotest
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; python3 jobs/run_multi_role_v3_worker.py' >/tmp/multi_role_v3_worker.log 2>&1 &
```

### 后端重启（不依赖 ripgrep）

```bash
cd /home/zanbo/zanbotest
pkill -f "python3 backend/server.py" || true
sleep 1
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; PORT=8000 python3 backend/server.py' >/tmp/stock_backend_8000.log 2>&1 &
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; PORT=8002 python3 backend/server.py' >/tmp/stock_backend_8002.log 2>&1 &
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; PORT=8004 python3 backend/server.py' >/tmp/stock_backend_8004.log 2>&1 &
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; PORT=8005 python3 backend/server.py' >/tmp/stock_backend_8005.log 2>&1 &
nohup bash -lc '. /home/zanbo/zanbotest/runtime_env.sh; PORT=8006 python3 backend/server.py' >/tmp/stock_backend_8006.log 2>&1 &
ss -ltnp | grep -E ':8000|:8002|:8004|:8005|:8006'
```

### 因子挖掘运行环境（QuantaAlpha）

首次部署或迁移机器时，先初始化 QuantaAlpha 专用运行环境：

```bash
cd /home/zanbo/zanbotest
bash scripts/setup_quantaalpha_runtime.sh
```

`runtime_env.sh` 会自动优先使用 `runtime/quantaalpha_venv/bin/python` 作为 `QUANTAALPHA_PYTHON_BIN`。  
可手工自检：

```bash
. /home/zanbo/zanbotest/runtime_env.sh
"$QUANTAALPHA_PYTHON_BIN" -c "import dotenv; import quantaalpha; print('ok')"
```

## SQLite 退役

dry-run 预览：

```bash
bash /home/zanbo/zanbotest/retire_sqlite.sh
```

正式退役：

```bash
bash /home/zanbo/zanbotest/retire_sqlite.sh --execute
```

退役后：

- SQLite 主文件会被压缩归档到 `backups/sqlite_retired/<UTC时间>/`
- 运行链路以 PostgreSQL + Redis 为准；若主目录仍有 SQLite 文件，不改变这一事实
- 退役说明会写入 [SQLITE_RETIRED.md](/home/zanbo/zanbotest/SQLITE_RETIRED.md)

3. 新开一个终端，单独启动前端（默认新版构建产物）：

```bash
cd /home/zanbo/zanbotest
./start_frontend.sh
```

4. 开发模式运行新版前端：

```bash
cd /home/zanbo/zanbotest/apps/web
npm run dev
```

## 局域网访问

在服务机器上查看本机 IP（示例）：

```bash
hostname -I
```

假设 IP 是 `192.168.1.23`，则局域网内其他设备访问：

- 前端页面：`http://192.168.1.23:8080`
- 后端健康检查：`http://192.168.1.23:8002/api/health`
- 后端查询接口：`http://192.168.1.23:8002/api/stocks?page=1&page_size=20`

## 数据迁移

初始化 PostgreSQL 表结构：

```bash
python3 /home/zanbo/zanbotest/init_postgres_schema.py --database-url postgresql://zanbo@/stockapp
```

全量迁移 SQLite 到 PostgreSQL：

```bash
python3 /home/zanbo/zanbotest/migrate_sqlite_to_postgres.py --database-url postgresql://zanbo@/stockapp --batch-size 5000
```

只补某一张表：

```bash
python3 /home/zanbo/zanbotest/migrate_sqlite_to_postgres.py --database-url postgresql://zanbo@/stockapp --table news_feed_items
```

## API 参数

- `keyword`：代码/名称模糊搜索
- `status`：`L`(上市) / `D`(退市) / `P`(暂停)
- `page`：页码，默认 1
- `page_size`：每页条数，默认 20，最大 200

## 定时任务调度（统一口径）

- 任务排期单一真源：`job_definitions`（由 `job_registry.py` 同步）
- cron 安装入口：`bash /home/zanbo/zanbotest/install_all_crons.sh`
- 安装脚本会自动：
  - 同步 `job_definitions`
  - 生成“cron -> job_orchestrator job_key”触发行
  - 对 `market_data` 分类任务启用交易日门禁（非交易日写入 `skipped_non_trading_day`）
  - 输出一致性检查与下次触发时间（UTC/CST）

常用检查命令：

```bash
python3 /home/zanbo/zanbotest/scripts/scheduler/check_cron_sync.py
python3 /home/zanbo/zanbotest/job_orchestrator.py list
python3 /home/zanbo/zanbotest/job_orchestrator.py runs --limit 50
```
