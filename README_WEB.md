# 前后端分离股票查询网页

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
- `stock_codes.db` 已可退役；退役后只保留 PostgreSQL 主库
- 只有迁移脚本 `migrate_sqlite_to_postgres.py` / `init_postgres_schema.py` 仍会直接读取 SQLite 源结构
- 任务触发、立即抓取、评分、日报生成、配置保存等受保护接口现在要求 `BACKEND_ADMIN_TOKEN`
- 新版前端会自动从 `localStorage['zanbo_admin_token']` / `sessionStorage['zanbo_admin_token']` / `VITE_ADMIN_API_TOKEN` 读取令牌
- 旧版 `frontend/` 已退役删除，系统仅保留 `apps/web/` 新前端

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
- 主目录只保留 PostgreSQL + Redis 运行链路
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
