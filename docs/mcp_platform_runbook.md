# MCP 平台运行手册

本项目的一期 Agent 化先落标准 MCP HTTP 工具底座。MCP 服务独立于现有前后端进程运行，建议由反代或内网穿透将 `/mcp` 路径映射到 MCP 服务。

## 访问入口

- 内网入口：`http://192.168.5.52:8077/mcp`
- 外网入口：`http://tianbo.asia:6273/mcp`
- 健康检查：`/mcp/health`

默认健康检查也需要 MCP token。所有请求都应带：

```text
Authorization: Bearer <MCP_ADMIN_TOKEN>
```

## 启动方式

```bash
cd /home/zanbo/zanbotest
export MCP_ADMIN_TOKEN='replace-with-strong-token'
./scripts/run_mcp_server.sh
```

默认监听：

```text
127.0.0.1:8765
```

可通过环境变量覆盖：

```bash
export MCP_HOST=127.0.0.1
export MCP_PORT=8765
export MCP_WRITE_ENABLED=1
export MCP_LAN_BASE_URL=http://192.168.5.52:8077
export MCP_PUBLIC_BASE_URL=http://tianbo.asia:6273
```

## 反代要求

两个入口都应将 `/mcp` 和 `/mcp/health` 转发到 MCP 服务，并保留 `Authorization` header。

示例：

```nginx
location /mcp {
    proxy_pass http://127.0.0.1:8765/mcp;
    proxy_set_header Host $host;
    proxy_set_header Authorization $http_authorization;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}

location /mcp/health {
    proxy_pass http://127.0.0.1:8765/mcp/health;
    proxy_set_header Host $host;
    proxy_set_header Authorization $http_authorization;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

如果 `192.168.5.52:8077` 当前已经承载 Web 前端，则按路径拆分：`/mcp` 转 MCP 服务，其余路径保持原有 Web 转发。

## 首批工具

- `system.health_snapshot`
- `db.table_counts`
- `db.readonly_query`
- `jobs.list_definitions`
- `jobs.list_runs`
- `jobs.list_alerts`
- `jobs.trigger`
- `scheduler.check_cron_sync`
- `business.closure_gap_scan`
- `business.repair_funnel_score_align`
- `business.repair_funnel_review_refresh`
- `business.run_decision_snapshot`
- `business.reconcile_portfolio_positions`

写工具默认 `dry_run=true`。真实执行必须提供：

```json
{
  "dry_run": false,
  "confirm": true,
  "actor": "ops-user",
  "reason": "明确的执行原因",
  "idempotency_key": "稳定幂等键"
}
```

## JSON-RPC 示例

```bash
curl -s \
  -H "Authorization: Bearer ${MCP_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' \
  http://127.0.0.1:8765/mcp
```

```bash
curl -s \
  -H "Authorization: Bearer ${MCP_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"business.closure_gap_scan","arguments":{}}}' \
  http://127.0.0.1:8765/mcp
```

## 审计

所有 `tools/call` 调用都会写入 `mcp_tool_audit_logs`，包含 tool name、参数、dry-run 状态、执行结果和错误信息。写工具执行前必须先确认审计表可写。

后端提供只读导出：`GET /api/agents/mcp-audit?limit=200&write_only=1`（需登录且具备 `research_advanced`），用于运营台「导出 MCP 审计 JSON」。

## 密钥与写开关（运维约定）

- **令牌轮换**：更新 `MCP_ADMIN_TOKEN` / `BACKEND_ADMIN_TOKEN` 时，同步修改加载 `runtime_env.sh` 的进程、systemd unit、以及反代/Secret；重启 `mcp_server` 与 `backend/server.py` 后生效。
- **生产写路径**：仓库内 `runtime_env.sh` 默认 `MCP_WRITE_ENABLED=0`；仅在维护窗口临时置 `1`。`AGENT_AUTO_WRITE_ENABLED=1` 且 MCP 写关闭时，漏斗 Agent 只会 dry-run 并记录 `mcp_write_disabled` 警告。
- **探活**：公开接口 `GET /api/agents/health`（可由 `bash scripts/check_agent_stack_health.sh` 调用）；MCP 进程本体使用 `GET /mcp/health`（需 Bearer）。
