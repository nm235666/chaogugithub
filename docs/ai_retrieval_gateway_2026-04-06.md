# AI Retrieval Gateway（2026-04-06）

## 新增能力
- 新接口：`POST /api/ai-retrieval/search`
- 新接口：`POST /api/ai-retrieval/context`
- 维护接口：`POST /api/ai-retrieval/sync`
- 观测接口：`GET /api/ai-retrieval/metrics?days=1`

## 运行开关
- `AI_RETRIEVAL_ENABLED=1|0`
- `AI_RETRIEVAL_SHADOW_MODE=1|0`

说明：
- 关闭 `AI_RETRIEVAL_ENABLED` 时接口返回可用空结果，不阻塞业务主链。
- `SHADOW_MODE` 当前仅用于 trace 标记，不改变接口结构兼容性。

## 存储与索引
- 新增表：
  - `ai_retrieval_documents`
  - `ai_retrieval_chunks`
  - `ai_retrieval_sync_state`
  - `ai_retrieval_audit_logs`
- PostgreSQL 若存在 `pgvector` 扩展，启用向量列与向量召回。
- 若 `pgvector` 不可用，自动降级为关键词召回，不中断主链。

## 配置
- `config/llm_providers.json` 新增 `embedding_profiles`：
  - `news`
  - `report`
  - `chatroom`

## 已接入链路（首批）
- 多角色研究：上下文构建阶段挂接检索上下文（`report + news`）
- 日报总结：生成脚本增加检索补充上下文（可关闭）
- 资讯/报告页：前端新增“语义检索”入口（保持原筛选与分页语义）
