# P0-3 决策动作可追溯性证据（2026-04-17）

## 审计方法

直接查询 `decision_actions` 表，检查每条记录是否具备：
- `id`（行主键，即 action 唯一标识）
- `action_type`（类型字段）
- `ts_code`（关联股票代码）
- `created_at`（时间戳）

## 审计结果

| id | action_type | ts_code | 可追溯 | 孤儿 |
|----|------------|---------|--------|------|
| 1 | defer | 601100.SH | ✓ | 否 |
| 2 | defer | 601899.SH | ✓ | 否 |

- 总记录数：2
- 可追溯：2/2 = **100%**（≥95% 目标）
- 孤儿动作：**0**（无 ts_code 且无 context 的记录）

## 字段结构说明

`action_payload_json` 包含：
- `context`：动作上下文（包含 `source`、`source_module` 字段）
- `source`：来源模块标识

## 前端链路验证

`DecisionBoardPage.vue` 中 `actionMutation` 的 context 字段：
```typescript
source: decisionContext.value.from || 'decision_board',
source_module: decisionContext.value.from || 'decision_board',
```

跨模块跳转（news/chatroom/signal_graph → decision）时，`from` 字段自动填充来源标签，存入每条动作的 `action_payload_json.context.source_module`。

**结论：所有可用动作 100% 可追溯，孤儿动作为 0，满足 P0-3 验收标准。**
