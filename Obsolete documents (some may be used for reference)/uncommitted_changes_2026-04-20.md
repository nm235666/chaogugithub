# 未提交改动汇总

**基准 commit**: `76adb90` — feat: harden decision-execution loop and add deep audit artifacts  
**统计**: 32 个已追踪文件改动 + 11 个新文件，+1465 / -475 行  
**涵盖轮次**: Round 27 / Round 28 / Round 29

---

## Round 27 — 22% Gap 补齐

### 前端路由与导航大重构

**`apps/web/src/app/navigation.config.json`** / **`router.ts`** / **`navigation.ts`**

- 所有业务路由从扁平结构迁移到带前缀的双轨结构：
  - `Pro` 用户功能路径统一加 `/app/` 前缀（如 `/research/workbench` → `/app/workbench`）
  - 管理端功能迁移至 `/admin/` 前缀（如 `/signals/audit` → `/admin/system/signals-audit`）
- 新增导航项：
  - `/app/macro-regime`（三周期状态，`research_advanced`）
  - `/app/allocation`（长线配置动作，`research_advanced`）
- 权限调整：`/macro/regime` 从 `macro_advanced` → `research_advanced`

### ResearchWorkbenchPage.vue（+542 行）

- **TopN 配置化**：工作台主题榜新增 3/5/10 切换，用 `localStorage` 持久，排序口径明确展示
- **macroFlowStatus**：三步流程（计算→人工复核→沉淀）状态指示器
- **待处理动作**：`pendingActions` computed（Round 29 进一步修复）
- **信号池卡片**：`signalsTotal` 动态显示信号数（Round 29 补入）
- **coreThemeConclusion**：展示主线热度 + 次选主题（Round 29 增强）

### DecisionBoardPage.vue（+67 行）

- 新增账户级仓位字段全链：`position_pct_range`、`target_position_pct`、`risk_budget_pct`
- confirm 类动作缺仓位信息时弹阻断提示（软约束）
- `actionPriority` 类型修正（`as 'high' | 'medium' | 'low'`）

### AppShell.vue

- 双轨路由兼容，导航激活态匹配 `/app/*` 和 `/admin/*` 前缀

### UpgradePage.vue（+73 行）

- 升级页面重构，RBAC 提示文字更新

### apps/web/tests/e2e/

**`mainline.spec.ts`**（+101 行）：5 条新 smoke test

1. 宏观三周期页可访问
2. 工作台 TopN 切换
3. 决策看板仓位字段录入
4. 决策转订单 execution_task_warning 检测
5. 组合配置页 Pro 门禁

**`navigation.spec.ts`** / **`smoke.spec.ts`**：路由前缀同步更新

### collect_daily_metrics.py（新文件）

- SLA 指标采集：承接率、近 7 日宏观录入次数、订单数等
- `--gate` 标志：指标不达标时非零退出（CI 集成用）

### backend/routes/analytics.py（新文件）

- `POST /api/analytics/event`：前端埋点事件收集
- 自动建 `user_analytics_events` 表

---

## Round 28 — 深度业务测试审计修复（5 项）

### backend/server.py（+23 行）

**`_request_is_protected()` 新增三条路径检查**（按顺序插入 `/api/macro` 通用检查之前）：

```python
if path.startswith("/api/macro/regime"):
    return _has("macro_advanced") or _has("research_advanced")
if path.startswith("/api/portfolio/allocation"):
    return _has("research_advanced")
if path.startswith("/api/portfolio"):
    return _has("research_advanced") or _has("stocks_advanced")
```

- 修复：Pro 用户无法访问 `/macro/regime`（`macro_advanced` 单条件过严）
- 修复：limited 用户可绕过前端直接访问 `/api/portfolio/allocation`
- 修复：Pro 用户 `/api/portfolio/orders` 403

新增 3 个路由模块 import：`macro_regime_routes`、`portfolio_allocation_routes`、`analytics_routes`

### backend/routes/decision.py（+56 行）

- **删除** LIKE-on-JSON 幂等检查（`action_payload_json::text LIKE '%"idempotency_key": "xxx"%'`）——交由 service 层 DB 级唯一索引处理
- **补 else 分支**：`create_order()` 返回 `ok=False` 时设置 `execution_task_warning`，不再静默失败：
  ```python
  else:
      response["execution_task_warning"] = (
          f"执行任务自动创建失败: {order_result.get('error', '未知错误')} "
          f"（决策记录已保存，需手动创建执行任务）"
      )
  ```
- 账户级仓位字段（`position_pct_range`、`target_position_pct`、`risk_budget_pct`）写入 `action_payload`

### services/decision_service/service.py（+35 行）

- 新增 `_ensure_decision_idempotency_column(conn)`：
  - `ALTER TABLE decision_actions ADD COLUMN IF NOT EXISTS idempotency_key TEXT DEFAULT NULL`
  - 部分唯一索引：`CREATE UNIQUE INDEX IF NOT EXISTS ... WHERE idempotency_key IS NOT NULL`
- `record_decision_action()` 改为 SELECT before INSERT：幂等键存在则返回已有记录，竞态安全

### apps/web/src/services/api/decision.ts（+12 行）

- `DecisionActionResponse` 新增 `execution_task_warning?: string` 字段

---

## Round 29 — 工作台 UX 修复 & 宏观三周期自动建议

### ResearchWorkbenchPage.vue（续）

**pendingActions filter 修复**

旧（有 bug）：
```javascript
allActions.value.filter((a: any) =>
  ['watch', 'pending'].includes(a.execution_status || a.action_type || '')
)
```
新（显式谓词）：
```javascript
(a.action_type === 'confirm' && (!a.execution_status || a.execution_status === 'planned'))
|| (a.action_type === 'watch')
|| (a.execution_status === 'planned')
```

**signalsTotal 动态信号数**

- 新增 `useQuery` → `/api/investment-signals?limit=1`（5 min refetch）
- `signalsTotal` computed 从 `total / count / signals.length / items.length` 中取值
- 机会池卡片：`{{ signalsTotal != null ? \`${signalsTotal} 信号\` : '信号 + 评分' }}`

**coreThemeConclusion 增强**

- 提取 `heat_level` / `strength` 字段，格式化为 `（热度 N）`
- 展示次选主题（最多 2 条），用 ` / ` 分隔
- 格式：`主线：{name}（热度 N），次选：{name2} / {name3}`
- 无信号 fallback：`建议先运行信号采集任务。`

### services/macro_service/（新目录）

**`regime.py`**：新增 `suggest_regime()`

- 查询 `theme_hotspots` 近 7 日方向分布（PostgreSQL 优先，SQLite fallback）
- 推断规则：bull_ratio ≥ 0.6 → `expansion`；bear_ratio ≥ 0.5 → `risk_rising`；bull_ratio ≥ 0.4 → `recovery`；否则 `volatile`
- 中/长期继承最新历史 regime 行（置信 0.6），无记录默认 `volatile`（置信 0.4）
- 返回：`{short/medium/long_term_state, confidence, basis, data_points}`

**`__init__.py`**：导出 `suggest_regime`

### backend/routes/macro_regime.py（新文件）

- `dispatch_get`：`?suggest=1` → `suggest_regime()` → `{"suggestion": {...}}`
- `dispatch_get`：`?history=1` → 历史列表
- `dispatch_post` / `dispatch_patch`：录入/更新三周期状态，含 `correction_suggestion` 字段

### backend/routes/portfolio_allocation.py（新文件）

- `GET /api/portfolio/allocation`：读取长线配置动作（`research_advanced` 门禁）
- `POST /api/portfolio/allocation`：录入配置动作

### apps/web/src/services/api/macro_regime.ts（新文件）

- `RegimeSuggestion` interface
- `fetchRegimeSuggestion()` → `GET /api/macro/regime?suggest=1`
- `fetchLatestRegime` / `fetchRegimeHistory` / `recordRegime` / `updateRegimeOutcome`
- `REGIME_STATE_LABELS` 枚举映射

### apps/web/src/services/api/portfolio_allocation.ts（新文件）

- `AllocationRecord` interface
- `fetchAllocation()` / `recordAllocation()`

### apps/web/src/pages/macro/MacroRegimePage.vue（新文件）

- 三周期录入表单（短/中/长期状态 + 变化原因）
- **系统建议卡片**（v-if="suggestion"，天蓝色）：
  - 三列展示短/中/长期建议 + 置信度
  - "一键填入表单"按钮（`applySuggestion()`）
  - 依据文字 + "建议仅供参考，请人工复核后确认录入"说明
- `useQuery(['macro-regime-suggestion'])` 10 min refetch
- 历史记录列表 + 长线复盘（`correction_suggestion`）内联编辑

### apps/web/src/pages/portfolio/AllocationPage.vue（新文件）

- 长线配置动作列表与录入页（`research_advanced` 权限）

---

## 其他散落改动

| 文件 | 改动 |
|------|------|
| `DashboardPage.vue` | 路由前缀同步，快速入口链接更新 |
| `OrdersPage.vue` / `PositionsPage.vue` | `execution_task_warning` 展示、路由前缀同步 |
| `ChatroomInvestmentPage.vue` / `MarketConclusionPage.vue` | 路由前缀同步 |
| `NewsListPageBlock.vue` / `ScoreboardPage.vue` / `TaskInboxPage.vue` | 路由前缀同步，小幅 RBAC 调整 |
| `SignalChainGraphPage.vue` / `SignalsOverviewPage.vue` | 路由前缀同步 |
| `PricesPage.vue` / `StockDetailPage.vue` / `StockScoresPage.vue` | 路由前缀同步，CandidateFunnelPage 同 |
| `config/llm_providers.json` (+302行) | multi_role_v3 各阶段模型路由策略大量补充 |
| `docs/database_audit_report.md` | 审计报告更新（新增表结构、字段描述） |
| `docs/system_overview_cn.md` | 系统概览更新（新模块、新 API） |
| `docs/DOCS_INDEX.md` | 索引新增本次文档条目 |

---

## 验证状态

| 检查项 | 结果 |
|--------|------|
| Python ast.parse（backend/server.py, routes/decision.py, decision_service, macro_service, macro_regime） | ✓ 全部 OK |
| npm run build（Round 27） | ✓ 2.90s 零错误 |
| npm run build（Round 28） | ✓ 2.99s 零错误 |
| npm run build（Round 29） | ✓ 3.10s 零错误 |

---

## 待人工验证（代码层无法覆盖）

1. pendingActions — 需数据库有 `action_type='confirm'` 且 `execution_status IS NULL` 的真实记录
2. signalsTotal — 需 `/api/investment-signals` 返回 `total` 字段，否则 fallback 静态文字
3. suggest_regime — 需 `theme_hotspots` 表有近 7 日数据；无数据时返回 volatile+置信 0.4
4. SLA gate（承接率 ≥ 100%，近 7 日宏观录入 ≥ 1）— 需真实用户行为积累
5. 宏观三周期"计算→复核→沉淀"完整流程 — 需真实用户在 MacroRegimePage 操作
