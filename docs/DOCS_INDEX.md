# 文档索引与生命周期（2026-04-18）

本索引重构为三层：
- `Active`：持续维护、对当前真实系统负责
- `Consolidated`：融合入口文档（减少重复阅读成本）
- `Local Archive`：本地归档，不再纳入 Git/GitHub

## A. Active（持续维护）

| 文档 | Owner | 更新触发 | 失效条件 | 最后校验 |
| --- | --- | --- | --- | --- |
| `AGENTS.md` | 工程治理 | 开发流程/规则变更 | 规则与实际执行不一致 | 2026-04-18 |
| `README_WEB.md` | 平台工程 | 启动、部署、运行链路变更 | 按文档无法跑起主链 | 2026-04-18 |
| `docs/system_overview_cn.md` | 产品+后端 | 新增/下线模块、主链改动 | 页面/API/流程描述失真 | 2026-04-18 |
| `docs/command_line_reference.md` | 平台工程 | 新增/废弃脚本命令 | 命令不可执行或误导 | 2026-04-18 |
| `docs/database_dictionary.md` | 数据层 | 表结构/字段语义变化 | 字段解释与真实库不一致 | 2026-04-18 |
| `docs/database_audit_report.md` | 数据层 | 审计结果更新 | 审计结论过期 | 2026-04-18 |
| `docs/scheduler_matrix_2026-04-06.md` | 调度系统 | 任务编排和 cron 调整 | 与 `job_registry` 不一致 | 2026-04-18 |
| `docs/repo_structure_rules.md` | 工程治理 | 目录治理策略调整 | 新增目录未纳入规则 | 2026-04-18 |
| `docs/project_final_state_projection_2026-04-15.md` | 产品规划 | 目标态更新 | 目标与当前主链脱节 | 2026-04-18 |
| `docs/uzi_skill_reuse_final_architecture_2026-04-18.md` | 架构设计 | 终局能力定义变更 | 能力约束过时 | 2026-04-18 |
| `docs/decision_productization_batches_A_to_C_2026-04-18.md` | 产品执行 | 批次策略/验收变更 | 批次边界不再可执行 | 2026-04-18 |
| `docs/current_project_gap_audit_2026-04-18.md` | 质量审计 | 缺口复盘更新 | 问题清单失真 | 2026-04-18 |
| `docs/document_lifecycle_rules_2026-04-18.md` | 工程治理 | 文档流程规则变更 | 规则无法约束回流 | 2026-04-18 |
| `docs/repo_cleanup_execution_report_2026-04-18.md` | 工程治理 | 仓库减负再执行 | 与实际清理状态不一致 | 2026-04-18 |
| `docs/consolidated_change_log_2026-04.md` | 工程治理 | 融合调整发生 | 无法追溯融合历史 | 2026-04-18 |

## B. Consolidated（融合入口）

- `docs/decision_productization_batches_A_to_C_2026-04-18.md`：当前产品化改造唯一执行入口（替代多份阶段计划）。
- `docs/current_project_gap_audit_2026-04-18.md`：当前不足项唯一审计入口（替代多份阶段性审计快照）。
- `docs/repo_cleanup_execution_report_2026-04-18.md`：仓库减负执行与资产去向唯一入口。
- `docs/consolidated_change_log_2026-04.md`：融合动作变更日志入口。

## C. Local Archive（本地归档，不入 Git）

- 归档根目录：`local_archive/`
- 文档归档路径：`local_archive/docs/`
- 其他归档路径：`local_archive/tmp/`、`local_archive/runtime/`、`local_archive/logs/`、`local_archive/reports/`
- 说明：此层仅本地可追溯，不再上传到 Git/GitHub。
