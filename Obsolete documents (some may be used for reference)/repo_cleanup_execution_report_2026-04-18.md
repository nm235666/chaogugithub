# 仓库减负执行报告（2026-04-18）

## 1. 执行摘要

本轮完成“全仓库减负 + 文档治理重构”：
- 文档层：主链收敛为 Active 集合，阶段性/实验性文档迁移到本地归档。
- 资产层：高噪音目录改为本地保留、Git 不再跟踪。
- 规则层：建立本地归档目录、忽略规则、防误提交流程。
- 实际变更规模（当前工作区统计）：
  - 停止跟踪/删除：`434` 项
  - 文档根层迁移归档：`43` 项
  - 新增治理文档与守卫脚本：`5+` 项

## 2. 资产去向

### 2.1 删除/停跟踪类（Git 不再保留）

- `apps/web/playwright-report/**`
- `apps/web/test-results/**`
- `tmp/**`（历史截图与扫描产物）
- `runtime/**`（运行态产物）
- `logs/**`（运行日志）
- `external/strategy`（本地同步数据源，不再纳入 Git）

代表性清单（完整清单见本次 git diff）：
- `apps/web/tmp_*.cjs` 一次性扫描脚本
- `tmp/layout-*`、`tmp/layout-scan*` 历史截图与汇总
- `runtime/quantaalpha_runtime.env`
- `logs/backend.out`

### 2.2 本地归档类（保留但不上传）

- `docs/archive/**`
- 过期阶段计划、阶段测试报告、阶段审计证据
- 实验提案与机会池文档
- dated metrics 快照

归档根路径：`local_archive/`

本轮迁移到 `local_archive/docs/` 的代表性文件：
- `docs/archive/**` 全量历史快照
- `docs/final_state_*` 阶段差距与进度文档
- `docs/p01_*`、`docs/p03_*`、`docs/p11_*` 阶段证据文档
- `docs/web_*report*`、`docs/QA_TEST_REPORT_2026-04-06.md`
- `docs/Opportunitiesforsomeideas/**` 实验提案集
- `docs/metrics/**` dated 指标快照
- 根目录历史报告：
  - `前端优化执行报告.md`
  - `前端美化建议报告.md`
  - `新闻评分积压问题分析.md`
  - `新浪接口.txt`

## 3. 文档融合结果

- 计划执行入口统一到：
  - `docs/decision_productization_batches_A_to_C_2026-04-18.md`
- 当前不足项审计统一到：
  - `docs/current_project_gap_audit_2026-04-18.md`
- 生命周期与治理规则统一到：
  - `docs/document_lifecycle_rules_2026-04-18.md`

## 4. 保留主链文档清单

以 `docs/DOCS_INDEX.md` 中 `Active` 为准。

本轮收敛后 `docs/` 根层保留（11 + 索引/治理文档）：
- `system_overview_cn.md`
- `command_line_reference.md`
- `database_dictionary.md`
- `database_audit_report.md`
- `scheduler_matrix_2026-04-06.md`
- `project_final_state_projection_2026-04-15.md`
- `uzi_skill_reuse_final_architecture_2026-04-18.md`
- `decision_productization_batches_A_to_C_2026-04-18.md`
- `current_project_gap_audit_2026-04-18.md`
- `repo_structure_rules.md`
- `DOCS_INDEX.md`
- `document_lifecycle_rules_2026-04-18.md`
- `repo_cleanup_execution_report_2026-04-18.md`

## 5. 防回流措施

- `.gitignore` 强化规则：本地归档、测试快照、运行态产物、外部同步目录。
- 本地 hook/守卫脚本：阻止归档与噪音目录进入提交。
- hooks 启用命令：
  - `git config core.hooksPath .githooks`
- 手工守卫检查：
  - `bash scripts/git_guard_no_archive_upload.sh`

## 6. 后续维护约定

- 每周一次文档巡检：验证 Active 文档与代码一致性。
- 每次大改后执行一次“归档清单更新”。
- 新增阶段性报告默认进入 `local_archive/docs/`，不进入 `docs/` 主链。

## 7. 体积对比（目录级）

执行前（采样）：
- `docs`: `1.3M`
- `apps/web/playwright-report`: `520K`
- `apps/web/test-results`: `8.0K`
- `tmp`: `284M`
- `runtime`: `2.6G`
- `logs`: `4.0K`

执行后（本地保留，Git 停跟踪）：
- `docs`: `208K`（主链收敛）
- `local_archive`: `1.2M`（文档与历史材料本地归档）
- 其余目录体积本地不变，但后续不再进入 Git 跟踪
