# 融合变更日志（2026-04）

## 2026-04-18

### 文档治理重构
- 主链文档重构为 `Active / Consolidated / Local Archive` 三层。
- 阶段性、实验性、dated 测试/证据文档迁移到 `local_archive/docs/`。
- `docs/DOCS_INDEX.md` 改为带 Owner/触发条件/失效条件/最后校验的治理索引。

### 仓库减负
- 停止跟踪前端测试报告、临时扫描脚本、运行态日志与大体积截图目录。
- `external/strategy` 从仓库跟踪改为“本地同步数据源目录”。

### 防回流机制
- 新增 `.githooks/pre-commit`、`.githooks/pre-push`。
- 新增 `scripts/git_guard_no_archive_upload.sh`。
- `.gitignore` 增加本地归档和高噪音目录强约束。
