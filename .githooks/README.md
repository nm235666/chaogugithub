# Git Hooks（本仓库）

启用方式：

```bash
git config core.hooksPath .githooks
```

当前 hooks：

- `pre-commit`：执行 `scripts/git_guard_no_archive_upload.sh`
- `pre-push`：执行 `scripts/git_guard_no_archive_upload.sh`

目的：阻止本地归档层、测试快照、运行态噪音目录被再次提交。
