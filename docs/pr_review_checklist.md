# 改造期 PR 评审清单

## 必查项
- 新增抓取是否落在 `collectors/*`
- 新增任务入口是否落在 `jobs/run_*_job.py`
- 是否新增了根目录业务脚本（若有需拒绝）
- 是否保持 API 路径兼容
- 是否优先使用 `analysis_markdown` 作为主字段

## 回归项
- `python3 -m unittest` 相关测试通过
- `cd apps/web && npm run build` 通过
- 相关 job `dry-run` 与 `--describe` 通过

## 发布项
- 是否更新迁移文档与回滚说明
- 是否记录兼容窗口与退场时间点
