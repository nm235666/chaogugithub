#!/usr/bin/env bash
set -euo pipefail

blocked_regex='^(local_archive/|apps/web/playwright-report/|apps/web/test-results/|runtime/|tmp/|logs/|external/strategy/)'

staged_lines="$(git diff --cached --name-status || true)"
if [[ -z "${staged_lines}" ]]; then
  exit 0
fi

# Allow deletions in blocked paths (cleanup commits), block only add/modify/rename/copy.
blocked="$(
  printf '%s\n' "${staged_lines}" \
    | awk '$1 != "D" && $1 != "D\t" {print $0}' \
    | awk '{print $NF}' \
    | rg -n "${blocked_regex}" -N || true
)"
if [[ -n "${blocked}" ]]; then
  echo "[guard] 检测到禁止提交路径："
  echo "${blocked}"
  echo "[guard] 请将上述文件迁移到本地归档层或取消暂存后再提交。"
  exit 1
fi

exit 0
