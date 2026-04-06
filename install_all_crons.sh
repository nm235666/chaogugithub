#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/home/zanbo/zanbotest"
TMP_EXISTING="$(mktemp)"
TMP_FILTERED="$(mktemp)"
TMP_MANAGED="$(mktemp)"
TMP_FINAL="$(mktemp)"

cleanup() {
  rm -f "${TMP_EXISTING}" "${TMP_FILTERED}" "${TMP_MANAGED}" "${TMP_FINAL}"
}
trap cleanup EXIT

cd "${BASE_DIR}"

# 先同步任务定义，确保 job_definitions 是最新真源。
python3 "${BASE_DIR}/job_orchestrator.py" sync >/tmp/job_orchestrator_sync.log 2>&1 || true

crontab -l 2>/dev/null >"${TMP_EXISTING}" || true

# 清理旧的“业务脚本直跑”与旧 managed job 行，保留非业务自定义行与基础守护行。
python3 - <<'PY' "${TMP_EXISTING}" "${TMP_FILTERED}"
from pathlib import Path
import re
import sys

src = Path(sys.argv[1])
dst = Path(sys.argv[2])
legacy_run_pattern = re.compile(r"/home/zanbo/zanbotest/run_[^ ]+\.sh")

out = []
for raw in src.read_text(encoding="utf-8", errors="ignore").splitlines():
    line = raw.rstrip()
    stripped = line.strip()
    if not stripped:
        out.append(line)
        continue
    if stripped.startswith("# BEGIN_ZANBO_JOBS") or stripped.startswith("# END_ZANBO_JOBS"):
        continue
    if "# zanbo_job:" in line:
        continue
    if legacy_run_pattern.search(line):
        # 老模式：业务任务直接 run_*.sh（含 run_job_always/if_trade_day）
        continue
    out.append(line)

dst.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")
PY

python3 "${BASE_DIR}/scripts/scheduler/render_crontab.py" >"${TMP_MANAGED}"

cat "${TMP_FILTERED}" >"${TMP_FINAL}"
printf "\n" >>"${TMP_FINAL}"
cat "${TMP_MANAGED}" >>"${TMP_FINAL}"

awk '!seen[$0]++' "${TMP_FINAL}" > "${TMP_FINAL}.dedup"
mv "${TMP_FINAL}.dedup" "${TMP_FINAL}"

crontab "${TMP_FINAL}"

echo "installed cron jobs from job_definitions(enabled=1):"
crontab -l

echo
echo "cron consistency report:"
python3 "${BASE_DIR}/scripts/scheduler/check_cron_sync.py" || true
