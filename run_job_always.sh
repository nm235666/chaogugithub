#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <job_key>" >&2
  exit 2
fi

JOB_KEY="$1"
BASE_DIR="/home/zanbo/zanbotest"

cd "${BASE_DIR}"
. "${BASE_DIR}/runtime_env.sh"

python3 -u "${BASE_DIR}/job_orchestrator.py" run "${JOB_KEY}" --trigger-mode cron
