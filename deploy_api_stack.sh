#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/home/zanbo/zanbotest"
BUILD_ID="${1:-deploy-$(date -u +%Y%m%dT%H%M%SZ)}"

declare -A START_SCRIPT
START_SCRIPT[8002]="${ROOT_DIR}/start_backend.sh"
START_SCRIPT[8004]="${ROOT_DIR}/start_backend_llm2.sh"
START_SCRIPT[8005]="${ROOT_DIR}/start_backend_macro.sh"
START_SCRIPT[8006]="${ROOT_DIR}/start_backend_multi_role.sh"

echo "Deploy build_id=${BUILD_ID}"

for port in 8002 8004 8005 8006; do
  pid="$(lsof -iTCP:${port} -sTCP:LISTEN -t 2>/dev/null || true)"
  if [[ -n "${pid}" ]]; then
    echo "Stopping :${port} pid=${pid}"
    kill "${pid}" || true
    sleep 1
  fi
done

for port in 8002 8004 8005 8006; do
  script="${START_SCRIPT[$port]}"
  if [[ ! -x "${script}" ]]; then
    echo "ERROR: start script not executable: ${script}" >&2
    exit 1
  fi
  log="/tmp/backend_${port}.log"
  echo "Starting :${port} via $(basename "${script}") -> ${log}"
  BACKEND_BUILD_ID="${BUILD_ID}" nohup "${script}" >"${log}" 2>&1 &
  sleep 1
done

echo "Probing health/build_id..."
for port in 8002 8004 8005 8006; do
  ok=0
  for _ in $(seq 1 30); do
    body="$(curl -sS --max-time 2 "http://127.0.0.1:${port}/api/health" || true)"
    if [[ -n "${body}" ]]; then
      parsed="$(python3 - <<'PY' "${body}" "${BUILD_ID}"
import json, sys
raw = sys.argv[1]
expect = sys.argv[2]
try:
    obj = json.loads(raw)
except Exception:
    print("bad-json")
    raise SystemExit(0)
build_id = str(obj.get("build_id") or "")
ok = bool(obj.get("ok"))
print(f"ok={ok} build_id={build_id} match={build_id==expect}")
PY
)"
      echo "  :${port} ${parsed}"
      if [[ "${parsed}" == *"ok=True"* && "${parsed}" == *"match=True"* ]]; then
        ok=1
        break
      fi
    fi
    sleep 1
  done
  if [[ "${ok}" -ne 1 ]]; then
    echo "ERROR: probe failed on :${port} (expected build_id=${BUILD_ID})" >&2
    exit 1
  fi
done

echo "Deploy success. build_id=${BUILD_ID}"
