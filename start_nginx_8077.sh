#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/home/zanbo/zanbotest"
RUNTIME_DIR="${BASE_DIR}/nginx_runtime"
CONF_FILE="${BASE_DIR}/nginx_8077.conf"
PID_FILE="${RUNTIME_DIR}/nginx.pid"

mkdir -p "${RUNTIME_DIR}"

if [[ -f "${PID_FILE}" ]]; then
  old_pid="$(cat "${PID_FILE}" || true)"
  if [[ -n "${old_pid}" ]] && kill -0 "${old_pid}" 2>/dev/null; then
    kill "${old_pid}" || true
    sleep 1
  fi
fi

nginx -p "${RUNTIME_DIR}" -c "${CONF_FILE}" >/tmp/nginx_8077.log 2>&1
sleep 0.5
echo "Nginx gateway started on :8077 (log: /tmp/nginx_8077.log)"
