#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-/home/zanbo/zanbotest}"
BACKEND_PORT="${BACKEND_PORT:-8002}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
BACKEND_LOG="${BACKEND_LOG:-/tmp/zanbotest_backend_${BACKEND_PORT}.log}"
FRONTEND_LOG="${FRONTEND_LOG:-/tmp/zanbotest_frontend_${FRONTEND_PORT}.log}"
PID_FILE="${PID_FILE:-/tmp/zanbotest_frontend_backend.pids}"

if [[ ! -d "$ROOT" ]]; then
  echo "Project root not found: $ROOT" >&2
  exit 1
fi

stop_port() {
  local port="$1"
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${port}/tcp" >/dev/null 2>&1 || true
    return
  fi
  echo "fuser is not installed; please stop processes on port ${port} manually if restart fails." >&2
}

echo "Stopping existing services on ${BACKEND_PORT} and ${FRONTEND_PORT}..."
stop_port "$BACKEND_PORT"
stop_port "$FRONTEND_PORT"
sleep 1

echo "Starting backend on port ${BACKEND_PORT}..."
(
  cd "$ROOT"
  if [[ -f "$ROOT/runtime_env.sh" ]]; then
    # shellcheck disable=SC1091
    . "$ROOT/runtime_env.sh"
  fi
  exec env PORT="$BACKEND_PORT" python3 backend/server.py
) >"$BACKEND_LOG" 2>&1 &
BACKEND_PID=$!

echo "Starting frontend on port ${FRONTEND_PORT}..."
(
  cd "$ROOT/apps/web"
  exec npm run dev -- --host 0.0.0.0 --port "$FRONTEND_PORT"
) >"$FRONTEND_LOG" 2>&1 &
FRONTEND_PID=$!

cat >"$PID_FILE" <<EOF
BACKEND_PID=$BACKEND_PID
FRONTEND_PID=$FRONTEND_PID
BACKEND_PORT=$BACKEND_PORT
FRONTEND_PORT=$FRONTEND_PORT
BACKEND_LOG=$BACKEND_LOG
FRONTEND_LOG=$FRONTEND_LOG
EOF

sleep 2

if ! kill -0 "$BACKEND_PID" >/dev/null 2>&1; then
  echo "Backend failed to start. Log: $BACKEND_LOG" >&2
  exit 1
fi

if ! kill -0 "$FRONTEND_PID" >/dev/null 2>&1; then
  echo "Frontend failed to start. Log: $FRONTEND_LOG" >&2
  exit 1
fi

IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
IP="${IP:-127.0.0.1}"

echo "Backend PID:  $BACKEND_PID"
echo "Frontend PID: $FRONTEND_PID"
echo "Backend log:  $BACKEND_LOG"
echo "Frontend log: $FRONTEND_LOG"
echo "PID file:     $PID_FILE"
echo "Frontend:     http://${IP}:${FRONTEND_PORT}/"
echo "Backend API:  http://${IP}:${BACKEND_PORT}/api/health"
