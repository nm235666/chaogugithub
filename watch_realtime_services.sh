#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/zanbo/zanbotest"
LOG="/tmp/realtime_services_watchdog.log"
BACKEND_LOG="/tmp/stock_backend.log"
BACKEND_LLM2_LOG="/tmp/stock_backend_llm2.log"
BACKEND_MACRO_LOG="/tmp/stock_backend_macro.log"
BACKEND_MULTI_ROLE_LOG="/tmp/stock_backend_multi_role.log"

is_running() {
  local pattern="$1"
  pgrep -f "$pattern" >/dev/null 2>&1
}

ts() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

mkdir -p /tmp

backend_port_alive() {
  lsof -ti tcp:8002 >/dev/null 2>&1
}

backend_health_ok() {
  curl -fsS --max-time 4 "http://127.0.0.1:8002/api/health" >/dev/null 2>&1
}

restart_backend() {
  local pids
  pids="$(lsof -ti tcp:8002 || true)"
  if [[ -n "$pids" ]]; then
    kill $pids >/dev/null 2>&1 || true
    sleep 1
  fi
  nohup bash "$ROOT/start_backend.sh" >"$BACKEND_LOG" 2>&1 &
  echo "[$(ts)] restarted backend server on :8002" >>"$LOG"
}

backend_llm2_port_alive() {
  lsof -ti tcp:8004 >/dev/null 2>&1
}

backend_macro_port_alive() {
  lsof -ti tcp:8005 >/dev/null 2>&1
}

backend_multi_role_port_alive() {
  lsof -ti tcp:8006 >/dev/null 2>&1
}

backend_llm2_health_ok() {
  curl -fsS --max-time 4 "http://127.0.0.1:8004/api/health" >/dev/null 2>&1
}

backend_macro_health_ok() {
  curl -fsS --max-time 4 "http://127.0.0.1:8005/api/health" >/dev/null 2>&1
}

backend_multi_role_health_ok() {
  curl -fsS --max-time 4 "http://127.0.0.1:8006/api/health" >/dev/null 2>&1
}

restart_backend_llm2() {
  local pids
  pids="$(lsof -ti tcp:8004 || true)"
  if [[ -n "$pids" ]]; then
    kill $pids >/dev/null 2>&1 || true
    sleep 1
  fi
  nohup bash "$ROOT/start_backend_llm2.sh" >"$BACKEND_LLM2_LOG" 2>&1 &
  echo "[$(ts)] restarted backend llm2 on :8004" >>"$LOG"
}

restart_backend_macro() {
  local pids
  pids="$(lsof -ti tcp:8005 || true)"
  if [[ -n "$pids" ]]; then
    kill $pids >/dev/null 2>&1 || true
    sleep 1
  fi
  nohup bash "$ROOT/start_backend_macro.sh" >"$BACKEND_MACRO_LOG" 2>&1 &
  echo "[$(ts)] restarted backend macro on :8005" >>"$LOG"
}

restart_backend_multi_role() {
  local pids
  pids="$(lsof -ti tcp:8006 || true)"
  if [[ -n "$pids" ]]; then
    kill $pids >/dev/null 2>&1 || true
    sleep 1
  fi
  nohup bash "$ROOT/start_backend_multi_role.sh" >"$BACKEND_MULTI_ROLE_LOG" 2>&1 &
  echo "[$(ts)] restarted backend multi-role on :8006" >>"$LOG"
}

if ! is_running "ws_realtime_server.py"; then
  nohup python3 "$ROOT/ws_realtime_server.py" --host 0.0.0.0 --port 8010 >/tmp/ws_realtime.log 2>&1 &
  echo "[$(ts)] restarted ws_realtime_server.py" >>"$LOG"
fi

if ! is_running "stream_news_worker.py"; then
  nohup python3 "$ROOT/stream_news_worker.py" >/tmp/stream_news_worker.log 2>&1 &
  echo "[$(ts)] restarted stream_news_worker.py" >>"$LOG"
fi

if ! backend_port_alive || ! backend_health_ok; then
  restart_backend
fi

if ! backend_llm2_port_alive || ! backend_llm2_health_ok; then
  restart_backend_llm2
fi

if ! backend_macro_port_alive || ! backend_macro_health_ok; then
  restart_backend_macro
fi

if ! backend_multi_role_port_alive || ! backend_multi_role_health_ok; then
  restart_backend_multi_role
fi
