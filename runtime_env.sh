#!/usr/bin/env bash
set -euo pipefail

export USE_POSTGRES="${USE_POSTGRES:-1}"
export DATABASE_URL="${DATABASE_URL:-postgresql://zanbo@/stockapp}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
export APP_DB_LABEL="${APP_DB_LABEL:-PostgreSQL 主库}"
export TUSHARE_TOKEN="${TUSHARE_TOKEN:-}"
export BACKEND_ADMIN_TOKEN="${BACKEND_ADMIN_TOKEN:-}"
export BACKEND_ALLOWED_ORIGINS="${BACKEND_ALLOWED_ORIGINS:-http://127.0.0.1:8077,http://localhost:8077,http://127.0.0.1:8080,http://localhost:8080,http://127.0.0.1:5173,http://localhost:5173,http://127.0.0.1:4173,http://localhost:4173}"
