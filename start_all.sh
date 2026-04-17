#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/zanbo/zanbotest"

cd "$ROOT"
. "$ROOT/runtime_env.sh"
PORT=8002 python3 backend/server.py > /tmp/stock_backend.log 2>&1 &
BACKEND_PID=$!

(
  cd "$ROOT/apps/web"
  if [[ ! -d node_modules ]]; then
    npm install
  fi
  npm run build
)

python3 ws_realtime_server.py --host 0.0.0.0 --port 8010 > /tmp/ws_realtime.log 2>&1 &
WS_PID=$!

python3 stream_news_worker.py > /tmp/stream_news_worker.log 2>&1 &
WORKER_PID=$!

python3 jobs/run_chief_roundtable_worker.py > /tmp/chief_roundtable_worker.log 2>&1 &
ROUNDTABLE_WORKER_PID=$!

python3 jobs/run_quantaalpha_worker.py > /tmp/quantaalpha_worker.log 2>&1 &
QUANT_WORKER_PID=$!

IP=$(hostname -I | awk '{print $1}')

echo "Backend PID: $BACKEND_PID"
echo "WebSocket PID: $WS_PID"
echo "Stream Worker PID: $WORKER_PID"
echo "Chief Roundtable Worker PID: $ROUNDTABLE_WORKER_PID"
echo "QuantaAlpha Worker PID: $QUANT_WORKER_PID"
echo "统一入口: http://$IP:8002/"
echo "API 健康检查: http://$IP:8002/api/health"
echo "实时WS地址: ws://$IP:8010/ws/realtime"
echo "停止服务: kill $BACKEND_PID $WS_PID $WORKER_PID $ROUNDTABLE_WORKER_PID $QUANT_WORKER_PID"

wait
