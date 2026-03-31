#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/zanbo/zanbotest"

cd "$ROOT"
. "$ROOT/runtime_env.sh"
PORT=8002 python3 backend/server.py > /tmp/stock_backend.log 2>&1 &
BACKEND_PID=$!

if [[ ! -f "$ROOT/apps/web/dist/index.html" ]]; then
  (
    cd "$ROOT/apps/web"
    if [[ ! -d node_modules ]]; then
      npm install
    fi
    npm run build
  )
fi

python3 "$ROOT/serve_spa.py" --host 0.0.0.0 --port 8080 --root "$ROOT/apps/web/dist" > /tmp/stock_frontend.log 2>&1 &
FRONTEND_PID=$!

python3 ws_realtime_server.py --host 0.0.0.0 --port 8010 > /tmp/ws_realtime.log 2>&1 &
WS_PID=$!

python3 stream_news_worker.py > /tmp/stream_news_worker.log 2>&1 &
WORKER_PID=$!

IP=$(hostname -I | awk '{print $1}')

echo "Backend PID: $BACKEND_PID"
echo "Frontend PID: $FRONTEND_PID"
echo "WebSocket PID: $WS_PID"
echo "Stream Worker PID: $WORKER_PID"
echo "后端地址: http://$IP:8002/api/health"
echo "前端地址(新版 Vue): http://$IP:8080"
echo "实时WS地址: ws://$IP:8010/ws/realtime"
echo "停止服务: kill $BACKEND_PID $FRONTEND_PID $WS_PID $WORKER_PID"

wait
