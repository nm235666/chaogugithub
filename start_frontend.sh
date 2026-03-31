#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/zanbo/zanbotest"
if [[ ! -f "$ROOT/apps/web/dist/index.html" ]]; then
  cd "$ROOT/apps/web"
  if [[ ! -d node_modules ]]; then
    npm install
  fi
  npm run build
fi
cd "$ROOT"
python3 "$ROOT/serve_spa.py" --host 0.0.0.0 --port 8080 --root "$ROOT/apps/web/dist"
