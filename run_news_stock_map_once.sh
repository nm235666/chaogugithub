#!/usr/bin/env bash
set -euo pipefail

cd /home/zanbo/zanbotest
. /home/zanbo/zanbotest/runtime_env.sh

python3 -u /home/zanbo/zanbotest/job_orchestrator.py run news_stock_map_refresh --trigger-mode cron
