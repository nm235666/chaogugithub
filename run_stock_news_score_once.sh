#!/usr/bin/env bash
set -euo pipefail

cd /home/zanbo/zanbotest
. /home/zanbo/zanbotest/runtime_env.sh

python3 -u /home/zanbo/zanbotest/job_orchestrator.py run stock_news_score_refresh --trigger-mode cron
