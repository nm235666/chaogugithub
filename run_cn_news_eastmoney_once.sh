#!/usr/bin/env bash
set -euo pipefail

LOCK_FILE="/tmp/cn_eastmoney_fetch.lock"
LOG_FILE="/tmp/cn_eastmoney_fetch.log"
FETCH_CMD="python3 -u /home/zanbo/zanbotest/fetch_cn_news_eastmoney.py --limit 60 --page-size 50 --timeout 15"
SCORE_CMD="timeout 60s python3 -u /home/zanbo/zanbotest/llm_score_news.py --source cn_eastmoney_fastnews --limit 20 --retry 1 --sleep 0.05 --model GPT-5.4"
MAP_CMD="timeout 180s python3 -u /home/zanbo/zanbotest/map_news_items_to_stocks.py --source cn_eastmoney_fastnews --limit 300 --days 7"

cd /home/zanbo/zanbotest
. /home/zanbo/zanbotest/runtime_env.sh

if ! flock -n "$LOCK_FILE" -c "$FETCH_CMD || true; $SCORE_CMD; $MAP_CMD" >>"$LOG_FILE" 2>&1; then
  echo "[$(date -Iseconds)] skip: previous eastmoney news job still running (${APP_DB_LABEL})" >>"$LOG_FILE"
fi
