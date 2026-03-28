#!/usr/bin/env bash
set -euo pipefail

cd /home/zanbo/zanbotest
. /home/zanbo/zanbotest/runtime_env.sh

LOG_FILE="/tmp/data_completion_once.log"
LOCK_FILE="/tmp/data_completion_once.lock"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -Iseconds)] another data completion task is running, skip" >> "$LOG_FILE"
  exit 0
fi

TUSHARE_TOKEN_VALUE="${TUSHARE_TOKEN:-}"
if [[ -z "${TUSHARE_TOKEN_VALUE}" ]]; then
  echo "[$(date -Iseconds)] WARN: TUSHARE_TOKEN is empty, will run with script defaults" >> "$LOG_FILE"
fi

run_step() {
  local name="$1"
  shift
  echo "[$(date -Iseconds)] START $name" >> "$LOG_FILE"
  if "$@" >> "$LOG_FILE" 2>&1; then
    echo "[$(date -Iseconds)] OK    $name" >> "$LOG_FILE"
  else
    echo "[$(date -Iseconds)] FAIL  $name" >> "$LOG_FILE"
  fi
}

echo "[$(date -Iseconds)] ===== data completion begin (${APP_DB_LABEL}) =====" >> "$LOG_FILE"

run_step "governance_backfill" \
  python3 backfill_company_governance.py \
    --token "${TUSHARE_TOKEN_VALUE}" \
    --pause 0.02 \
    --retry 2

run_step "stock_events_backfill" \
  python3 backfill_stock_events.py \
    --token "${TUSHARE_TOKEN_VALUE}" \
    --rows-per-source 120 \
    --pause 0.02 \
    --retry 2

run_step "capital_flow_stock_365d" \
  python3 backfill_capital_flow_stock.py \
    --token "${TUSHARE_TOKEN_VALUE}" \
    --lookback-days 365 \
    --pause 0.01 \
    --all-status

run_step "capital_flow_stock_bj_akshare_missing" \
  python3 backfill_capital_flow_stock_akshare.py \
    --only-bj \
    --missing-only \
    --pause 0.05

run_step "capital_flow_market_365d" \
  python3 backfill_capital_flow_market.py \
    --token "${TUSHARE_TOKEN_VALUE}" \
    --lookback-days 365 \
    --pause 0.01

# 分钟线补最近两天，避免单日失败导致断层
TRADE_DATE_TODAY="$(TZ=Asia/Shanghai date +%Y%m%d)"
TRADE_DATE_PREV="$(TZ=Asia/Shanghai date -d '1 day' +%Y%m%d)"

run_step "minline_backfill_prev_day" \
  python3 fetch_sina_minline_all_listed.py \
    --trade-date "${TRADE_DATE_PREV}" \
    --workers 6 \
    --min-workers 2 \
    --max-workers 10 \
    --retry 2 \
    --batch-size 300 \
    --max-rounds 3 \
    --max-fail-per-stock 5 \
    --stagnation-rounds 2

run_step "minline_backfill_today" \
  python3 fetch_sina_minline_all_listed.py \
    --trade-date "${TRADE_DATE_TODAY}" \
    --workers 6 \
    --min-workers 2 \
    --max-workers 10 \
    --retry 2 \
    --batch-size 300 \
    --max-rounds 3 \
    --max-fail-per-stock 5 \
    --stagnation-rounds 2

run_step "stock_scores_refresh" \
  python3 backfill_stock_scores_daily.py --truncate-date

run_step "news_archive_and_index" \
  python3 optimize_and_archive_news.py --retain-days 180 --batch-size 5000 --max-batches 10

run_step "loop_score_unscored_news" \
  python3 loop_score_unscored_news.py --batch-limit 50 --max-rounds 20 --sleep-seconds 2

echo "[$(date -Iseconds)] ===== data completion end (${APP_DB_LABEL}) =====" >> "$LOG_FILE"
