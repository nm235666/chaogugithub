情绪分 -
#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/home/zanbo/zanbotest"
TMP_FILE="$(mktemp)"
EXISTING_FILE="$(mktemp)"

crontab -l 2>/dev/null >"${EXISTING_FILE}" || true
cp "${EXISTING_FILE}" "${TMP_FILE}"

add_or_replace() {
  local marker="$1"
  local line="$2"
  grep -vF "$marker" "${TMP_FILE}" > "${TMP_FILE}.next" || true
  mv "${TMP_FILE}.next" "${TMP_FILE}"
  echo "$line" >> "${TMP_FILE}"
}

add_or_replace "run_news_fetch_once.sh" "*/5 * * * * /bin/bash ${BASE_DIR}/run_news_fetch_once.sh"
add_or_replace "run_cn_news_fetch_once.sh" "*/2 * * * * /bin/bash ${BASE_DIR}/run_cn_news_fetch_once.sh"
add_or_replace "run_news_stock_map_once.sh" "*/5 * * * * /bin/bash ${BASE_DIR}/run_news_stock_map_once.sh"
add_or_replace "watch_cn_news_eastmoney_10s.sh" "* * * * * /bin/bash ${BASE_DIR}/watch_cn_news_eastmoney_10s.sh"
add_or_replace "start_cn_news_eastmoney_10s.sh" "@reboot /bin/bash ${BASE_DIR}/start_cn_news_eastmoney_10s.sh"
add_or_replace "run_news_archive_once.sh" "30 3,15 * * * /bin/bash ${BASE_DIR}/run_news_archive_once.sh"
add_or_replace "run_news_daily_summary_once.sh" "30 3,15 * * * /bin/bash ${BASE_DIR}/run_news_daily_summary_once.sh"
add_or_replace "run_daily_postclose_update.sh" "30 7 * * * /bin/bash ${BASE_DIR}/run_daily_postclose_update.sh"
add_or_replace "run_chatroom_fetch_once.sh" "5 0 * * * /bin/bash ${BASE_DIR}/run_chatroom_fetch_once.sh"
add_or_replace "run_chatroom_tagging_safe_once.sh" "12 0 */3 * * /bin/bash ${BASE_DIR}/run_chatroom_tagging_safe_once.sh"
add_or_replace "run_monitored_chatlog_fetch_once.sh" "*/3 * * * * /bin/bash ${BASE_DIR}/run_monitored_chatlog_fetch_once.sh"
add_or_replace "run_monitored_chatlog_backfill_midnight.sh" "10 0 * * * /bin/bash ${BASE_DIR}/run_monitored_chatlog_backfill_midnight.sh"
add_or_replace "start_ws_realtime.sh" "@reboot /bin/bash ${BASE_DIR}/start_ws_realtime.sh"
add_or_replace "start_stream_news_worker.sh" "@reboot /bin/bash ${BASE_DIR}/start_stream_news_worker.sh"
add_or_replace "watch_realtime_services.sh" "* * * * * /bin/bash ${BASE_DIR}/watch_realtime_services.sh"
add_or_replace "run_stock_news_score_once.sh" "*/10 * * * * /bin/bash ${BASE_DIR}/run_stock_news_score_once.sh"
add_or_replace "run_stock_news_backfill_missing_once.sh" "55 * * * * /bin/bash ${BASE_DIR}/run_stock_news_backfill_missing_once.sh"
add_or_replace "run_news_sentiment_once.sh" "12 * * * * /bin/bash ${BASE_DIR}/run_news_sentiment_once.sh"
add_or_replace "run_macro_series_akshare_once.sh" "20 17 * * * /bin/bash ${BASE_DIR}/run_macro_series_akshare_once.sh"
add_or_replace "run_data_completion_nightly.sh" "0 17 * * * /bin/bash ${BASE_DIR}/run_data_completion_nightly.sh"
add_or_replace "run_fx_daily_akshare_once.sh" "35 7 * * * /bin/bash ${BASE_DIR}/run_fx_daily_akshare_once.sh"
add_or_replace "run_minline_backfill_recent.sh" "45 7 * * * /bin/bash ${BASE_DIR}/run_minline_backfill_recent.sh"
add_or_replace "run_minline_akshare_patch_once.sh" "5 8 * * * /bin/bash ${BASE_DIR}/run_minline_akshare_patch_once.sh"
add_or_replace "run_chatroom_analysis_pipeline_once.sh" "15 * * * * /bin/bash ${BASE_DIR}/run_chatroom_analysis_pipeline_once.sh"
add_or_replace "run_chatroom_sentiment_once.sh" "18 * * * * /bin/bash ${BASE_DIR}/run_chatroom_sentiment_once.sh"
add_or_replace "run_stock_news_expand_once.sh" "25 * * * * /bin/bash ${BASE_DIR}/run_stock_news_expand_once.sh"
add_or_replace "run_theme_hotspot_engine_once.sh" "32 * * * * /bin/bash ${BASE_DIR}/run_theme_hotspot_engine_once.sh"
add_or_replace "run_signal_state_machine_once.sh" "38 * * * * /bin/bash ${BASE_DIR}/run_signal_state_machine_once.sh"
add_or_replace "run_market_expectations_once.sh" "42 * * * * /bin/bash ${BASE_DIR}/run_market_expectations_once.sh"
add_or_replace "run_research_reports_once.sh" "30 7,15 * * * /bin/bash ${BASE_DIR}/run_research_reports_once.sh"
add_or_replace "run_minline_intraday_focus_once.sh" "*/10 1-3,5-7 * * 1-5 /bin/bash ${BASE_DIR}/run_minline_intraday_focus_once.sh"
add_or_replace "run_news_dedupe_once.sh" "20 16 * * * /bin/bash ${BASE_DIR}/run_news_dedupe_once.sh"
add_or_replace "run_db_health_check_once.sh" "40 16 * * * /bin/bash ${BASE_DIR}/run_db_health_check_once.sh"
add_or_replace "run_database_audit_once.sh" "50 16 * * * /bin/bash ${BASE_DIR}/run_database_audit_once.sh"
add_or_replace "run_investment_signal_tracker_once.sh" "35 * * * * /bin/bash ${BASE_DIR}/run_investment_signal_tracker_once.sh"
add_or_replace "run_logic_view_cache_once.sh" "55 17 * * * /bin/bash ${BASE_DIR}/run_logic_view_cache_once.sh"

awk '!seen[$0]++' "${TMP_FILE}" > "${TMP_FILE}.dedup"
mv "${TMP_FILE}.dedup" "${TMP_FILE}"
crontab "${TMP_FILE}"
python3 "${BASE_DIR}/job_orchestrator.py" sync >/tmp/job_orchestrator_sync.log 2>&1 || true

rm -f "${TMP_FILE}" "${EXISTING_FILE}"

echo "installed all cron jobs for PostgreSQL 主库:"
crontab -l
