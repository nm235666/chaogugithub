#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <job_key>" >&2
  exit 2
fi

JOB_KEY="$1"
BASE_DIR="/home/zanbo/zanbotest"

cd "${BASE_DIR}"
. "${BASE_DIR}/runtime_env.sh"

IS_OPEN="$(
python3 - <<'PY'
from datetime import datetime, timedelta, timezone

open_today = False
cn_now = datetime.now(timezone.utc) + timedelta(hours=8)
today = cn_now.strftime("%Y%m%d")

try:
    from market_calendar import recent_open_trade_dates

    dates = recent_open_trade_dates(token="", count=1, end_date=today)
    open_today = bool(dates and str(dates[-1]).strip() == today)
except Exception:
    # 无法访问交易日历时兜底为工作日，避免把交易日误判成休市完全停跑。
    open_today = cn_now.weekday() < 5

print("1" if open_today else "0")
PY
)"

if [[ "${IS_OPEN}" == "1" ]]; then
  python3 -u "${BASE_DIR}/job_orchestrator.py" run "${JOB_KEY}" --trigger-mode cron
else
  python3 -u "${BASE_DIR}/job_orchestrator.py" skip "${JOB_KEY}" --trigger-mode cron-gate --reason skipped_non_trading_day
fi
