#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/home/zanbo/zanbotest"
LOCK_FILE="/tmp/un_news_score_backlog_parallel.lock"
LOG_FILE="/tmp/un_news_score_backlog_parallel.log"

MODE="all"                 # all | intl | cn
MODEL="GPT-5.4"            # 按你的要求：并发模式仅用 GPT
PARALLEL=6                 # 并发 source 数
LIMIT_PER_SOURCE=300       # 每个 source 每轮最多处理条数
RETRY=1
SLEEP=0.02
ROUND_SLEEP=2
MAX_ROUNDS=0               # 0=不限，直到清空
STOP_ON_ERROR=0

usage() {
  cat <<'EOF'
用法:
  bash un_news_score_backlog_parallel.sh [选项]

选项:
  --mode <all|intl|cn>         处理范围，默认 all
  --model <name>               模型名，默认 GPT-5.4
  --parallel <n>               并发数，默认 6
  --limit-per-source <n>       每个 source 每轮条数，默认 300
  --retry <n>                  llm_score_news 重试次数，默认 1
  --sleep <sec>                llm_score_news 单条间隔，默认 0.02
  --round-sleep <sec>          轮次间隔，默认 2
  --max-rounds <n>             最大轮数，0=不限
  --stop-on-error              任一 source 失败立即退出
  -h, --help                   显示帮助

示例:
  bash un_news_score_backlog_parallel.sh --mode all --parallel 8 --limit-per-source 400
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="${2:-}"; shift 2 ;;
    --model) MODEL="${2:-}"; shift 2 ;;
    --parallel) PARALLEL="${2:-}"; shift 2 ;;
    --limit-per-source) LIMIT_PER_SOURCE="${2:-}"; shift 2 ;;
    --retry) RETRY="${2:-}"; shift 2 ;;
    --sleep) SLEEP="${2:-}"; shift 2 ;;
    --round-sleep) ROUND_SLEEP="${2:-}"; shift 2 ;;
    --max-rounds) MAX_ROUNDS="${2:-}"; shift 2 ;;
    --stop-on-error) STOP_ON_ERROR=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数: $1"; usage; exit 1 ;;
  esac
done

if [[ "$MODE" != "all" && "$MODE" != "intl" && "$MODE" != "cn" ]]; then
  echo "错误: --mode 仅支持 all/intl/cn"
  exit 1
fi

if [[ "$PARALLEL" -lt 1 ]]; then
  PARALLEL=1
fi

cd "$BASE_DIR"
. "$BASE_DIR/runtime_env.sh"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -Iseconds)] 已有同名任务运行中，跳过。" | tee -a "$LOG_FILE"
  exit 0
fi

count_unscored() {
  local mode="$1"
  python3 - "$mode" <<'PY'
import sys
import db_compat as sqlite3

mode = sys.argv[1]
where = "(llm_system_score IS NULL OR llm_finance_impact_score IS NULL OR llm_finance_importance IS NULL)"
if mode == "cn":
    where += " AND source LIKE 'cn_%'"
elif mode == "intl":
    where += " AND (source IS NULL OR source NOT LIKE 'cn_%')"

conn = sqlite3.connect("")
try:
    row = conn.execute(f"SELECT COUNT(*) FROM news_feed_items WHERE {where}").fetchone()
    print(int(row[0] if row else 0))
finally:
    conn.close()
PY
}

list_sources() {
  local mode="$1"
  python3 - "$mode" <<'PY'
import sys
import db_compat as sqlite3

mode = sys.argv[1]
where = "(llm_system_score IS NULL OR llm_finance_impact_score IS NULL OR llm_finance_importance IS NULL)"
if mode == "cn":
    where += " AND source LIKE 'cn_%'"
elif mode == "intl":
    where += " AND (source IS NULL OR source NOT LIKE 'cn_%')"

sql = f"""
SELECT DISTINCT source
FROM news_feed_items
WHERE {where}
  AND source IS NOT NULL
  AND source <> ''
ORDER BY source
"""
conn = sqlite3.connect("")
try:
    rows = conn.execute(sql).fetchall()
    for r in rows:
        s = str(r[0] or "").strip()
        if s:
            print(s)
finally:
    conn.close()
PY
}

score_one_source() {
  local src="$1"
  local status_file="$2"
  local cmd=(
    python3 -u "$BASE_DIR/llm_score_news.py"
    --model "$MODEL"
    --source "$src"
    --limit "$LIMIT_PER_SOURCE"
    --retry "$RETRY"
    --sleep "$SLEEP"
  )

  echo "[$(date -Iseconds)] [source=$src] 开始评分" | tee -a "$LOG_FILE"
  if "${cmd[@]}" >>"$LOG_FILE" 2>&1; then
    echo "${src}|0" >>"$status_file"
    echo "[$(date -Iseconds)] [source=$src] 完成" | tee -a "$LOG_FILE"
  else
    local ec=$?
    echo "${src}|${ec}" >>"$status_file"
    echo "[$(date -Iseconds)] [source=$src] 失败 exit=${ec}" | tee -a "$LOG_FILE"
  fi
}

round=0
echo "[$(date -Iseconds)] ===== un_news_score_backlog_parallel start mode=${MODE} model=${MODEL} parallel=${PARALLEL} =====" | tee -a "$LOG_FILE"

while true; do
  round=$((round + 1))
  cn_remaining="$(count_unscored cn)"
  intl_remaining="$(count_unscored intl)"

  case "$MODE" in
    all) remaining=$((cn_remaining + intl_remaining)) ;;
    cn) remaining="$cn_remaining" ;;
    intl) remaining="$intl_remaining" ;;
  esac

  echo "[$(date -Iseconds)] [round ${round}] remaining total=${remaining}, cn=${cn_remaining}, intl=${intl_remaining}" | tee -a "$LOG_FILE"

  if [[ "$remaining" -le 0 ]]; then
    echo "[$(date -Iseconds)] 未评分新闻已清空，任务结束。" | tee -a "$LOG_FILE"
    break
  fi

  if [[ "$MAX_ROUNDS" -gt 0 && "$round" -gt "$MAX_ROUNDS" ]]; then
    echo "[$(date -Iseconds)] 达到 max-rounds=${MAX_ROUNDS}，任务结束。" | tee -a "$LOG_FILE"
    break
  fi

  mapfile -t sources < <(
    {
      if [[ "$MODE" == "all" || "$MODE" == "cn" ]]; then
        list_sources cn
      fi
      if [[ "$MODE" == "all" || "$MODE" == "intl" ]]; then
        list_sources intl
      fi
    } | awk 'NF' | sort -u
  )

  if [[ "${#sources[@]}" -eq 0 ]]; then
    echo "[$(date -Iseconds)] 本轮无可处理 source，等待下一轮。" | tee -a "$LOG_FILE"
    sleep "$ROUND_SLEEP"
    continue
  fi

  status_file="$(mktemp /tmp/un_news_score_status.XXXXXX)"
  trap 'rm -f "$status_file"' EXIT

  for src in "${sources[@]}"; do
    while [[ "$(jobs -pr | wc -l)" -ge "$PARALLEL" ]]; do
      sleep 0.2
    done
    score_one_source "$src" "$status_file" &
  done
  wait

  fail_count="$(awk -F'|' '$2 != 0 {c++} END{print c+0}' "$status_file")"
  if [[ "$fail_count" -gt 0 ]]; then
    echo "[$(date -Iseconds)] [round ${round}] 失败 source 数: ${fail_count}" | tee -a "$LOG_FILE"
    if [[ "$STOP_ON_ERROR" -eq 1 ]]; then
      echo "[$(date -Iseconds)] stop-on-error 已启用，提前退出。" | tee -a "$LOG_FILE"
      rm -f "$status_file"
      trap - EXIT
      exit 1
    fi
  fi

  rm -f "$status_file"
  trap - EXIT
  sleep "$ROUND_SLEEP"
done

echo "[$(date -Iseconds)] ===== un_news_score_backlog_parallel done =====" | tee -a "$LOG_FILE"
