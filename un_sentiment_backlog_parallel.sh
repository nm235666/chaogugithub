#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/home/zanbo/zanbotest"
LOCK_FILE="/tmp/un_sentiment_backlog_parallel.lock"
LOG_FILE="/tmp/un_sentiment_backlog_parallel.log"

MODE="all"                 # all | intl | cn
MODEL="auto"               # auto / GPT-5.4 / kimi-k2.5 / deepseek-chat
PARALLEL=4                 # 并发实例数
LIMIT_PER_INSTANCE=200     # 每个实例每轮处理条数
RETRY=1
SLEEP=0.05
ROUND_SLEEP=3
MAX_ROUNDS=0               # 0=不限
STOP_ON_ERROR=0

usage() {
  cat <<'EOF'
用法:
  bash un_sentiment_backlog_parallel.sh [选项]

选项:
  --mode <all|intl|cn>         处理范围，默认 all
  --model <name>               模型名，默认 auto
  --parallel <n>               并发数，默认 4
  --limit-per-instance <n>     每实例每轮条数，默认 200
  --retry <n>                  重试次数，默认 1
  --sleep <sec>                单条间隔，默认 0.05
  --round-sleep <sec>          轮次间隔，默认 3
  --max-rounds <n>             最大轮数，0=不限
  --stop-on-error              任一实例失败立即退出
  -h, --help                   显示帮助

示例:
  bash un_sentiment_backlog_parallel.sh --mode intl --parallel 6 --limit-per-instance 300
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="${2:-}"; shift 2 ;;
    --model) MODEL="${2:-}"; shift 2 ;;
    --parallel) PARALLEL="${2:-}"; shift 2 ;;
    --limit-per-instance) LIMIT_PER_INSTANCE="${2:-}"; shift 2 ;;
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
where = "(llm_sentiment_score IS NULL OR COALESCE(llm_sentiment_label,'')='')"
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

score_instance() {
  local instance_id="$1"
  local status_file="$2"
  local target_flag=""
  
  case "$MODE" in
    intl) target_flag="--target news" ;;
    cn) target_flag="--target news" ;;
    all) target_flag="--target news" ;;
  esac
  
  local cmd=(
    python3 -u "$BASE_DIR/llm_score_sentiment.py"
    $target_flag
    --model "$MODEL"
    --limit "$LIMIT_PER_INSTANCE"
    --retry "$RETRY"
    --sleep "$SLEEP"
  )

  echo "[$(date -Iseconds)] [instance=$instance_id] 开始情绪分析" | tee -a "$LOG_FILE"
  if "${cmd[@]}" >>"$LOG_FILE" 2>&1; then
    echo "${instance_id}|0" >>"$status_file"
    echo "[$(date -Iseconds)] [instance=$instance_id] 完成" | tee -a "$LOG_FILE"
  else
    local ec=$?
    echo "${instance_id}|${ec}" >>"$status_file"
    echo "[$(date -Iseconds)] [instance=$instance_id] 失败 exit=${ec}" | tee -a "$LOG_FILE"
  fi
}

round=0
echo "[$(date -Iseconds)] ===== un_sentiment_backlog_parallel start mode=${MODE} model=${MODEL} parallel=${PARALLEL} =====" | tee -a "$LOG_FILE"

while true; do
  round=$((round + 1))
  remaining=$(count_unscored "$MODE")

  echo "[$(date -Iseconds)] [round ${round}] remaining=${remaining}" | tee -a "$LOG_FILE"

  if [[ "$remaining" -le 0 ]]; then
    echo "[$(date -Iseconds)] 情绪分析缺口已清空，任务结束。" | tee -a "$LOG_FILE"
    break
  fi

  if [[ "$MAX_ROUNDS" -gt 0 && "$round" -gt "$MAX_ROUNDS" ]]; then
    echo "[$(date -Iseconds)] 达到 max-rounds=${MAX_ROUNDS}，任务结束。" | tee -a "$LOG_FILE"
    break
  fi

  status_file="$(mktemp /tmp/un_sentiment_status.XXXXXX)"
  trap 'rm -f "$status_file"' EXIT

  # 启动并行实例
  for i in $(seq 1 "$PARALLEL"); do
    score_instance "$i" "$status_file" &
  done
  wait

  fail_count=$(awk -F'|' '$2 != 0 {c++} END{print c+0}' "$status_file")
  if [[ "$fail_count" -gt 0 ]]; then
    echo "[$(date -Iseconds)] [round ${round}] 失败实例数: ${fail_count}" | tee -a "$LOG_FILE"
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

echo "[$(date -Iseconds)] ===== un_sentiment_backlog_parallel done =====" | tee -a "$LOG_FILE"
