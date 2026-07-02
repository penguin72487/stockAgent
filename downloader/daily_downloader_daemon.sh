#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/scripts/runtime_env.sh" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/scripts/runtime_env.sh"
fi

ACTION="${1:-start}"
ENV_FILE="${DAILY_DOWNLOADER_ENV_FILE:-configs/daily_downloader.env}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

RUN_LOG_DIR="${RUN_LOG_DIR:-artifacts/daily_downloader}"
PID_FILE="${DAILY_DOWNLOADER_PID_FILE:-${RUN_LOG_DIR}/daily_downloader.pid}"
LAUNCH_LOG="${DAILY_DOWNLOADER_LAUNCH_LOG:-${RUN_LOG_DIR}/daily_downloader_daemon.log}"
STOP_TIMEOUT_SECONDS="${DAILY_DOWNLOADER_STOP_TIMEOUT_SECONDS:-30}"

log() {
  echo "[daily-daemon] ts=$(date +%F' '%T) $*"
}

read_pid() {
  if [[ ! -f "$PID_FILE" ]]; then
    return 1
  fi
  local pid
  pid="$(tr -d '[:space:]' < "$PID_FILE")"
  if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  printf "%s" "$pid"
}

is_running() {
  local pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}

has_own_session() {
  local pid="$1"
  local sid
  sid="$(ps -o sid= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)"
  [[ "$sid" == "$pid" ]]
}

export_default_runtime() {
  prepend_fintech_path
  export PYTHON_BIN="${PYTHON_BIN:-$(resolve_fintech_python 2>/dev/null || true)}"
  export RUN_MODE="${RUN_MODE:-market-daemon}"
  export MARKET_CHECK_INTERVAL_SECONDS="${MARKET_CHECK_INTERVAL_SECONDS:-300}"
  export MAX_CYCLES="${MAX_CYCLES:-0}"
  export FAIL_FAST="${FAIL_FAST:-0}"
  export DAILY_PARALLEL_GROUPS="${DAILY_PARALLEL_GROUPS:-1}"
  export TEE_LOG="${TEE_LOG:-1}"
  export RUN_LOG_DIR
  export RUN_RECORD_FILE="${RUN_RECORD_FILE:-${RUN_LOG_DIR}/daily_runs.tsv}"
  export LOCK_FILE="${LOCK_FILE:-${RUN_LOG_DIR}/daily.lock}"
  export SCHEDULE_STATE_FILE="${SCHEDULE_STATE_FILE:-${RUN_LOG_DIR}/market_schedule.state}"
  export RUN_TW_PUBLIC_DATA="${RUN_TW_PUBLIC_DATA:-1}"
  export TW_PUBLIC_DATASETS="${TW_PUBLIC_DATASETS:-all}"
  export TW_PUBLIC_OUTPUT_DIR="${TW_PUBLIC_OUTPUT_DIR:-data_tw_public}"
  export TW_PUBLIC_WORKERS="${TW_PUBLIC_WORKERS:-4}"
  export TW_PUBLIC_TIMEOUT="${TW_PUBLIC_TIMEOUT:-30}"
  export TW_PUBLIC_RETRIES="${TW_PUBLIC_RETRIES:-3}"
  export TW_PUBLIC_RETRY_BACKOFF="${TW_PUBLIC_RETRY_BACKOFF:-1.0}"
  export TW_PUBLIC_SLEEP="${TW_PUBLIC_SLEEP:-0.15}"
  export TW_PUBLIC_SKIP_RAW="${TW_PUBLIC_SKIP_RAW:-0}"
  export TW_PUBLIC_MAX_DATES="${TW_PUBLIC_MAX_DATES:-}"
  export RUN_TW_PUBLIC_FEATURES="${RUN_TW_PUBLIC_FEATURES:-1}"
  export TW_PUBLIC_FEATURE_PATH="${TW_PUBLIC_FEATURE_PATH:-data_tw_public/features/tw_public_stock_daily.parquet}"
  export TW_PUBLIC_FEATURE_SYMBOLS_ROOT="${TW_PUBLIC_FEATURE_SYMBOLS_ROOT:-data_yahoo/tw_stocks}"
  export TW_PUBLIC_MARKET_SYMBOL="${TW_PUBLIC_MARKET_SYMBOL:-__MARKET__}"
}

start_daemon() {
  mkdir -p "$RUN_LOG_DIR" "$(dirname "$PID_FILE")" "$(dirname "$LAUNCH_LOG")"

  local pid
  if pid="$(read_pid)" && is_running "$pid"; then
    log "already running pid=${pid} pid_file=${PID_FILE}"
    return 0
  fi
  if [[ -f "$PID_FILE" ]]; then
    log "remove stale pid_file=${PID_FILE}"
    rm -f "$PID_FILE"
  fi

  export_default_runtime
  log "start mode=${RUN_MODE} check_interval_sec=${MARKET_CHECK_INTERVAL_SECONDS} log=${LAUNCH_LOG}"
  if command -v setsid >/dev/null 2>&1; then
    setsid "$ROOT_DIR/downloader/run_daily_all_markets.sh" >> "$LAUNCH_LOG" 2>&1 &
  else
    "$ROOT_DIR/downloader/run_daily_all_markets.sh" >> "$LAUNCH_LOG" 2>&1 &
  fi
  pid="$!"
  printf "%s\n" "$pid" > "$PID_FILE"
  sleep 1
  if is_running "$pid"; then
    log "started pid=${pid} pid_file=${PID_FILE}"
    return 0
  fi

  rm -f "$PID_FILE"
  log "failed to start; see log=${LAUNCH_LOG}"
  return 1
}

stop_daemon() {
  local pid
  if ! pid="$(read_pid)" || ! is_running "$pid"; then
    log "not running"
    rm -f "$PID_FILE"
    return 0
  fi

  log "stop pid=${pid}"
  if has_own_session "$pid"; then
    kill -TERM -- "-${pid}" >/dev/null 2>&1 || kill -TERM "$pid" >/dev/null 2>&1 || true
  else
    kill -TERM "$pid" >/dev/null 2>&1 || true
  fi

  local waited=0
  while is_running "$pid" && (( waited < STOP_TIMEOUT_SECONDS )); do
    sleep 1
    waited="$((waited + 1))"
  done

  if is_running "$pid"; then
    log "force stop pid=${pid}"
    if has_own_session "$pid"; then
      kill -KILL -- "-${pid}" >/dev/null 2>&1 || kill -KILL "$pid" >/dev/null 2>&1 || true
    else
      kill -KILL "$pid" >/dev/null 2>&1 || true
    fi
  fi
  rm -f "$PID_FILE"
  log "stopped"
}

status_daemon() {
  local pid
  if pid="$(read_pid)" && is_running "$pid"; then
    log "running pid=${pid} mode=${RUN_MODE:-market-daemon} pid_file=${PID_FILE} log=${LAUNCH_LOG}"
    return 0
  fi
  log "not running pid_file=${PID_FILE}"
  return 1
}

tail_logs() {
  local lines="${2:-80}"
  if [[ ! "$lines" =~ ^[0-9]+$ ]]; then
    lines=80
  fi
  if [[ ! -f "$LAUNCH_LOG" ]]; then
    log "log not found log=${LAUNCH_LOG}"
    return 1
  fi
  tail -n "$lines" "$LAUNCH_LOG"
}

case "$ACTION" in
  start)
    start_daemon
    ;;
  stop)
    stop_daemon
    ;;
  restart)
    stop_daemon
    start_daemon
    ;;
  status)
    status_daemon
    ;;
  logs)
    tail_logs "$@"
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs [lines]}" >&2
    exit 2
    ;;
esac
