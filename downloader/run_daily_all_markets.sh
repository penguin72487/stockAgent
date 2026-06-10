#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-/home/user/miniforge3/envs/fintech/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3 || command -v python || true)"
fi
if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then
  echo "[daily] python not found in PATH" >&2
  exit 2
fi

RUN_MODE="${RUN_MODE:-once}"                        # once | daemon | market-daemon
INTERVAL_SECONDS="${INTERVAL_SECONDS:-86400}"       # daemon mode sleep interval
MARKET_CHECK_INTERVAL_SECONDS="${MARKET_CHECK_INTERVAL_SECONDS:-300}"
MAX_CYCLES="${MAX_CYCLES:-0}"                       # 0 means unlimited
FAIL_FAST="${FAIL_FAST:-0}"                         # 1 => fail immediately on any step error
LOCK_FILE="${LOCK_FILE:-/tmp/stockagent_daily.lock}"
SCHEDULE_STATE_FILE="${SCHEDULE_STATE_FILE:-/tmp/stockagent_market_schedule.state}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_LOG_DIR="${RUN_LOG_DIR:-artifacts/daily_downloader}"
RUN_LOG_FILE="${RUN_LOG_FILE:-${RUN_LOG_DIR}/${RUN_ID}.log}"
RUN_RECORD_FILE="${RUN_RECORD_FILE:-${RUN_LOG_DIR}/daily_runs.tsv}"
TEE_LOG="${TEE_LOG:-1}"

WORKERS="${WORKERS:-16}"
ASSET_WORKERS="${ASSET_WORKERS:-1}"
RETRIES="${RETRIES:-2}"
REPAIR_OVERLAP_DAYS="${REPAIR_OVERLAP_DAYS:-7}"
DAILY_STALE_MAX_LAG_DAYS="${DAILY_STALE_MAX_LAG_DAYS:-14}"
PRECHECK_FILE_TIMEOUT_SECONDS="${PRECHECK_FILE_TIMEOUT_SECONDS:-20}"
REPAIR_SYMBOL_TIMEOUT_SECONDS="${REPAIR_SYMBOL_TIMEOUT_SECONDS:-90}"
YAHOO_DAILY_DISCOVER_SYMBOLS="${YAHOO_DAILY_DISCOVER_SYMBOLS:-1}"
YAHOO_INCLUDE_TW_DELISTED="${YAHOO_INCLUDE_TW_DELISTED:-1}"
YAHOO_INCLUDE_US_DELISTED="${YAHOO_INCLUDE_US_DELISTED:-1}"
FRANKFURTER_TIMEOUT="${FRANKFURTER_TIMEOUT:-30}"
FRANKFURTER_SYMBOLS_FILE="${FRANKFURTER_SYMBOLS_FILE:-configs/forex_all_pairs_frankfurter.txt}"
RUN_PEPPERSTONE_GROUPS="${RUN_PEPPERSTONE_GROUPS:-1}"
PEPPERSTONE_WORKERS="${PEPPERSTONE_WORKERS:-8}"
RUN_CEX_PERP="${RUN_CEX_PERP:-1}"
OKX_WORKERS="${OKX_WORKERS:-16}"
OKX_REQUEST_INTERVAL="${OKX_REQUEST_INTERVAL:-0.1}"
OKX_MAX_RETRIES="${OKX_MAX_RETRIES:-8}"
BYBIT_WORKERS="${BYBIT_WORKERS:-16}"
BYBIT_REQUEST_INTERVAL="${BYBIT_REQUEST_INTERVAL:-0.1}"
BYBIT_MAX_RETRIES="${BYBIT_MAX_RETRIES:-8}"
BYBIT_CATEGORIES="${BYBIT_CATEGORIES:-linear inverse}"
YAHOO_ASSETS="${YAHOO_ASSETS:-tw_stocks us_stocks crypto forex}"
YAHOO_STEP_TIMEOUT_SECONDS="${YAHOO_STEP_TIMEOUT_SECONDS:-0}"  # 0 disables timeout
RUN_DATA_QUALITY_AUDIT="${RUN_DATA_QUALITY_AUDIT:-1}"
AUDIT_ROOTS="${AUDIT_ROOTS:-data_yahoo/tw_stocks data_yahoo/us_stocks data_yahoo/forex data_yahoo/crypto data_okx data_bybit data_forex_frankfurter data_peperstone}"
AUDIT_OUTPUT_DIR="${AUDIT_OUTPUT_DIR:-artifacts/data_quality}"
AUDIT_WORKERS="${AUDIT_WORKERS:-16}"
AUDIT_STALE_MAX_LAG_DAYS="${AUDIT_STALE_MAX_LAG_DAYS:-14}"
AUDIT_DAILY_GAP_DAYS="${AUDIT_DAILY_GAP_DAYS:-10}"
AUDIT_INTRADAY_GAP_MULTIPLE="${AUDIT_INTRADAY_GAP_MULTIPLE:-4}"

TW_CLOSE_TZ="${TW_CLOSE_TZ:-Asia/Taipei}"
TW_CLOSE_TIME="${TW_CLOSE_TIME:-13:40}"
US_CLOSE_TZ="${US_CLOSE_TZ:-America/New_York}"
US_CLOSE_TIME="${US_CLOSE_TIME:-16:20}"
FOREX_CLOSE_TZ="${FOREX_CLOSE_TZ:-America/New_York}"
FOREX_CLOSE_TIME="${FOREX_CLOSE_TIME:-17:10}"
CEX_CLOSE_TZ="${CEX_CLOSE_TZ:-UTC}"
CEX_CLOSE_TIME="${CEX_CLOSE_TIME:-00:10}"

FAILED_STEPS=()
LAST_RUN_TW=""
LAST_RUN_US=""
LAST_RUN_FOREX=""
LAST_RUN_CEX=""

log() {
  local message="$1"
  echo "[daily] ts=$(date +%F' '%T) ${message}"
}

init_run_logging() {
  mkdir -p "$RUN_LOG_DIR"
  if [[ "$TEE_LOG" == "1" ]]; then
    exec > >(tee -a "$RUN_LOG_FILE") 2>&1
  fi
}

append_cycle_record() {
  local cycle_id="$1"
  local status="$2"
  local elapsed_sec="$3"
  local failed=""

  if (( ${#FAILED_STEPS[@]} > 0 )); then
    local IFS=","
    failed="${FAILED_STEPS[*]}"
  fi

  mkdir -p "$(dirname "$RUN_RECORD_FILE")"
  if [[ ! -f "$RUN_RECORD_FILE" ]]; then
    printf "timestamp_utc\trun_id\trun_mode\tcycle_id\tstatus\telapsed_sec\tfailed_steps\tlog_file\n" >> "$RUN_RECORD_FILE"
  fi
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$(date -u +%F'T'%T'Z')" \
    "$RUN_ID" \
    "$RUN_MODE" \
    "$cycle_id" \
    "$status" \
    "$elapsed_sec" \
    "$failed" \
    "$RUN_LOG_FILE" >> "$RUN_RECORD_FILE"
}

record_failure() {
  local step_name="$1"
  FAILED_STEPS+=("$step_name")
  if [[ "$FAIL_FAST" == "1" ]]; then
    log "fail_fast=1 step=${step_name} action=exit"
    exit 1
  fi
}

run_step() {
  local name="$1"
  shift
  local start_ts
  local end_ts
  local elapsed

  start_ts="$(date +%s)"
  log "step=${name} start"
  if "$@"; then
    end_ts="$(date +%s)"
    elapsed="$((end_ts - start_ts))"
    log "step=${name} done elapsed_sec=${elapsed}"
    return 0
  fi

  end_ts="$(date +%s)"
  elapsed="$((end_ts - start_ts))"
  log "step=${name} failed elapsed_sec=${elapsed}"
  record_failure "$name"
  return 1
}

to_minutes() {
  local hhmm="$1"
  local hour="${hhmm%%:*}"
  local minute="${hhmm##*:}"
  echo $((10#${hour} * 60 + 10#${minute}))
}

load_schedule_state() {
  if [[ -f "$SCHEDULE_STATE_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$SCHEDULE_STATE_FILE"
  fi
}

save_schedule_state() {
  mkdir -p "$(dirname "$SCHEDULE_STATE_FILE")"
  cat > "$SCHEDULE_STATE_FILE" <<EOF
LAST_RUN_TW=${LAST_RUN_TW}
LAST_RUN_US=${LAST_RUN_US}
LAST_RUN_FOREX=${LAST_RUN_FOREX}
LAST_RUN_CEX=${LAST_RUN_CEX}
EOF
}

market_due_today() {
  local tz="$1"
  local close_time="$2"
  local last_run_date="$3"
  local now_date
  local now_hhmm
  local now_minutes
  local close_minutes

  now_date="$(TZ="$tz" date +%F)"
  now_hhmm="$(TZ="$tz" date +%H:%M)"
  now_minutes="$(to_minutes "$now_hhmm")"
  close_minutes="$(to_minutes "$close_time")"

  if [[ "$last_run_date" == "$now_date" ]]; then
    return 1
  fi
  if [[ "$now_minutes" -lt "$close_minutes" ]]; then
    return 1
  fi
  return 0
}

run_yahoo_incremental_assets() {
  local assets_text="$1"
  local prev_assets="$YAHOO_ASSETS"
  local rc=0
  YAHOO_ASSETS="$assets_text"
  if ! run_yahoo_incremental; then
    rc=$?
  fi
  YAHOO_ASSETS="$prev_assets"
  return "$rc"
}

run_yahoo_incremental() {
  local today
  local asset
  local -a assets=()
  local -a base_cmd=()
  local -a run_cmd=()
  local -a yahoo_flags=()

  today="$(date +%F)"
  read -r -a assets <<< "$YAHOO_ASSETS"
  if (( ${#assets[@]} == 0 )); then
    log "skip=yahoo_incremental reason=empty_YAHOO_ASSETS"
    return 0
  fi

  if [[ "$YAHOO_DAILY_DISCOVER_SYMBOLS" == "1" ]]; then
    yahoo_flags+=(--daily-discover-symbols)
  else
    yahoo_flags+=(--no-daily-discover-symbols)
  fi
  if [[ "$YAHOO_INCLUDE_TW_DELISTED" == "1" ]]; then
    yahoo_flags+=(--include-tw-delisted)
  else
    yahoo_flags+=(--no-include-tw-delisted)
  fi
  if [[ "$YAHOO_INCLUDE_US_DELISTED" == "1" ]]; then
    yahoo_flags+=(--include-us-delisted)
  else
    yahoo_flags+=(--no-include-us-delisted)
  fi

  for asset in "${assets[@]}"; do
    base_cmd=(
      "$PYTHON_BIN" downloader/download_yahoo_ohlcv.py
      --mode daily-update
      --asset "$asset"
      --end-date "$today"
      --workers "$WORKERS"
      --asset-workers "$ASSET_WORKERS"
      --retries "$RETRIES"
      --repair-overlap-days "$REPAIR_OVERLAP_DAYS"
      --daily-stale-max-lag-days "$DAILY_STALE_MAX_LAG_DAYS"
      --precheck-file-timeout-seconds "$PRECHECK_FILE_TIMEOUT_SECONDS"
      --repair-symbol-timeout-seconds "$REPAIR_SYMBOL_TIMEOUT_SECONDS"
      "${yahoo_flags[@]}"
    )

    run_cmd=("${base_cmd[@]}")
    if [[ "$YAHOO_STEP_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] && [[ "$YAHOO_STEP_TIMEOUT_SECONDS" -gt 0 ]]; then
      if command -v timeout >/dev/null 2>&1; then
        run_cmd=(timeout --signal=TERM --kill-after=30s "${YAHOO_STEP_TIMEOUT_SECONDS}" "${base_cmd[@]}")
      else
        log "timeout command not found; continue without per-asset timeout"
      fi
    fi

    run_step "yahoo_${asset}_daily_update" "${run_cmd[@]}"
  done
}

run_frankfurter_incremental() {
  local today
  today="$(date +%F)"
  if [[ -f "$FRANKFURTER_SYMBOLS_FILE" ]]; then
    run_step frankfurter_forex_incremental \
      "$PYTHON_BIN" downloader/download_forex_frankfurter.py \
      --mode daily-update \
      --output-dir data_yahoo/forex \
      --symbols-file "$FRANKFURTER_SYMBOLS_FILE" \
      --end-date "$today" \
      --workers "$WORKERS" \
      --timeout "$FRANKFURTER_TIMEOUT" \
      --skip-manifest
  else
    log "skip=frankfurter_forex_incremental reason=missing_symbols_file file=${FRANKFURTER_SYMBOLS_FILE}"
  fi
}

run_pepperstone_incremental() {
  local today
  today="$(date +%F)"
  if [[ "$RUN_PEPPERSTONE_GROUPS" == "1" ]]; then
    run_step pepperstone_groups_daily_update \
      "$PYTHON_BIN" downloader/download_pepperstone.py \
      --mode daily-update \
      --groups all \
      --end-date "$today" \
      --workers "$PEPPERSTONE_WORKERS" \
      --retries "$RETRIES" \
      --repair-overlap-days "$REPAIR_OVERLAP_DAYS"
  else
    log "skip=pepperstone_groups_daily_update reason=RUN_PEPPERSTONE_GROUPS=${RUN_PEPPERSTONE_GROUPS}"
  fi
}

run_cex_incremental() {
  local today
  local -a bybit_categories=()

  today="$(date +%F)"
  if [[ "$RUN_CEX_PERP" != "1" ]]; then
    log "skip=cex_perp_daily_update reason=RUN_CEX_PERP=${RUN_CEX_PERP}"
    return 0
  fi

  run_step okx_perp_daily_update \
    "$PYTHON_BIN" downloader/download_okx_perp_daily.py \
    --mode daily-update \
    --end-date "$today" \
    --workers "$OKX_WORKERS" \
    --request-interval "$OKX_REQUEST_INTERVAL" \
    --max-retries "$OKX_MAX_RETRIES"

  read -r -a bybit_categories <<< "$BYBIT_CATEGORIES"
  run_step bybit_perp_daily_update \
    "$PYTHON_BIN" downloader/download_bybit_perp_daily.py \
    --mode daily-update \
    --end-date "$today" \
    --workers "$BYBIT_WORKERS" \
    --request-interval "$BYBIT_REQUEST_INTERVAL" \
    --max-retries "$BYBIT_MAX_RETRIES" \
    --categories "${bybit_categories[@]}"
}

run_data_quality_audit() {
  local cycle_id="$1"
  local today
  local -a audit_roots=()

  if [[ "$RUN_DATA_QUALITY_AUDIT" != "1" ]]; then
    log "skip=data_quality_audit reason=RUN_DATA_QUALITY_AUDIT=${RUN_DATA_QUALITY_AUDIT}"
    return 0
  fi

  today="$(date +%F)"
  read -r -a audit_roots <<< "$AUDIT_ROOTS"
  if (( ${#audit_roots[@]} == 0 )); then
    log "skip=data_quality_audit reason=empty_AUDIT_ROOTS"
    return 0
  fi

  run_step data_quality_audit \
    "$PYTHON_BIN" downloader/audit_ohlcv_data.py \
    --roots "${audit_roots[@]}" \
    --output-dir "$AUDIT_OUTPUT_DIR" \
    --run-id "${RUN_ID}-cycle-${cycle_id}" \
    --workers "$AUDIT_WORKERS" \
    --end-date "$today" \
    --stale-max-lag-days "$AUDIT_STALE_MAX_LAG_DAYS" \
    --daily-gap-days "$AUDIT_DAILY_GAP_DAYS" \
    --intraday-gap-multiple "$AUDIT_INTRADAY_GAP_MULTIPLE"
}

run_market_close_cycle() {
  local cycle_id="$1"
  local cycle_start
  local cycle_end
  local cycle_elapsed
  local did_run=0
  local failures_before
  local tw_date
  local us_date
  local fx_date
  local cex_date

  FAILED_STEPS=()
  cycle_start="$(date +%s)"
  log "cycle=${cycle_id} start mode=${RUN_MODE} root=${ROOT_DIR}"

  if market_due_today "$TW_CLOSE_TZ" "$TW_CLOSE_TIME" "$LAST_RUN_TW"; then
    tw_date="$(TZ="$TW_CLOSE_TZ" date +%F)"
    log "market=tw due date=${tw_date} close=${TW_CLOSE_TIME} tz=${TW_CLOSE_TZ}"
    failures_before="${#FAILED_STEPS[@]}"
    run_yahoo_incremental_assets "tw_stocks" || true
    did_run=1
    if (( ${#FAILED_STEPS[@]} == failures_before )); then
      LAST_RUN_TW="$tw_date"
    fi
  fi

  if market_due_today "$US_CLOSE_TZ" "$US_CLOSE_TIME" "$LAST_RUN_US"; then
    us_date="$(TZ="$US_CLOSE_TZ" date +%F)"
    log "market=us due date=${us_date} close=${US_CLOSE_TIME} tz=${US_CLOSE_TZ}"
    failures_before="${#FAILED_STEPS[@]}"
    run_yahoo_incremental_assets "us_stocks" || true
    did_run=1
    if (( ${#FAILED_STEPS[@]} == failures_before )); then
      LAST_RUN_US="$us_date"
    fi
  fi

  if market_due_today "$FOREX_CLOSE_TZ" "$FOREX_CLOSE_TIME" "$LAST_RUN_FOREX"; then
    fx_date="$(TZ="$FOREX_CLOSE_TZ" date +%F)"
    log "market=forex due date=${fx_date} close=${FOREX_CLOSE_TIME} tz=${FOREX_CLOSE_TZ}"
    failures_before="${#FAILED_STEPS[@]}"
    run_yahoo_incremental_assets "forex" || true
    run_frankfurter_incremental || true
    run_pepperstone_incremental || true
    did_run=1
    if (( ${#FAILED_STEPS[@]} == failures_before )); then
      LAST_RUN_FOREX="$fx_date"
    fi
  fi

  if market_due_today "$CEX_CLOSE_TZ" "$CEX_CLOSE_TIME" "$LAST_RUN_CEX"; then
    cex_date="$(TZ="$CEX_CLOSE_TZ" date +%F)"
    log "market=cex due date=${cex_date} close=${CEX_CLOSE_TIME} tz=${CEX_CLOSE_TZ}"
    failures_before="${#FAILED_STEPS[@]}"
    run_yahoo_incremental_assets "crypto" || true
    run_cex_incremental || true
    did_run=1
    if (( ${#FAILED_STEPS[@]} == failures_before )); then
      LAST_RUN_CEX="$cex_date"
    fi
  fi

  if [[ "$did_run" == "1" ]]; then
    run_data_quality_audit "$cycle_id" || true
  else
    log "cycle=${cycle_id} no_market_due"
  fi

  save_schedule_state
  cycle_end="$(date +%s)"
  cycle_elapsed="$((cycle_end - cycle_start))"

  if (( ${#FAILED_STEPS[@]} > 0 )); then
    log "cycle=${cycle_id} completed_with_failures elapsed_sec=${cycle_elapsed} failed_steps=${FAILED_STEPS[*]}"
    append_cycle_record "$cycle_id" "completed_with_failures" "$cycle_elapsed"
    return 1
  fi
  log "cycle=${cycle_id} completed elapsed_sec=${cycle_elapsed}"
  append_cycle_record "$cycle_id" "completed" "$cycle_elapsed"
  return 0
}

run_once_cycle() {
  local cycle_id="$1"
  local cycle_start
  local cycle_end
  local cycle_elapsed

  FAILED_STEPS=()
  cycle_start="$(date +%s)"
  log "cycle=${cycle_id} start mode=${RUN_MODE} root=${ROOT_DIR}"

  run_yahoo_incremental || true
  run_frankfurter_incremental || true
  run_pepperstone_incremental || true
  run_cex_incremental || true
  run_data_quality_audit "$cycle_id" || true

  cycle_end="$(date +%s)"
  cycle_elapsed="$((cycle_end - cycle_start))"

  if (( ${#FAILED_STEPS[@]} > 0 )); then
    log "cycle=${cycle_id} completed_with_failures elapsed_sec=${cycle_elapsed} failed_steps=${FAILED_STEPS[*]}"
    append_cycle_record "$cycle_id" "completed_with_failures" "$cycle_elapsed"
    return 1
  fi

  log "cycle=${cycle_id} completed elapsed_sec=${cycle_elapsed}"
  append_cycle_record "$cycle_id" "completed" "$cycle_elapsed"
  return 0
}

acquire_lock() {
  mkdir -p "$(dirname "$LOCK_FILE")"
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    log "another scheduler instance is running lock_file=${LOCK_FILE}"
    exit 3
  fi
}

validate_settings() {
  if [[ "$RUN_MODE" != "once" && "$RUN_MODE" != "daemon" && "$RUN_MODE" != "market-daemon" ]]; then
    echo "[daily] invalid RUN_MODE=${RUN_MODE} (supported: once|daemon|market-daemon)" >&2
    exit 2
  fi
  if ! [[ "$INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || [[ "$INTERVAL_SECONDS" -le 0 ]]; then
    echo "[daily] INTERVAL_SECONDS must be a positive integer" >&2
    exit 2
  fi
  if ! [[ "$MAX_CYCLES" =~ ^[0-9]+$ ]]; then
    echo "[daily] MAX_CYCLES must be an integer >= 0" >&2
    exit 2
  fi
  if [[ "$FAIL_FAST" != "0" && "$FAIL_FAST" != "1" ]]; then
    echo "[daily] FAIL_FAST must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$TEE_LOG" != "0" && "$TEE_LOG" != "1" ]]; then
    echo "[daily] TEE_LOG must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$YAHOO_DAILY_DISCOVER_SYMBOLS" != "0" && "$YAHOO_DAILY_DISCOVER_SYMBOLS" != "1" ]]; then
    echo "[daily] YAHOO_DAILY_DISCOVER_SYMBOLS must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$YAHOO_INCLUDE_TW_DELISTED" != "0" && "$YAHOO_INCLUDE_TW_DELISTED" != "1" ]]; then
    echo "[daily] YAHOO_INCLUDE_TW_DELISTED must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$YAHOO_INCLUDE_US_DELISTED" != "0" && "$YAHOO_INCLUDE_US_DELISTED" != "1" ]]; then
    echo "[daily] YAHOO_INCLUDE_US_DELISTED must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$RUN_DATA_QUALITY_AUDIT" != "0" && "$RUN_DATA_QUALITY_AUDIT" != "1" ]]; then
    echo "[daily] RUN_DATA_QUALITY_AUDIT must be 0 or 1" >&2
    exit 2
  fi
  if ! [[ "$YAHOO_STEP_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "[daily] YAHOO_STEP_TIMEOUT_SECONDS must be an integer >= 0" >&2
    exit 2
  fi
  if ! [[ "$MARKET_CHECK_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || [[ "$MARKET_CHECK_INTERVAL_SECONDS" -le 0 ]]; then
    echo "[daily] MARKET_CHECK_INTERVAL_SECONDS must be a positive integer" >&2
    exit 2
  fi
}

run_scheduler() {
  local cycle=1
  local last_status=0
  local next_interval

  while true; do
    if [[ "$RUN_MODE" == "market-daemon" ]]; then
      if run_market_close_cycle "$cycle"; then
        last_status=0
      else
        last_status=1
      fi
      next_interval="$MARKET_CHECK_INTERVAL_SECONDS"
    elif run_once_cycle "$cycle"; then
      last_status=0
      next_interval="$INTERVAL_SECONDS"
    else
      last_status=1
      next_interval="$INTERVAL_SECONDS"
    fi

    if [[ "$RUN_MODE" == "once" ]]; then
      return "$last_status"
    fi

    if [[ "$MAX_CYCLES" -gt 0 && "$cycle" -ge "$MAX_CYCLES" ]]; then
      log "reached MAX_CYCLES=${MAX_CYCLES} stop scheduler"
      return "$last_status"
    fi

    log "next_cycle_in_sec=${next_interval}"
    sleep "$next_interval"
    cycle="$((cycle + 1))"
  done
}

init_run_logging
validate_settings
acquire_lock
load_schedule_state
log "scheduler boot run_id=${RUN_ID} run_mode=${RUN_MODE} interval_sec=${INTERVAL_SECONDS} max_cycles=${MAX_CYCLES} python=${PYTHON_BIN} log_file=${RUN_LOG_FILE}"
run_scheduler
