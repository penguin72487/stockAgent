#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/scripts/runtime_env.sh" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/scripts/runtime_env.sh"
  prepend_fintech_path
fi

PYTHON_BIN="${PYTHON_BIN:-$(resolve_fintech_python 2>/dev/null || true)}"
if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then
  echo "[daily] python not found in PATH" >&2
  exit 2
fi

RUN_MODE="${RUN_MODE:-once}"                        # once | daemon | market-daemon
INTERVAL_SECONDS="${INTERVAL_SECONDS:-86400}"       # daemon mode sleep interval
MARKET_CHECK_INTERVAL_SECONDS="${MARKET_CHECK_INTERVAL_SECONDS:-300}"
MAX_CYCLES="${MAX_CYCLES:-0}"                       # 0 means unlimited
FAIL_FAST="${FAIL_FAST:-0}"                         # 1 => fail immediately on any step error
DAILY_PARALLEL_GROUPS="${DAILY_PARALLEL_GROUPS:-1}" # 1 => run independent provider groups concurrently
LOCK_FILE="${LOCK_FILE:-/tmp/stockagent_daily.lock}"
SCHEDULE_STATE_FILE="${SCHEDULE_STATE_FILE:-/tmp/stockagent_market_schedule.state}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_LOG_DIR="${RUN_LOG_DIR:-artifacts/daily_downloader}"
RUN_LOG_FILE="${RUN_LOG_FILE:-${RUN_LOG_DIR}/${RUN_ID}.log}"
RUN_RECORD_FILE="${RUN_RECORD_FILE:-${RUN_LOG_DIR}/daily_runs.tsv}"
TEE_LOG="${TEE_LOG:-1}"

WORKERS="${WORKERS:-16}"
ASSET_WORKERS="${ASSET_WORKERS:-2}"
RETRIES="${RETRIES:-2}"
REPAIR_OVERLAP_DAYS="${REPAIR_OVERLAP_DAYS:-7}"
DAILY_STALE_MAX_LAG_DAYS="${DAILY_STALE_MAX_LAG_DAYS:-14}"
PRECHECK_FILE_TIMEOUT_SECONDS="${PRECHECK_FILE_TIMEOUT_SECONDS:-20}"
REPAIR_SYMBOL_TIMEOUT_SECONDS="${REPAIR_SYMBOL_TIMEOUT_SECONDS:-90}"
YAHOO_RATE_LIMIT_ABORT_AFTER="${YAHOO_RATE_LIMIT_ABORT_AFTER:-20}"
YAHOO_DAILY_DISCOVER_SYMBOLS="${YAHOO_DAILY_DISCOVER_SYMBOLS:-1}"
YAHOO_DAILY_RETRY_KNOWN_MISSING_SYMBOLS="${YAHOO_DAILY_RETRY_KNOWN_MISSING_SYMBOLS:-0}"
YAHOO_RETRY_BLACKLISTED_REPAIR_SYMBOLS="${YAHOO_RETRY_BLACKLISTED_REPAIR_SYMBOLS:-0}"
YAHOO_INCLUDE_US_DELISTED="${YAHOO_INCLUDE_US_DELISTED:-1}"
FRANKFURTER_TIMEOUT="${FRANKFURTER_TIMEOUT:-30}"
FRANKFURTER_OUTPUT_DIR="${FRANKFURTER_OUTPUT_DIR:-data_yahoo/forex}"
FRANKFURTER_SYMBOLS_FILE="${FRANKFURTER_SYMBOLS_FILE:-configs/forex_all_pairs_frankfurter.txt}"
FRANKFURTER_SKIP_MANIFEST="${FRANKFURTER_SKIP_MANIFEST:-0}"
FRANKFURTER_REQUEST_INTERVAL="${FRANKFURTER_REQUEST_INTERVAL:-}"
RUN_FRANKFURTER="${RUN_FRANKFURTER:-1}"
RUN_PEPPERSTONE_GROUPS="${RUN_PEPPERSTONE_GROUPS:-0}"
PEPPERSTONE_WORKERS="${PEPPERSTONE_WORKERS:-8}"
RUN_CEX_PERP="${RUN_CEX_PERP:-1}"
OKX_WORKERS="${OKX_WORKERS:-16}"
OKX_REQUEST_INTERVAL="${OKX_REQUEST_INTERVAL:-}"
OKX_MAX_RETRIES="${OKX_MAX_RETRIES:-8}"
BYBIT_WORKERS="${BYBIT_WORKERS:-16}"
BYBIT_REQUEST_INTERVAL="${BYBIT_REQUEST_INTERVAL:-}"
BYBIT_MAX_RETRIES="${BYBIT_MAX_RETRIES:-8}"
BYBIT_CATEGORIES="${BYBIT_CATEGORIES:-linear inverse}"
RUN_BINANCE_PERP="${RUN_BINANCE_PERP:-1}"
BINANCE_WORKERS="${BINANCE_WORKERS:-32}"
BINANCE_REQUEST_WEIGHT_PER_MINUTE="${BINANCE_REQUEST_WEIGHT_PER_MINUTE:-}"
BINANCE_MAX_RETRIES="${BINANCE_MAX_RETRIES:-8}"
BINANCE_FEATURE_WORKERS="${BINANCE_FEATURE_WORKERS:-$BINANCE_WORKERS}"
CRYPTO_DAILY_MATERIALIZE_WORKERS="${CRYPTO_DAILY_MATERIALIZE_WORKERS:-8}"
RUN_FREE_PUBLIC_CONTEXT="${RUN_FREE_PUBLIC_CONTEXT:-1}"
FREE_PUBLIC_CONTEXT_OUTPUT_DIR="${FREE_PUBLIC_CONTEXT_OUTPUT_DIR:-data_free_public}"
RUN_COINMETRICS_COMMUNITY="${RUN_COINMETRICS_COMMUNITY:-1}"
COINMETRICS_OUTPUT_DIR="${COINMETRICS_OUTPUT_DIR:-data_coinmetrics_community}"
COINMETRICS_WORKERS="${COINMETRICS_WORKERS:-4}"
COINMETRICS_BATCH_ASSETS="${COINMETRICS_BATCH_ASSETS:-50}"
RUN_CRYPTO_REFERENCE="${RUN_CRYPTO_REFERENCE:-1}"
CRYPTO_REFERENCE_OUTPUT_DIR="${CRYPTO_REFERENCE_OUTPUT_DIR:-data_crypto_reference}"
RUN_DUNE_CRYPTO_HISTORY="${RUN_DUNE_CRYPTO_HISTORY:-0}"
DUNE_CRYPTO_OUTPUT_DIR="${DUNE_CRYPTO_OUTPUT_DIR:-data_dune_crypto}"
DUNE_CRYPTO_WORKERS="${DUNE_CRYPTO_WORKERS:-3}"
RUN_CRYPTO_ETF_HISTORY="${RUN_CRYPTO_ETF_HISTORY:-0}"
CRYPTO_ETF_OUTPUT_DIR="${CRYPTO_ETF_OUTPUT_DIR:-data_crypto_etf}"
CRYPTO_ETF_SEC_WORKERS="${CRYPTO_ETF_SEC_WORKERS:-10}"
CRYPTO_ETF_ISSUER_WORKERS="${CRYPTO_ETF_ISSUER_WORKERS:-4}"
CRYPTO_ETF_PRIMARY_DOCUMENTS="${CRYPTO_ETF_PRIMARY_DOCUMENTS:-1}"
CRYPTO_ACTIVE_INTRADAY_GRAIN="${CRYPTO_ACTIVE_INTRADAY_GRAIN:-1m}"
RUN_CRYPTO_TRADE_TICKS="${RUN_CRYPTO_TRADE_TICKS:-0}"
RUN_CRYPTO_ORDER_BOOK="${RUN_CRYPTO_ORDER_BOOK:-0}"
RUN_CRYPTO_LIQUIDATIONS="${RUN_CRYPTO_LIQUIDATIONS:-0}"
RUN_ALPACA_US="${RUN_ALPACA_US:-1}"
ALPACA_US_WORKERS="${ALPACA_US_WORKERS:-16}"
ALPACA_US_METADATA_WORKERS="${ALPACA_US_METADATA_WORKERS:-4}"
ALPACA_US_BATCH_SIZE="${ALPACA_US_BATCH_SIZE:-500}"
ALPACA_US_RETRIES="${ALPACA_US_RETRIES:-4}"
ALPACA_US_TIMEOUT="${ALPACA_US_TIMEOUT:-30}"
ALPACA_REQUESTS_PER_MINUTE="${ALPACA_REQUESTS_PER_MINUTE:-200}"
ALPACA_US_STEP_TIMEOUT_SECONDS="${ALPACA_US_STEP_TIMEOUT_SECONDS:-1800}"  # 0 disables timeout
RUN_YAHOO="${RUN_YAHOO:-0}"
YAHOO_ASSETS="${YAHOO_ASSETS:-}"
YAHOO_STEP_TIMEOUT_SECONDS="${YAHOO_STEP_TIMEOUT_SECONDS:-900}"  # 0 disables timeout
RUN_TW_PUBLIC_DATA="${RUN_TW_PUBLIC_DATA:-1}"
TW_PUBLIC_CONFIG="${TW_PUBLIC_CONFIG:-configs/markets/tw_public.yaml}"
TW_PUBLIC_OUTPUT_DIR="${TW_PUBLIC_OUTPUT_DIR:-data_tw_public}"
TW_PUBLIC_STOCKS_ROOT="${TW_PUBLIC_STOCKS_ROOT:-data_tw_public/stocks}"
TW_PUBLIC_WORKERS="${TW_PUBLIC_WORKERS:-4}"
TW_PUBLIC_DATE_WORKERS="${TW_PUBLIC_DATE_WORKERS:-4}"
TW_PUBLIC_TIMEOUT="${TW_PUBLIC_TIMEOUT:-30}"
TW_PUBLIC_RETRIES="${TW_PUBLIC_RETRIES:-3}"
TW_PUBLIC_REQUEST_INTERVAL="${TW_PUBLIC_REQUEST_INTERVAL:-}"
TW_PUBLIC_FLUSH_EVERY_DATES="${TW_PUBLIC_FLUSH_EVERY_DATES:-250}"
TW_PUBLIC_DAILY_OVERLAP_DAYS="${TW_PUBLIC_DAILY_OVERLAP_DAYS:-7}"
TW_PUBLIC_EMPTY_RECHECK_DAYS="${TW_PUBLIC_EMPTY_RECHECK_DAYS:-30}"
TW_PUBLIC_SKIP_RAW="${TW_PUBLIC_SKIP_RAW:-0}"
TW_PUBLIC_OHLCV_FALLBACK="${TW_PUBLIC_OHLCV_FALLBACK:-yahoo}"
TW_PUBLIC_FALLBACK_START_DATE="${TW_PUBLIC_FALLBACK_START_DATE:-2000-01-01}"
TW_PUBLIC_YAHOO_FALLBACK_DIR="${TW_PUBLIC_YAHOO_FALLBACK_DIR:-}"
TW_PUBLIC_YAHOO_WORKERS="${TW_PUBLIC_YAHOO_WORKERS:-1}"
TW_PUBLIC_YAHOO_RETRIES="${TW_PUBLIC_YAHOO_RETRIES:-3}"
TW_PUBLIC_YAHOO_REQUEST_INTERVAL="${TW_PUBLIC_YAHOO_REQUEST_INTERVAL:-1.5}"
TW_PUBLIC_SKIP_YAHOO_DOWNLOAD="${TW_PUBLIC_SKIP_YAHOO_DOWNLOAD:-0}"
TW_PUBLIC_PIPELINE_ATTEMPTS="${TW_PUBLIC_PIPELINE_ATTEMPTS:-1}"
TW_PUBLIC_PIPELINE_RETRY_SECONDS="${TW_PUBLIC_PIPELINE_RETRY_SECONDS:-60}"
RUN_TW_PUBLIC_FEATURES="${RUN_TW_PUBLIC_FEATURES:-1}"
RUN_TW_SHORT_RESTRICTIONS="${RUN_TW_SHORT_RESTRICTIONS:-1}"
TW_PUBLIC_FEATURE_PATH="${TW_PUBLIC_FEATURE_PATH:-data_tw_public/features/tw_public_stock_daily.parquet}"
TW_OFFICIAL_BACKFILL_WORKERS="${TW_OFFICIAL_BACKFILL_WORKERS:-8}"
RUN_DATA_QUALITY_AUDIT="${RUN_DATA_QUALITY_AUDIT:-0}"
AUDIT_ROOTS="${AUDIT_ROOTS:-data_tw_public/stocks data_yahoo/us_stocks data_yahoo/forex data_okx/1m data_okx/daily data_bybit/1m data_bybit/daily data_binance/1m data_binance/daily data_crypto_reference data_forex_frankfurter data_peperstone}"
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

run_parallel_groups() {
  if [[ "$DAILY_PARALLEL_GROUPS" != "1" || "$FAIL_FAST" == "1" ]]; then
    while (( "$#" > 0 )); do
      local group_name="$1"
      local group_func="$2"
      shift 2
      "$group_func" || true
    done
    return 0
  fi

  local tmp_dir
  local pids=()
  local names=()
  local funcs=()
  local idx
  local pid
  local name
  local rc

  tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/stockagent_daily_groups.XXXXXX")"
  while (( "$#" > 0 )); do
    names+=("$1")
    funcs+=("$2")
    shift 2
  done

  for idx in "${!names[@]}"; do
    name="${names[$idx]}"
    (
      "${funcs[$idx]}"
      printf "%s\n" "$?" > "${tmp_dir}/${name}.rc"
    ) &
    pids+=("$!")
    log "group=${name} pid=${pids[$idx]} start_async"
  done

  for idx in "${!pids[@]}"; do
    pid="${pids[$idx]}"
    name="${names[$idx]}"
    wait "$pid" || true
    rc=1
    if [[ -f "${tmp_dir}/${name}.rc" ]]; then
      rc="$(<"${tmp_dir}/${name}.rc")"
    fi
    if [[ "$rc" == "0" ]]; then
      log "group=${name} done"
    else
      log "group=${name} failed rc=${rc}"
      record_failure "$name"
    fi
  done

  rm -rf "$tmp_dir"
  return 0
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
  local rc=0
  local -a assets=()
  local -a base_cmd=()
  local -a run_cmd=()
  local -a yahoo_flags=()

  today="$(date +%F)"
  if [[ "$RUN_YAHOO" != "1" ]]; then
    log "skip=yahoo_incremental reason=RUN_YAHOO=${RUN_YAHOO}"
    return 0
  fi

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
  if [[ "$YAHOO_DAILY_RETRY_KNOWN_MISSING_SYMBOLS" == "1" ]]; then
    yahoo_flags+=(--daily-retry-known-missing-symbols)
  else
    yahoo_flags+=(--no-daily-retry-known-missing-symbols)
  fi
  if [[ "$YAHOO_RETRY_BLACKLISTED_REPAIR_SYMBOLS" == "1" ]]; then
    yahoo_flags+=(--retry-blacklisted-repair-symbols)
  else
    yahoo_flags+=(--no-retry-blacklisted-repair-symbols)
  fi
  # The canonical TW data layer lives entirely below data_tw_public.  Keep the
  # generic Yahoo updater from recreating the retired data_yahoo/tw_stocks tree;
  # the official updater manages its audited Yahoo fallback inside
  # data_tw_public/fallback instead.
  yahoo_flags+=(--no-include-tw-delisted)
  if [[ "$YAHOO_INCLUDE_US_DELISTED" == "1" ]]; then
    yahoo_flags+=(--include-us-delisted)
  else
    yahoo_flags+=(--no-include-us-delisted)
  fi

  for asset in "${assets[@]}"; do
    local yahoo_mode="daily-update"
    local step_suffix="daily_update"
    if [[ "$asset" == "crypto" ]]; then
      yahoo_mode="incremental"
      step_suffix="1m_update"
    fi
    base_cmd=(
      "$PYTHON_BIN" downloader/download_yahoo_ohlcv.py
      --mode "$yahoo_mode"
      --asset "$asset"
      --end-date "$today"
      --workers "$WORKERS"
      --asset-workers "$ASSET_WORKERS"
      --retries "$RETRIES"
      --repair-overlap-days "$REPAIR_OVERLAP_DAYS"
      --daily-stale-max-lag-days "$DAILY_STALE_MAX_LAG_DAYS"
      --precheck-file-timeout-seconds "$PRECHECK_FILE_TIMEOUT_SECONDS"
      --repair-symbol-timeout-seconds "$REPAIR_SYMBOL_TIMEOUT_SECONDS"
      --rate-limit-abort-after "$YAHOO_RATE_LIMIT_ABORT_AFTER"
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

    if ! run_step "yahoo_${asset}_${step_suffix}" "${run_cmd[@]}"; then
      rc=1
      continue
    fi
    if [[ "$asset" == "crypto" ]]; then
      run_step yahoo_crypto_daily_materialize \
        "$PYTHON_BIN" downloader/materialize_ohlcv_daily.py \
        --input-dir data_yahoo/crypto/1m \
        --output-dir data_yahoo/crypto/daily \
        --provider Yahoo || rc=1
    fi
  done
  return "$rc"
}

run_alpaca_us_incremental() {
  local today
  local -a base_cmd=()
  local -a run_cmd=()

  today="$(TZ="$US_CLOSE_TZ" date +%F)"
  if [[ "$RUN_ALPACA_US" != "1" ]]; then
    log "skip=alpaca_us_incremental reason=RUN_ALPACA_US=${RUN_ALPACA_US}"
    return 0
  fi

  base_cmd=(
    "$PYTHON_BIN" downloader/download_alpaca_us_ohlcv.py
    --mode daily-update
    --end-date "$today"
    --output-root data_yahoo
    --workers "$ALPACA_US_WORKERS"
    --metadata-workers "$ALPACA_US_METADATA_WORKERS"
    --batch-size "$ALPACA_US_BATCH_SIZE"
    --retries "$ALPACA_US_RETRIES"
    --timeout "$ALPACA_US_TIMEOUT"
    --requests-per-minute "$ALPACA_REQUESTS_PER_MINUTE"
    --repair-overlap-days "$REPAIR_OVERLAP_DAYS"
    --daily-stale-max-lag-days "$DAILY_STALE_MAX_LAG_DAYS"
  )
  run_cmd=("${base_cmd[@]}")
  if [[ "$ALPACA_US_STEP_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] && [[ "$ALPACA_US_STEP_TIMEOUT_SECONDS" -gt 0 ]]; then
    if command -v timeout >/dev/null 2>&1; then
      run_cmd=(timeout --signal=TERM --kill-after=30s "${ALPACA_US_STEP_TIMEOUT_SECONDS}" "${base_cmd[@]}")
    else
      log "timeout command not found; continue without per-asset timeout"
    fi
  fi

  run_step "alpaca_us_daily_update" "${run_cmd[@]}"
}

run_frankfurter_incremental() {
  local today
  local -a cmd=()
  if [[ "$RUN_FRANKFURTER" != "1" ]]; then
    log "skip=frankfurter_forex_incremental reason=RUN_FRANKFURTER=${RUN_FRANKFURTER}"
    return 0
  fi
  today="$(date +%F)"
  if [[ -f "$FRANKFURTER_SYMBOLS_FILE" ]]; then
    cmd=(
      "$PYTHON_BIN" downloader/download_forex_frankfurter.py
      --mode daily-update
      --output-dir "$FRANKFURTER_OUTPUT_DIR"
      --symbols-file "$FRANKFURTER_SYMBOLS_FILE"
      --end-date "$today"
      --workers "$WORKERS"
      --timeout "$FRANKFURTER_TIMEOUT"
    )
    if [[ -n "$FRANKFURTER_REQUEST_INTERVAL" ]]; then
      cmd+=(--request-interval "$FRANKFURTER_REQUEST_INTERVAL")
    fi
    if [[ "$FRANKFURTER_SKIP_MANIFEST" == "1" ]]; then
      cmd+=(--skip-manifest)
    fi
    run_step frankfurter_forex_incremental "${cmd[@]}"
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

run_okx_perp_incremental() {
  local today
  local -a cmd=()

  today="$(date +%F)"
  if [[ "$RUN_CEX_PERP" != "1" ]]; then
    log "skip=okx_perp_1m_update reason=RUN_CEX_PERP=${RUN_CEX_PERP}"
    return 0
  fi
  cmd=(
    "$PYTHON_BIN" downloader/download_okx_perp_1m.py
    --mode incremental
    --end-date "$today"
    --workers "$OKX_WORKERS"
    --max-retries "$OKX_MAX_RETRIES"
  )
  if [[ -n "$OKX_REQUEST_INTERVAL" ]]; then
    cmd+=(--request-interval "$OKX_REQUEST_INTERVAL")
  fi
  run_step okx_perp_1m_update "${cmd[@]}" || return $?
  run_step okx_perp_daily_materialize \
    "$PYTHON_BIN" downloader/materialize_ohlcv_daily.py \
    --input-dir data_okx/1m --output-dir data_okx/daily \
    --provider OKX --workers "$CRYPTO_DAILY_MATERIALIZE_WORKERS"
}

run_bybit_perp_incremental() {
  local today
  local -a categories=()
  local -a cmd=()

  today="$(date +%F)"
  if [[ "$RUN_CEX_PERP" != "1" ]]; then
    log "skip=bybit_perp_1m_update reason=RUN_CEX_PERP=${RUN_CEX_PERP}"
    return 0
  fi
  read -r -a categories <<< "$BYBIT_CATEGORIES"
  cmd=(
    "$PYTHON_BIN" downloader/download_bybit_perp_1m.py
    --mode incremental
    --end-date "$today"
    --workers "$BYBIT_WORKERS"
    --max-retries "$BYBIT_MAX_RETRIES"
    --categories "${categories[@]}"
  )
  if [[ -n "$BYBIT_REQUEST_INTERVAL" ]]; then
    cmd+=(--request-interval "$BYBIT_REQUEST_INTERVAL")
  fi
  run_step bybit_perp_1m_update "${cmd[@]}" || return $?
  run_step bybit_perp_daily_materialize \
    "$PYTHON_BIN" downloader/materialize_ohlcv_daily.py \
    --input-dir data_bybit/1m --output-dir data_bybit/daily \
    --provider Bybit --workers "$CRYPTO_DAILY_MATERIALIZE_WORKERS"
}

run_binance_perp_incremental() {
  local today
  local -a cmd=()

  today="$(date +%F)"
  if [[ "$RUN_CEX_PERP" != "1" || "$RUN_BINANCE_PERP" != "1" ]]; then
    log "skip=binance_perp_1m_update reason=RUN_CEX_PERP:${RUN_CEX_PERP},RUN_BINANCE_PERP:${RUN_BINANCE_PERP}"
    return 0
  fi
  cmd=(
    "$PYTHON_BIN" downloader/download_binance_perp_1m.py
    --mode incremental
    --end-date "$today"
    --workers "$BINANCE_WORKERS"
    --feature-workers "$BINANCE_FEATURE_WORKERS"
    --max-retries "$BINANCE_MAX_RETRIES"
  )
  if [[ -n "$BINANCE_REQUEST_WEIGHT_PER_MINUTE" ]]; then
    cmd+=(--request-weight-per-minute "$BINANCE_REQUEST_WEIGHT_PER_MINUTE")
  fi
  run_step binance_perp_1m_update "${cmd[@]}" || return $?
  run_step binance_perp_daily_materialize \
    "$PYTHON_BIN" downloader/materialize_ohlcv_daily.py \
    --input-dir data_binance/1m --output-dir data_binance/daily \
    --provider Binance --workers "$CRYPTO_DAILY_MATERIALIZE_WORKERS"
}

run_free_public_context_incremental() {
  if [[ "$RUN_FREE_PUBLIC_CONTEXT" == "1" ]]; then
    run_step free_public_context_update \
      "$PYTHON_BIN" downloader/download_free_public_context.py \
      --output-dir "$FREE_PUBLIC_CONTEXT_OUTPUT_DIR"
    return
  fi
  log "skip=free_public_context_update reason=RUN_FREE_PUBLIC_CONTEXT=${RUN_FREE_PUBLIC_CONTEXT}"
}

run_coinmetrics_community_incremental() {
  local today

  today="$(date +%F)"
  if [[ "$RUN_COINMETRICS_COMMUNITY" == "1" ]]; then
    run_step coinmetrics_community_update \
      "$PYTHON_BIN" downloader/download_coinmetrics_community.py \
      --output-dir "$COINMETRICS_OUTPUT_DIR" \
      --end-date "$today" \
      --workers "$COINMETRICS_WORKERS" \
      --batch-assets "$COINMETRICS_BATCH_ASSETS"
    return
  fi
  log "skip=coinmetrics_community_update reason=RUN_COINMETRICS_COMMUNITY=${RUN_COINMETRICS_COMMUNITY}"
}

run_crypto_reference_incremental() {
  if [[ "$RUN_CRYPTO_REFERENCE" == "1" ]]; then
    run_step crypto_reference_update \
      "$PYTHON_BIN" downloader/download_crypto_keyed_context.py \
      --output-dir "$CRYPTO_REFERENCE_OUTPUT_DIR"
    return
  fi
  log "skip=crypto_reference_update reason=RUN_CRYPTO_REFERENCE=${RUN_CRYPTO_REFERENCE}"
}

run_dune_crypto_history_incremental() {
  if [[ "$RUN_DUNE_CRYPTO_HISTORY" == "1" ]]; then
    run_step dune_crypto_history_update \
      "$PYTHON_BIN" downloader/download_dune_crypto_history.py \
      --output-dir "$DUNE_CRYPTO_OUTPUT_DIR" \
      --end-date today \
      --workers "$DUNE_CRYPTO_WORKERS"
    return
  fi
  log "skip=dune_crypto_history_update reason=RUN_DUNE_CRYPTO_HISTORY=${RUN_DUNE_CRYPTO_HISTORY}"
}

run_crypto_etf_history_incremental() {
  local -a cmd=(
    "$PYTHON_BIN" downloader/download_crypto_etf_history.py
    --output-dir "$CRYPTO_ETF_OUTPUT_DIR"
    --sec-workers "$CRYPTO_ETF_SEC_WORKERS"
    --issuer-workers "$CRYPTO_ETF_ISSUER_WORKERS"
  )
  if [[ "$RUN_CRYPTO_ETF_HISTORY" != "1" ]]; then
    log "skip=crypto_etf_history_update reason=RUN_CRYPTO_ETF_HISTORY=${RUN_CRYPTO_ETF_HISTORY}"
    return 0
  fi
  if [[ "$CRYPTO_ETF_PRIMARY_DOCUMENTS" == "1" ]]; then
    cmd+=(--primary-documents)
  else
    cmd+=(--no-primary-documents)
  fi
  run_step crypto_etf_history_update "${cmd[@]}"
}

run_tw_public_data_update() {
  local rc=0
  local attempt=1
  local -a cmd=()

  if [[ "$RUN_TW_PUBLIC_DATA" != "1" ]]; then
    log "skip=tw_public_data_daily_update reason=RUN_TW_PUBLIC_DATA=${RUN_TW_PUBLIC_DATA}"
    return 0
  fi

  cmd=(
    "$PYTHON_BIN" downloader/download_tw_official_data.py
    --mode daily
    --config "$TW_PUBLIC_CONFIG"
    --public-dir "$TW_PUBLIC_OUTPUT_DIR"
    --stock-root "$TW_PUBLIC_STOCKS_ROOT"
    --public-feature-path "$TW_PUBLIC_FEATURE_PATH"
    --workers "$TW_OFFICIAL_BACKFILL_WORKERS"
    --public-workers "$TW_PUBLIC_WORKERS"
    --date-workers "$TW_PUBLIC_DATE_WORKERS"
    --timeout "$TW_PUBLIC_TIMEOUT"
    --retries "$TW_PUBLIC_RETRIES"
    --flush-every-dates "$TW_PUBLIC_FLUSH_EVERY_DATES"
    --daily-overlap-days "$TW_PUBLIC_DAILY_OVERLAP_DAYS"
    --empty-recheck-days "$TW_PUBLIC_EMPTY_RECHECK_DAYS"
    --ohlcv-fallback "$TW_PUBLIC_OHLCV_FALLBACK"
    --fallback-start-date "$TW_PUBLIC_FALLBACK_START_DATE"
    --yahoo-workers "$TW_PUBLIC_YAHOO_WORKERS"
    --yahoo-retries "$TW_PUBLIC_YAHOO_RETRIES"
    --yahoo-request-interval "$TW_PUBLIC_YAHOO_REQUEST_INTERVAL"
  )
  if [[ -n "$TW_PUBLIC_REQUEST_INTERVAL" ]]; then
    cmd+=(--request-interval "$TW_PUBLIC_REQUEST_INTERVAL")
  fi
  if [[ "$TW_PUBLIC_SKIP_RAW" == "1" ]]; then
    cmd+=(--skip-raw)
  fi
  if [[ -n "$TW_PUBLIC_YAHOO_FALLBACK_DIR" ]]; then
    cmd+=(--yahoo-fallback-dir "$TW_PUBLIC_YAHOO_FALLBACK_DIR")
  fi
  if [[ "$TW_PUBLIC_SKIP_YAHOO_DOWNLOAD" == "1" ]]; then
    cmd+=(--skip-yahoo-download)
  fi
  if [[ "$RUN_TW_SHORT_RESTRICTIONS" != "1" ]]; then
    cmd+=(--skip-short-rules)
  fi
  if [[ "$RUN_TW_PUBLIC_FEATURES" != "1" ]]; then
    cmd+=(--skip-feature-build)
  fi

  while (( attempt <= TW_PUBLIC_PIPELINE_ATTEMPTS )); do
    log "step=tw_public_data_daily_update pipeline_attempt=${attempt}/${TW_PUBLIC_PIPELINE_ATTEMPTS}"
    if "${cmd[@]}"; then
      log "step=tw_public_data_daily_update pipeline_attempt=${attempt} status=done"
      return 0
    fi
    rc=1
    if (( attempt < TW_PUBLIC_PIPELINE_ATTEMPTS )); then
      log "step=tw_public_data_daily_update pipeline_attempt=${attempt} status=failed retry_in_sec=${TW_PUBLIC_PIPELINE_RETRY_SECONDS}"
      sleep "$TW_PUBLIC_PIPELINE_RETRY_SECONDS"
    fi
    attempt="$((attempt + 1))"
  done

  log "step=tw_public_data_daily_update failed attempts=${TW_PUBLIC_PIPELINE_ATTEMPTS}"
  record_failure tw_public_data_daily_update

  return "$rc"
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
    run_tw_public_data_update
    did_run=1
    if (( ${#FAILED_STEPS[@]} == failures_before )); then
      LAST_RUN_TW="$tw_date"
    fi
  fi

  if market_due_today "$US_CLOSE_TZ" "$US_CLOSE_TIME" "$LAST_RUN_US"; then
    us_date="$(TZ="$US_CLOSE_TZ" date +%F)"
    log "market=us due date=${us_date} close=${US_CLOSE_TIME} tz=${US_CLOSE_TZ}"
    failures_before="${#FAILED_STEPS[@]}"
    run_alpaca_us_incremental || true
    did_run=1
    if (( ${#FAILED_STEPS[@]} == failures_before )); then
      LAST_RUN_US="$us_date"
    fi
  fi

  if market_due_today "$FOREX_CLOSE_TZ" "$FOREX_CLOSE_TIME" "$LAST_RUN_FOREX"; then
    fx_date="$(TZ="$FOREX_CLOSE_TZ" date +%F)"
    log "market=forex due date=${fx_date} close=${FOREX_CLOSE_TIME} tz=${FOREX_CLOSE_TZ}"
    failures_before="${#FAILED_STEPS[@]}"
    run_parallel_groups \
      frankfurter run_frankfurter_incremental \
      pepperstone run_pepperstone_incremental
    did_run=1
    if (( ${#FAILED_STEPS[@]} == failures_before )); then
      LAST_RUN_FOREX="$fx_date"
    fi
  fi

  if market_due_today "$CEX_CLOSE_TZ" "$CEX_CLOSE_TIME" "$LAST_RUN_CEX"; then
    cex_date="$(TZ="$CEX_CLOSE_TZ" date +%F)"
    log "market=cex due date=${cex_date} close=${CEX_CLOSE_TIME} tz=${CEX_CLOSE_TZ}"
    failures_before="${#FAILED_STEPS[@]}"
    run_parallel_groups \
      okx_perpetuals run_okx_perp_incremental \
      bybit_perpetuals run_bybit_perp_incremental \
      binance_perpetuals run_binance_perp_incremental \
      crypto_reference run_crypto_reference_incremental \
      dune_crypto_history run_dune_crypto_history_incremental \
      crypto_etf_history run_crypto_etf_history_incremental \
      free_public_context run_free_public_context_incremental \
      coinmetrics_community run_coinmetrics_community_incremental
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

  run_parallel_groups \
    alpaca_us run_alpaca_us_incremental \
    yahoo run_yahoo_incremental \
    tw_public run_tw_public_data_update \
    frankfurter run_frankfurter_incremental \
    pepperstone run_pepperstone_incremental \
    okx_perpetuals run_okx_perp_incremental \
    bybit_perpetuals run_bybit_perp_incremental \
    binance_perpetuals run_binance_perp_incremental \
    crypto_reference run_crypto_reference_incremental \
    dune_crypto_history run_dune_crypto_history_incremental \
    crypto_etf_history run_crypto_etf_history_incremental \
    free_public_context run_free_public_context_incremental \
    coinmetrics_community run_coinmetrics_community_incremental

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
  if [[ "$DAILY_PARALLEL_GROUPS" != "0" && "$DAILY_PARALLEL_GROUPS" != "1" ]]; then
    echo "[daily] DAILY_PARALLEL_GROUPS must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$CRYPTO_ACTIVE_INTRADAY_GRAIN" != "1m" ]]; then
    echo "[daily] CRYPTO_ACTIVE_INTRADAY_GRAIN must be 1m while the active crypto scope is candle-only" >&2
    exit 2
  fi
  if [[ "$RUN_CRYPTO_TRADE_TICKS" != "0" || "$RUN_CRYPTO_ORDER_BOOK" != "0" || "$RUN_CRYPTO_LIQUIDATIONS" != "0" ]]; then
    echo "[daily] crypto event acquisition is deferred; trade ticks, order books and liquidations must remain disabled" >&2
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
  if [[ "$YAHOO_DAILY_RETRY_KNOWN_MISSING_SYMBOLS" != "0" && "$YAHOO_DAILY_RETRY_KNOWN_MISSING_SYMBOLS" != "1" ]]; then
    echo "[daily] YAHOO_DAILY_RETRY_KNOWN_MISSING_SYMBOLS must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$YAHOO_RETRY_BLACKLISTED_REPAIR_SYMBOLS" != "0" && "$YAHOO_RETRY_BLACKLISTED_REPAIR_SYMBOLS" != "1" ]]; then
    echo "[daily] YAHOO_RETRY_BLACKLISTED_REPAIR_SYMBOLS must be 0 or 1" >&2
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
  if [[ "$RUN_DUNE_CRYPTO_HISTORY" != "0" && "$RUN_DUNE_CRYPTO_HISTORY" != "1" ]]; then
    echo "[daily] RUN_DUNE_CRYPTO_HISTORY must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$RUN_CRYPTO_ETF_HISTORY" != "0" && "$RUN_CRYPTO_ETF_HISTORY" != "1" ]]; then
    echo "[daily] RUN_CRYPTO_ETF_HISTORY must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$CRYPTO_ETF_PRIMARY_DOCUMENTS" != "0" && "$CRYPTO_ETF_PRIMARY_DOCUMENTS" != "1" ]]; then
    echo "[daily] CRYPTO_ETF_PRIMARY_DOCUMENTS must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$TW_PUBLIC_OHLCV_FALLBACK" != "yahoo" && "$TW_PUBLIC_OHLCV_FALLBACK" != "none" ]]; then
    echo "[daily] TW_PUBLIC_OHLCV_FALLBACK must be yahoo or none" >&2
    exit 2
  fi
  if [[ "$TW_PUBLIC_SKIP_YAHOO_DOWNLOAD" != "0" && "$TW_PUBLIC_SKIP_YAHOO_DOWNLOAD" != "1" ]]; then
    echo "[daily] TW_PUBLIC_SKIP_YAHOO_DOWNLOAD must be 0 or 1" >&2
    exit 2
  fi
  if ! [[ "$TW_PUBLIC_PIPELINE_ATTEMPTS" =~ ^[1-9][0-9]*$ ]]; then
    echo "[daily] TW_PUBLIC_PIPELINE_ATTEMPTS must be a positive integer" >&2
    exit 2
  fi
  if ! [[ "$TW_PUBLIC_PIPELINE_RETRY_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "[daily] TW_PUBLIC_PIPELINE_RETRY_SECONDS must be an integer >= 0" >&2
    exit 2
  fi
  if [[ "$RUN_YAHOO" != "0" && "$RUN_YAHOO" != "1" ]]; then
    echo "[daily] RUN_YAHOO must be 0 or 1" >&2
    exit 2
  fi
  if [[ " $YAHOO_ASSETS " == *" tw_stocks "* ]]; then
    echo "[daily] tw_stocks is managed only by downloader/download_tw_official_data.py under data_tw_public; remove it from YAHOO_ASSETS" >&2
    exit 2
  fi
  if [[ "$FRANKFURTER_SKIP_MANIFEST" != "0" && "$FRANKFURTER_SKIP_MANIFEST" != "1" ]]; then
    echo "[daily] FRANKFURTER_SKIP_MANIFEST must be 0 or 1" >&2
    exit 2
  fi
  if [[ "$RUN_FRANKFURTER" != "0" && "$RUN_FRANKFURTER" != "1" ]]; then
    echo "[daily] RUN_FRANKFURTER must be 0 or 1" >&2
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
log "scheduler boot run_id=${RUN_ID} run_mode=${RUN_MODE} parallel_groups=${DAILY_PARALLEL_GROUPS} interval_sec=${INTERVAL_SECONDS} max_cycles=${MAX_CYCLES} crypto_grain=${CRYPTO_ACTIVE_INTRADAY_GRAIN} crypto_events=disabled python=${PYTHON_BIN} log_file=${RUN_LOG_FILE}"
run_scheduler
