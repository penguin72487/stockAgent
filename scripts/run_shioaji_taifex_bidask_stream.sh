#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

FUTURES_ENV_FILE="${SHIOAJI_TAIFEX_ENV_FILE:-$REPO_ROOT/.env.futures}"
FALLBACK_ENV_FILE="${SHIOAJI_ENV_FILE:-$REPO_ROOT/.env}"
if [[ ! -f "$FUTURES_ENV_FILE" ]]; then
  echo "[shioaji-taifex] missing futures env file: $FUTURES_ENV_FILE" >&2
  exit 2
fi
set -a
source "$FUTURES_ENV_FILE"
set +a
if [[ -z "${SHIOAJI_API_KEY:-}" && -z "${SHIOAJI_SECRET_KEY:-}" ]]; then
  if [[ ! -f "$FALLBACK_ENV_FILE" ]]; then
    echo "[shioaji-taifex] futures credentials are empty and fallback is missing: $FALLBACK_ENV_FILE" >&2
    exit 2
  fi
  set -a
  source "$FALLBACK_ENV_FILE"
  set +a
  CREDENTIAL_SOURCE="$FALLBACK_ENV_FILE"
else
  CREDENTIAL_SOURCE="$FUTURES_ENV_FILE"
fi
if [[ -z "${SHIOAJI_API_KEY:-}" || -z "${SHIOAJI_SECRET_KEY:-}" ]]; then
  echo "[shioaji-taifex] both SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required in $CREDENTIAL_SOURCE" >&2
  exit 2
fi
source scripts/runtime_env.sh

# Fail at service startup if the package entry point cannot be imported.  Running
# the collector with ``-m`` keeps the repository root on sys.path; executing the
# file path directly sets sys.path[0] to downloader/ and breaks its package
# imports before any capture manifest can be written.
run_fintech_python -c 'import downloader.stream_shioaji_taifex_bidask'

RUN_ROOT="${SHIOAJI_TAIFEX_RUN_ROOT:-artifacts/data_capture/shioaji_taifex_bidask}"
CAPTURE_ROOT="${SHIOAJI_TAIFEX_CAPTURE_ROOT:-data_tw_index_derivatives_ticks/shioaji_fop_captures}"
FUTURE_CODE="${SHIOAJI_TAIFEX_FUTURE_CODE:-TXFR1}"
HEDGE_FUTURE_CODE="${SHIOAJI_TAIFEX_HEDGE_FUTURE_CODE:-TMFR1}"
OPTION_ROOTS="${SHIOAJI_TAIFEX_OPTION_ROOTS:-TXO,TX1,TX2,TX4,TX5,TXU,TXV,TXX,TXY,TXZ}"
OPTION_EXPIRIES="${SHIOAJI_TAIFEX_OPTION_EXPIRIES:-1}"
WEEKLY_EXPIRIES="${SHIOAJI_TAIFEX_WEEKLY_EXPIRIES:-2}"
STRIKES_PER_EXPIRY="${SHIOAJI_TAIFEX_STRIKES_PER_EXPIRY:-100}"
WORKERS="${SHIOAJI_TAIFEX_WORKERS:-3}"
EXECUTE_STRATEGIES="${SHIOAJI_TAIFEX_EXECUTE_STRATEGIES:-true}"
STRATEGY_STATE_DIR="${SHIOAJI_TAIFEX_STRATEGY_STATE_DIR:-artifacts/live/shioaji_taifex_volatility_simulation}"
FINAL_SETTLEMENT_PATH="${SHIOAJI_TAIFEX_FINAL_SETTLEMENT_PATH:-data_tw_index_options_daily/txo_final_settlement_history.parquet}"
STRATEGY_MODE="${SHIOAJI_TAIFEX_STRATEGY_MODE:-intraday_futures}"
STRATEGY_INTRADAY_INTERVAL_SECONDS="${SHIOAJI_TAIFEX_STRATEGY_INTRADAY_INTERVAL_SECONDS:-60}"
STRATEGY_INTRADAY_ENTRY_CUTOFF="${SHIOAJI_TAIFEX_STRATEGY_INTRADAY_ENTRY_CUTOFF:-13:20:00}"
STRATEGY_INTRADAY_FLATTEN_TIME="${SHIOAJI_TAIFEX_STRATEGY_INTRADAY_FLATTEN_TIME:-13:35:00}"
STRATEGY_NIGHT_ENTRY_CUTOFF="${SHIOAJI_TAIFEX_STRATEGY_NIGHT_ENTRY_CUTOFF:-04:40:00}"
STRATEGY_NIGHT_FLATTEN_TIME="${SHIOAJI_TAIFEX_STRATEGY_NIGHT_FLATTEN_TIME:-04:55:00}"
STRATEGY_OPTION_RISK_MARGIN_A_TWD="${SHIOAJI_TAIFEX_STRATEGY_OPTION_RISK_MARGIN_A_TWD:-187000}"
STRATEGY_OPTION_RISK_MARGIN_B_TWD="${SHIOAJI_TAIFEX_STRATEGY_OPTION_RISK_MARGIN_B_TWD:-94000}"
STRATEGY_OPTION_RISK_MARGIN_C_TWD="${SHIOAJI_TAIFEX_STRATEGY_OPTION_RISK_MARGIN_C_TWD:-18800}"
STRATEGY_CAPITAL_BUFFER_MULTIPLE="${SHIOAJI_TAIFEX_STRATEGY_CAPITAL_BUFFER_MULTIPLE:-2.0}"
STRATEGY_CATALOG_EXPANSION_ENTRY_POLICY="${SHIOAJI_TAIFEX_STRATEGY_CATALOG_EXPANSION_ENTRY_POLICY:-immediate_live}"
LOG_FILE="$RUN_ROOT/run.log"
LOCK_FILE="$RUN_ROOT/runner.lock"
mkdir -p "$RUN_ROOT"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[shioaji-taifex] another runner holds $LOCK_FILE" >&2
  exit 3
fi
exec > >(tee -a "$LOG_FILE") 2>&1

next_capture_spec() {
  run_fintech_python - <<'PY'
from datetime import datetime

from stockagent.data.taifex_sessions import TAIPEI, next_taifex_capture_window

now = datetime.now(TAIPEI)
window = next_taifex_capture_window(now)
print(
    "\t".join(
        (
            str(window.delay_seconds(now)),
            window.session,
            window.trading_date.isoformat(),
            window.stops_at.isoformat(),
        )
    )
)
PY
}

if (( WORKERS < 1 || WORKERS > 3 )); then
  echo "[shioaji-taifex] workers must be within 1..3 to preserve the five-connection account limit" >&2
  exit 2
fi

if [[ "$EXECUTE_STRATEGIES" != true && "$EXECUTE_STRATEGIES" != false ]]; then
  echo "[shioaji-taifex] SHIOAJI_TAIFEX_EXECUTE_STRATEGIES must be true or false" >&2
  exit 2
fi

echo "[shioaji-taifex] runner_started=$(TZ=Asia/Taipei date --iso-8601=seconds) credential_source=$CREDENTIAL_SOURCE future=$FUTURE_CODE hedge_future=$HEDGE_FUTURE_CODE option_roots=$OPTION_ROOTS monthly_expiries=$OPTION_EXPIRIES weekly_expiries=$WEEKLY_EXPIRIES strikes_per_expiry=$STRIKES_PER_EXPIRY workers=$WORKERS execute_strategies=$EXECUTE_STRATEGIES strategy_mode=$STRATEGY_MODE intraday_interval_seconds=$STRATEGY_INTRADAY_INTERVAL_SECONDS intraday_entry_cutoff=$STRATEGY_INTRADAY_ENTRY_CUTOFF intraday_flatten_time=$STRATEGY_INTRADAY_FLATTEN_TIME night_entry_cutoff=$STRATEGY_NIGHT_ENTRY_CUTOFF night_flatten_time=$STRATEGY_NIGHT_FLATTEN_TIME"
while true; do
  IFS=$'\t' read -r delay capture_session trade_date stop_at < <(next_capture_spec)
  if (( delay > 0 )); then
    echo "[shioaji-taifex] waiting_seconds=$delay reason=next_capture_window session=$capture_session trade_date=$trade_date stop_at=$stop_at"
    sleep "$delay"
  fi
  if [[ "$EXECUTE_STRATEGIES" == true ]]; then
    if [[ "$capture_session" == night ]]; then
      # The day-session TXO/TX expiry has finished before night pre-open.  Pull
      # through today so Friday-night and other expiry-night strategies do not
      # wait until the next business-day bootstrap for an already-published FSP.
      settlement_end="$(TZ=Asia/Taipei date +%F)"
    else
      settlement_end="$(TZ=Asia/Taipei date -d yesterday +%F)"
    fi
    echo "[shioaji-taifex] final_settlement_refresh_session=$capture_session final_settlement_refresh_end=$settlement_end"
    if ! run_fintech_python scripts/download_taifex_final_settlement_history.py \
      --start-date 2012-11-01 \
      --end-date "$settlement_end" \
      --refresh; then
      echo "[shioaji-taifex] final_settlement_refresh_failed end=$settlement_end strategy_will_fail_closed_if_needed" >&2
    fi
    echo "[shioaji-taifex] settlement_bootstrap_session=$capture_session trade_date=$trade_date"
    if ! run_fintech_python -m downloader.stream_shioaji_taifex_bidask \
      --simulation \
      --settle-expired-strategy-state-only \
      --capture-session "$capture_session" \
      --trade-date "$trade_date" \
      --stop-at "$stop_at" \
      --future-code "$FUTURE_CODE" \
      --hedge-future-code "$HEDGE_FUTURE_CODE" \
      --strategy-state-dir "$STRATEGY_STATE_DIR" \
      --final-settlement-path "$FINAL_SETTLEMENT_PATH" \
      --strategy-mode "$STRATEGY_MODE" \
      --strategy-intraday-interval-seconds "$STRATEGY_INTRADAY_INTERVAL_SECONDS" \
      --strategy-intraday-entry-cutoff "$STRATEGY_INTRADAY_ENTRY_CUTOFF" \
      --strategy-intraday-flatten-time "$STRATEGY_INTRADAY_FLATTEN_TIME" \
      --strategy-night-entry-cutoff "$STRATEGY_NIGHT_ENTRY_CUTOFF" \
      --strategy-night-flatten-time "$STRATEGY_NIGHT_FLATTEN_TIME" \
      --strategy-option-risk-margin-a-twd "$STRATEGY_OPTION_RISK_MARGIN_A_TWD" \
      --strategy-option-risk-margin-b-twd "$STRATEGY_OPTION_RISK_MARGIN_B_TWD" \
      --strategy-option-risk-margin-c-twd "$STRATEGY_OPTION_RISK_MARGIN_C_TWD" \
      --strategy-capital-buffer-multiple "$STRATEGY_CAPITAL_BUFFER_MULTIPLE" \
      --strategy-catalog-expansion-entry-policy "$STRATEGY_CATALOG_EXPANSION_ENTRY_POLICY"; then
      echo "[shioaji-taifex] settlement_bootstrap_failed trade_date=$trade_date retry_seconds=30" >&2
      sleep 30
      continue
    fi
  fi
  capture_id="$(run_fintech_python -c 'import uuid; print(uuid.uuid4().hex)')"
  required_option_codes=""
  if [[ "$EXECUTE_STRATEGIES" == true ]]; then
    required_option_codes="$(run_fintech_python -c 'from pathlib import Path; import sys; from stockagent.live.taifex_strategy_state import load_required_option_codes; print(",".join(load_required_option_codes(Path(sys.argv[1]))))' "$STRATEGY_STATE_DIR")"
  fi
  echo "[shioaji-taifex] capture_start=$(TZ=Asia/Taipei date --iso-8601=seconds) capture_id=$capture_id session=$capture_session trade_date=$trade_date stop_at=$stop_at"
  worker_pids=()
  worker_rcs=()
  set +e
  for (( worker_index=0; worker_index<WORKERS; worker_index++ )); do
    strategy_args=()
    if [[ "$EXECUTE_STRATEGIES" == true && "$worker_index" -eq 0 ]]; then
      strategy_args+=(
        --execute-strategies
        --final-settlement-path "$FINAL_SETTLEMENT_PATH"
        --strategy-mode "$STRATEGY_MODE"
        --strategy-intraday-interval-seconds "$STRATEGY_INTRADAY_INTERVAL_SECONDS"
        --strategy-intraday-entry-cutoff "$STRATEGY_INTRADAY_ENTRY_CUTOFF"
        --strategy-intraday-flatten-time "$STRATEGY_INTRADAY_FLATTEN_TIME"
        --strategy-night-entry-cutoff "$STRATEGY_NIGHT_ENTRY_CUTOFF"
        --strategy-night-flatten-time "$STRATEGY_NIGHT_FLATTEN_TIME"
        --strategy-option-risk-margin-a-twd "$STRATEGY_OPTION_RISK_MARGIN_A_TWD"
        --strategy-option-risk-margin-b-twd "$STRATEGY_OPTION_RISK_MARGIN_B_TWD"
        --strategy-option-risk-margin-c-twd "$STRATEGY_OPTION_RISK_MARGIN_C_TWD"
        --strategy-capital-buffer-multiple "$STRATEGY_CAPITAL_BUFFER_MULTIPLE"
        --strategy-catalog-expansion-entry-policy "$STRATEGY_CATALOG_EXPANSION_ENTRY_POLICY"
      )
    fi
    run_fintech_python -m downloader.stream_shioaji_taifex_bidask \
      --simulation \
      "${strategy_args[@]}" \
      --capture-id "$capture_id" \
      --capture-session "$capture_session" \
      --trade-date "$trade_date" \
      --stop-at "$stop_at" \
      --output-dir "$CAPTURE_ROOT" \
      --future-code "$FUTURE_CODE" \
      --hedge-future-code "$HEDGE_FUTURE_CODE" \
      --option-roots "$OPTION_ROOTS" \
      --option-expiries "$OPTION_EXPIRIES" \
      --weekly-expiries "$WEEKLY_EXPIRIES" \
      --strikes-per-expiry "$STRIKES_PER_EXPIRY" \
      --strategy-state-dir "$STRATEGY_STATE_DIR" \
      --strategy-required-option-codes "$required_option_codes" \
      --worker-index "$worker_index" \
      --workers "$WORKERS" \
      > >(tee -a "$RUN_ROOT/worker_${worker_index}.log") 2>&1 &
    worker_pids+=("$!")
    sleep 2
  done
  cleanup_workers() {
    kill -TERM "${worker_pids[@]}" 2>/dev/null || true
    wait "${worker_pids[@]}" 2>/dev/null || true
  }
  trap cleanup_workers TERM INT
  capture_rc=0
  for (( worker_index=0; worker_index<WORKERS; worker_index++ )); do
    wait "${worker_pids[$worker_index]}"
    worker_rc=$?
    worker_rcs+=("$worker_rc")
    if (( worker_rc != 0 )); then
      capture_rc=1
    fi
  done
  trap - TERM INT
  set -e
  echo "[shioaji-taifex] capture_exit=$capture_rc worker_rcs=${worker_rcs[*]} date=$trade_date session=$capture_session"
  if (( capture_rc != 0 )); then
    echo "[shioaji-taifex] retry_seconds=30 reason=capture_failed date=$trade_date session=$capture_session" >&2
    sleep 30
    continue
  fi
  run_fintech_python scripts/audit_shioaji_taifex_bidask_capture.py \
    --trade-date "$trade_date" \
    --session "$capture_session" \
    --capture-root "$CAPTURE_ROOT"
done
