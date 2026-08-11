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
OPTION_ROOTS="${SHIOAJI_TAIFEX_OPTION_ROOTS:-TXO,TX1,TX2,TX4,TX5,TXU,TXV,TXX,TXY,TXZ}"
OPTION_EXPIRIES="${SHIOAJI_TAIFEX_OPTION_EXPIRIES:-1}"
WEEKLY_EXPIRIES="${SHIOAJI_TAIFEX_WEEKLY_EXPIRIES:-1}"
STRIKES_PER_EXPIRY="${SHIOAJI_TAIFEX_STRIKES_PER_EXPIRY:-100}"
WORKERS="${SHIOAJI_TAIFEX_WORKERS:-3}"
LOG_FILE="$RUN_ROOT/run.log"
LOCK_FILE="$RUN_ROOT/runner.lock"
mkdir -p "$RUN_ROOT"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[shioaji-taifex] another runner holds $LOCK_FILE" >&2
  exit 3
fi
exec > >(tee -a "$LOG_FILE") 2>&1

seconds_until_capture_window() {
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
if now.weekday() < 5 and time(8, 44, 30) <= now.time() < time(13, 30):
    print(0)
else:
    target_date = now.date()
    if now.weekday() >= 5 or now.time() >= time(13, 30):
        target_date += timedelta(days=1)
    while target_date.weekday() >= 5:
        target_date += timedelta(days=1)
    target = datetime.combine(target_date, time(8, 44, 30), tzinfo=tz)
    print(max(1, int((target - now).total_seconds())))
PY
}

seconds_until_next_capture_day() {
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
target_date = now.date() + timedelta(days=1)
while target_date.weekday() >= 5:
    target_date += timedelta(days=1)
target = datetime.combine(target_date, time(8, 44, 30), tzinfo=tz)
print(max(1, int((target - now).total_seconds())))
PY
}

if (( WORKERS < 1 || WORKERS > 3 )); then
  echo "[shioaji-taifex] workers must be within 1..3 to preserve the five-connection account limit" >&2
  exit 2
fi

echo "[shioaji-taifex] runner_started=$(TZ=Asia/Taipei date --iso-8601=seconds) credential_source=$CREDENTIAL_SOURCE future=$FUTURE_CODE option_roots=$OPTION_ROOTS monthly_expiries=$OPTION_EXPIRIES weekly_expiries=$WEEKLY_EXPIRIES strikes_per_expiry=$STRIKES_PER_EXPIRY workers=$WORKERS"
while true; do
  delay="$(seconds_until_capture_window)"
  if (( delay > 0 )); then
    echo "[shioaji-taifex] waiting_seconds=$delay reason=next_capture_window"
    sleep "$delay"
  fi
  trade_date="$(TZ=Asia/Taipei date +%F)"
  capture_id="$(run_fintech_python -c 'import uuid; print(uuid.uuid4().hex)')"
  echo "[shioaji-taifex] capture_start=$(TZ=Asia/Taipei date --iso-8601=seconds) capture_id=$capture_id"
  worker_pids=()
  worker_rcs=()
  set +e
  for (( worker_index=0; worker_index<WORKERS; worker_index++ )); do
    run_fintech_python -m downloader.stream_shioaji_taifex_bidask \
      --simulation \
      --capture-id "$capture_id" \
      --output-dir "$CAPTURE_ROOT" \
      --future-code "$FUTURE_CODE" \
      --option-roots "$OPTION_ROOTS" \
      --option-expiries "$OPTION_EXPIRIES" \
      --weekly-expiries "$WEEKLY_EXPIRIES" \
      --strikes-per-expiry "$STRIKES_PER_EXPIRY" \
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
  echo "[shioaji-taifex] capture_exit=$capture_rc worker_rcs=${worker_rcs[*]} date=$trade_date"
  if (( capture_rc != 0 )); then
    echo "[shioaji-taifex] retry_seconds=30 reason=capture_failed date=$trade_date" >&2
    sleep 30
    continue
  fi
  run_fintech_python scripts/audit_shioaji_taifex_bidask_capture.py \
    --trade-date "$trade_date" \
    --capture-root "$CAPTURE_ROOT"
  next_delay="$(seconds_until_next_capture_day)"
  echo "[shioaji-taifex] waiting_seconds=$next_delay reason=next_capture_day"
  sleep "$next_delay"
done
