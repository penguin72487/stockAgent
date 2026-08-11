#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ENV_FILE="${SHIOAJI_ENV_FILE:-$REPO_ROOT/.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[shioaji-top200] missing env file: $ENV_FILE" >&2
  exit 2
fi
set -a
source "$ENV_FILE"
set +a
source scripts/runtime_env.sh

RUN_ROOT="${SHIOAJI_MICRO_RUN_ROOT:-artifacts/data_capture/shioaji_top200}"
CAPTURE_ROOT="${SHIOAJI_MICRO_CAPTURE_ROOT:-data_tw_microstructure/captures}"
UNIVERSE="${SHIOAJI_MICRO_UNIVERSE:-data_tw_microstructure/universe/top_200.csv}"
HFT_DATASET_ROOT="${SHIOAJI_HFT_DATASET_ROOT:-data_tw_microstructure/hft_dataset}"
TAIFEX_PRIORITY_GATE="${SHIOAJI_TAIFEX_PRIORITY_GATE:-true}"
TAIFEX_WORKERS="${SHIOAJI_TAIFEX_WORKERS:-3}"
LOG_FILE="$RUN_ROOT/run.log"
LOCK_FILE="$RUN_ROOT/runner.lock"
mkdir -p "$RUN_ROOT"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[shioaji-top200] another runner holds $LOCK_FILE" >&2
  exit 3
fi
exec > >(tee -a "$LOG_FILE") 2>&1

seconds_until_capture_window() {
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
if now.weekday() < 5 and time(8, 45) <= now.time() < time(13, 30):
    print(0)
else:
    target_date = now.date()
    if now.weekday() >= 5 or now.time() >= time(13, 30):
        target_date += timedelta(days=1)
    while target_date.weekday() >= 5:
        target_date += timedelta(days=1)
    target = datetime.combine(target_date, time(8, 45), tzinfo=tz)
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
target = datetime.combine(target_date, time(8, 45), tzinfo=tz)
print(max(1, int((target - now).total_seconds())))
PY
}

wait_for_taifex_priority() {
  if [[ "$TAIFEX_PRIORITY_GATE" != true ]]; then
    echo "[shioaji-top200] priority_gate=disabled"
    return 0
  fi
  local last_log=0
  while true; do
    local taifex_processes
    taifex_processes="$(pgrep -fc '[p]ython .*downloader\.stream_shioaji_taifex_bidask' || true)"
    if systemctl is-active --quiet stockagent-shioaji-taifex-bidask.service \
      && (( taifex_processes >= TAIFEX_WORKERS )); then
      echo "[shioaji-top200] priority_gate=taifex_collectors_running count=$taifex_processes"
      return 0
    fi
    local now_epoch
    now_epoch="$(date +%s)"
    if (( now_epoch - last_log >= 60 )); then
      echo "[shioaji-top200] waiting_seconds=5 reason=taifex_priority_gate"
      last_log="$now_epoch"
    fi
    sleep 5
  done
}

echo "[shioaji-top200] runner_started=$(TZ=Asia/Taipei date --iso-8601=seconds)"
while true; do
  delay="$(seconds_until_capture_window)"
  if (( delay > 0 )); then
    echo "[shioaji-top200] waiting_seconds=$delay reason=next_capture_window"
    sleep "$delay"
  fi

  wait_for_taifex_priority

  trade_date="$(TZ=Asia/Taipei date +%F)"
  echo "[shioaji-top200] universe_refresh date=$trade_date"
  run_fintech_python downloader/build_tw_top_market_cap_universe.py --count 200

  echo "[shioaji-top200] capture_start=$(TZ=Asia/Taipei date --iso-8601=seconds)"
  capture_id="$(run_fintech_python -c 'import uuid; print(uuid.uuid4().hex)')"
  echo "[shioaji-top200] capture_id=$capture_id"
  set +e
  run_fintech_python downloader/stream_shioaji_tw_microstructure.py \
    --simulation --universe "$UNIVERSE" --output-dir "$CAPTURE_ROOT" \
    --worker-index 0 --workers 2 --capture-id "$capture_id" \
    > >(tee -a "$RUN_ROOT/worker_0.log") 2>&1 &
  worker_0=$!
  sleep 6
  run_fintech_python downloader/stream_shioaji_tw_microstructure.py \
    --simulation --universe "$UNIVERSE" --output-dir "$CAPTURE_ROOT" \
    --worker-index 1 --workers 2 --capture-id "$capture_id" \
    > >(tee -a "$RUN_ROOT/worker_1.log") 2>&1 &
  worker_1=$!

  cleanup_workers() {
    kill -TERM "$worker_0" "$worker_1" 2>/dev/null || true
    wait "$worker_0" "$worker_1" 2>/dev/null || true
  }
  trap cleanup_workers TERM INT
  wait "$worker_0"
  rc_0=$?
  wait "$worker_1"
  rc_1=$?
  trap - TERM INT
  set -e
  echo "[shioaji-top200] worker_exit worker_0=$rc_0 worker_1=$rc_1"
  if (( rc_0 == 0 && rc_1 == 0 )); then
    audit_path="data_tw_microstructure/audits/$trade_date.json"
    run_fintech_python scripts/audit_shioaji_microstructure.py \
      --trade-date "$trade_date" --universe "$UNIVERSE" \
      --capture-root "$CAPTURE_ROOT" --output "$audit_path"
    run_fintech_python scripts/build_shioaji_hft_dataset.py \
      --trade-date "$trade_date" --universe "$UNIVERSE" \
      --capture-root "$CAPTURE_ROOT" --output-root "$HFT_DATASET_ROOT"
    run_fintech_python scripts/audit_shioaji_hft_dataset.py \
      --trade-date "$trade_date" --dataset-root "$HFT_DATASET_ROOT" \
      --output "data_tw_microstructure/audits/hft_$trade_date.json"
  else
    echo "[shioaji-top200] capture_failed date=$trade_date" >&2
  fi
  next_delay="$(seconds_until_next_capture_day)"
  echo "[shioaji-top200] waiting_seconds=$next_delay reason=next_capture_day"
  sleep "$next_delay"
done
