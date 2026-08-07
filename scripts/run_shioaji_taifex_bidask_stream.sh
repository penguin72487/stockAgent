#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ENV_FILE="${SHIOAJI_ENV_FILE:-$REPO_ROOT/.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[shioaji-taifex] missing env file: $ENV_FILE" >&2
  exit 2
fi
set -a
source "$ENV_FILE"
set +a
source scripts/runtime_env.sh

RUN_ROOT="${SHIOAJI_TAIFEX_RUN_ROOT:-artifacts/data_capture/shioaji_taifex_bidask}"
CAPTURE_ROOT="${SHIOAJI_TAIFEX_CAPTURE_ROOT:-data_tw_index_derivatives_ticks/shioaji_fop_captures}"
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

echo "[shioaji-taifex] runner_started=$(TZ=Asia/Taipei date --iso-8601=seconds)"
while true; do
  delay="$(seconds_until_capture_window)"
  if (( delay > 0 )); then
    echo "[shioaji-taifex] waiting_seconds=$delay reason=next_capture_window"
    sleep "$delay"
  fi
  trade_date="$(TZ=Asia/Taipei date +%F)"
  capture_id="$(run_fintech_python -c 'import uuid; print(uuid.uuid4().hex)')"
  echo "[shioaji-taifex] capture_start=$(TZ=Asia/Taipei date --iso-8601=seconds) capture_id=$capture_id"
  set +e
  run_fintech_python downloader/stream_shioaji_taifex_bidask.py \
    --simulation \
    --capture-id "$capture_id" \
    --output-dir "$CAPTURE_ROOT"
  capture_rc=$?
  set -e
  echo "[shioaji-taifex] capture_exit=$capture_rc date=$trade_date"
  if (( capture_rc == 0 )); then
    run_fintech_python scripts/audit_shioaji_taifex_bidask_capture.py \
      --trade-date "$trade_date" \
      --capture-root "$CAPTURE_ROOT"
  fi
  next_delay="$(seconds_until_next_capture_day)"
  echo "[shioaji-taifex] waiting_seconds=$next_delay reason=next_capture_day"
  sleep "$next_delay"
done
