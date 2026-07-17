#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ENV_FILE="${SHIOAJI_ENV_FILE:-$REPO_ROOT/.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[shioaji-runner] missing env file: $ENV_FILE" >&2
  exit 2
fi
set -a
source "$ENV_FILE"
set +a
source scripts/runtime_env.sh

BACKFILL_END_DATE="${SHIOAJI_BACKFILL_END_DATE:-2026-07-17}"
RUN_ROOT="${SHIOAJI_RUN_ROOT:-artifacts/data_repair/shioaji_full}"
LOG_FILE="$RUN_ROOT/run.log"
LOCK_FILE="$RUN_ROOT/runner.lock"
mkdir -p "$RUN_ROOT"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[shioaji-runner] another full backfill runner already holds $LOCK_FILE" >&2
  exit 3
fi
exec > >(tee -a "$LOG_FILE") 2>&1

seconds_until_after_close() {
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
if now.weekday() >= 5:
    print(0)
elif now.time() < time(14, 31):
    target = datetime.combine(now.date(), time(14, 31), tzinfo=tz)
    print(max(0, int((target - now).total_seconds())))
else:
    print(0)
PY
}

seconds_until_next_weekday_close() {
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
target_date = now.date() + timedelta(days=1)
while target_date.weekday() >= 5:
    target_date += timedelta(days=1)
target = datetime.combine(target_date, time(14, 31), tzinfo=tz)
print(max(60, int((target - now).total_seconds())))
PY
}

echo "[shioaji-runner] started_at=$(TZ=Asia/Taipei date --iso-8601=seconds) end_date=$BACKFILL_END_DATE"

while true; do
  initial_delay="$(seconds_until_after_close)"
  if (( initial_delay > 0 )); then
    echo "[shioaji-runner] waiting_seconds=$initial_delay reason=taiwan_market_hours"
    sleep "$initial_delay"
  fi

  echo "[shioaji-runner] download_start=$(TZ=Asia/Taipei date --iso-8601=seconds)"
  set +e
  run_fintech_python downloader/download_shioaji_tw_kbars.py \
    --simulation \
    --start-date 2020-03-02 \
    --end-date "$BACKFILL_END_DATE"
  download_rc=$?
  set -e
  echo "[shioaji-runner] download_exit=$download_rc at=$(TZ=Asia/Taipei date --iso-8601=seconds)"

  summary_state="$(run_fintech_python - <<'PY'
import json
from pathlib import Path

path = Path("data_tw_public/shioaji/download_summary.json")
if not path.is_file():
    print("missing")
else:
    payload = json.loads(path.read_text(encoding="utf-8"))
    print(
        "complete=" + str(bool(payload.get("universe_coverage_complete"))).lower()
        + " reported=" + str(payload.get("reported_symbols", 0))
        + " done=" + str(payload.get("complete_symbols", 0))
        + " unavailable=" + str(payload.get("contract_unavailable_symbols", 0))
        + " failed=" + str(payload.get("failed_symbols", 0))
        + " partial=" + str(payload.get("partial_symbols", 0))
        + " traffic=" + str(payload.get("traffic_used_bytes"))
        + "/" + str(payload.get("traffic_limit_bytes"))
    )
PY
)"
  echo "[shioaji-runner] summary $summary_state"

  if [[ "$summary_state" == complete=true* ]]; then
    echo "[shioaji-runner] building hybrid dataset"
    run_fintech_python scripts/build_tw_shioaji_dataset.py
    run_fintech_python scripts/audit_tw_shioaji_dataset.py
    run_fintech_python -m pytest -q test/test_shioaji_tw_dataset.py
    echo "[shioaji-runner] completed_at=$(TZ=Asia/Taipei date --iso-8601=seconds)"
    exit 0
  fi

  next_delay="$(seconds_until_next_weekday_close)"
  echo "[shioaji-runner] waiting_seconds=$next_delay reason=next_daily_quota_window"
  sleep "$next_delay"
done
