#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ENV_FILE="${SHIOAJI_ENV_FILE:-$REPO_ROOT/.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[shioaji-minute-runner] missing env file: $ENV_FILE" >&2
  exit 2
fi
set -a
source "$ENV_FILE"
set +a
source scripts/runtime_env.sh

RUN_ROOT="${SHIOAJI_MINUTE_RUN_ROOT:-artifacts/data_repair/shioaji_minute_full}"
LOG_FILE="$RUN_ROOT/run.log"
LOCK_FILE="$RUN_ROOT/runner.lock"
TARGET_FILE="$RUN_ROOT/target_end_date.txt"
mkdir -p "$RUN_ROOT"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[shioaji-minute-runner] another runner holds $LOCK_FILE" >&2
  exit 3
fi
exec > >(tee -a "$LOG_FILE") 2>&1

if [[ -n "${SHIOAJI_MINUTE_END_DATE:-}" ]]; then
  BACKFILL_END_DATE="$SHIOAJI_MINUTE_END_DATE"
elif [[ -s "$TARGET_FILE" ]]; then
  BACKFILL_END_DATE="$(tr -d '[:space:]' < "$TARGET_FILE")"
else
  BACKFILL_END_DATE="$(TZ=Asia/Taipei date -d yesterday +%F)"
  printf '%s\n' "$BACKFILL_END_DATE" > "$TARGET_FILE"
fi

seconds_until_after_close() {
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
if now.weekday() >= 5:
    print(0)
elif time(8, 30) <= now.time() <= time(14, 30):
    target = datetime.combine(now.date(), time(14, 31), tzinfo=tz)
    print(max(60, int((target - now).total_seconds())))
else:
    print(0)
PY
}

seconds_until_next_quota_window() {
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
target = datetime.combine(now.date() + timedelta(days=1), time(0, 5), tzinfo=tz)
print(max(60, int((target - now).total_seconds())))
PY
}

echo "[shioaji-minute-runner] started_at=$(TZ=Asia/Taipei date --iso-8601=seconds) end_date=$BACKFILL_END_DATE"

while true; do
  market_delay="$(seconds_until_after_close)"
  if (( market_delay > 0 )); then
    echo "[shioaji-minute-runner] waiting_seconds=$market_delay reason=taiwan_market_hours"
    sleep "$market_delay"
  fi

  echo "[shioaji-minute-runner] download_start=$(TZ=Asia/Taipei date --iso-8601=seconds)"
  set +e
  run_fintech_python downloader/download_shioaji_tw_minute_kbars.py \
    --simulation \
    --all-symbols \
    --start-date 2020-03-02 \
    --end-date "$BACKFILL_END_DATE"
  download_rc=$?
  set -e
  echo "[shioaji-minute-runner] download_exit=$download_rc at=$(TZ=Asia/Taipei date --iso-8601=seconds)"

  summary_state="$(run_fintech_python - <<'PY'
import json
from pathlib import Path

path = Path("data_tw_minute/shioaji_1m/download_summary.json")
if not path.is_file():
    print("missing")
else:
    payload = json.loads(path.read_text(encoding="utf-8"))
    print(
        "complete=" + str(bool(payload.get("selected_coverage_complete"))).lower()
        + " selected=" + str(payload.get("selected_symbols", 0))
        + " reported=" + str(payload.get("reported_symbols", 0))
        + " done=" + str(payload.get("complete_symbols", 0))
        + " unavailable=" + str(payload.get("contract_unavailable_symbols", 0))
        + " failed=" + str(payload.get("failed_symbols", 0))
        + " partial=" + str(payload.get("partial_symbols", 0))
        + " traffic_stop=" + str(bool(payload.get("stopped_for_traffic"))).lower()
        + " market_stop=" + str(bool(payload.get("stopped_for_market_hours"))).lower()
        + " traffic=" + str(payload.get("traffic_used_bytes"))
        + "/" + str(payload.get("traffic_limit_bytes"))
    )
PY
)"
  echo "[shioaji-minute-runner] summary $summary_state"

  if [[ "$summary_state" == complete=true* ]]; then
    echo "[shioaji-minute-runner] building full-market research dataset"
    run_fintech_python scripts/build_shioaji_tw_minute_dataset.py
    latest_date="$(
      run_fintech_python - <<'PY'
import json
from pathlib import Path
payload = json.loads(Path("data_tw_minute/research_dataset/manifest.json").read_text())
print(payload["dates"][-1])
PY
    )"
    run_fintech_python scripts/audit_shioaji_tw_minute_dataset.py \
      --trade-date "$latest_date"
    run_fintech_python scripts/research_shioaji_tw_minute_strategies.py \
      --holding-mode minute_rebalance \
      --first-decision-minute 1 \
      --last-decision-minute 269
    run_fintech_python -m pytest -q \
      test/test_shioaji_tw_minute_kbars.py
    echo "[shioaji-minute-runner] completed_at=$(TZ=Asia/Taipei date --iso-8601=seconds)"
    exit 0
  fi

  if [[ "$summary_state" == *"market_stop=true"* ]]; then
    next_delay="$(seconds_until_after_close)"
    if (( next_delay <= 0 )); then
      next_delay=300
    fi
    echo "[shioaji-minute-runner] waiting_seconds=$next_delay reason=market_hours"
  else
    next_delay="$(seconds_until_next_quota_window)"
    echo "[shioaji-minute-runner] waiting_seconds=$next_delay reason=next_quota_window"
  fi
  sleep "$next_delay"
done
