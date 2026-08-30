#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
env_file="${SHIOAJI_TAIFEX_ENV_FILE:-$repo_root/.env.futures}"
if [[ ! -f "$env_file" ]]; then
  echo "[shioaji-history-runner] missing env file: $env_file" >&2
  exit 2
fi
set -a
source "$env_file"
set +a
source scripts/runtime_env.sh

run_root="${SHIOAJI_HISTORICAL_MARKET_RUN_ROOT:-artifacts/data_repair/shioaji_historical_market_data}"
output_root="${SHIOAJI_HISTORICAL_MARKET_OUTPUT_ROOT:-data_tw_shioaji_history}"
log_file="$run_root/run.log"
max_queries="${SHIOAJI_HISTORICAL_MARKET_BATCH_QUERIES:-500}"
max_traffic_fraction="${SHIOAJI_HISTORICAL_MARKET_MAX_TRAFFIC_FRACTION:-0.90}"
connection_retry="${SHIOAJI_HISTORICAL_MARKET_CONNECTION_RETRY_SECONDS:-60}"
quota_recheck="${SHIOAJI_HISTORICAL_MARKET_QUOTA_RECHECK_SECONDS:-300}"
inventory_refresh="${SHIOAJI_HISTORICAL_MARKET_INVENTORY_REFRESH_SECONDS:-3600}"

for pair in \
  "max_queries:$max_queries:1:10000" \
  "connection_retry:$connection_retry:1:300" \
  "quota_recheck:$quota_recheck:60:3600" \
  "inventory_refresh:$inventory_refresh:300:86400"; do
  IFS=: read -r label value minimum maximum <<< "$pair"
  if [[ ! "$value" =~ ^[0-9]+$ ]] || (( value < minimum || value > maximum )); then
    echo "[shioaji-history-runner] $label must be an integer within $minimum..$maximum" >&2
    exit 2
  fi
done
run_fintech_python - "$max_traffic_fraction" <<'PY'
import sys
value = float(sys.argv[1])
if not 0.0 < value <= 0.90:
    raise SystemExit("max traffic fraction must be within (0, 0.90]")
PY

mkdir -p "$run_root"
exec 9>"$run_root/runner.lock"
flock -n 9 || exit 3
exec > >(tee -a "$log_file") 2>&1

protected_delay() {
  run_fintech_python - <<'PY'
from stockagent.live.shioaji_schedule import historical_query_pause_seconds
print(historical_query_pause_seconds())
PY
}

quota_delay() {
  run_fintech_python - "$quota_recheck" <<'PY'
from datetime import datetime, time, timedelta
import sys
from zoneinfo import ZoneInfo

now = datetime.now(ZoneInfo("Asia/Taipei"))
fallback = int(sys.argv[1])
if now.weekday() >= 5:
    print(fallback)
    raise SystemExit
target_date = now.date()
target = datetime.combine(target_date, time(14, 31), tzinfo=now.tzinfo)
if now >= target:
    target_date += timedelta(days=1)
    while target_date.weekday() >= 5:
        target_date += timedelta(days=1)
    target = datetime.combine(target_date, time(14, 31), tzinfo=now.tzinfo)
print(max(60, int((target - now).total_seconds())))
PY
}

summary_complete() {
  run_fintech_python - "$output_root/summary.json" <<'PY'
import json
from pathlib import Path
import sys
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
raise SystemExit(0 if payload.get("state") == "complete" and not payload.get("pending_queries") else 1)
PY
}

last_inventory_refresh=0
echo "[shioaji-history-runner] started=$(TZ=Asia/Taipei date --iso-8601=seconds) priority=weekly,monthly,exact_futures,indices max_traffic_fraction=$max_traffic_fraction batch_queries=$max_queries"
while true; do
  delay="$(protected_delay)"
  if (( delay > 0 )); then
    echo "[shioaji-history-runner] waiting_seconds=$delay reason=live_priority_window"
    sleep "$delay"
    continue
  fi

  now_epoch="$(date +%s)"
  refresh_args=()
  if (( now_epoch - last_inventory_refresh < inventory_refresh )); then
    refresh_args+=(--no-refresh-inventory)
  fi
  set +e
  run_fintech_python -m downloader.download_shioaji_historical_market_data \
    --simulation \
    --output-dir "$output_root" \
    --max-queries "$max_queries" \
    --max-traffic-fraction "$max_traffic_fraction" \
    "${refresh_args[@]}"
  rc=$?
  set -e
  if (( ${#refresh_args[@]} == 0 )); then
    last_inventory_refresh="$now_epoch"
  fi

  case "$rc" in
    0)
      if summary_complete; then
        echo "[shioaji-history-runner] state=complete waiting_seconds=$quota_recheck reason=await_catalog_or_session_advance"
        sleep "$quota_recheck"
      else
        echo "[shioaji-history-runner] state=resuming delay_seconds=2"
        sleep 2
      fi
      ;;
    75)
      delay="$(quota_delay)"
      echo "[shioaji-history-runner] waiting_seconds=$delay reason=next_safe_quota_window"
      sleep "$delay"
      ;;
    76)
      delay="$(protected_delay)"
      (( delay > 0 )) || delay=60
      echo "[shioaji-history-runner] waiting_seconds=$delay reason=live_priority_window"
      sleep "$delay"
      ;;
    79)
      echo "[shioaji-history-runner] waiting_seconds=$connection_retry reason=connection_capacity"
      sleep "$connection_retry"
      ;;
    *)
      echo "[shioaji-history-runner] failed rc=$rc" >&2
      exit "$rc"
      ;;
  esac
done
