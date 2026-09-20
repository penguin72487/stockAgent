#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
set -a
source "${SHIOAJI_TAIFEX_ENV_FILE:-$repo_root/.env.futures}"
set +a
source scripts/runtime_env.sh
source scripts/shioaji_history_runner_common.sh

run_root="${SHIOAJI_FUTURES_HISTORY_RUN_ROOT:-artifacts/data_repair/shioaji_futures_history}"
aliases="${SHIOAJI_FUTURES_ALIAS_FILE:-data_tw_futures/shioaji_contracts/continuous_contracts.csv}"
calendar="${SHIOAJI_FUTURES_HISTORY_CALENDAR_FILE:-data_tw_index_futures/day_session_contracts.parquet}"
query_calendar="$run_root/receipt_verified_query_calendar.parquet"
max_traffic_fraction="${SHIOAJI_FUTURES_HISTORY_MAX_TRAFFIC_FRACTION:-0.90}"
batch_queries="${SHIOAJI_FUTURES_HISTORY_BATCH_QUERIES:-64}"
recheck="${SHIOAJI_FUTURES_HISTORY_TARGET_RECHECK_SECONDS:-3600}"
[[ "$batch_queries" =~ ^[0-9]+$ ]] && (( batch_queries >= 1 && batch_queries <= 10000 )) || exit 2
[[ "$recheck" =~ ^[0-9]+$ ]] && (( recheck >= 60 && recheck <= 10000 )) || exit 2
mkdir -p "$run_root"
exec 9>"$run_root/runner.lock"
flock -n 9 || exit 3
exec > >(tee -a "$run_root/run.log") 2>&1

while true; do
  delay="$(history_protected_delay)"
  if (( delay > 0 )); then history_wait "$delay" live_priority_window; continue; fi
  end_date="$(run_fintech_python - "$calendar" "$query_calendar" <<'PY'
from pathlib import Path
import sys
from downloader.shioaji_history_repair import prepare_query_calendar
print(prepare_query_calendar(Path(sys.argv[1]), Path(sys.argv[2]), Path('data_tw_public')))
PY
)"
  printf '%s\n' "$end_date" > "$run_root/target_end_date.txt"
  history_wait 0 refresh_catalog_and_repair
  set +e
  run_fintech_python -m downloader.download_shioaji_tx_futures_ticks \
    --simulation --contracts-file "$aliases" --refresh-inventory --refresh-empty \
    --calendar-path "$query_calendar" --end-date "$end_date" \
    --batch-receipt "$run_root/latest_batch.json" --max-dates "$batch_queries" \
    --dates-per-contract 32 --empty-probes-per-contract 2 \
    --max-traffic-fraction "$max_traffic_fraction"
  rc=$?
  set -e
  if (( rc == 0 )); then
    history_publish "$run_root/latest_batch.json" continuous tw-futures tw-index-futures
    history_wait "$recheck" next_incremental_sweep
  elif (( rc == 75 )); then
    history_wait "$(history_quota_delay)" next_safe_quota_window
  elif (( rc == 76 )); then
    delay="$(history_protected_delay)"
    (( delay > 0 )) || delay=60
    history_wait "$delay" live_priority_window
  elif (( rc == 79 )); then
    history_wait 60 connection_capacity
  else
    history_wait 300 retry_failed_batch
  fi
done
