#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
set -a
source "${SHIOAJI_TAIFEX_ENV_FILE:-$repo_root/.env.futures}"
set +a
source scripts/runtime_env.sh
source scripts/shioaji_history_runner_common.sh

run_root="${SHIOAJI_HISTORICAL_MARKET_RUN_ROOT:-artifacts/data_repair/shioaji_historical_market_data}"
output_root="${SHIOAJI_HISTORICAL_MARKET_OUTPUT_ROOT:-data_tw_shioaji_history}"
max_queries="${SHIOAJI_HISTORICAL_MARKET_BATCH_QUERIES:-500}"
max_traffic_fraction="${SHIOAJI_HISTORICAL_MARKET_MAX_TRAFFIC_FRACTION:-0.90}"
inventory_refresh="${SHIOAJI_HISTORICAL_MARKET_INVENTORY_REFRESH_SECONDS:-3600}"
for value in "$max_queries" "$inventory_refresh"; do
  [[ "$value" =~ ^[0-9]+$ ]] && (( value >= 1 && value <= 10000 )) || exit 2
done
mkdir -p "$run_root"
exec 9>"$run_root/runner.lock"
flock -n 9 || exit 3
exec > >(tee -a "$run_root/run.log") 2>&1

last_inventory_refresh=0
while true; do
  delay="$(history_protected_delay)"
  if (( delay > 0 )); then history_wait "$delay" live_priority_window; continue; fi
  now_epoch="$(date +%s)"
  refresh_args=()
  if (( now_epoch - last_inventory_refresh < inventory_refresh )); then
    refresh_args+=(--no-refresh-inventory)
  fi
  history_wait 0 refresh_catalog_and_repair
  set +e
  run_fintech_python -m downloader.download_shioaji_historical_market_data \
    --simulation --output-dir "$output_root" --max-queries "$max_queries" \
    --max-traffic-fraction "$max_traffic_fraction" \
    --refresh-empty --prioritize-futures "${refresh_args[@]}"
  rc=$?
  set -e
  if (( ${#refresh_args[@]} == 0 && (rc == 0 || rc == 75 || rc == 76) )); then
    last_inventory_refresh="$now_epoch"
  fi
  case "$rc" in
    0)
      history_publish "$output_root/summary.json" exact tw-shioaji-history
      pending="$(run_fintech_python - "$output_root/summary.json" <<'PY'
import json, sys
print(int(json.load(open(sys.argv[1]))['pending_queries']))
PY
)"
      if (( pending > 0 )); then history_wait 30 next_repair_batch;
      else history_wait "$inventory_refresh" await_catalog_or_retry_deadline; fi
      ;;
    75) history_wait "$(history_quota_delay)" next_safe_quota_window ;;
    76)
      delay="$(history_protected_delay)"
      (( delay > 0 )) || delay=60
      history_wait "$delay" live_priority_window
      ;;
    79) history_wait 60 connection_capacity ;;
    *) history_wait 300 retry_failed_batch ;;
  esac
done
