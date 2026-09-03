#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source "$repo_root/scripts/runtime_env.sh"

state_dir="$repo_root/data_taifex_public_history/state"
mkdir -p "$state_dir"
exec 9>"$state_dir/collector.lock"
if ! flock -n 9; then
  echo "[taifex-public-history] another collector owns the lock; leaving it running"
  exit 0
fi

run_fintech_python "$repo_root/scripts/download_taifex_public_history.py" \
  --output-dir "$repo_root/data_taifex_public_history" \
  --session-parquet "$repo_root/data_tw_index_futures/day_session_contracts.parquet" \
  "$@"

if (( $# == 0 )); then
  run_fintech_python "$repo_root/scripts/download_taifex_openapi_catalog.py" \
    --output-dir "$repo_root/data_taifex_public_history"
  run_fintech_python "$repo_root/scripts/download_taifex_vix_recent.py" \
    --output-dir "$repo_root/data_taifex_public_history" \
    --session-parquet "$repo_root/data_tw_index_futures/day_session_contracts.parquet"
fi
