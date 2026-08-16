#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source "$repo_root/scripts/runtime_env.sh"

state_dir="$repo_root/data_tw_index_options_daily/state"
mkdir -p "$state_dir"
exec 9>"$state_dir/registered_auxiliary_daily.lock"
if ! flock -n 9; then
  echo "[taifex-auxiliary] another refresh owns the lock; leaving it running"
  exit 0
fi

run_fintech_python "$repo_root/scripts/download_taifex_option_daily_history.py" \
  --output-dir "$repo_root/data_tw_index_options_daily" \
  --futures-path "$repo_root/data_tw_index_futures/day_session_contracts.parquet" \
  --start-year 2001 \
  --series-scope all

run_fintech_python "$repo_root/scripts/download_taifex_recent_index_derivatives_ticks.py" \
  --output-dir "$repo_root/data_tw_index_derivatives_ticks" \
  --days 30

run_fintech_python "$repo_root/scripts/download_taifex_final_settlement_history.py" \
  --output-dir "$repo_root/data_tw_index_options_daily" \
  --refresh
