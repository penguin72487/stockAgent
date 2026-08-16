#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source "$repo_root/scripts/runtime_env.sh"

state_dir="$repo_root/data_tw_index_futures/state"
mkdir -p "$state_dir"
exec 9>"$state_dir/all_futures_daily.lock"
if ! flock -n 9; then
  echo "[taifex-futures-daily] another refresh owns the lock; leaving it running"
  exit 0
fi

run_fintech_python "$repo_root/scripts/download_tw_index_futures_day_session.py" \
  --output-dir "$repo_root/data_tw_index_futures" \
  --start-year 1998
