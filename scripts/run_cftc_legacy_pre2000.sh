#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source "$repo_root/scripts/runtime_env.sh"

state_dir="$repo_root/data_cftc_legacy/state"
mkdir -p "$state_dir"
exec 9>"$state_dir/collector.lock"
if ! flock -n 9; then
  echo "[cftc-legacy] another collector owns the lock; leaving it running"
  exit 0
fi

run_fintech_python "$repo_root/scripts/download_cftc_legacy_pre2000.py" \
  --output-dir "$repo_root/data_cftc_legacy" \
  "$@"
