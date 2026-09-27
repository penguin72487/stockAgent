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

# Independent public sources must still run if another endpoint is unavailable.
# The overall exit status remains nonzero, so release publication stays closed.
failed=0
run_stage() {
  local stage="$1"
  shift
  if "$@"; then
    echo "[taifex-public-history] $stage finished"
  else
    local result=$?
    echo "[taifex-public-history] $stage failed ($result); continuing independent sources" >&2
    failed=1
  fi
}

# Download through today's official session if available. Causal availability
# is a separate calendar gate; a missing next session must not delay acquisition.
end_date="$(TZ=Asia/Taipei date +%F)"
history_args=(
  "$repo_root/scripts/download_taifex_public_history.py"
  --output-dir "$repo_root/data_taifex_public_history"
  --session-parquet "$repo_root/data_tw_index_futures/day_session_contracts.parquet"
  --end-date "$end_date"
)

if (( $# == 0 )); then
  for phase in put-call positioning large-trader-range; do
    run_stage "$phase" run_fintech_python "${history_args[@]}" --phase "$phase"
  done
  run_stage openapi run_fintech_python "$repo_root/scripts/download_taifex_openapi_catalog.py" \
    --output-dir "$repo_root/data_taifex_public_history"
  run_stage vix run_fintech_python "$repo_root/scripts/download_taifex_vix_recent.py" \
    --output-dir "$repo_root/data_taifex_public_history" \
    --session-parquet "$repo_root/data_tw_index_futures/day_session_contracts.parquet"
else
  run_stage requested-history run_fintech_python "${history_args[@]}" "$@"
fi
exit "$failed"
