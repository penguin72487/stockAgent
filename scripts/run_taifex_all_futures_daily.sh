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

# Keep the contract-v4 training panel causally synchronized with the official
# source archive. New adjusted/listed product codes must enter through the
# official TAIFEX master; an unknown code fails closed in the dataset builder.
run_fintech_python "$repo_root/scripts/download_taifex_contract_codes.py" \
  --output "$repo_root/data_tw_futures/taifex_contract_codes.csv"

run_fintech_python "$repo_root/scripts/build_taifex_futures_portfolio_daily.py" \
  --source "$repo_root/data_tw_index_futures/all_futures_daily_sessions.parquet" \
  --official-product-codes "$repo_root/data_tw_futures/taifex_contract_codes.csv" \
  --output-root "$repo_root/data_tw_futures/taifex_portfolio_daily_v4"
