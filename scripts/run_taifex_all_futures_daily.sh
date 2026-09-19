#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source "$repo_root/scripts/runtime_env.sh"

# This runner is scheduled after the TAIFEX day session has completed.  The
# downloader's generic default is yesterday so that ad-hoc daytime invocations
# fail conservatively, but using that default here made the scheduled archive
# permanently one session late.
target_session="${TAIFEX_FUTURES_TARGET_SESSION:-$(TZ=Asia/Taipei date +%F)}"
if [[ ! "$target_session" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "[taifex-futures-daily] invalid target session: $target_session" >&2
  exit 2
fi

state_dir="$repo_root/data_tw_index_futures/state"
mkdir -p "$state_dir"
exec 9>"$state_dir/all_futures_daily.lock"
if ! flock -n 9; then
  echo "[taifex-futures-daily] another refresh owns the lock; leaving it running"
  exit 0
fi

run_fintech_python "$repo_root/scripts/download_tw_index_futures_day_session.py" \
  --output-dir "$repo_root/data_tw_index_futures" \
  --start-year 1998 \
  --end-date "$target_session"

# Keep the contract-v4 training panel causally synchronized with the official
# source archive. New adjusted/listed product codes must enter through the
# official TAIFEX master; an unknown code fails closed in the dataset builder.
run_fintech_python "$repo_root/scripts/download_taifex_contract_codes.py" \
  --output "$repo_root/data_tw_futures/taifex_contract_codes.csv"

run_fintech_python "$repo_root/scripts/build_taifex_futures_portfolio_daily.py" \
  --source "$repo_root/data_tw_index_futures/all_futures_daily_sessions.parquet" \
  --official-product-codes "$repo_root/data_tw_futures/taifex_contract_codes.csv" \
  --output-root "$repo_root/data_tw_futures/taifex_portfolio_daily_v4"

# Preserve official final settlement separately from daily settlement.  The
# collector reuses immutable old receipts and refreshes only the open year and
# recent delivery months on scheduled runs.
run_fintech_python "$repo_root/scripts/download_taifex_futures_final_settlement_history.py" \
  --start-date 2014-01-01 \
  --end-date "$target_session" \
  --output-dir "$repo_root/data_tw_futures/final_settlement_v1" \
  --refresh-current

# Read-only dashboard enrichment has a separate acceptance gate. A failure is
# visible in maintenance status, but must not prevent the canonical panel build.
run_fintech_python "$repo_root/scripts/download_taifex_contract_codes.py" \
  --output "$repo_root/data_tw_futures/taifex_contract_codes.csv" \
  --stock-futures-only
