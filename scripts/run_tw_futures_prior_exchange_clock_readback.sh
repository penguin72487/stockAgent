#!/usr/bin/env bash
# Read back the v18 clock from the pinned source without changing the release.
set -euo pipefail
cd /root/stockAgent
export FINTECH_ENV_PATH="$(sed -n 's/^environment=FINTECH_ENV_PATH="\([^"]*\)".*/\1/p' /etc/supervisor/conf.d/stockagent-futures-halt345-v9-complete.conf)"
[[ -n "$FINTECH_ENV_PATH" ]]
export STOCKAGENT_WORKSPACE=/root/stockAgent
export STOCKAGENT_FROZEN_SOURCE=/root/stockAgent/artifacts/code_releases/tw_futures_spread_delta_clock_20261028_v3/20261027T163243165417Z-3b31101d252c/build-source
source "$STOCKAGENT_FROZEN_SOURCE/scripts/runtime_env.sh"
export PYTHONPATH="$STOCKAGENT_FROZEN_SOURCE" POLARS_MAX_THREADS=4 OMP_NUM_THREADS=4

root=data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1
formal=artifacts/markets/tw_futures_universe765_recovery_20261027_v1
run_fintech_python artifacts/operations/futures-universe765-repair-20261027-v1/readback-prior-exchange-clocks-v2.py \
  --receipt "${STOCKAGENT_FROZEN_SOURCE%/build-source}/release.json" \
  --preflight "$formal/prior_exchange_clock_preflight_v1.json" \
  --old-release "$root/financial_all_twd_v13_dl_early2011_research/release" \
  --new-release "$root/financial_all_twd_v18_prior_exchange_liquidity_v1/release" \
  --old-clock "$root/execution_all_twd_v17_dl_early2011_v1/all_twd/clocks" \
  --new-clock "$root/execution_all_twd_v18_prior_exchange_v1/all_twd/clocks" \
  --output "$formal/prior_exchange_clock_frozen_readback_v2.json"
