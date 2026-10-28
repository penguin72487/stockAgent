#!/usr/bin/env bash
set -euo pipefail
cd /root/stockAgent
export FINTECH_ENV_PATH="$(sed -n 's/^environment=FINTECH_ENV_PATH="\([^"]*\)".*/\1/p' /etc/supervisor/conf.d/stockagent-futures-halt345-v9-complete.conf)"
[[ -n "$FINTECH_ENV_PATH" ]]
export STOCKAGENT_WORKSPACE=/root/stockAgent
code=/root/stockAgent/artifacts/code_releases/tw_futures_prior_exchange_liquidity_20261028_v1/20261027T220725138272Z-ff1e3b8ae1ee/build-source
source "$code/scripts/runtime_env.sh"
export PYTHONPATH="$code" POLARS_MAX_THREADS=4 OMP_NUM_THREADS=4
run_fintech_python artifacts/operations/futures-universe765-repair-20261027-v1/readback-prior-exchange-publication-v1.py \
  --receipt "${code%/build-source}/release.json" \
  --publication artifacts/markets/tw_futures_universe765_recovery_20261027_v1/prior_exchange_candidate_publication_v1.json \
  --final-settlement data_tw_index_futures/preparation_sources/margin_sources/final/futures_final_settlement_history.parquet \
  --output artifacts/markets/tw_futures_universe765_recovery_20261027_v1/prior_exchange_frozen_publication_readback_v1.json
