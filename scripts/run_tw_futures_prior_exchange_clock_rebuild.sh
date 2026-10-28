#!/usr/bin/env bash
# Canonical 08:45/09:00 clock rebuild for the source-audited v18 release.
set -euo pipefail
cd /root/stockAgent
export FINTECH_ENV_PATH="$(sed -n 's/^environment=FINTECH_ENV_PATH="\([^"]*\)".*/\1/p' /etc/supervisor/conf.d/stockagent-futures-halt345-v9-complete.conf)"
[[ -n "$FINTECH_ENV_PATH" ]]
export STOCKAGENT_WORKSPACE=/root/stockAgent
export STOCKAGENT_FROZEN_SOURCE=/root/stockAgent/artifacts/code_releases/tw_futures_spread_delta_clock_20261028_v3/20261027T163243165417Z-3b31101d252c/build-source
source "$STOCKAGENT_FROZEN_SOURCE/scripts/runtime_env.sh"
export POLARS_MAX_THREADS=4 OMP_NUM_THREADS=4
export PYTHONPATH="$STOCKAGENT_FROZEN_SOURCE"

root=data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1
source_root=data_tw_index_futures/preparation_sources/universe765_repair_20261027_v1
old="$root/financial_all_twd_v13_dl_early2011_research/release"
new="$root/financial_all_twd_v18_prior_exchange_liquidity_v1/release"
old_clock="$root/execution_all_twd_v17_dl_early2011_v1/all_twd/clocks"
new_clock="$root/execution_all_twd_v18_prior_exchange_v1/all_twd/clocks"
checkpoint="$old_clock/bulk_readback_checkpoints/3a0a8ea306a4a1a5ce1b66f4d1c6dedde0eaccf5ec580b1fc2e609ece10e57fd"
spec="$source_root/ordinary_rule_margin_delta_v12_ewf_reference_rebind/specification_delta/specifications.parquet"
quarantine="$source_root/execution_quarantine_v18_prior_exchange_v1.json"
formal=artifacts/markets/tw_futures_universe765_recovery_20261027_v1

[[ ! -e "$new_clock" && ! -e "$quarantine" ]]
run_fintech_python artifacts/operations/futures-universe765-repair-20261027-v1/preflight-prior-exchange-clock-v1.py \
  --old-daily "$old/daily/continuous_daily.parquet" --old-rules "$old/rules/rules.parquet" \
  --new-daily "$new/daily/continuous_daily.parquet" --new-rules "$new/rules/rules.parquet" \
  --old-quarantine "$source_root/execution_quarantine_v17_dl_early2011_v1.json" \
  --gaps "$old_clock/coverage_gaps.parquet" --spec-path "$spec" --checkpoint "$checkpoint" \
  --frozen-receipt "${STOCKAGENT_FROZEN_SOURCE%/build-source}/release.json" \
  --publication "$formal/prior_exchange_candidate_publication_v1.json" \
  --output-quarantine "$quarantine" --report "$formal/prior_exchange_clock_preflight_v1.json" \
  --start 2011-01-03 --end 2026-09-24

run_fintech_python "$STOCKAGENT_FROZEN_SOURCE/scripts/prepare_tw_futures_decision_clocks.py" \
  --daily "$new/daily/continuous_daily.parquet" --rules "$new/rules/rules.parquet" \
  --output "$new_clock" --start 2011-01-03 --end 2026-09-24 --asset-scope all_twd \
  --finmind-bulk-root data_finmind/preparation_sources/stock_futures_history2011_20261021_v5/tick_inventory.json \
  --block-manifest data_tw_index_futures/preparation_sources/stock_futures_block_history2011_20261021_v5/manifest.json \
  --tick-root data_tw_futures/shioaji_history --entry-specifications "$spec" \
  --community-rpt-inventory "$source_root/public_rpt_mirror_20261027_v1/inventory-532-v1.json" \
  --official-block-detail-inventory "$source_root/official_block_detail_v1/manifest.json" \
  --source-workers 4 --entry-window-minutes 1 --also-entry-window-minutes 300 \
  --with-opening-reference --execution-quarantine "$quarantine" \
  --reuse-bulk-checkpoint "$checkpoint" \
  --reuse-bulk-source-receipt "${STOCKAGENT_FROZEN_SOURCE%/build-source}/release.json"
