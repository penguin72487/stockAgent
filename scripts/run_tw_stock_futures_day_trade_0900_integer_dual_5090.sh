#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

required_data=(
  data_tw_futures/taifex_portfolio_daily_v4/manifest.json
  data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet
  data_tw_public/features/tw_public_stock_daily.parquet
)
for path in "${required_data[@]}"; do
  if [[ ! -s "$path" ]]; then
    echo "[tw-stock-futures-integer] missing required artifact: $path" >&2
    exit 2
  fi
done

echo "[tw-stock-futures-integer] exact integer standard+mini contracts; full absolute-notional collateral; TWD 40/contract/side"
echo "[tw-stock-futures-integer] daily 08:45 OPEN-to-CLOSE remains a counterfactual proxy for the 09:00 decision"

run_fintech_python train.py \
  --config configs/markets/tw_stock_futures_day_trade_0900_integer_full_features_multi_basis_projection_l1_cash_capital10m.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --cpu-threads 56 \
  --torch-compile-threads 16 \
  "$@"
