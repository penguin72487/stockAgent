#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

required_data=(
  data_tw_futures/taifex_portfolio_daily_v4/manifest.json
  data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet
  data_tw_futures/final_settlement_v1/manifest.json
  data_tw_futures/final_settlement_v1/futures_final_settlement_history.parquet
  data_tw_public/features/tw_public_stock_daily.parquet
)
for path in "${required_data[@]}"; do
  if [[ ! -s "${path}" ]]; then
    echo "[all-futures-carry-to-expiry] missing required artifact: ${path}" >&2
    if [[ "${path}" == data_tw_futures/final_settlement_v1/* ]]; then
      echo "[all-futures-carry-to-expiry] build it with:" >&2
      echo "  run_fintech_python scripts/download_taifex_futures_final_settlement_history.py --start-date 2014-01-01" >&2
    fi
    exit 2
  fi
done

echo "[all-futures-carry-to-expiry] 99 stock features; 22 families/524 components; TWD 10M"
echo "[all-futures-carry-to-expiry] exact whole contracts; prior-volume 50%; cross-session carry; settlement-valued expiry close"
echo "[all-futures-carry-to-expiry] missing official expiry settlement quarantines the complete physical contract; no redistribution"
echo "[all-futures-carry-to-expiry] full-notional collateral is a conservative research assumption; no stock T+3 ledger"

# train.py owns fold isolation and DDP. Do not wrap this command in torchrun.
run_fintech_python train.py \
  --config configs/markets/tw_stock_context_all_futures_carry_to_expiry_0845_integer_22_effective_rank_full_features_multi_basis_projection_l1_cash_capital10m.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --cpu-threads 56 \
  --torch-compile-threads 16 \
  "$@"
