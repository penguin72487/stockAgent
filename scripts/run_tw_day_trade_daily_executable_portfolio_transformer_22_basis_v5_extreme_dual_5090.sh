#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/tw_day_trade_daily_executable_portfolio_transformer_22_basis_tplus2_close_capital10m_v5_extreme.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --cpu-threads 112 \
  --torch-compile-threads 16 \
  "$@"
