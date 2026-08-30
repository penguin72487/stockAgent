#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

TW_PUBLIC_SNAPSHOT_ID="tw-public-20260820T015018219623218Z-l0-penguin-6716296ea30636ba"
run_fintech_python scripts/data_cache.py use tw-public \
  --snapshot-id "$TW_PUBLIC_SNAPSHOT_ID" \
  --ttl-days 365 \
  --link "$REPO_ROOT/data_tw_public"

run_fintech_python scripts/run_tw_day_trade_ablation.py \
  --spec configs/ablations/executable_portfolio_transformer_v5_model_performance_ofat_v1.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --parallel-jobs 1 \
  "$@"
