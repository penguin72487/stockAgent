#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

DAY_CONTEXT=data_tw_index_futures/all_products_day_session_front.parquet
NIGHT_CONTEXT=data_tw_index_futures/all_products_afterhours_front.parquet
if [[ ! -s "$DAY_CONTEXT" && ! -s "$NIGHT_CONTEXT" ]]; then
  run_fintech_python scripts/build_tw_all_futures_day_context.py --session both
elif [[ ! -s "$DAY_CONTEXT" ]]; then
  run_fintech_python scripts/build_tw_all_futures_day_context.py --session regular
elif [[ ! -s "$NIGHT_CONTEXT" ]]; then
  run_fintech_python scripts/build_tw_all_futures_day_context.py --session afterhours
fi

run_fintech_python train.py \
  --config configs/markets/tw_index_futures_day_directional_night_dual_5090_v11.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --cpu-threads 112 \
  --torch-compile-threads 16 \
  "$@"
