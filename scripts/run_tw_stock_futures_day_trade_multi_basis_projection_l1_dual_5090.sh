#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

# Keep this hardware alias thin; source preparation and training belong to the
# normal 08:45 launcher.
exec bash scripts/run_tw_stock_futures_day_trade_0845_minute.sh \
  --multi-gpu-strategy distributed_data_parallel \
  --cpu-threads 56 \
  --torch-compile-threads 16 \
  "$@"
