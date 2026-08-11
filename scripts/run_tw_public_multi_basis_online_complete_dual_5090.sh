#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
# This host's nvidia-smi topology: GPU0 -> NUMA3, GPU1 -> NUMA2.  train.py
# applies the matching set after assigning LOCAL_RANK, before panel/training work.
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_public_multi_basis_online_complete_dual_5090.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --cpu-threads 112 \
  --torch-compile-threads 16 \
  "$@"
