#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export CUDA_DEVICE_ORDER="${CUDA_DEVICE_ORDER:-PCI_BUS_ID}"
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"

# AMD EPYC 7662: 64 physical cores / 128 logical threads.  The DDP launcher
# inside train.py splits this across two ranks; keep per-rank defaults bounded.
export STOCKAGENT_CPU_THREADS="${STOCKAGENT_CPU_THREADS:-64}"
export STOCKAGENT_TORCH_COMPILE_THREADS="${STOCKAGENT_TORCH_COMPILE_THREADS:-16}"
export STOCKAGENT_POLARS_THREADS="${STOCKAGENT_POLARS_THREADS:-64}"
export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-$STOCKAGENT_POLARS_THREADS}"
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-$STOCKAGENT_POLARS_THREADS}"
export STOCKAGENT_BACKTEST_COMPILE_PREP="${STOCKAGENT_BACKTEST_COMPILE_PREP:-1}"

export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"
export PYTORCH_ALLOC_CONF="${PYTORCH_ALLOC_CONF:-$PYTORCH_CUDA_ALLOC_CONF}"
export TORCHINDUCTOR_CACHE_DIR="${TORCHINDUCTOR_CACHE_DIR:-$HOME/.cache/torchinductor}"
export TRITON_CACHE_DIR="${TRITON_CACHE_DIR:-$HOME/.cache/triton}"
export CUDA_CACHE_PATH="${CUDA_CACHE_PATH:-$HOME/.cache/nv_cuda}"

exec ./coda_runner.sh -c configs/markets/tw_parallel.yaml -- "$@"
