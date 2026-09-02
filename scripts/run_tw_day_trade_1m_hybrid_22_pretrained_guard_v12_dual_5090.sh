#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"
source scripts/runtime_env.sh

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-8}"
export STOCKAGENT_STRICT_NO_FALLBACK="${STOCKAGENT_STRICT_NO_FALLBACK:-1}"

run_fintech_python train.py \
  --config configs/markets/tw_day_trade_1m_hybrid_22_effective_rank_pretrained_guard_v12.yaml \
  "$@"
