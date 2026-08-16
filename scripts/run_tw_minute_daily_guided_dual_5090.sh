#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
# nvidia-smi topo -m on the measured dual-RTX-5090 host: GPU0 -> NUMA3 and
# GPU1 -> NUMA2.  Callers may override this when the physical topology differs.
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"

source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_minute_daily_guided_dual_5090.yaml \
  "$@"
