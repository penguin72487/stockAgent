#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

minute_root="$REPO_ROOT/data_tw_minute/research_dataset"
source_root="$REPO_ROOT/artifacts/markets/tw_day_trade_daily_multi_basis_22_effective_rank_projection_l1_tplus2_close_commission20_capital10m_v1"
if [[ ! -s "$minute_root/manifest.json" ]]; then
  echo "[tw-day-trade-pretrained] missing minute manifest: $minute_root/manifest.json" >&2
  exit 2
fi
if [[ ! -s "$source_root/run_manifest.json" ]]; then
  echo "[tw-day-trade-pretrained] missing source baseline: $source_root/run_manifest.json" >&2
  exit 2
fi

echo "[tw-day-trade-pretrained] old 22-basis alpha -> exact target execution loss"
echo "[tw-day-trade-pretrained] 10M, commission discount 0.2, 99 features, batch 64/32"
echo "[tw-day-trade-pretrained] epoch 0 exact validation guard; 1000 formal epochs"

run_fintech_python train.py \
  --config configs/markets/tw_day_trade_1m_hybrid_22_effective_rank_pretrained_guard_v11.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --batch-size-train 64 \
  --batch-size-eval 32 \
  --cpu-threads 14 \
  --torch-compile-threads 8 \
  "$@"
