#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
# Current host topology: GPU0 -> NUMA3, GPU1 -> NUMA2.  Each rank receives the
# 14 physical cores plus their SMT siblings nearest its GPU.
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

minute_root="$REPO_ROOT/data_tw_minute/research_dataset"
if [[ ! -d "$minute_root" ]]; then
  if ! command -v stockagent-data >/dev/null 2>&1; then
    echo "[tw-day-trade-1m] missing canonical minute data and stockagent-data CLI: $minute_root" >&2
    exit 2
  fi
  stockagent-data use tw-minute-train --link "$minute_root" >/dev/null
fi
if [[ ! -s "$minute_root/manifest.json" ]]; then
  echo "[tw-day-trade-1m] missing canonical minute manifest: $minute_root/manifest.json" >&2
  exit 2
fi

echo "[tw-day-trade-1m] 10M commission20 multi-basis FinancialTransformer + projection-L1"
echo "[tw-day-trade-1m] adverse daily proxy before minute history, exact minute execution thereafter"
echo "[tw-day-trade-1m] DDP global train/eval batch=128 (64/rank); proxy starts 2014-01-06"
echo "[tw-day-trade-1m] minute_root=$(readlink -f "$minute_root")"

run_fintech_python train.py \
  --config configs/markets/tw_day_trade_1m_realistic.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --batch-size-train 128 \
  --batch-size-eval 128 \
  --cpu-threads 14 \
  --torch-compile-threads 8 \
  "$@"
