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
if [[ ! -d "$minute_root" ]]; then
  if ! command -v stockagent-data >/dev/null 2>&1; then
    echo "[tw-day-trade-strict-minute] missing canonical minute data and stockagent-data CLI: $minute_root" >&2
    exit 2
  fi
  stockagent-data use tw-minute-train --link "$minute_root" >/dev/null
fi
if [[ ! -s "$minute_root/manifest.json" ]]; then
  echo "[tw-day-trade-strict-minute] missing canonical minute manifest: $minute_root/manifest.json" >&2
  exit 2
fi

echo "[tw-day-trade-strict-minute] 10M, commission discount 0.2, all features, 22 bases"
echo "[tw-day-trade-strict-minute] exact minute objective from 2020-03-02; daily proxy forbidden"
echo "[tw-day-trade-strict-minute] exact whole-lot forward + STE backward; global batch train=64 eval=32"
echo "[tw-day-trade-strict-minute] minute_root=$(readlink -f "$minute_root")"

run_fintech_python train.py \
  --config configs/markets/tw_day_trade_1m_strict_exact_2020.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --batch-size-train 64 \
  --batch-size-eval 32 \
  --cpu-threads 14 \
  --torch-compile-threads 8 \
  "$@"
