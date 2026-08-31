#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

required_data=(
  data_tw_futures/taifex_portfolio_daily_v4/manifest.json
  data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet
  data_tw_public/features/tw_public_stock_daily.parquet
  data_tw_public/stocks/panel_cache_v2/variants/94e53e8113f5f84106cbe1bdc7891ba4c5e4002370773bcee2c4005420375571.json
)
for path in "${required_data[@]}"; do
  if [[ ! -s "$path" ]]; then
    echo "[fop-substitution-ablation] missing required artifact: $path" >&2
    exit 2
  fi
done

run_fintech_python scripts/run_ablation_experiments.py \
  --spec configs/ablations/tw_stock_futures_substitution_day_trade_v1.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --parallel-jobs 1 \
  --cpu-threads 112 \
  --torch-compile-threads 16 \
  --stop-on-fail \
  "$@"
