#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict

TW_PUBLIC_SNAPSHOT_ID="tw-public-20260820T015018219623218Z-l0-penguin-6716296ea30636ba"
SPEC="configs/ablations/executable_portfolio_transformer_v5_local_metadata_fold11_v1.yaml"
OUTPUT_ROOT="artifacts/ablations/executable_portfolio_transformer_v5_local_metadata_fold11_v1"
EPOCHS="${STOCKAGENT_V5_ABLATION_EPOCHS:-3}"
SEED="${STOCKAGENT_V5_ABLATION_SEED:-42}"

run_fintech_python scripts/data_cache.py use tw-public \
  --snapshot-id "$TW_PUBLIC_SNAPSHOT_ID" \
  --ttl-days 365 \
  --link "$REPO_ROOT/data_tw_public"

EXPERIMENTS=(
  baseline
  metadata_collectives
  symbol_sharded_ledger
  eager_model
  eager_ledger
  batch64
  host_panel
)

for experiment in "${EXPERIMENTS[@]}"; do
  run_fintech_python scripts/run_ablation_experiments.py \
    --spec "$SPEC" \
    --only "$experiment" \
    --start-fold 11 \
    --max-folds 1 \
    --epochs "$EPOCHS" \
    --seed "$SEED" \
    --multi-gpu-strategy distributed_data_parallel \
    --parallel-jobs 1 \
    --cpu-threads 112 \
    --torch-compile-threads 16 \
    --stop-on-fail

  if find "$OUTPUT_ROOT/baseline" -path '*/epoch_curve.jsonl' -type f -print -quit | grep -q .; then
    run_fintech_python scripts/plot_distributed_ledger_ablation.py \
      --root "$OUTPUT_ROOT" \
      --suite v5_local
  fi
done
