#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
# Explainability shards whole folds across the two ranks.  Bound the inner
# numerical pools per rank; panel loading already owns outer file parallelism.
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-28}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-28}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-28}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-28}"
export POLARS_MAX_THREADS="${POLARS_MAX_THREADS:-1}"
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh
run_fintech_python -m torch.distributed.run \
  --standalone \
  --nnodes=1 \
  --nproc-per-node=2 \
  explain_model.py \
  --config configs/markets/tw_public_multi_basis_online_complete_dual_5090.yaml \
  --device cuda \
  --cpu-threads 28 \
  --max-rows 0 \
  --row-chunk-size 16 \
  --amp-dtype bf16 \
  --no-compile-model \
  --ig-steps 8 \
  --ig-batch-size 1 \
  --sample-method even \
  --perturb \
  --perturb-batch-size 2 \
  --perturb-max-auto-batch-size 32 \
  --perturb-max-input-elements 536870912 \
  --no-counterfactual-compile \
  --j-lens \
  --j-lens-vjp-batch-size 8 \
  --shap \
  --regime-analysis \
  --fold-stability \
  --umap \
  --umap-max-points 0 \
  --umap-max-projections 0 \
  --umap-n-neighbors 16 \
  --plot-backend auto \
  --plots \
  --standard-plots \
  --plot-theme paper \
  --report-style paper \
  --no-interactive-plots \
  --strict-no-fallback \
  "$@"
