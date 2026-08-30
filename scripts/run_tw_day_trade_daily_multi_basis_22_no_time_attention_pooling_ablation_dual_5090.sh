#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh

# This suite pins a panel cache stored inside this exact materialized snapshot.
# Keep the source tree and its derived cache hot for the whole long-running
# experiment instead of allowing the default seven-day cache lease to expire.
TW_PUBLIC_SNAPSHOT_ID="tw-public-20260820T015018219623218Z-l0-penguin-6716296ea30636ba"
run_fintech_python scripts/data_cache.py use tw-public \
  --snapshot-id "$TW_PUBLIC_SNAPSHOT_ID" \
  --ttl-days 365 \
  --link "$REPO_ROOT/data_tw_public"

run_fintech_python scripts/run_tw_day_trade_ablation.py \
  --spec configs/ablations/tw_day_trade_daily_multi_basis_22_effective_rank_projection_l1_no_time_attention_pooling_tplus2_close_commission20_capital10m_v1.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --parallel-jobs 1 \
  "$@"
