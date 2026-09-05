#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh

# Use exactly the panel generation and completed v12 fold checkpoints pinned by
# the OFAT spec.  Do not launch this second dual-GPU suite concurrently with the
# temporal-basis v1 suite.
TW_PUBLIC_SNAPSHOT_ID="tw-public-20260820T015018219623218Z-l0-penguin-6716296ea30636ba"
# Dispatch through the canonical cache entrypoint.  On an index-only edge this
# hydrates and verifies exactly the pinned release before materializing it; on a
# full replica it retains the ordinary local-cache path.
"$REPO_ROOT/scripts/run_data_cache.sh" use tw-public \
  --snapshot-id "$TW_PUBLIC_SNAPSHOT_ID" \
  --link "$REPO_ROOT/data_tw_public"

run_fintech_python scripts/run_tw_day_trade_ablation.py \
  --spec configs/ablations/tw_day_trade_hybrid_minute_v12_reference_architecture_checkpoint_finetune_ofat_v2.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --parallel-jobs 1 \
  "$@"
