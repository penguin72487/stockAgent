#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

source scripts/runtime_env.sh

# This suite owns both visible GPUs through within-fold DDP.  Refuse to start
# beside another repository training process; sharing the devices invalidates
# memory/throughput evidence and can OOM an otherwise valid candidate.
ACTIVE_TRAINING_PATTERN='(^|[[:space:]])([^[:space:]]*/)?train\.py([[:space:]]|$)|torch\.distributed\.run'
if pgrep -f "$ACTIVE_TRAINING_PATTERN" >/dev/null; then
  echo "[tw-minute-v12] another training/DDP job is active; refusing to share GPUs:" >&2
  pgrep -af "$ACTIVE_TRAINING_PATTERN" >&2 || true
  exit 1
fi

# Fail before an expensive suite if the actual CUDA/BF16/compile environment is
# unavailable.  Fold isolation owns torchrun/DDP; do not wrap this script in an
# outer torchrun process.
run_fintech_python scripts/check_environment.py --require-cuda --strict

# Rebuild both immutable-by-hash OOF guides from completed fold artifacts.  The
# builder rejects non-causal owners, incomplete artifacts, missing symbols,
# missing dates, or a source whose execution mode is not tw_day_trade.
run_fintech_python scripts/build_tw_minute_daily_guidance.py \
  --daily-output-dir artifacts/markets/tw_day_trade_hybrid_minute_22_effective_rank_pretrained_exact_official_close_commission20_capital10m_all_features_v12 \
  --minute-manifest data_tw_minute/research_dataset_developing_v5/manifest.json \
  --output data_tw_minute/daily_guidance/oof_v12_exact_official_close_requested_weights.parquet

run_fintech_python scripts/build_tw_minute_daily_guidance.py \
  --daily-output-dir artifacts/ablations/tw_day_trade_hybrid_minute_v12_reference_architecture_checkpoint_finetune_ofat_v2/attention_pooling \
  --minute-manifest data_tw_minute/research_dataset_developing_v5/manifest.json \
  --output data_tw_minute/daily_guidance/oof_v12_attention_pooling_exact_official_close_requested_weights.parquet

run_fintech_python scripts/run_ablation_experiments.py \
  --spec configs/ablations/tw_minute_daily_guided_v12_optimization_v1.yaml \
  --multi-gpu-strategy distributed_data_parallel \
  --parallel-jobs 1 \
  "$@"
