#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
source scripts/runtime_env.sh

TRAIN_CONFIG="configs/markets/tw_day_trade_1m_hybrid_v12_attention_full_then_last_layernorm.yaml"
TW_PUBLIC_SNAPSHOT_ID="tw-public-20260820T015018219623218Z-l0-penguin-6716296ea30636ba"
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export STOCKAGENT_DDP_CPU_AFFINITY="${STOCKAGENT_DDP_CPU_AFFINITY:-42-55,154-167;28-41,140-153}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

# Pin the exact derived generation independently of the mutable current link.
# These are the reference OFAT's version/generation/source-hash contract.
export STOCKAGENT_PINNED_PANEL_CACHE_MANIFEST="$REPO_ROOT/artifacts/cache/tw_day_trade_hybrid_minute_v12_reference/$TW_PUBLIC_SNAPSHOT_ID/stocks/panel_cache_v2/variants/fe882a5c1a14a6f10a78759ff5c11bed190fdf7edd35f4020f5b4af2c52bc577.json"
export STOCKAGENT_PINNED_PANEL_CACHE_VERSION="54"
export STOCKAGENT_PINNED_PANEL_CACHE_GENERATION="3726b829b37d4d56833a80b20280e4a2"
export STOCKAGENT_PINNED_PANEL_CACHE_SOURCE_HASH="31f3e4f1e31338bb9c0fd52494cc0d840a5081ffeab00a1fceddfa3412258fe0"

# --check-only validates the config and local dependencies without hydration
# or launching training. All other arguments are forwarded to train.py.
run_fintech_python - "$TRAIN_CONFIG" <<'PY'
import os
import sys
from pathlib import Path

from stockagent.config import load_config
from stockagent.data.panel_cache import load_panel_cache_v2_manifest

config = load_config(sys.argv[1])
manifest = Path(os.environ["STOCKAGENT_PINNED_PANEL_CACHE_MANIFEST"])
if manifest.parent.parent != (Path(config.data.panel_cache_root) / "panel_cache_v2").resolve():
    raise SystemExit("pinned manifest and data.panel_cache_root disagree")
panel = load_panel_cache_v2_manifest(
    manifest,
    mmap_mode="r",
    expected_version=int(os.environ["STOCKAGENT_PINNED_PANEL_CACHE_VERSION"]),
    expected_generation=os.environ["STOCKAGENT_PINNED_PANEL_CACHE_GENERATION"],
    expected_source_hash=os.environ["STOCKAGENT_PINNED_PANEL_CACHE_SOURCE_HASH"],
)
for required in (
    Path(config.data.day_trade_minute_execution_root) / "manifest.json",
    Path(config.training.pretrained_initialization_root) / "run_manifest.json",
):
    if not required.is_file() or required.stat().st_size == 0:
        raise SystemExit(f"missing training dependency: {required}")
model = config.training.financial_transformer
print(f"[training] {model.temporal_pooling}/{model.temporal_query_mode}, "
      f"{model.norm_type}, basis_input={model.temporal_basis_input}")
print(f"[training] {len(config.data.feature_include)} selected features, "
      f"{len(model.temporal_basis_families)} basis families, "
      f"batch={config.training.batch_size_train}/{config.training.batch_size_eval}, "
      f"epochs={config.training.epochs}")
print(f"[training] pinned panel: {manifest}")
print(f"[training] pretrained source: {config.training.pretrained_initialization_root}")
print(f"[training] output: {config.runner.output_dir}")
PY

if [[ "${1:-}" == "--check-only" ]]; then
  exit 0
fi

run_fintech_python scripts/check_environment.py --require-cuda --strict
"$REPO_ROOT/scripts/run_data_cache.sh" use tw-public \
  --snapshot-id "$TW_PUBLIC_SNAPSHOT_ID" \
  --link "$REPO_ROOT/data_tw_public"

run_fintech_python train.py \
  --config "$TRAIN_CONFIG" \
  --multi-gpu-strategy distributed_data_parallel \
  --cpu-threads 112 \
  --torch-compile-threads 16 \
  "$@"
