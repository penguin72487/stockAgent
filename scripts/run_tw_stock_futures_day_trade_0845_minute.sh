#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
source scripts/runtime_env.sh

TRAIN_CONFIG="configs/markets/tw_stock_futures_day_trade_0845_minute.yaml"
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"
export PYTORCH_CUDA_ALLOC_CONF="${PYTORCH_CUDA_ALLOC_CONF:-expandable_segments:True}"

# Training consumes verified one-minute bars, including direct KBar builds;
# this entrypoint never requires or downloads raw ticks.
# Data checks and CLI help do not hydrate releases or launch CUDA/DDP training.
# train.py remains the single owner of argument parsing and source validation.
for argument in "$@"; do
  case "$argument" in
    --check-data-only|--help|-h)
      run_fintech_python train.py --config "$TRAIN_CONFIG" "$@"
      exit $?
      ;;
  esac
done

# Read the effective config, including a caller's --config override. Source IDs
# live in that config, rather than a generated operations YAML or duplicate pins.
dataset_roots="$(run_fintech_python - --config "$TRAIN_CONFIG" "$@" <<'PY'
from pathlib import Path
from stockagent.config import load_config
from stockagent.data.tw_stock_futures_minute import MINUTE_MODE, preflight_futures_minute_training
from train import parse_args

args = parse_args()
config = load_config(args.config)
if config.trading.execution_mode != MINUTE_MODE:
    raise SystemExit("this launcher requires the 08:45 stock-futures day-trade config")
public = Path(config.data.parquet_root).parent
futures = Path(config.trading.tw_stock_futures_day_trade_data_path).parent.parent
# If already local, reject a known history gap before any expensive hydration.
# A new node still uses the canonical cache workflow below to obtain its inputs.
daily_path = Path(config.trading.tw_stock_futures_day_trade_data_path)
minute_path = Path(config.trading.tw_stock_futures_day_trade_minute_data_path)
if all(path.is_file() for path in (daily_path, daily_path.with_name("manifest.json"),
                                  minute_path, minute_path.with_name("manifest.json"))):
    try:
        preflight_futures_minute_training(config, config_path=args.config)
    except (OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
for root, dataset, files in (
    (public, "tw-public", [config.data.parquet_root, config.data.tw_public_feature_path]),
    (futures, "tw-futures", [config.trading.tw_stock_futures_day_trade_data_path,
                            config.trading.tw_stock_futures_day_trade_minute_data_path]),
):
    if (not root.is_absolute() or root.parent.name != dataset
            or not root.name.startswith(dataset + "-") or root.name == "current"):
        raise SystemExit(f"config must pin an exact materialized {dataset} release: {root}")
    if any(not Path(path).is_relative_to(root) for path in files):
        raise SystemExit(f"config mixes source releases for {dataset}")
    print(root)
PY
)"
mapfile -t dataset_roots <<< "$dataset_roots"

run_fintech_python scripts/check_environment.py --require-cuda --strict

# Reuse the canonical full-replica/edge cache workflow. Its use operation owns
# verification and lease renewal. Open directory FDs keep those leases visible
# to the process-reference monitor after individual parquet reads finish.
dataset_fds=()
for dataset_root in "${dataset_roots[@]}"; do
  snapshot_id="${dataset_root##*/}"
  dataset_parent="${dataset_root%/*}"
  dataset="${dataset_parent##*/}"
  dataset_use="$(bash scripts/run_data_cache.sh use "$dataset" --snapshot-id "$snapshot_id")"
  run_fintech_python - "$dataset_root" "$dataset_use" <<'PY'
import json
from pathlib import Path
import sys

receipt = json.loads(sys.argv[2])
lease = receipt.get("lease", receipt)
root = Path(sys.argv[1])
if lease["target"] != str(root) or lease["snapshot_id"] != root.name:
    raise SystemExit(f"cache use result differs from the configured source: {root}")
print(sys.argv[2])
PY
  exec {dataset_fd}< "$dataset_root"
  dataset_fds+=("$dataset_fd")
done

# One shared daily training lifecycle owns models, loss, folds, reports and DDP.
run_fintech_python train.py \
  --config "$TRAIN_CONFIG" \
  "$@"
