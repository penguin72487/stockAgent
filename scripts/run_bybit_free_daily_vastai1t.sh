#!/usr/bin/env bash
# Thin canonical train.py launcher; never download data or change a release head.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh

mode="${1:---check}"
if [[ "$#" -gt 1 || ( "$mode" != "--check" && "$mode" != "--train" ) ]]; then
  echo "usage: bash scripts/run_bybit_free_daily_vastai1t.sh [--check|--train]" >&2
  exit 2
fi
config=configs/remote/bybit_free_daily_multibasis_vastai1t_20260927.yaml
output=artifacts/markets/bybit_free_daily_multibasis_pinned_20260927
mkdir -p artifacts/markets
exec 9>"${output}.launch.lock"
flock -n 9 || { echo "Another Bybit launcher owns this run." >&2; exit 1; }

export CUDA_VISIBLE_DEVICES=0,1 PYTHONUNBUFFERED=1 STOCKAGENT_STRICT_NO_FALLBACK=1
export PYTORCH_ALLOC_CONF=expandable_segments:True
export STOCKAGENT_POLARS_THREADS=1 POLARS_MAX_THREADS=1 RAYON_NUM_THREADS=1
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python - "$config" <<'PY'
import json
import hashlib
import sys
from pathlib import Path
import torch
from stockagent.config import load_config
from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id
from stockagent.data_sync.materialized_cache import _ready_matches, use_materialized_snapshot

snapshot = "bybit-20260926T051004983477667Z-l0-penguin-857d2f9a99f1b88f"
sync = Path("/srv/stockagent-packed")
cache = Path("/srv/stockagent-packed-materialized")
resolved = resolve_packed_snapshot_id(sync, "bybit", snapshot)
assert resolved.manifest_sha256 == "7af635eb057024bd9e26ed0b0c621c0c124c89c93af7469d1c48e090ab272df6"
assert _ready_matches(cache, resolved), "Immutable materialization/READY does not match"
assert (Path.cwd() / "data_bybit").resolve() == cache / "bybit" / snapshot
# Reuse the existing verified hot release and renew its managed seven-day lease.
# No materialization or network operation is needed after the READY gate above.
use_materialized_snapshot(sync, cache, "bybit", snapshot_id=snapshot)
c = load_config(sys.argv[1])
assert c.training.lookback == 32 and c.trading.frequency == "daily"
assert c.training.epochs == 1000 and c.training.multi_gpu_strategy == "distributed_data_parallel"
assert c.environment.amp_dtype == "bf16" and torch.cuda.device_count() == 2
assert c.trading.execution_mode == "crypto_perpetual"
assert c.trading.buy_fee_rate == c.trading.sell_fee_rate == 0.00055
assert c.trading.crypto_execution_minute_utc == 0
assert c.training.financial_transformer.portfolio_output_mode == "score_entmax_cash_v2"
assert c.training.financial_transformer.temporal_basis_input == "input_features"
assert len(c.training.financial_transformer.temporal_basis_families) >= 2
assert c.training.crypto_optimizer_step_per_trajectory
assert len(c.data.feature_include) + len(c.data.feature_availability_indicators) == 133
assert Path(c.data.parquet_root) == Path("artifacts/cache/bybit_perpetual_daily_0000_repaired/perpetual_daily")
repair_manifest = Path(c.data.parquet_root).parent / "midnight_manifest.json"
assert hashlib.sha256(repair_manifest.read_bytes()).hexdigest() == "0fef32c982571f309b03499b8e1ca8dd48ad541c6e17e877ce862dd0f917e639", "Repair view changed since the verified smoke run; re-audit before training"
assert Path(c.data.external_feature_path).parent.parent == cache / "bybit" / snapshot
print(json.dumps({"snapshot_id": snapshot, "lookback": 32, "features": 133, "state": "release_and_config_verified"}))
PY
run_fintech_python train.py --config "$config" \
  --output-dir artifacts/data_quality/bybit_free_remote_20260927/pinned_preflight \
  --check-data-only
if [[ "$mode" == "--train" ]]; then
  if [[ -n "$(nvidia-smi --query-compute-apps=pid --format=csv,noheader)" ]]; then
    echo "GPU compute processes are active; leave the existing work running and retry later." >&2
    exit 1
  fi
  # Inherited formal lifecycle: six walk-forward folds, full curves and resume.
  run_fintech_python train.py --config "$config" --resume
fi
