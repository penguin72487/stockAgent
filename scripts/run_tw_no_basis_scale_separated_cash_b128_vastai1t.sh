#!/usr/bin/env bash
# Output-only Fold 11 experiment; canonical resume is confined to the new root.
set -euo pipefail
task_root=/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_scale_separated_cash_v1
check_only=0
if [ "$#" -eq 1 ] && [ "$1" = '--check-only' ]; then
  check_only=1
elif [ "$#" -ne 0 ]; then
  echo '固定 fold11／雙卡／batch128／scale-separated cash；僅接受 --check-only。' >&2
  exit 2
fi
cd /root/stockAgent
source scripts/runtime_env.sh

# Reuse the accepted exact-source SHA checks, leases, pins and CUDA gate.
# This entry performs preflight only, never launches the original training.
bash scripts/run_tw_no_basis_flat_b128_vastai1t.sh --check-only

release_receipt="$(run_fintech_python -c 'import json,sys; from pathlib import Path; print(json.loads((Path(sys.argv[1])/"run-spec.json").read_text())["source_receipt"])' "$task_root")"
cd "$(dirname -- "$release_receipt")/build-source"
source scripts/runtime_env.sh
export CUDA_VISIBLE_DEVICES=0,1 PYTHONNOUSERSITE=1
export STOCKAGENT_CODE_RELEASE_RECEIPT="$release_receipt"
export STOCKAGENT_TRAINING_TRANSFORM_CACHE_DIR=/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261005_gaprepair_v4/.training_transform_cache_v1
export STOCKAGENT_DAY_TRADE_CARRY_FLAT_TERMINAL=1
export STOCKAGENT_DAY_TRADE_CARRY_CHECKPOINT_BLOCK_ROWS=16
export STOCKAGENT_DAY_TRADE_CARRY_COMMIT_COMPILE=1
export OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export STOCKAGENT_CPU_THREADS=8 STOCKAGENT_TORCH_COMPILE_THREADS=16
export STOCKAGENT_POLARS_THREADS=1 POLARS_MAX_THREADS=1 RAYON_NUM_THREADS=1
export STOCKAGENT_BACKTEST_COMPILE_PREP=1 STOCKAGENT_STRICT_NO_FALLBACK=1
unset PYTORCH_CUDA_ALLOC_CONF
export PYTORCH_ALLOC_CONF=expandable_segments:True
run_fintech_python - "$task_root" "$check_only" <<'PY'
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from stockagent.config import load_config
from stockagent.data.factorized_panel import VALUE_ONLY_CONTRACT
from stockagent.portfolio_contract import normalize_portfolio_output_mode
from stockagent.runtime_identity import verify_release_bundles, verify_source_release
from scripts.manage_gpu_jobs import _busy_gpu_indices, _gpu_lease_command

root = Path(sys.argv[1])
spec = json.loads((root / "run-spec.json").read_text())
receipt = Path(spec["source_receipt"])
source = verify_source_release(receipt, Path.cwd())
verify_release_bundles(receipt)
assert source["source_sha256"] == spec["source_sha256"]
for name, expected in spec["config_sha256"].items():
    assert hashlib.sha256(Path(name).read_bytes()).hexdigest() == expected, name
config_path = root / "config.yaml"
baseline = load_config(Path(spec["baseline_config"]))
config = load_config(config_path)
assert normalize_portfolio_output_mode(
    config.training.financial_transformer.portfolio_output_mode
) == "score_entmax_scale_separated_cash"
assert Path(config.runner.output_dir) == root / "training-bf16"
assert config.training.pretrained_initialization_root is None

# Prove that all inherited experiment settings remain identical. The changed
# code release supports the new head; neither old weights nor optimizers donate.
expected = asdict(baseline)
expected["training"]["financial_transformer"]["portfolio_output_mode"] = (
    "score_entmax_scale_separated_cash"
)
# load_config mirrors the selected neural model into this executor alias.
expected["training"]["executable_portfolio_transformer"]["portfolio_output_mode"] = (
    "score_entmax_scale_separated_cash"
)
expected["runner"]["output_dir"] = config.runner.output_dir
assert asdict(config) == expected, "unexpected inherited experiment change"

# The six price/rule columns are only the base cache. This selected experiment
# requires the full value-only manifest; never fall back to a six-column run.
assert config.data.factorized_feature_manifest, "full feature manifest is required"
feature = json.loads(Path(config.data.factorized_feature_manifest).read_text())
assert feature["contract"] == VALUE_ONLY_CONTRACT and feature["status"] == "complete"
assert feature["model_channel_policy"] == "value_only"
counts = {"base": len(feature["base_feature_names"]),
          "individual": len(feature["individual_channels"]),
          "common": len(feature["common_channels"])}
assert counts == {"base": 6, "individual": 14655, "common": 65}, "full feature count mismatch"
assert feature["logical_model_channels"] == sum(counts.values()) == 14726
assert feature["value_features"] == counts["individual"] + counts["common"]
assert config.data.feature_include == feature["base_feature_names"]
print("[features] expected model input=14726 (base=6 + individual=14655 + common=65); "
      "the earlier [panel] features (6) describes only the base cache. "
      "Attachment success is reported separately as [panel-model].", flush=True)
print("[fold11] score_entmax_scale_separated_cash／global128／BF16+TF32；獨立新root。", flush=True)
print("[resume] 首次從頭訓練；之後只續跑本root相容checkpoint，不沿用舊模型。", flush=True)
if int(sys.argv[2]):
    print("[fold11] --check-only 通過；未啟動正式訓練。", flush=True)
    raise SystemExit(0)
if _busy_gpu_indices().intersection({0, 1}):
    raise RuntimeError("GPU 0/1已有其他工作；不重疊啟動。")
command = [sys.executable, "train.py", "--config", str(config_path),
           "--start-fold", "11", "--max-folds", "1", "--torch-compile-threads", "16",
           "--resume", "--no-retrain-completed-folds", "--no-profile-timing",
           "--no-debug-timing-sync", "--no-isolate-train-folds"]
raise SystemExit(subprocess.run(_gpu_lease_command(command, [0, 1], sharing=False)).returncode)
PY
