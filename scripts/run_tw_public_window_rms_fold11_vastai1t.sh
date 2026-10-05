#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"
source scripts/runtime_env.sh

config="${1:-configs/deployments/tw_public_all_observed_window_rms_projection_l1_fold11_v1.yaml}"
case "${config}" in
  configs/deployments/tw_public_all_observed_window_rms_projection_l1_fold11_v1.yaml)
    batch_size=124
    output="artifacts/markets/tw_public_all_observed_window_rms_projection_l1_fold11_v1"
    ;;
  configs/deployments/tw_public_all_observed_window_rms_projection_l1_fold11_batch128_v2.yaml)
    batch_size=128
    output="artifacts/markets/tw_public_all_observed_window_rms_projection_l1_fold11_batch128_v2"
    ;;
  *)
    echo "unsupported fold-11 deployment config: ${config}" >&2
    exit 2
    ;;
esac
lock_path="${output}.launch.lock"

mkdir -p "$(dirname "${lock_path}")"
exec 9>"${lock_path}"
if ! flock -n 9; then
  echo "another fold-11 launcher owns ${lock_path}" >&2
  exit 1
fi

export CUDA_VISIBLE_DEVICES=0,1
export STOCKAGENT_CPU_THREADS=112
export STOCKAGENT_TORCH_COMPILE_THREADS=16
export STOCKAGENT_POLARS_THREADS=1
export POLARS_MAX_THREADS=1
export RAYON_NUM_THREADS=1
export PYTORCH_ALLOC_CONF=expandable_segments:True
export STOCKAGENT_STRICT_NO_FALLBACK=1

run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python - "${config}" "${batch_size}" "${output}" <<'PY'
import sys
from pathlib import Path

from stockagent.config import load_config

config = load_config(sys.argv[1])
model = config.training.financial_transformer
assert config.trading.execution_mode == "naive"
assert config.training.model_name == "financial_transformer"
assert model.portfolio_output_mode == "projection_l1"
assert model.projection_l1_scale_by_active_count
assert model.causal_feature_window_rms_normalization
assert config.training.lookback == 32
assert config.training.batch_size_train == int(sys.argv[2])
assert config.training.batch_size_eval == 16
assert config.training.eval_model_chunk_rows == 16
assert config.training.epochs == 1000
assert Path(config.runner.output_dir).resolve() == (Path.cwd() / sys.argv[3]).resolve()
assert Path(config.data.parquet_root).is_dir()
assert Path(config.data.tw_public_feature_path).is_file()
PY
run_fintech_python train.py \
  --config "${config}" \
  --check-data-only \
  --start-fold 11 --max-folds 1 \
  --cpu-threads 112 --torch-compile-threads 16

run_fintech_python train.py \
  --config "${config}" \
  --output-dir "${output}" \
  --start-fold 11 --max-folds 1 \
  --multi-gpu-strategy distributed_data_parallel \
  --batch-size-train "${batch_size}" --batch-size-eval 16 \
  --cpu-threads 112 --torch-compile-threads 16 \
  --no-isolate-train-folds \
  --resume
