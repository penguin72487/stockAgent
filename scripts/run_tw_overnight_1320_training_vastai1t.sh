#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

source scripts/runtime_env.sh

config="configs/deployments/tw_overnight_1320_training_vastai1t.yaml"
output="artifacts/markets/tw_overnight_1320_close_fallback_next_open_multi_basis_22_projection_l1_capital10m_v1"
lock_path="${output}.launch.lock"
check_only=false
if [[ "${1:-}" == "--check-only" ]]; then
  check_only=true
  shift
fi
if (( $# != 0 )); then
  echo "usage: $0 [--check-only]" >&2
  exit 2
fi

mkdir -p "$(dirname "${lock_path}")"
exec 9>"${lock_path}"
if ! flock -n 9; then
  echo "another 13:20 overnight launcher owns ${lock_path}" >&2
  exit 1
fi

gpu_count="$(run_fintech_python - <<'PY'
import torch
print(torch.cuda.device_count())
PY
)"
if (( gpu_count < 2 )); then
  echo "vastai1T formal acceptance requires two visible CUDA GPUs; found ${gpu_count}" >&2
  exit 1
fi
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-0,1}"

run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py --config "${config}" --check-data-only

if [[ "${check_only}" == true ]]; then
  echo "13:20 overnight two-GPU preflight accepted: ${config}"
  exit 0
fi

run_fintech_python train.py \
  --config "${config}" \
  --resume \
  --profile-timing
