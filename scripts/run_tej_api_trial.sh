#!/usr/bin/env bash
set -euo pipefail
tej_api_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$tej_api_repo_root"
source scripts/runtime_env.sh
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"
tej_api_python_bin="$(resolve_fintech_python)"
exec "$tej_api_python_bin" -m downloader.download_tej_api run \
  --root "$tej_api_repo_root/data_tej/api_trial_v1" --env "$tej_api_repo_root/.env"
