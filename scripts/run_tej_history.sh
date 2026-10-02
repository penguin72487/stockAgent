#!/usr/bin/env bash
set -euo pipefail

tej_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$tej_repo_root"
source scripts/runtime_env.sh

# systemd does not inherit an interactive shell's WSL relay. The stable init
# relay is recreated by WSL; do not pin a terminal PID or launch a second login.
if [[ ! -S "${WSL_INTEROP:-}" && -S /run/WSL/1_interop ]]; then
  export WSL_INTEROP=/run/WSL/1_interop
fi
export PYTHONUNBUFFERED=1
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
tej_python_bin="$(resolve_fintech_python)"
exec "$tej_python_bin" -m downloader.download_tej_history watch \
  --root "$tej_repo_root/data_tej" \
  --config "$tej_repo_root/configs/tej_history.json" \
  --session "$tej_repo_root/data_tej/desktop_session.json"
