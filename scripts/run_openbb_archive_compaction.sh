#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

source scripts/runtime_env.sh

python_bin="$(resolve_fintech_python)" || {
  echo "Unable to resolve the fintech Python runtime." >&2
  exit 2
}

exec "$python_bin" scripts/process_openbb_archive.py \
  --output-dir data_openBB \
  "$@"
