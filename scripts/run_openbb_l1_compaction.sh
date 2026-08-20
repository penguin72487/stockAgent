#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

source scripts/runtime_env.sh

output_dir="${OPENBB_OUTPUT_DIR:-data_openBB}"
python_bin="$(resolve_fintech_python)" || {
  echo "Unable to resolve the fintech Python runtime." >&2
  exit 2
}
exec "$python_bin" -m scripts.compact_openbb_l1 \
  --output-dir "$output_dir" \
  "$@"
