#!/usr/bin/env bash
# Non-destructive migration preparation: copy, verify current, verify every
# present D archive object. This never changes a live mount or deletes data.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
cd "$repo_root"

source "$script_dir/runtime_env.sh"
run_fintech_python "$script_dir/copy_packed_d_streaming.py"
run_fintech_python "$script_dir/verify_packed_d_stage.py" --scope current
run_fintech_python "$script_dir/verify_packed_d_stage.py" --scope all
