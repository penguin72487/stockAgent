#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

# shellcheck source=scripts/runtime_env.sh
source "${repo_root}/scripts/runtime_env.sh"
run_fintech_python "${repo_root}/scripts/data_cache.py" "$@"
