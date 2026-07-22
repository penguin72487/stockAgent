#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/runtime_env.sh"
export PYTHONPATH="$repo_root${PYTHONPATH:+:$PYTHONPATH}"
export PATH="${HOME}/.local/bin:$PATH"
run_fintech_python "$repo_root/scripts/desync_snapshot.py" "$@"
