#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)"
"$python_bin" scripts/check_stockagent_time_sync.py --repair
exec "$python_bin" scripts/run_tw_public_0830_check.py "$@"
