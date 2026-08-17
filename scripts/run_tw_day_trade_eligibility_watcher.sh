#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)"
exec "$python_bin" scripts/fetch_tw_day_trade_eligibility_on_publish.py "$@"
