#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
export STOCKAGENT_COLD_INVENTORY_CACHE_PATH="$repo_root/artifacts/live/data_monitor/tw_public_cold_inventory_cache.json"
python_bin="$(resolve_fintech_python)"
exec "$python_bin" scripts/snapshot_data_refresh_services.py
