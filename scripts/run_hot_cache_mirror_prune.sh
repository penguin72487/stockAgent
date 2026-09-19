#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
  echo "Usage: bash scripts/run_hot_cache_mirror_prune.sh PLAN_FINGERPRINT" >&2
  exit 2
fi
if (( EUID != 0 )); then
  echo "Root privileges are required for the controlled hot bridge pause." >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$repo_root/scripts/runtime_env.sh"

bridge_stopped=false
restart_bridge() {
  if [[ "$bridge_stopped" == true ]]; then
    if ! systemctl start stockagent-hot-artifact-sync.service; then
      echo "CRITICAL: hot artifact bridge failed to restart." >&2
      return 1
    fi
  fi
}
trap restart_bridge EXIT

if systemctl is-active --quiet stockagent-hot-artifact-sync.service; then
  systemctl stop stockagent-hot-artifact-sync.service
  bridge_stopped=true
fi

run_fintech_python "$repo_root/scripts/prune_hot_cache_mirror.py" \
  --apply --plan-fingerprint "$1"
