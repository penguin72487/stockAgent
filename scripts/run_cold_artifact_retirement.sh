#!/usr/bin/env bash
set -euo pipefail

if (( $# != 2 )); then
  echo "Usage: bash scripts/run_cold_artifact_retirement.sh DATASET PLAN_FINGERPRINT" >&2
  exit 2
fi
if (( EUID != 0 )); then
  echo "Root privileges are required for the controlled hot bridge pause." >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dataset="$1"
plan_fingerprint="$2"
source "$repo_root/scripts/runtime_env.sh"

bridge_stopped=false
restart_bridge() {
  if [[ "$bridge_stopped" == true ]]; then
    if ! systemctl start stockagent-hot-artifact-sync.service; then
      echo "CRITICAL: hot artifact bridge failed to restart; inspect systemctl status." >&2
      return 1
    fi
  fi
}
trap restart_bridge EXIT

if systemctl is-active --quiet stockagent-hot-artifact-sync.service; then
  systemctl stop stockagent-hot-artifact-sync.service
  bridge_stopped=true
fi

run_fintech_python "$repo_root/scripts/manage_cold_artifacts.py" \
  retire "$dataset" --apply --plan-fingerprint "$plan_fingerprint"
