#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "[artifact-sync] legacy live installer retired; installing stockagent-artifacts-hot" >&2
exec "$repo_root/scripts/install_hot_artifact_sync_service.sh" "$@"
