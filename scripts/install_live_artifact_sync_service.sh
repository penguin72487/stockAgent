#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "[artifact-sync] legacy live and hot transport are both retired" >&2
exec "$repo_root/scripts/install_hot_artifact_sync_service.sh" "$@"
