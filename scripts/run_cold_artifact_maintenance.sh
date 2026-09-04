#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

edge_state_root="${STOCKAGENT_PACKED_EDGE_STATE_ROOT:-/var/lib/stockagent-packed-edge}"
if [[ -f "$edge_state_root/state.json" ]]; then
  echo "Packed store is in index-only edge mode; local cold publication is disabled."
  exit 0
fi

exec 9>/run/lock/stockagent-cold-artifact-maintenance.lock
if ! flock -n 9; then
  echo "Cold-artifact maintenance is already running; skipping this invocation."
  exit 0
fi

if [[ -r /etc/environment ]]; then
  set -a
  # shellcheck disable=SC1091
  source /etc/environment
  set +a
fi
source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)" || {
  echo "Unable to resolve the fintech Python runtime." >&2
  exit 2
}

exec "$python_bin" scripts/maintain_cold_artifacts.py \
  --scope "${COLD_ARTIFACT_SCOPE:-ablations}" \
  --stable-hours "${COLD_ARTIFACT_STABLE_HOURS:-24}" \
  --retention-days "${COLD_ARTIFACT_RETENTION_DAYS:-7}" \
  --max-publish "${COLD_ARTIFACT_MAX_PUBLISH:-1}" \
  --apply
