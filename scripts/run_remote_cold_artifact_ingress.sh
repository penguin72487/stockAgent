#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

exec 9>/run/lock/stockagent-remote-cold-artifact-ingress.lock
if ! flock -n 9; then
  echo "Remote cold-artifact ingress is already running; skipping this invocation."
  exit 0
fi

if [[ -r /etc/stockagent/remote-cold-artifact-ingress.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source /etc/stockagent/remote-cold-artifact-ingress.env
  set +a
fi
source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)" || {
  echo "Unable to resolve the fintech Python runtime." >&2
  exit 2
}

exec "$python_bin" scripts/ingest_remote_cold_artifacts.py \
  --scope "${COLD_ARTIFACT_INGRESS_SCOPE:-ablations}" \
  --stable-hours "${COLD_ARTIFACT_INGRESS_STABLE_HOURS:-0}" \
  --max-publish "${COLD_ARTIFACT_INGRESS_MAX_PUBLISH:-1}" \
  --apply
