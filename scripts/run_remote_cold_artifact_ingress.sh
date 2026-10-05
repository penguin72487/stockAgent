#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

exec 9>/run/lock/stockagent-remote-cold-artifact-ingress.lock
lock_wait_seconds="${COLD_ARTIFACT_INGRESS_LOCK_WAIT_SECONDS:-1800}"
if [[ ! "$lock_wait_seconds" =~ ^[1-9][0-9]*$ ]] || (( lock_wait_seconds > 3600 )); then
  echo "Ingress owner wait must be an integer from 1 to 3600 seconds."
  exit 64
fi
echo "Waiting up to ${lock_wait_seconds}s for the existing ingress owner at its transaction boundary."
if ! flock -w "$lock_wait_seconds" 9; then
  echo "The existing ingress owner is still busy; no source was changed."
  exit 75
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

extra_args=()
if [[ -n "${COLD_ARTIFACT_RETURN_POLICY:-}" ]]; then
  extra_args+=(--policy "$COLD_ARTIFACT_RETURN_POLICY")
fi

exec "$python_bin" scripts/ingest_remote_cold_artifacts.py \
  --scope "${COLD_ARTIFACT_INGRESS_SCOPE:-ablations}" \
  --stable-hours "${COLD_ARTIFACT_INGRESS_STABLE_HOURS:-0}" \
  --max-publish "${COLD_ARTIFACT_INGRESS_MAX_PUBLISH:-1}" \
  --output /var/lib/stockagent-cold-artifacts/remote-ingress-status.json \
  "${extra_args[@]}" --apply
