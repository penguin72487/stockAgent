#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# The Python entrypoint owns one ingress cycle and takes the existing shared
# owner only for publication and source retirement, after independent reads.

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
