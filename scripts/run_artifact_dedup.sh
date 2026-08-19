#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

exec 9>/run/lock/stockagent-artifact-dedup.lock
if ! flock -n 9; then
  echo "Artifact deduplication is already running; skipping this invocation."
  exit 0
fi

source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)" || {
  echo "Unable to resolve the fintech Python runtime." >&2
  exit 2
}
exec "$python_bin" scripts/deduplicate_artifacts.py \
  --root "$repo_root/artifacts" \
  --min-age-hours "${ARTIFACT_DEDUP_MIN_AGE_HOURS:-24}" \
  --complete-runs-only \
  --apply
