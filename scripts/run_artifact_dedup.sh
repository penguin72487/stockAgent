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
# Only a separately enrolled local policy enables ABI-aware service sharing.
# Do not expand the generic artifact allowlist or add a second scheduler.
service_cache_policy="/etc/stockagent/service-cache-compaction.json"
service_cache_status=0
if [[ -e "$service_cache_policy" || -L "$service_cache_policy" ]]; then
  "$python_bin" scripts/deduplicate_artifacts.py \
    --service-cache-policy "$service_cache_policy" --sharing-candidates-only --apply || service_cache_status=$?
fi
completed_forward_cache_policy="/etc/stockagent/completed-forward-compaction.json"
if [[ -e "$completed_forward_cache_policy" || -L "$completed_forward_cache_policy" ]]; then
  "$python_bin" scripts/deduplicate_artifacts.py \
    --completed-forward-cache-policy "$completed_forward_cache_policy" --sharing-candidates-only --apply || service_cache_status=$?
fi

"$python_bin" scripts/deduplicate_artifacts.py \
  --root "$repo_root/artifacts" \
  --min-age-hours "${ARTIFACT_DEDUP_MIN_AGE_HOURS:-24}" \
  --complete-runs-only \
  --apply
exit "$service_cache_status"
