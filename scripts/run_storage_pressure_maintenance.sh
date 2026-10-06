#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -r /etc/environment ]]; then
  set -a
  # shellcheck disable=SC1091
  source /etc/environment
  set +a
fi

# shellcheck source=scripts/runtime_env.sh
source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)"

scope_args=()
policy_path="/etc/stockagent/storage-pressure.json"
if [[ -e "$policy_path" || -L "$policy_path" ]]; then
  # This is a root-owned, locally enrolled, data-only policy; the CLI validates
  # it before any unlink. Never source synced settings or add a second cron.
  scope_args+=(--policy "$policy_path")
fi

exec nice -n 19 ionice -c3 "$python_bin" scripts/maintain_storage_pressure.py \
  --min-age-days "${STOCKAGENT_CACHE_MIN_AGE_DAYS:-14}" \
  --high-watermark-percent "${STOCKAGENT_STORAGE_HIGH_WATERMARK_PERCENT:-89}" \
  --target-percent "${STOCKAGENT_STORAGE_TARGET_PERCENT:-88}" \
  "${scope_args[@]}" --apply
