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

exec nice -n 19 ionice -c3 "$python_bin" scripts/maintain_storage_pressure.py \
  --min-age-days "${STOCKAGENT_CACHE_MIN_AGE_DAYS:-14}" \
  --high-watermark-percent "${STOCKAGENT_STORAGE_HIGH_WATERMARK_PERCENT:-95}" \
  --target-percent "${STOCKAGENT_STORAGE_TARGET_PERCENT:-92}" \
  --apply
