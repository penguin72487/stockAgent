#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

source scripts/runtime_env.sh
selected_python="$(resolve_fintech_python)"
# Homepage prewarming deliberately fans out independent local readers.  Keep
# glibc from retaining one large malloc arena per short-lived worker thread.
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"
exec "$selected_python" scripts/serve_public_dashboards.py \
  --host "${STOCKAGENT_PUBLIC_DASHBOARD_HOST:-127.0.0.1}" \
  --port "${STOCKAGENT_PUBLIC_DASHBOARD_PORT:-8770}"
