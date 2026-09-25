#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source "$repo_root/scripts/runtime_env.sh"

run_fintech_python -m downloader.download_finmind_free \
  --root "$repo_root/data_finmind" \
  --max-requests "${FINMIND_MAX_REQUESTS_PER_CYCLE:-240}" \
  --loop
