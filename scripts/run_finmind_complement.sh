#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source "$repo_root/scripts/runtime_env.sh"

run_fintech_python -m downloader.download_finmind_complement \
  --root "$repo_root/data_finmind/complement" \
  --max-requests "${FINMIND_COMPLEMENT_MAX_REQUESTS_PER_CYCLE:-120}" \
  --loop
