#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
run_fintech_python -m downloader.download_finmind_sponsor \
  --root "$repo_root/data_finmind/sponsor" \
  --max-requests 100 --workers 4 --loop
