#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)"
exec bash scripts/run_downloader_with_release.sh tw-public /srv/stockagent-packed -- \
  "$python_bin" scripts/refresh_tw_day_trade_margin_actions.py --start-year 2026 "$@"
