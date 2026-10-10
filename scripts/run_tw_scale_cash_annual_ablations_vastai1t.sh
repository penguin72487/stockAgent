#!/usr/bin/env bash
# Foreground only. Fresh v6 numerical ABI; canonical resume within that version.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
run_fintech_python scripts/run_tw_scale_cash_annual_ablations.py --numerical-repair "$@"
