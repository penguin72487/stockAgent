#!/usr/bin/env bash
# Baseline, optimizer recipes, then the original OFAT catalog; fresh v8 ABI.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
run_fintech_python scripts/run_tw_scale_cash_annual_ablations.py --frontier "$@"
