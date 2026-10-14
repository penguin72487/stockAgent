#!/usr/bin/env bash
# Baseline, optimizer recipes, then the original OFAT catalog; resident v10 ABI.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
launch_mode=(--resident-sparse)
for argument in "$@"; do
  case "$argument" in
    --frontier|--optimized|--numerical-repair|--structural|--resident-sparse) launch_mode=();;
  esac
done
run_fintech_python scripts/run_tw_scale_cash_annual_ablations.py "${launch_mode[@]}" "$@"
