#!/usr/bin/env bash
# Baseline, optimizer recipes, then OFAT; accepted fused-ledger v12 ABI.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
launch_mode=(--ledger-graph)
for argument in "$@"; do
  case "$argument" in
    --frontier|--optimized|--numerical-repair|--structural|--resident-sparse|--ledger-graph) launch_mode=();;
  esac
done
run_fintech_python scripts/run_tw_scale_cash_annual_ablations.py "${launch_mode[@]}" "$@"
