#!/usr/bin/env bash
# Annual foreground canonical resume; --start-fold 1 selects every fold.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
launch="$repo_root/artifacts/operations/training_launches/tw_day_trade_factorized_values_20261007_no_basis_scale_separated_cash_annual_v1/runtime-launch.json"
run_fintech_python scripts/run_relocated_training.py --launch "$launch" "$@"
