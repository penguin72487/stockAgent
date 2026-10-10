#!/usr/bin/env bash
# Compatibility name; the latest selected experiment is annual fold10.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
if [[ "$#" -gt 1 || ( "$#" -eq 1 && "$1" != --check-only ) ]]; then
  echo '年度重置 fold10／雙卡／scale-separated cash；僅接受 --check-only，batch 依驗收設定。' >&2
  exit 2
fi
launch="$repo_root/artifacts/operations/training_launches/tw_day_trade_factorized_values_20261007_no_basis_scale_separated_cash_annual_v1/runtime-launch.json"
run_fintech_python scripts/run_relocated_training.py --launch "$launch" "$@"
