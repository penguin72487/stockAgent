#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")")"
source scripts/runtime_env.sh
ensure_enrolled_wsl_identity "$PWD"
run_fintech_python scripts/maintain_cold_tiers.py --apply --seed-existing --yield-to-migration
