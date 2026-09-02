#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
source scripts/runtime_env.sh
exec run_fintech_python scripts/deploy_tw_day_trade_multi_basis_22_history.py "$@"
