#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TRADE_ENV_FILE="${SHIOAJI_TRADE_ENV_FILE:-$REPO_ROOT/.env.trade}"
if [[ ! -f "$TRADE_ENV_FILE" ]]; then
  echo "[shioaji-simulation] missing env file: $TRADE_ENV_FILE" >&2
  exit 2
fi
set -a
source "$TRADE_ENV_FILE"
set +a

source scripts/runtime_env.sh
run_fintech_python scripts/test_shioaji_simulation_lifecycle.py "$@"
