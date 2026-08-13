#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

FUTURES_ENV_FILE="${SHIOAJI_TAIFEX_ENV_FILE:-$REPO_ROOT/.env.futures}"
if [[ ! -f "$FUTURES_ENV_FILE" ]]; then
  echo "[shioaji-futures-simulation] missing env file: $FUTURES_ENV_FILE" >&2
  exit 2
fi
set -a
source "$FUTURES_ENV_FILE"
set +a

source scripts/runtime_env.sh
run_fintech_python -m scripts.test_shioaji_futures_simulation_lifecycle "$@"
