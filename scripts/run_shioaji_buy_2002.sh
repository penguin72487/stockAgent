#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

LIVE_ACK_OVERRIDE="${SHIOAJI_TRADE_LIVE_ACK:-}"
MAX_NOTIONAL_OVERRIDE="${SHIOAJI_TRADE_MAX_NOTIONAL_NTD:-}"
CA_PASSWORD_OVERRIDE="${SHIOAJI_TRADE_CA_PASSWORD:-}"
TRADE_ENV_FILE="${SHIOAJI_TRADE_ENV_FILE:-$REPO_ROOT/.env.trade}"
if [[ -f "$TRADE_ENV_FILE" ]]; then
  set -a
  source "$TRADE_ENV_FILE"
  set +a
fi
if [[ -n "$LIVE_ACK_OVERRIDE" ]]; then
  export SHIOAJI_TRADE_LIVE_ACK="$LIVE_ACK_OVERRIDE"
fi
if [[ -n "$MAX_NOTIONAL_OVERRIDE" ]]; then
  export SHIOAJI_TRADE_MAX_NOTIONAL_NTD="$MAX_NOTIONAL_OVERRIDE"
fi
if [[ -n "$CA_PASSWORD_OVERRIDE" ]]; then
  export SHIOAJI_TRADE_CA_PASSWORD="$CA_PASSWORD_OVERRIDE"
fi

source scripts/runtime_env.sh
run_fintech_python scripts/place_shioaji_buy_2002.py "$@"
