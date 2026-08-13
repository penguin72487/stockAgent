#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
source scripts/runtime_env.sh

selected_python="$(resolve_fintech_python)"
exec "$selected_python" scripts/serve_shioaji_taifex_simulation_dashboard.py \
  --host "${SHIOAJI_TAIFEX_DASHBOARD_HOST:-127.0.0.1}" \
  --port "${SHIOAJI_TAIFEX_DASHBOARD_PORT:-8765}" \
  --state-dir "${SHIOAJI_TAIFEX_STRATEGY_STATE_DIR:-artifacts/live/shioaji_taifex_volatility_simulation}" \
  --api-receipt-dir "${SHIOAJI_TAIFEX_API_RECEIPT_DIR:-artifacts/orders/shioaji_futures_simulation}" \
  --static-root "${SHIOAJI_TAIFEX_DASHBOARD_STATIC_ROOT:-services/taifex_dashboard}"
