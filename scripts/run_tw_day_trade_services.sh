#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
source scripts/runtime_env.sh

selected_python="$(resolve_fintech_python)"
child_pids=()
SHIOAJI_ENV_FILE="${SHIOAJI_ENV_FILE:-$REPO_ROOT/.env}"

if [[ ! -f "$SHIOAJI_ENV_FILE" ]]; then
  echo "[tw-day-trade] missing Shioaji quote environment file: $SHIOAJI_ENV_FILE" >&2
  exit 2
fi

stop_children() {
  for child_pid in "${child_pids[@]:-}"; do
    kill "$child_pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap stop_children EXIT INT TERM

(
  set -a
  source "$SHIOAJI_ENV_FILE"
  set +a
  if [[ -z "${SHIOAJI_API_KEY:-}" || -z "${SHIOAJI_SECRET_KEY:-}" ]]; then
    echo "[tw-day-trade] SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required" >&2
    exit 2
  fi
  exec "$selected_python" scripts/run_tw_day_trade_simulation.py \
    --markets-dir "${TW_DAY_TRADE_MARKETS_DIR:-services/discord_bot/markets}" \
    --state-dir "${TW_DAY_TRADE_STATE_DIR:-artifacts/live/tw_day_trade_simulation}" \
    --poll-seconds "${TW_DAY_TRADE_POLL_SECONDS:-0.1}"
) &
child_pids+=("$!")

"$selected_python" scripts/serve_tw_day_trade_dashboard.py \
  --host "${TW_DAY_TRADE_DASHBOARD_HOST:-127.0.0.1}" \
  --port "${TW_DAY_TRADE_DASHBOARD_PORT:-8766}" \
  --state-dir "${TW_DAY_TRADE_STATE_DIR:-artifacts/live/tw_day_trade_simulation}" \
  --static-root "${TW_DAY_TRADE_DASHBOARD_STATIC_ROOT:-services/tw_day_trade_dashboard}" &
child_pids+=("$!")

wait -n "${child_pids[@]}"
