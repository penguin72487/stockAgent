#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
source scripts/runtime_env.sh

selected_python="$(resolve_fintech_python)"
engine_pid=""
dashboard_pid=""
dashboard_started_at=0
dashboard_failures=0
SHIOAJI_ENV_FILE="${SHIOAJI_ENV_FILE:-$REPO_ROOT/.env}"
STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR="${STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR:-$REPO_ROOT/artifacts/cache/tw_day_trade_dashboard_indexes}"
mkdir -p "$STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR"
export STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR

if [[ ! -f "$SHIOAJI_ENV_FILE" ]]; then
  echo "[tw-day-trade] missing Shioaji quote environment file: $SHIOAJI_ENV_FILE" >&2
  exit 2
fi

stop_children() {
  for child_pid in "$engine_pid" "$dashboard_pid"; do
    if [[ -n "$child_pid" ]]; then
      kill "$child_pid" 2>/dev/null || true
    fi
  done
  wait 2>/dev/null || true
}
trap stop_children EXIT
trap 'exit 0' INT TERM

start_dashboard() {
  "$selected_python" scripts/serve_tw_day_trade_dashboard.py \
    --host "${TW_DAY_TRADE_DASHBOARD_HOST:-127.0.0.1}" \
    --port "${TW_DAY_TRADE_DASHBOARD_PORT:-8766}" \
    --state-dir "${TW_DAY_TRADE_STATE_DIR:-artifacts/live/tw_day_trade_simulation}" \
    --static-root "${TW_DAY_TRADE_DASHBOARD_STATIC_ROOT:-services/tw_day_trade_dashboard}" &
  dashboard_pid=$!
  dashboard_started_at=$SECONDS
}

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
engine_pid=$!

start_dashboard
while true; do
  completed_pid=""
  child_status=0
  wait -n -p completed_pid "$engine_pid" "$dashboard_pid" || child_status=$?
  if [[ "${completed_pid:-}" == "$engine_pid" ]]; then
    engine_pid=""
    echo "[tw-day-trade] paper engine exited status=$child_status; restarting unit" >&2
    # Even a clean unexpected engine exit must be restarted by systemd.
    if (( child_status == 0 )); then
      exit 1
    fi
    exit "$child_status"
  fi
  if [[ "${completed_pid:-}" != "$dashboard_pid" ]]; then
    echo "[tw-day-trade] child wait failed status=$child_status; restarting unit" >&2
    exit 1
  fi
  dashboard_pid=""
  if (( SECONDS - dashboard_started_at >= 60 )); then
    dashboard_failures=0
  fi
  dashboard_failures=$((dashboard_failures < 5 ? dashboard_failures + 1 : 5))
  retry_seconds=$((1 << dashboard_failures))
  if (( retry_seconds > 30 )); then
    retry_seconds=30
  fi
  echo "[tw-day-trade] read-only dashboard exited status=$child_status; engine remains running; retry in ${retry_seconds}s" >&2
  sleep "$retry_seconds"
  start_dashboard
done
