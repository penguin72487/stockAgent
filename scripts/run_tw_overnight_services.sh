#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh

selected_python="$(resolve_fintech_python)"
TW_OVERNIGHT_STATE_DIR="${TW_OVERNIGHT_STATE_DIR:-$(run_fintech_python -c 'from stockagent.service_layout import service_path; print(service_path("tw_overnight_simulation"))')}"
TW_DAY_TRADE_STATE_DIR="${TW_DAY_TRADE_STATE_DIR:-$(run_fintech_python -c 'from stockagent.service_layout import service_path; print(service_path("tw_day_trade_simulation"))')}"
exec "$selected_python" scripts/run_tw_overnight_simulation.py \
  --markets-dir "${TW_OVERNIGHT_MARKETS_DIR:-services/discord_bot/markets}" \
  --state-dir "$TW_OVERNIGHT_STATE_DIR" \
  --quote-broker-state-dir "$TW_DAY_TRADE_STATE_DIR" \
  --poll-seconds "${TW_OVERNIGHT_POLL_SECONDS:-0.1}"
