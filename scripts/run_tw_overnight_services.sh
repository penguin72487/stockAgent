#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh

selected_python="$(resolve_fintech_python)"
exec "$selected_python" scripts/run_tw_overnight_simulation.py \
  --markets-dir "${TW_OVERNIGHT_MARKETS_DIR:-services/discord_bot/markets}" \
  --state-dir "${TW_OVERNIGHT_STATE_DIR:-artifacts/live/tw_overnight_simulation}" \
  --quote-broker-state-dir "${TW_DAY_TRADE_STATE_DIR:-artifacts/live/tw_day_trade_simulation}" \
  --poll-seconds "${TW_OVERNIGHT_POLL_SECONDS:-0.1}"
