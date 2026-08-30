#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BOT_ENV_FILE="${STOCKAGENT_DISCORD_ENV_FILE:-$REPO_ROOT/services/discord_bot/.env}"
SHIOAJI_ENV_FILE="${SHIOAJI_ENV_FILE:-$REPO_ROOT/.env}"
if [[ ! -f "$BOT_ENV_FILE" ]]; then
  echo "[discord-bot] missing environment file: $BOT_ENV_FILE" >&2
  exit 2
fi
set -a
if [[ -f "$SHIOAJI_ENV_FILE" ]]; then
  source "$SHIOAJI_ENV_FILE"
fi
source "$BOT_ENV_FILE"
set +a
if [[ -z "${DISCORD_BOT_TOKEN:-}" || -z "${DISCORD_CHANNEL_ID:-}" ]]; then
  echo "[discord-bot] DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID are required" >&2
  exit 2
fi
# Scheduled TW day-trade model inputs use the official MIS opening snapshot.
# Shioaji credentials belong to the independent simulation executor that reads
# actual candidate-only best Bid/Ask. Keep them optional here so Discord
# connectivity cannot become an execution-start dependency.

source scripts/runtime_env.sh
run_fintech_python services/discord_bot/bot.py
