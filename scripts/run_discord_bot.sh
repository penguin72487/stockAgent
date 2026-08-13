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
if [[ ! -f "$SHIOAJI_ENV_FILE" ]]; then
  echo "[discord-bot] missing Shioaji quote environment file: $SHIOAJI_ENV_FILE" >&2
  exit 2
fi
set -a
source "$SHIOAJI_ENV_FILE"
source "$BOT_ENV_FILE"
set +a
if [[ -z "${DISCORD_BOT_TOKEN:-}" || -z "${DISCORD_CHANNEL_ID:-}" ]]; then
  echo "[discord-bot] DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID are required" >&2
  exit 2
fi
if [[ -z "${SHIOAJI_API_KEY:-}" || -z "${SHIOAJI_SECRET_KEY:-}" ]]; then
  echo "[discord-bot] SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required for TW opening quotes" >&2
  exit 2
fi

source scripts/runtime_env.sh
run_fintech_python services/discord_bot/bot.py
