#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_NAME="stockagent-discord-bot.service"
TEMPLATE="$REPO_ROOT/deploy/systemd/$SERVICE_NAME.in"
UNIT_PATH="/etc/systemd/system/$SERVICE_NAME"
START_NOW=true

if [[ "${1:-}" == "--no-start" ]]; then
  START_NOW=false
elif [[ -n "${1:-}" ]]; then
  echo "Usage: sudo bash scripts/install_discord_bot_service.sh [--no-start]" >&2
  exit 2
fi
if (( EUID != 0 )); then
  echo "[discord-service] root privileges are required" >&2
  exit 2
fi
if [[ ! -f "$TEMPLATE" || ! -f "$REPO_ROOT/services/discord_bot/.env" ]]; then
  echo "[discord-service] unit template or Discord env file is missing" >&2
  exit 2
fi

SERVICE_USER="${STOCKAGENT_DISCORD_SERVICE_USER:-$(stat -c '%U' "$REPO_ROOT")}"
if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  echo "[discord-service] unknown service user: $SERVICE_USER" >&2
  exit 2
fi
SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
SERVICE_HOME="$(getent passwd "$SERVICE_USER" | cut -d: -f6)"
if [[ -z "$SERVICE_HOME" ]]; then
  echo "[discord-service] cannot resolve service home" >&2
  exit 2
fi

escape_replacement() {
  printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}

rendered_unit="$(mktemp --suffix=.service)"
trap 'rm -f "$rendered_unit"' EXIT
sed \
  -e "s|@REPO_ROOT@|$(escape_replacement "$REPO_ROOT")|g" \
  -e "s|@SERVICE_USER@|$(escape_replacement "$SERVICE_USER")|g" \
  -e "s|@SERVICE_GROUP@|$(escape_replacement "$SERVICE_GROUP")|g" \
  -e "s|@SERVICE_HOME@|$(escape_replacement "$SERVICE_HOME")|g" \
  "$TEMPLATE" > "$rendered_unit"

systemd-analyze verify "$rendered_unit"
install -m 0644 "$rendered_unit" "$UNIT_PATH"
chmod 0600 "$REPO_ROOT/services/discord_bot/.env"
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
if [[ "$START_NOW" == true ]]; then
  systemctl restart "$SERVICE_NAME"
fi
echo "[discord-service] installed=$UNIT_PATH enabled=$(systemctl is-enabled "$SERVICE_NAME")"
if [[ "$START_NOW" == true ]]; then
  echo "[discord-service] active=$(systemctl is-active "$SERVICE_NAME")"
fi
