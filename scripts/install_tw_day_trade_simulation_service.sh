#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_NAME="stockagent-tw-day-trade-simulation.service"
TEMPLATE="$REPO_ROOT/deploy/systemd/$SERVICE_NAME.in"
UNIT_PATH="/etc/systemd/system/$SERVICE_NAME"

if (( EUID != 0 )); then
  echo "[tw-day-trade-service] root privileges are required" >&2
  exit 2
fi

SERVICE_USER="${TW_DAY_TRADE_SERVICE_USER:-$(stat -c '%U' "$REPO_ROOT")}" 
SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
SERVICE_HOME="$(getent passwd "$SERVICE_USER" | cut -d: -f6)"

escape_replacement() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }

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
systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"
echo "[tw-day-trade-service] installed=$UNIT_PATH active=$(systemctl is-active "$SERVICE_NAME")"
