#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_NAME="stockagent-discord-bot.service"
MAINTENANCE_SERVICE_NAME="stockagent-discord-artifact-maintenance.service"
MAINTENANCE_TIMER_NAME="stockagent-discord-artifact-maintenance.timer"
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
for unit_name in "$SERVICE_NAME" "$MAINTENANCE_SERVICE_NAME" "$MAINTENANCE_TIMER_NAME"; do
  if [[ ! -f "$REPO_ROOT/deploy/systemd/$unit_name.in" ]]; then
    echo "[discord-service] unit template is missing: $unit_name.in" >&2
    exit 2
  fi
done
if [[ ! -f "$REPO_ROOT/services/discord_bot/.env" ]]; then
  echo "[discord-service] Discord env file is missing" >&2
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

rendered_dir="$(mktemp -d)"
trap 'rm -rf -- "$rendered_dir"' EXIT
for unit_name in "$SERVICE_NAME" "$MAINTENANCE_SERVICE_NAME" "$MAINTENANCE_TIMER_NAME"; do
  sed \
    -e "s|@REPO_ROOT@|$(escape_replacement "$REPO_ROOT")|g" \
    -e "s|@SERVICE_USER@|$(escape_replacement "$SERVICE_USER")|g" \
    -e "s|@SERVICE_GROUP@|$(escape_replacement "$SERVICE_GROUP")|g" \
    -e "s|@SERVICE_HOME@|$(escape_replacement "$SERVICE_HOME")|g" \
    "$REPO_ROOT/deploy/systemd/$unit_name.in" > "$rendered_dir/$unit_name"
done

systemd-analyze verify \
  "$rendered_dir/$SERVICE_NAME" \
  "$rendered_dir/$MAINTENANCE_SERVICE_NAME" \
  "$rendered_dir/$MAINTENANCE_TIMER_NAME"
install -m 0644 "$rendered_dir/$SERVICE_NAME" "/etc/systemd/system/$SERVICE_NAME"
install -m 0644 "$rendered_dir/$MAINTENANCE_SERVICE_NAME" "/etc/systemd/system/$MAINTENANCE_SERVICE_NAME"
install -m 0644 "$rendered_dir/$MAINTENANCE_TIMER_NAME" "/etc/systemd/system/$MAINTENANCE_TIMER_NAME"
chmod 0600 "$REPO_ROOT/services/discord_bot/.env"
chmod 0755 \
  "$REPO_ROOT/scripts/run_discord_bot.sh" \
  "$REPO_ROOT/scripts/run_discord_artifact_maintenance.sh" \
  "$REPO_ROOT/scripts/run_discord_artifact_maintenance.py"
systemctl daemon-reload
systemctl enable "$SERVICE_NAME" "$MAINTENANCE_TIMER_NAME"
if [[ "$START_NOW" == true ]]; then
  systemctl restart "$SERVICE_NAME"
  systemctl start "$MAINTENANCE_TIMER_NAME"
fi
echo "[discord-service] installed=/etc/systemd/system/$SERVICE_NAME enabled=$(systemctl is-enabled "$SERVICE_NAME")"
echo "[discord-service] maintenance_service=/etc/systemd/system/$MAINTENANCE_SERVICE_NAME"
echo "[discord-service] maintenance_timer=/etc/systemd/system/$MAINTENANCE_TIMER_NAME enabled=$(systemctl is-enabled "$MAINTENANCE_TIMER_NAME")"
if [[ "$START_NOW" == true ]]; then
  echo "[discord-service] active=$(systemctl is-active "$SERVICE_NAME")"
  echo "[discord-service] timer_active=$(systemctl is-active "$MAINTENANCE_TIMER_NAME")"
fi
