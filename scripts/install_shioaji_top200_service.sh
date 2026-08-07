#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_NAME="stockagent-shioaji-top200.service"
START_NOW=true

usage() {
  echo "Usage: sudo bash scripts/install_shioaji_top200_service.sh [--taifex-bidask] [--no-start]" >&2
}

for argument in "$@"; do
  case "$argument" in
    --no-start)
      START_NOW=false
      ;;
    --taifex-bidask)
      SERVICE_NAME="stockagent-shioaji-taifex-bidask.service"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

TEMPLATE="$REPO_ROOT/deploy/systemd/$SERVICE_NAME.in"
UNIT_PATH="/etc/systemd/system/$SERVICE_NAME"

if (( EUID != 0 )); then
  echo "[shioaji-service] root privileges are required; run with sudo." >&2
  exit 2
fi
if [[ ! -f "$TEMPLATE" ]]; then
  echo "[shioaji-service] missing unit template: $TEMPLATE" >&2
  exit 2
fi
if [[ ! -f "$REPO_ROOT/.env" ]]; then
  echo "[shioaji-service] missing Shioaji environment file: $REPO_ROOT/.env" >&2
  exit 2
fi

SERVICE_USER="${SHIOAJI_SERVICE_USER:-$(stat -c '%U' "$REPO_ROOT")}"
if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  echo "[shioaji-service] unknown service user: $SERVICE_USER" >&2
  exit 2
fi
SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
SERVICE_HOME="$(getent passwd "$SERVICE_USER" | cut -d: -f6)"
if [[ -z "$SERVICE_HOME" ]]; then
  echo "[shioaji-service] cannot resolve home for service user: $SERVICE_USER" >&2
  exit 2
fi
if ! runuser -u "$SERVICE_USER" -- test -r "$REPO_ROOT/.env"; then
  echo "[shioaji-service] $SERVICE_USER cannot read $REPO_ROOT/.env" >&2
  exit 2
fi

escape_replacement() {
  printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}

rendered_unit="$(mktemp --suffix=.service)"
trap 'rm -f "$rendered_unit"' EXIT
repo_replacement="$(escape_replacement "$REPO_ROOT")"
user_replacement="$(escape_replacement "$SERVICE_USER")"
group_replacement="$(escape_replacement "$SERVICE_GROUP")"
home_replacement="$(escape_replacement "$SERVICE_HOME")"
sed \
  -e "s|@REPO_ROOT@|$repo_replacement|g" \
  -e "s|@SERVICE_USER@|$user_replacement|g" \
  -e "s|@SERVICE_GROUP@|$group_replacement|g" \
  -e "s|@SERVICE_HOME@|$home_replacement|g" \
  "$TEMPLATE" > "$rendered_unit"

systemd-analyze verify "$rendered_unit"
install -m 0644 "$rendered_unit" "$UNIT_PATH"
chmod go-rwx "$REPO_ROOT/.env"
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
if [[ "$START_NOW" == true ]]; then
  systemctl restart "$SERVICE_NAME"
fi

echo "[shioaji-service] installed=$UNIT_PATH enabled=$(systemctl is-enabled "$SERVICE_NAME")"
if [[ "$START_NOW" == true ]]; then
  echo "[shioaji-service] active=$(systemctl is-active "$SERVICE_NAME")"
else
  echo "[shioaji-service] start_deferred=true"
fi
