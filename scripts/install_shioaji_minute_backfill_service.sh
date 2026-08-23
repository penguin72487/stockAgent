#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="stockagent-shioaji-minute-backfill.service"
TIMER_NAME="stockagent-shioaji-minute-backfill.timer"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE="$REPO_ROOT/deploy/systemd/$SERVICE_NAME.in"
TIMER_TEMPLATE="$REPO_ROOT/deploy/systemd/$TIMER_NAME.in"
UNIT_PATH="/etc/systemd/system/$SERVICE_NAME"
TIMER_PATH="/etc/systemd/system/$TIMER_NAME"
START_NOW=false

usage() {
  echo "Usage: sudo bash scripts/install_shioaji_minute_backfill_service.sh [--run-now|--no-start]" >&2
}

for argument in "$@"; do
  case "$argument" in
    --run-now)
      START_NOW=true
      ;;
    --no-start)
      START_NOW=false
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

if (( EUID != 0 )); then
  echo "[shioaji-minute-service] root privileges are required" >&2
  exit 2
fi
if [[ ! -f "$TEMPLATE" || ! -f "$TIMER_TEMPLATE" || ! -f "$REPO_ROOT/.env" ]]; then
  echo "[shioaji-minute-service] service/timer template or .env is missing" >&2
  exit 2
fi

SERVICE_USER="${SHIOAJI_SERVICE_USER:-$(stat -c '%U' "$REPO_ROOT")}"
if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  echo "[shioaji-minute-service] unknown service user: $SERVICE_USER" >&2
  exit 2
fi
SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
SERVICE_HOME="$(getent passwd "$SERVICE_USER" | cut -d: -f6)"
if [[ -z "$SERVICE_HOME" ]]; then
  echo "[shioaji-minute-service] cannot resolve home for $SERVICE_USER" >&2
  exit 2
fi
if ! runuser -u "$SERVICE_USER" -- test -r "$REPO_ROOT/.env"; then
  echo "[shioaji-minute-service] $SERVICE_USER cannot read .env" >&2
  exit 2
fi

escape_replacement() {
  printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}

rendered_unit="$(mktemp --suffix=.service)"
rendered_timer="$(mktemp --suffix=.timer)"
trap 'rm -f "$rendered_unit" "$rendered_timer"' EXIT
sed \
  -e "s|@REPO_ROOT@|$(escape_replacement "$REPO_ROOT")|g" \
  -e "s|@SERVICE_USER@|$(escape_replacement "$SERVICE_USER")|g" \
  -e "s|@SERVICE_GROUP@|$(escape_replacement "$SERVICE_GROUP")|g" \
  -e "s|@SERVICE_HOME@|$(escape_replacement "$SERVICE_HOME")|g" \
  "$TEMPLATE" > "$rendered_unit"
cp "$TIMER_TEMPLATE" "$rendered_timer"

systemd-analyze verify "$rendered_unit" "$rendered_timer"
install -m 0644 "$rendered_unit" "$UNIT_PATH"
install -m 0644 "$rendered_timer" "$TIMER_PATH"
chmod 0755 "$REPO_ROOT/scripts/run_shioaji_minute_full_backfill.sh"
chmod go-rwx "$REPO_ROOT/.env"
systemctl daemon-reload
# The full-market repair has reached an 8+ GiB working set in production. It
# must remain resumable background work, never part of the boot target.
systemctl disable "$SERVICE_NAME" >/dev/null 2>&1 || true
systemctl enable --now "$TIMER_NAME"
if [[ "$START_NOW" == true ]]; then
  systemctl restart "$SERVICE_NAME"
fi

echo "[shioaji-minute-service] installed=$UNIT_PATH trigger=$(systemctl is-enabled "$SERVICE_NAME" 2>/dev/null || true)"
echo "[shioaji-minute-service] timer=$TIMER_PATH enabled=$(systemctl is-enabled "$TIMER_NAME")"
if [[ "$START_NOW" == true ]]; then
  echo "[shioaji-minute-service] active=$(systemctl is-active "$SERVICE_NAME")"
fi
