#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_NAME="stockagent-tw-day-trade-simulation.service"
PREOPEN_TIMER="stockagent-tw-day-trade-preopen-gate.timer"

if (( EUID != 0 )); then
  echo "[tw-day-trade-service] root privileges are required" >&2
  exit 2
fi

SERVICE_USER="${TW_DAY_TRADE_SERVICE_USER:-$(stat -c '%U' "$REPO_ROOT")}" 
SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
SERVICE_HOME="$(getent passwd "$SERVICE_USER" | cut -d: -f6)"

escape_replacement() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }

temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
units=(
  "$SERVICE_NAME"
  stockagent-tw-day-trade-preopen-gate.service
  "$PREOPEN_TIMER"
)
for unit in "${units[@]}"; do
  sed \
    -e "s|@REPO_ROOT@|$(escape_replacement "$REPO_ROOT")|g" \
    -e "s|@SERVICE_USER@|$(escape_replacement "$SERVICE_USER")|g" \
    -e "s|@SERVICE_GROUP@|$(escape_replacement "$SERVICE_GROUP")|g" \
    -e "s|@SERVICE_HOME@|$(escape_replacement "$SERVICE_HOME")|g" \
    "$REPO_ROOT/deploy/systemd/$unit.in" > "$temporary_dir/$unit"
done

systemd-analyze verify "$temporary_dir"/*.service "$temporary_dir"/*.timer
install -m 0644 "$temporary_dir"/* /etc/systemd/system/
chmod 0755 \
  "$REPO_ROOT/scripts/check_tw_day_trade_preopen_readiness.py" \
  "$REPO_ROOT/scripts/run_tw_day_trade_preopen_gate.sh"
systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"
systemctl enable --now "$PREOPEN_TIMER"
echo "[tw-day-trade-service] service_active=$(systemctl is-active "$SERVICE_NAME") preopen_timer_active=$(systemctl is-active "$PREOPEN_TIMER")"
