#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
units=(
  stockagent-tw-overnight-simulation.service
  stockagent-tw-overnight-history.service
  stockagent-tw-overnight-history.timer
)
if (( EUID != 0 )); then
  echo "[tw-overnight-service] root privileges are required" >&2
  exit 2
fi
service_user="${TW_OVERNIGHT_SERVICE_USER:-$(stat -c '%U' "$repo_root")}" 
service_group="$(id -gn "$service_user")"
service_home="$(getent passwd "$service_user" | cut -d: -f6)"
escape_replacement() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
for unit in "${units[@]}"; do
  sed \
    -e "s|@REPO_ROOT@|$(escape_replacement "$repo_root")|g" \
    -e "s|@SERVICE_USER@|$(escape_replacement "$service_user")|g" \
    -e "s|@SERVICE_GROUP@|$(escape_replacement "$service_group")|g" \
    -e "s|@SERVICE_HOME@|$(escape_replacement "$service_home")|g" \
    "$repo_root/deploy/systemd/$unit.in" > "$temporary_dir/$unit"
done
systemd-analyze verify "$temporary_dir"/*
install -m 0644 "$temporary_dir"/* /etc/systemd/system/
chmod 0755 \
  "$repo_root/scripts/run_tw_overnight_services.sh" \
  "$repo_root/scripts/run_tw_overnight_simulation.py" \
  "$repo_root/scripts/run_tw_overnight_history_maintenance.sh" \
  "$repo_root/scripts/maintain_tw_overnight_history.py" \
  "$repo_root/scripts/deploy_tw_overnight_history.py"
systemctl daemon-reload
systemctl enable --now \
  stockagent-tw-overnight-simulation.service \
  stockagent-tw-overnight-history.timer
echo "[tw-overnight-service] service_active=$(systemctl is-active stockagent-tw-overnight-simulation.service)"
echo "[tw-overnight-service] history_timer=$(systemctl is-active stockagent-tw-overnight-history.timer)"
