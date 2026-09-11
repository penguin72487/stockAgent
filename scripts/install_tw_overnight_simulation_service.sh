#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_name="stockagent-tw-overnight-simulation.service"
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
sed \
  -e "s|@REPO_ROOT@|$(escape_replacement "$repo_root")|g" \
  -e "s|@SERVICE_USER@|$(escape_replacement "$service_user")|g" \
  -e "s|@SERVICE_GROUP@|$(escape_replacement "$service_group")|g" \
  -e "s|@SERVICE_HOME@|$(escape_replacement "$service_home")|g" \
  "$repo_root/deploy/systemd/$service_name.in" > "$temporary_dir/$service_name"
systemd-analyze verify "$temporary_dir/$service_name"
install -m 0644 "$temporary_dir/$service_name" /etc/systemd/system/
chmod 0755 \
  "$repo_root/scripts/run_tw_overnight_services.sh" \
  "$repo_root/scripts/run_tw_overnight_simulation.py"
systemctl daemon-reload
systemctl enable --now "$service_name"
echo "[tw-overnight-service] service_active=$(systemctl is-active "$service_name")"
