#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_name="stockagent-hot-artifact-sync.service"

if (( EUID != 0 )); then
  echo "[hot-artifact-sync] root privileges are required" >&2
  exit 2
fi

service_user="${LIVE_ARTIFACT_SYNC_USER:-$(stat -c '%U' "$repo_root")}"
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
install -d -m 0755 /srv/stockagent-artifacts-hot
install -m 0644 \
  "$repo_root/deploy/syncthing/stockagent-artifacts-live.stignore" \
  /srv/stockagent-artifacts-hot/.stignore

source "$repo_root/scripts/runtime_env.sh"
run_fintech_python "$repo_root/scripts/manage_cold_artifacts.py" \
  --live-sync-root /srv/stockagent-artifacts-hot \
  rebuild-ignore

install -m 0644 "$temporary_dir/$service_name" /etc/systemd/system/$service_name
chmod 0755 \
  "$repo_root/scripts/manage_cold_artifacts.py" \
  "$repo_root/scripts/run_live_artifact_sync.py" \
  "$repo_root/scripts/run_live_artifact_sync.sh" \
  "$repo_root/scripts/install_hot_artifact_sync_service.sh"
systemctl daemon-reload
systemctl enable --now "$service_name"
systemctl restart "$service_name"
echo "[hot-artifact-sync] service_active=$(systemctl is-active "$service_name")"
