#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_NAME="stockagent-live-artifact-sync.service"

if (( EUID != 0 )); then
  echo "[live-artifact-sync] root privileges are required" >&2
  exit 2
fi

SERVICE_USER="${LIVE_ARTIFACT_SYNC_USER:-$(stat -c '%U' "$REPO_ROOT")}" 
SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
SERVICE_HOME="$(getent passwd "$SERVICE_USER" | cut -d: -f6)"

escape_replacement() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }

temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
sed \
  -e "s|@REPO_ROOT@|$(escape_replacement "$REPO_ROOT")|g" \
  -e "s|@SERVICE_USER@|$(escape_replacement "$SERVICE_USER")|g" \
  -e "s|@SERVICE_GROUP@|$(escape_replacement "$SERVICE_GROUP")|g" \
  -e "s|@SERVICE_HOME@|$(escape_replacement "$SERVICE_HOME")|g" \
  "$REPO_ROOT/deploy/systemd/$SERVICE_NAME.in" > "$temporary_dir/$SERVICE_NAME"

systemd-analyze verify "$temporary_dir/$SERVICE_NAME"
install -d -m 0755 /srv/stockagent-artifacts-live
if [[ ! -e /srv/stockagent-artifacts-live/.stignore-cold-local ]]; then
  install -m 0644 /dev/null /srv/stockagent-artifacts-live/.stignore-cold-local
fi
install -m 0644 \
  "$REPO_ROOT/deploy/syncthing/stockagent-artifacts-live.stignore" \
  /srv/stockagent-artifacts-live/.stignore
install -m 0644 "$temporary_dir/$SERVICE_NAME" /etc/systemd/system/$SERVICE_NAME
chmod 0755 \
  "$REPO_ROOT/scripts/run_live_artifact_sync.py" \
  "$REPO_ROOT/scripts/run_live_artifact_sync.sh"
systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"
echo "[live-artifact-sync] service_active=$(systemctl is-active "$SERVICE_NAME")"
