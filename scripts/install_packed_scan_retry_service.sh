#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if (( EUID != 0 )); then
  echo "Root privileges are required." >&2
  exit 2
fi

temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-d-cold-scan-retry.service stockagent-d-cold-scan-retry.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" \
    "$repo_root/deploy/systemd/${unit}.in" >"$temporary_dir/$unit"
done
systemd-analyze verify "$temporary_dir"/*.service "$temporary_dir"/*.timer
install -m 0644 "$temporary_dir"/*.service "$temporary_dir"/*.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now stockagent-d-cold-scan-retry.timer
systemctl start stockagent-d-cold-scan-retry.service

echo "Installed bounded D-primary cold scan retry timer"
