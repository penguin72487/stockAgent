#!/usr/bin/env bash
# Select the existing Vast identity for this read-only observer only.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
key=/root/.ssh/stockagent_vastai_migration_20261023
if [[ ! -f "$key" || -L "$key" || "$(stat -c '%a:%u' "$key")" != 600:0 ]]; then
  echo 'The enrolled Vast observer key is missing or has unsafe permissions.' >&2
  exit 1
fi
directory=/etc/systemd/system/stockagent-storage-monitor.service.d
install -d -m 0755 "$directory"
install -m 0644 "$repo_root/deploy/systemd/stockagent-storage-monitor-vast-identity.conf" \
  "$directory/40-vast-readonly-identity.conf"
systemctl daemon-reload
systemctl restart stockagent-storage-monitor.service
systemctl is-active --quiet stockagent-storage-monitor.service
