#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-storage-monitor-guardian.service" "$temporary/stockagent-storage-monitor-guardian.timer"; rmdir -- "$temporary"' EXIT
for unit in stockagent-storage-monitor-guardian.service stockagent-storage-monitor-guardian.timer; do
  install -m 0644 "$repo_root/deploy/systemd/$unit.in" "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-storage-monitor-guardian.service" "$temporary/stockagent-storage-monitor-guardian.timer"
install -m 0644 "$temporary/stockagent-storage-monitor-guardian.service" /etc/systemd/system/stockagent-storage-monitor-guardian.service
install -m 0644 "$temporary/stockagent-storage-monitor-guardian.timer" /etc/systemd/system/stockagent-storage-monitor-guardian.timer
systemctl daemon-reload
systemctl enable --now stockagent-storage-monitor-guardian.timer
systemctl start --no-block stockagent-storage-monitor-guardian.service
systemctl is-active --quiet stockagent-storage-monitor-guardian.timer
