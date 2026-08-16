#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_template="$repo_root/deploy/systemd/stockagent-shioaji-storage-monitor.service.in"
timer_template="$repo_root/deploy/systemd/stockagent-shioaji-storage-monitor.timer.in"
service_target="/etc/systemd/system/stockagent-shioaji-storage-monitor.service"
timer_target="/etc/systemd/system/stockagent-shioaji-storage-monitor.timer"
service_tmp="$(mktemp)"
timer_tmp="$(mktemp)"
trap 'rm -f "$service_tmp" "$timer_tmp"' EXIT

escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
sed "s&__REPO_ROOT__&$escaped_root&g" "$service_template" > "$service_tmp"
sed "s&__REPO_ROOT__&$escaped_root&g" "$timer_template" > "$timer_tmp"
install -d -m 0700 "$repo_root/artifacts/live/shioaji_storage"
install -m 0644 "$service_tmp" "$service_target"
install -m 0644 "$timer_tmp" "$timer_target"
systemd-analyze verify "$service_target" "$timer_target"
systemctl daemon-reload
systemctl start stockagent-shioaji-storage-monitor.service
systemctl enable --now stockagent-shioaji-storage-monitor.timer
systemctl is-active --quiet stockagent-shioaji-storage-monitor.timer
echo "stockagent-shioaji-storage-monitor.timer active; snapshot refreshed"
