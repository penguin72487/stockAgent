#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-prepared-stream-install.service" "$temporary/stockagent-prepared-stream-install.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-prepared-stream-install.service stockagent-prepared-stream-install.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-prepared-stream-install.service" "$temporary/stockagent-prepared-stream-install.timer"
install -m 0644 "$temporary/stockagent-prepared-stream-install.service" /etc/systemd/system/stockagent-prepared-stream-install.service
install -m 0644 "$temporary/stockagent-prepared-stream-install.timer" /etc/systemd/system/stockagent-prepared-stream-install.timer
systemctl daemon-reload
systemctl enable --now stockagent-prepared-stream-install.timer
systemctl is-active --quiet stockagent-prepared-stream-install.timer
printf '%s\n' 'Prepared source E to D install timer enabled.'
