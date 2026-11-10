#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-source-local-retirement.service" "$temporary/stockagent-source-local-retirement.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-source-local-retirement.service stockagent-source-local-retirement.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-source-local-retirement.service" "$temporary/stockagent-source-local-retirement.timer"
install -m 0644 "$temporary/stockagent-source-local-retirement.service" /etc/systemd/system/stockagent-source-local-retirement.service
install -m 0644 "$temporary/stockagent-source-local-retirement.timer" /etc/systemd/system/stockagent-source-local-retirement.timer
systemctl daemon-reload
systemctl enable --now stockagent-source-local-retirement.timer
systemctl is-active --quiet stockagent-source-local-retirement.timer
printf '%s\n' 'Independent source-local D retirement timer enabled.'
