#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-handoff-index-integration.service" "$temporary/stockagent-handoff-index-integration.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-handoff-index-integration.service stockagent-handoff-index-integration.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-handoff-index-integration.service" "$temporary/stockagent-handoff-index-integration.timer"
install -m 0644 "$temporary/stockagent-handoff-index-integration.service" /etc/systemd/system/stockagent-handoff-index-integration.service
install -m 0644 "$temporary/stockagent-handoff-index-integration.timer" /etc/systemd/system/stockagent-handoff-index-integration.timer
systemctl daemon-reload
systemctl enable --now stockagent-handoff-index-integration.timer
systemctl is-active --quiet stockagent-handoff-index-integration.timer
printf '%s\n' 'Source-generation comparison timer enabled.'
