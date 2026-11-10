#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-handoff-index-d.service" "$temporary/stockagent-handoff-index-d.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-handoff-index-d.service stockagent-handoff-index-d.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-handoff-index-d.service" "$temporary/stockagent-handoff-index-d.timer"
install -m 0644 "$temporary/stockagent-handoff-index-d.service" /etc/systemd/system/stockagent-handoff-index-d.service
install -m 0644 "$temporary/stockagent-handoff-index-d.timer" /etc/systemd/system/stockagent-handoff-index-d.timer
systemctl daemon-reload
systemctl enable --now stockagent-handoff-index-d.timer
systemctl is-active --quiet stockagent-handoff-index-d.timer
printf '%s\n' 'Source-index E-to-private-D publication timer enabled.'
