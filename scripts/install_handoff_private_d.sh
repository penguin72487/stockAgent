#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-handoff-private-d.service" "$temporary/stockagent-handoff-private-d.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-handoff-private-d.service stockagent-handoff-private-d.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-handoff-private-d.service" "$temporary/stockagent-handoff-private-d.timer"
install -m 0644 "$temporary/stockagent-handoff-private-d.service" /etc/systemd/system/stockagent-handoff-private-d.service
install -m 0644 "$temporary/stockagent-handoff-private-d.timer" /etc/systemd/system/stockagent-handoff-private-d.timer
systemctl daemon-reload
systemctl enable --now stockagent-handoff-private-d.timer
systemctl is-active --quiet stockagent-handoff-private-d.timer
printf '%s\n' 'Private E-to-D handoff publication timer enabled.'
