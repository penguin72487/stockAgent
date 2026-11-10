#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-handoff-private-retirement.service" "$temporary/stockagent-handoff-private-retirement.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-handoff-private-retirement.service stockagent-handoff-private-retirement.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-handoff-private-retirement.service" "$temporary/stockagent-handoff-private-retirement.timer"
install -m 0644 "$temporary/stockagent-handoff-private-retirement.service" /etc/systemd/system/stockagent-handoff-private-retirement.service
install -m 0644 "$temporary/stockagent-handoff-private-retirement.timer" /etc/systemd/system/stockagent-handoff-private-retirement.timer
systemctl daemon-reload
systemctl enable --now stockagent-handoff-private-retirement.timer
systemctl is-active --quiet stockagent-handoff-private-retirement.timer
printf '%s\n' 'Private D obsolete encoding retirement timer enabled.'
