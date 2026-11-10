#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-d-cold-e-handoff.service" "$temporary/stockagent-d-cold-e-handoff.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-d-cold-e-handoff.service stockagent-d-cold-e-handoff.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-d-cold-e-handoff.service" "$temporary/stockagent-d-cold-e-handoff.timer"
install -m 0644 "$temporary/stockagent-d-cold-e-handoff.service" /etc/systemd/system/stockagent-d-cold-e-handoff.service
install -m 0644 "$temporary/stockagent-d-cold-e-handoff.timer" /etc/systemd/system/stockagent-d-cold-e-handoff.timer
systemctl daemon-reload
systemctl enable --now stockagent-d-cold-e-handoff.timer
systemctl start --no-block stockagent-d-cold-e-handoff.service
systemctl is-active --quiet stockagent-d-cold-e-handoff.timer
printf '%s\n' 'D-to-E staged handoff timer enabled; each wave retains its E copy.'
