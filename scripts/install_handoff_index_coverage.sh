#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-handoff-index-coverage.service" "$temporary/stockagent-handoff-index-coverage.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-handoff-index-coverage.service stockagent-handoff-index-coverage.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-handoff-index-coverage.service" "$temporary/stockagent-handoff-index-coverage.timer"
install -m 0644 "$temporary/stockagent-handoff-index-coverage.service" /etc/systemd/system/stockagent-handoff-index-coverage.service
install -m 0644 "$temporary/stockagent-handoff-index-coverage.timer" /etc/systemd/system/stockagent-handoff-index-coverage.timer
systemctl daemon-reload
systemctl enable --now stockagent-handoff-index-coverage.timer
systemctl is-active --quiet stockagent-handoff-index-coverage.timer
printf '%s\n' 'Fixed futures D coverage comparison timer enabled.'
