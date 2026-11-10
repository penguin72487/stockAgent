#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-handoff-index-archive.service" "$temporary/stockagent-handoff-index-archive.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-handoff-index-archive.service stockagent-handoff-index-archive.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-handoff-index-archive.service" "$temporary/stockagent-handoff-index-archive.timer"
install -m 0644 "$temporary/stockagent-handoff-index-archive.service" /etc/systemd/system/stockagent-handoff-index-archive.service
install -m 0644 "$temporary/stockagent-handoff-index-archive.timer" /etc/systemd/system/stockagent-handoff-index-archive.timer
systemctl daemon-reload
systemctl enable --now stockagent-handoff-index-archive.timer
systemctl is-active --quiet stockagent-handoff-index-archive.timer
printf '%s\n' 'Fixed C source-index archive timer enabled.'
