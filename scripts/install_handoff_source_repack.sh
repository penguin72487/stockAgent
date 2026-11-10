#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-handoff-source-repack.service" "$temporary/stockagent-handoff-source-repack.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-handoff-source-repack.service stockagent-handoff-source-repack.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-handoff-source-repack.service" "$temporary/stockagent-handoff-source-repack.timer"
install -m 0644 "$temporary/stockagent-handoff-source-repack.service" /etc/systemd/system/stockagent-handoff-source-repack.service
install -m 0644 "$temporary/stockagent-handoff-source-repack.timer" /etc/systemd/system/stockagent-handoff-source-repack.timer
systemctl daemon-reload
systemctl enable --now stockagent-handoff-source-repack.timer
systemctl is-active --quiet stockagent-handoff-source-repack.timer
printf '%s\n' 'Enrolled C handoff payload repack and retirement timer enabled.'
