#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-handoff-index-prepack.service" "$temporary/stockagent-handoff-index-prepack.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-handoff-index-prepack.service stockagent-handoff-index-prepack.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-handoff-index-prepack.service" "$temporary/stockagent-handoff-index-prepack.timer"
install -m 0644 "$temporary/stockagent-handoff-index-prepack.service" /etc/systemd/system/stockagent-handoff-index-prepack.service
install -m 0644 "$temporary/stockagent-handoff-index-prepack.timer" /etc/systemd/system/stockagent-handoff-index-prepack.timer
systemctl daemon-reload
systemctl enable --now stockagent-handoff-index-prepack.timer
systemctl is-active --quiet stockagent-handoff-index-prepack.timer
printf '%s\n' 'Fixed-source opportunistic E preparation timer enabled.'
