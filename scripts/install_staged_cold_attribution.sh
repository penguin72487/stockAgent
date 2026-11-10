#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-staged-cold-attribution.service" "$temporary/stockagent-staged-cold-attribution.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-staged-cold-attribution.service stockagent-staged-cold-attribution.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-staged-cold-attribution.service" "$temporary/stockagent-staged-cold-attribution.timer"
install -m 0644 "$temporary/stockagent-staged-cold-attribution.service" /etc/systemd/system/stockagent-staged-cold-attribution.service
install -m 0644 "$temporary/stockagent-staged-cold-attribution.timer" /etc/systemd/system/stockagent-staged-cold-attribution.timer
systemctl daemon-reload
systemctl enable --now stockagent-staged-cold-attribution.timer
systemctl is-active --quiet stockagent-staged-cold-attribution.timer
printf '%s\n' 'Staged cold metadata attribution timer enabled.'
