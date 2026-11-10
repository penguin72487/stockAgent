#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bash "$repo_root/scripts/install_source_local_retirement.sh"
bash "$repo_root/scripts/install_prepared_stream_install.sh"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-source-local-repack.service" "$temporary/stockagent-source-local-repack.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-source-local-repack.service stockagent-source-local-repack.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-source-local-repack.service" "$temporary/stockagent-source-local-repack.timer"
install -m 0644 "$temporary/stockagent-source-local-repack.service" /etc/systemd/system/stockagent-source-local-repack.service
install -m 0644 "$temporary/stockagent-source-local-repack.timer" /etc/systemd/system/stockagent-source-local-repack.timer
systemctl daemon-reload
systemctl enable --now stockagent-source-local-repack.timer
systemctl start --no-block stockagent-source-local-repack.service
systemctl is-active --quiet stockagent-source-local-repack.timer
printf '%s\n' 'Fixed E/D-source dataset repack and retirement queue enabled.'
