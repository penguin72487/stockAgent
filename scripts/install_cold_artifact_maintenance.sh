#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if (( EUID != 0 )); then
  echo "Root privileges are required." >&2
  exit 2
fi

edge_state_root="${STOCKAGENT_PACKED_EDGE_STATE_ROOT:-/var/lib/stockagent-packed-edge}"
if [[ -f "$edge_state_root/state.json" ]]; then
  echo "Refusing full-replica cold maintenance on an index-only edge node." >&2
  exit 2
fi

chmod 0755 \
  "$repo_root/scripts/maintain_cold_artifacts.py" \
  "$repo_root/scripts/run_cold_artifact_maintenance.sh" \
  "$repo_root/scripts/install_cold_artifact_maintenance.sh"

if [[ -d /run/systemd/system ]]; then
  temporary_dir="$(mktemp -d)"
  trap 'rm -rf "$temporary_dir"' EXIT
  escaped_root="${repo_root//\\/\\\\}"
  escaped_root="${escaped_root//&/\\&}"
  for unit in stockagent-cold-artifact-maintenance.service stockagent-cold-artifact-maintenance.timer; do
    sed "s&__REPO_ROOT__&$escaped_root&g" \
      "$repo_root/deploy/systemd/${unit}.in" >"$temporary_dir/$unit"
  done
  systemd-analyze verify "$temporary_dir"/*.service "$temporary_dir"/*.timer
  install -m 0644 "$temporary_dir"/* /etc/systemd/system/
  systemctl daemon-reload
  systemctl enable --now stockagent-cold-artifact-maintenance.timer
else
  install -m 0644 /dev/stdin /etc/cron.d/stockagent-cold-artifact-maintenance <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
*/5 * * * * root $repo_root/scripts/run_cold_artifact_maintenance.sh >>/var/log/stockagent-cold-artifact-maintenance.log 2>&1
EOF
fi

echo "Installed automatic cold-artifact maintenance for $repo_root/artifacts/ablations"
