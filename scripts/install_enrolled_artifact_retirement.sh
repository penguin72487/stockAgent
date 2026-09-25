#!/usr/bin/env bash
set -euo pipefail

if (( EUID != 0 )); then
  echo "Root privileges are required." >&2
  exit 2
fi
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -f /var/lib/stockagent-packed-edge/state.json ]]; then
  echo "Refusing artifact retirement on an index-only edge." >&2
  exit 2
fi
if [[ ! -d /run/systemd/system ]]; then
  echo "This installer requires systemd." >&2
  exit 2
fi
temporary_dir="$(mktemp -d)"
trap 'rm -f "$temporary_dir/stockagent-enrolled-artifact-retirement.service" "$temporary_dir/stockagent-enrolled-artifact-retirement.timer"; rmdir "$temporary_dir"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
for unit in stockagent-enrolled-artifact-retirement.service stockagent-enrolled-artifact-retirement.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" \
    "$repo_root/deploy/systemd/${unit}.in" > "$temporary_dir/$unit"
done
systemd-analyze verify "$temporary_dir"/*.service "$temporary_dir"/*.timer
install -m 0644 "$temporary_dir"/* /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now stockagent-enrolled-artifact-retirement.timer
