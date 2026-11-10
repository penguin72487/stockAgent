#!/usr/bin/env bash
# Select an already-enrolled Vast key for the existing return owner only.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
key=/root/.ssh/stockagent_vastai_migration_20261023
if [[ "$repo_root" != /root/stockAgent || ! -f "$key" || -L "$key" || "$(stat -c '%a:%u' "$key")" != 600:0 ]]; then
  echo 'The enrolled Vast return key is missing or has unsafe permissions.' >&2
  exit 1
fi
directory=/etc/systemd/system/stockagent-remote-cold-artifact-ingress.service.d
install -d -m 0755 "$directory"
install -m 0644 "$repo_root/deploy/systemd/stockagent-remote-cold-artifact-ingress-vast-identity.conf" \
  "$directory/40-vast-return-identity.conf"
systemctl daemon-reload
