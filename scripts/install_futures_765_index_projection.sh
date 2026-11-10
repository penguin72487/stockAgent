#!/usr/bin/env bash
# Install the metadata-only 765 projection under the existing E workspace.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
if [[ "$repo_root" != /root/stockAgent || "$EUID" != 0 ]]; then
  echo 'Use the original penguin storage owner.' >&2
  exit 1
fi
for unit in stockagent-futures-765-index-projection.service stockagent-futures-765-index-projection.timer; do
  install -m 0644 "$repo_root/deploy/systemd/$unit" "/etc/systemd/system/$unit"
done
systemd-analyze verify /etc/systemd/system/stockagent-futures-765-index-projection.service \
  /etc/systemd/system/stockagent-futures-765-index-projection.timer
systemctl daemon-reload
systemctl enable --now stockagent-futures-765-index-projection.timer
systemctl start --no-block stockagent-futures-765-index-projection.service
