#!/usr/bin/env bash
# Install the reversible WSL-root mount policy without touching data services.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
if [[ "$repo_root" != /root/stockAgent ]]; then
  echo 'The WSL root policy is scoped to /root/stockAgent.' >&2
  exit 1
fi
unit=stockagent-wsl-no-online-discard.service
install -m 0644 "$repo_root/deploy/systemd/$unit" "/etc/systemd/system/$unit"
systemctl daemon-reload
systemctl enable --now "$unit"
