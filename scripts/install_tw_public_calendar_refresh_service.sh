#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
escaped_root="$(printf '%s' "$repo_root" | sed 's/[&|\\]/\\&/g')"
sed "s|__REPO_ROOT__|$escaped_root|g" \
  "$repo_root/deploy/systemd/stockagent-tw-public-calendar-refresh.service.in" \
  | install -m 0644 /dev/stdin /etc/systemd/system/stockagent-tw-public-calendar-refresh.service
install -m 0644 "$repo_root/deploy/systemd/stockagent-tw-public-calendar-refresh.timer" /etc/systemd/system/stockagent-tw-public-calendar-refresh.timer
systemctl daemon-reload
systemctl enable --now stockagent-tw-public-calendar-refresh.timer
