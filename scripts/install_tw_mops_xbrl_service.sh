#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
authorization_source="${1:-}"
authorization_target="/etc/stockagent/mops_xbrl_authorization.json"
service_name="stockagent-tw-mops-xbrl.service"
timer_name="stockagent-tw-mops-xbrl.timer"

if (( EUID != 0 )) || [[ -z "$authorization_source" || ! -f "$authorization_source" ]]; then
  echo "usage: sudo $0 /path/to/TWSE-authorization-attestation.json" >&2
  exit 2
fi
cd "$repo_root"
source scripts/runtime_env.sh
run_fintech_python -c 'from pathlib import Path; import sys; from downloader.download_tw_mops_xbrl import _authorization; _authorization(Path(sys.argv[1]))' "$authorization_source"
if [[ -e "$authorization_target" ]] && ! cmp -s "$authorization_source" "$authorization_target"; then
  echo "Existing $authorization_target differs; review and update it manually" >&2
  exit 2
fi
for unit in "$service_name" "$timer_name"; do
  if [[ ! -f "$repo_root/deploy/systemd/$unit.in" || -e "/etc/systemd/system/$unit" ]]; then
    echo "Missing template or existing unit for $unit requires manual review" >&2
    exit 2
  fi
done
install -d -m 0700 /etc/stockagent
if [[ ! -e "$authorization_target" ]]; then
  install -m 0600 "$authorization_source" "$authorization_target"
fi
for unit in "$service_name" "$timer_name"; do
  template="$repo_root/deploy/systemd/$unit.in"
  target="/etc/systemd/system/$unit"
  escaped_root="$(printf '%s' "$repo_root" | sed 's/[&|\\]/\\&/g')"
  sed "s|__REPO_ROOT__|$escaped_root|g" "$template" | install -m 0644 /dev/stdin "$target"
done
systemd-analyze verify "/etc/systemd/system/$service_name" "/etc/systemd/system/$timer_name"
systemctl daemon-reload
systemctl enable --now "$timer_name"
echo "Enabled $timer_name; the downloader still validates authorization each run."
