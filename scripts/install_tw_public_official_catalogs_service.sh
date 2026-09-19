#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_name="stockagent-tw-public-official-catalogs.service"
timer_name="stockagent-tw-public-official-catalogs.timer"
if (( EUID != 0 )); then
  echo "[tw-public-catalogs] root privileges are required" >&2
  exit 2
fi

rendered_service="$(mktemp --suffix=.service)"
rendered_timer="$(mktemp --suffix=.timer)"
trap 'rm -- "$rendered_service" "$rendered_timer"' EXIT
escaped_repo_root="$(printf '%s' "$repo_root" | sed 's/[&|\\]/\\&/g')"
sed "s|__REPO_ROOT__|$escaped_repo_root|g" \
  "$repo_root/deploy/systemd/$service_name.in" > "$rendered_service"
cp "$repo_root/deploy/systemd/$timer_name.in" "$rendered_timer"
systemd-analyze verify "$rendered_service" "$rendered_timer"
install -m 0644 "$rendered_service" "/etc/systemd/system/$service_name"
install -m 0644 "$rendered_timer" "/etc/systemd/system/$timer_name"
systemctl daemon-reload
systemctl enable --now "$timer_name"
echo "[tw-public-catalogs] timer=$timer_name state=$(systemctl is-active "$timer_name")"
