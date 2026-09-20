#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_name="stockagent-tw-public-release-archives.service"
timer_name="stockagent-tw-public-release-archives.timer"
service_template="$repo_root/deploy/systemd/$service_name.in"
timer_template="$repo_root/deploy/systemd/$timer_name.in"
service_target="/etc/systemd/system/$service_name"
timer_target="/etc/systemd/system/$timer_name"

if (( EUID != 0 )); then
  echo "[tw-public-release-archives] root privileges are required" >&2
  exit 2
fi
if [[ ! -f "$service_template" || ! -f "$timer_template" ]]; then
  echo "[tw-public-release-archives] unit templates are missing" >&2
  exit 2
fi
rendered_service="$(mktemp --suffix=.service)"
rendered_timer="$(mktemp --suffix=.timer)"
trap 'rm -f "$rendered_service" "$rendered_timer"' EXIT
escaped_repo_root="$(printf '%s' "$repo_root" | sed 's/[&|\\]/\\&/g')"
sed "s|__REPO_ROOT__|$escaped_repo_root|g" "$service_template" > "$rendered_service"
cp "$timer_template" "$rendered_timer"
systemd-analyze verify "$rendered_service" "$rendered_timer"
install -m 0644 "$rendered_service" "$service_target"
install -m 0644 "$rendered_timer" "$timer_target"
systemctl daemon-reload
systemctl enable --now "$timer_name"
echo "[tw-public-release-archives] timer=$timer_name state=$(systemctl is-active "$timer_name")"
