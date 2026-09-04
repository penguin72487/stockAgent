#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_name=stockagent-storage-pressure

for path in \
  "$repo_root/scripts/maintain_storage_pressure.py" \
  "$repo_root/scripts/run_storage_pressure_maintenance.sh"; do
  test -f "$path"
done
chmod 0755 \
  "$repo_root/scripts/maintain_storage_pressure.py" \
  "$repo_root/scripts/run_storage_pressure_maintenance.sh"

if [[ -d /run/systemd/system ]] && command -v systemctl >/dev/null 2>&1; then
  temporary_dir="$(mktemp -d)"
  trap 'rm -rf -- "$temporary_dir"' EXIT
  for unit in service timer; do
    sed "s|__REPO_ROOT__|$repo_root|g" \
      "$repo_root/deploy/systemd/${service_name}.${unit}.in" \
      >"$temporary_dir/${service_name}.${unit}"
    install -m 0644 "$temporary_dir/${service_name}.${unit}" \
      "/etc/systemd/system/${service_name}.${unit}"
  done
  systemctl daemon-reload
  systemctl enable --now "${service_name}.timer"
  systemctl restart "${service_name}.timer"
  systemctl status --no-pager "${service_name}.timer"
  exit 0
fi

cron_path="/etc/cron.d/${service_name}"
temporary="$(mktemp)"
trap 'rm -f -- "$temporary"' EXIT
printf '%s\n' \
  'SHELL=/bin/bash' \
  'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' \
  "17 * * * * root $repo_root/scripts/run_storage_pressure_maintenance.sh >>/var/log/${service_name}.log 2>&1" \
  >"$temporary"
install -m 0644 "$temporary" "$cron_path"
echo "Installed cron fallback at $cron_path"
