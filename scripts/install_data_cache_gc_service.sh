#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT

escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
units=(
  stockagent-data-cache-gc.service
  stockagent-data-cache-gc.timer
)
for unit in "${units[@]}"; do
  sed "s&__REPO_ROOT__&$escaped_root&g" \
    "$repo_root/deploy/systemd/${unit}.in" >"$temporary_dir/$unit"
done

chmod 0755 \
  "$repo_root/scripts/data_cache.py" \
  "$repo_root/scripts/run_data_cache.sh" \
  "$repo_root/scripts/install_data_cache_gc_service.sh"

cli_path=/usr/local/bin/stockagent-data
if [ -e "$cli_path" ] && [ ! -L "$cli_path" ]; then
  printf 'Refusing to replace non-symlink CLI: %s\n' "$cli_path" >&2
  exit 64
fi
ln -sfn "$repo_root/scripts/run_data_cache.sh" "$cli_path"

if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  systemd-analyze verify "$temporary_dir"/*.service "$temporary_dir"/*.timer
  install -m 0644 "$temporary_dir"/* /etc/systemd/system/
  systemctl daemon-reload
  systemctl enable --now stockagent-data-cache-gc.timer
  systemctl restart stockagent-data-cache-gc.timer
  systemctl status --no-pager stockagent-data-cache-gc.timer
  exit 0
fi

cron_path=/etc/cron.d/stockagent-data-cache-gc
escaped_command="${repo_root}/scripts/run_data_cache.sh gc"
printf '%s\n' \
  'SHELL=/bin/bash' \
  'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' \
  "*/5 * * * * root ${escaped_command} >>/var/log/stockagent-data-cache-gc.log 2>&1" \
  >"$temporary_dir/stockagent-data-cache-gc.cron"
install -m 0644 "$temporary_dir/stockagent-data-cache-gc.cron" "$cron_path"
printf 'Installed cron fallback: %s\n' "$cron_path"
printf 'Installed self-service CLI: %s\n' "$cli_path"
