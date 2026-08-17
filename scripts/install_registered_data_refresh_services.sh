#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_user="${DATA_REFRESH_SERVICE_USER:-}"
if [[ -z "$service_user" ]]; then
  service_user="$(stat -c '%U' "$repo_root")"
fi
if (( EUID != 0 )); then
  echo "[registered-data] root privileges are required" >&2
  exit 2
fi
if ! id "$service_user" >/dev/null 2>&1; then
  echo "[registered-data] unknown service user: $service_user" >&2
  exit 2
fi
service_group="$(id -gn "$service_user")"
service_home="$(getent passwd "$service_user" | cut -d: -f6)"

escape_replacement() {
  printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}

units=(
  stockagent-registered-data-daily.service
  stockagent-registered-data-daily.timer
  stockagent-registered-data-intraday.service
  stockagent-registered-data-intraday.timer
  stockagent-binance-public-archive.service
  stockagent-binance-public-archive.timer
  stockagent-taifex-auxiliary-daily.service
  stockagent-taifex-auxiliary-daily.timer
)
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
for unit in "${units[@]}"; do
  template="$repo_root/deploy/systemd/${unit}.in"
  target="$temporary_dir/$unit"
  sed \
    -e "s|@REPO_ROOT@|$(escape_replacement "$repo_root")|g" \
    -e "s|@SERVICE_USER@|$(escape_replacement "$service_user")|g" \
    -e "s|@SERVICE_GROUP@|$(escape_replacement "$service_group")|g" \
    -e "s|@SERVICE_HOME@|$(escape_replacement "$service_home")|g" \
    "$template" > "$target"
done

systemd-analyze verify "$temporary_dir"/*.service "$temporary_dir"/*.timer
install -m 0644 "$temporary_dir"/* /etc/systemd/system/
chmod 0755 \
  "$repo_root/scripts/run_registered_data_refresh.sh" \
  "$repo_root/scripts/run_binance_public_archive.sh" \
  "$repo_root/scripts/run_taifex_auxiliary_daily.sh"
systemctl daemon-reload
systemctl enable --now \
  stockagent-registered-data-daily.timer \
  stockagent-registered-data-intraday.timer \
  stockagent-binance-public-archive.timer \
  stockagent-taifex-auxiliary-daily.timer

if [[ "${START_DATA_REFRESH_NOW:-1}" == "1" ]]; then
  systemctl start --no-block stockagent-registered-data-daily.service
  systemctl start --no-block stockagent-registered-data-intraday.service
  systemctl start --no-block stockagent-binance-public-archive.service
  systemctl start --no-block stockagent-taifex-auxiliary-daily.service
  echo "[registered-data] timers enabled; current refreshes requested"
else
  echo "[registered-data] timers enabled; existing jobs left untouched"
fi
