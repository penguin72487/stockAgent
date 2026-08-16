#!/usr/bin/env bash
set -euo pipefail

service_name="stockagent-taifex-futures-daily.service"
timer_name="stockagent-taifex-futures-daily.timer"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_template="$repo_root/deploy/systemd/$service_name.in"
timer_template="$repo_root/deploy/systemd/$timer_name.in"
service_target="/etc/systemd/system/$service_name"
timer_target="/etc/systemd/system/$timer_name"
start_now=true

usage() {
  echo "Usage: sudo bash scripts/install_taifex_futures_daily_service.sh [--no-start]" >&2
}

for argument in "$@"; do
  case "$argument" in
    --no-start)
      start_now=false
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

if (( EUID != 0 )); then
  echo "[taifex-futures-daily] root privileges are required" >&2
  exit 2
fi
if [[ ! -f "$service_template" || ! -f "$timer_template" ]]; then
  echo "[taifex-futures-daily] service or timer template is missing" >&2
  exit 2
fi

service_user="${TAIFEX_SERVICE_USER:-$(stat -c '%U' "$repo_root")}"
if ! id "$service_user" >/dev/null 2>&1; then
  echo "[taifex-futures-daily] unknown service user: $service_user" >&2
  exit 2
fi
service_group="$(id -gn "$service_user")"
service_home="$(getent passwd "$service_user" | cut -d: -f6)"
if [[ -z "$service_home" ]]; then
  echo "[taifex-futures-daily] cannot resolve home for $service_user" >&2
  exit 2
fi

escape_replacement() {
  printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}

rendered_service="$(mktemp --suffix=.service)"
rendered_timer="$(mktemp --suffix=.timer)"
trap 'rm -f "$rendered_service" "$rendered_timer"' EXIT
sed \
  -e "s|@REPO_ROOT@|$(escape_replacement "$repo_root")|g" \
  -e "s|@SERVICE_USER@|$(escape_replacement "$service_user")|g" \
  -e "s|@SERVICE_GROUP@|$(escape_replacement "$service_group")|g" \
  -e "s|@SERVICE_HOME@|$(escape_replacement "$service_home")|g" \
  "$service_template" > "$rendered_service"
cp "$timer_template" "$rendered_timer"

systemd-analyze verify "$rendered_service" "$rendered_timer"
install -m 0644 "$rendered_service" "$service_target"
install -m 0644 "$rendered_timer" "$timer_target"
chmod 0755 "$repo_root/scripts/run_taifex_all_futures_daily.sh"
systemctl daemon-reload
systemctl enable "$service_name"
systemctl enable --now "$timer_name"
if [[ "$start_now" == true ]]; then
  systemctl restart "$service_name"
fi

echo "[taifex-futures-daily] service=$service_target enabled=$(systemctl is-enabled "$service_name")"
echo "[taifex-futures-daily] timer=$timer_target enabled=$(systemctl is-enabled "$timer_name")"
