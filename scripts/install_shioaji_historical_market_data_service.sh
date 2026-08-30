#!/usr/bin/env bash
set -euo pipefail

service_name="stockagent-shioaji-historical-market-data.service"
timer_name="stockagent-shioaji-historical-market-data.timer"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_template="$repo_root/deploy/systemd/$service_name.in"
timer_template="$repo_root/deploy/systemd/$timer_name.in"
service_target="/etc/systemd/system/$service_name"
timer_target="/etc/systemd/system/$timer_name"
run_now=false

for argument in "$@"; do
  case "$argument" in
    --run-now) run_now=true ;;
    --no-start) run_now=false ;;
    -h|--help)
      echo "Usage: sudo bash $0 [--run-now|--no-start]"
      exit 0
      ;;
    *)
      echo "unknown argument: $argument" >&2
      exit 2
      ;;
  esac
done
if (( EUID != 0 )); then
  echo "[shioaji-history] root privileges are required" >&2
  exit 2
fi
for required in "$service_template" "$timer_template" "$repo_root/.env.futures"; do
  [[ -f "$required" ]] || { echo "[shioaji-history] missing $required" >&2; exit 2; }
done

service_user="${SHIOAJI_SERVICE_USER:-$(stat -c '%U' "$repo_root")}"
service_group="$(id -gn "$service_user")"
service_home="$(getent passwd "$service_user" | cut -d: -f6)"
runuser -u "$service_user" -- test -r "$repo_root/.env.futures"

escape_replacement() { printf '%s' "$1" | sed 's/[&|\]/\&/g'; }
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
chmod 0755 "$repo_root/scripts/run_shioaji_historical_market_data.sh"
chmod go-rwx "$repo_root/.env.futures"
systemctl daemon-reload
systemctl disable "$service_name" >/dev/null 2>&1 || true
systemctl enable --now "$timer_name"
if [[ "$run_now" == true ]]; then
  systemctl restart "$service_name"
fi
echo "[shioaji-history] service_active=$(systemctl is-active "$service_name" 2>/dev/null || true) timer_enabled=$(systemctl is-enabled "$timer_name")"
