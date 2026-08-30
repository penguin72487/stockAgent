#!/usr/bin/env bash
set -euo pipefail

service_name="stockagent-shioaji-tx-history-backfill.service"
timer_name="stockagent-shioaji-tx-history-backfill.timer"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
service_template="$repo_root/deploy/systemd/$service_name.in"
timer_template="$repo_root/deploy/systemd/$timer_name.in"
service_target="/etc/systemd/system/$service_name"
timer_target="/etc/systemd/system/$timer_name"
run_now=false

usage() {
  echo "Usage: sudo bash scripts/install_shioaji_tx_history_backfill_service.sh [--run-now|--no-start]" >&2
}

for argument in "$@"; do
  case "$argument" in
    --run-now)
      run_now=true
      ;;
    --no-start)
      run_now=false
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
  echo "[shioaji-tx-history] root privileges are required" >&2
  exit 2
fi
if [[ ! -f "$service_template" || ! -f "$timer_template" || ! -f "$repo_root/.env.futures" ]]; then
  echo "[shioaji-tx-history] service/timer template or .env.futures is missing" >&2
  exit 2
fi

service_user="${SHIOAJI_SERVICE_USER:-$(stat -c '%U' "$repo_root")}"
if ! id "$service_user" >/dev/null 2>&1; then
  echo "[shioaji-tx-history] unknown service user: $service_user" >&2
  exit 2
fi
service_group="$(id -gn "$service_user")"
service_home="$(getent passwd "$service_user" | cut -d: -f6)"
if [[ -z "$service_home" ]]; then
  echo "[shioaji-tx-history] cannot resolve home for $service_user" >&2
  exit 2
fi
if ! runuser -u "$service_user" -- test -r "$repo_root/.env.futures"; then
  echo "[shioaji-tx-history] $service_user cannot read .env.futures" >&2
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
chmod 0755 "$repo_root/scripts/run_shioaji_tx_history_backfill.sh"
chmod go-rwx "$repo_root/.env.futures"
systemctl daemon-reload
# Historical recovery is resumable background work. Keep the live quote and
# dashboard path out of its CPU, memory, disk, and API-login startup contention.
systemctl disable "$service_name" >/dev/null 2>&1 || true
systemctl enable --now "$timer_name"
if [[ "$run_now" == "true" ]]; then
  systemctl restart "$service_name"
fi

echo "[shioaji-tx-history] service=$service_target trigger=$(systemctl is-enabled "$service_name" 2>/dev/null || true) active=$(systemctl is-active "$service_name" 2>/dev/null || true)"
echo "[shioaji-tx-history] timer=$timer_target enabled=$(systemctl is-enabled "$timer_name")"
