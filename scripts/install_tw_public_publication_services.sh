#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
universe_only=false
for option in "$@"; do
  case "$option" in
    --universe-only) universe_only=true ;;
    *) echo "Usage: sudo bash scripts/install_tw_public_publication_services.sh [--universe-only]" >&2; exit 2 ;;
  esac
done
service_user="${TW_PUBLIC_SERVICE_USER:-}"
if [[ -z "$service_user" ]]; then
  service_user="$(stat -c '%U' "$repo_root")"
fi
if (( EUID != 0 )); then
  echo "[tw-public-publication] root privileges are required" >&2
  exit 2
fi
if ! id "$service_user" >/dev/null 2>&1; then
  echo "[tw-public-publication] unknown service user: $service_user" >&2
  exit 2
fi
service_group="$(id -gn "$service_user")"
service_uid="$(id -u "$service_user")"
service_home="$(getent passwd "$service_user" | cut -d: -f6)"

escape_replacement() {
  printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}

temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT
units=(
  stockagent-heavy-data.slice
  stockagent-tw-public-source-events.service
  stockagent-tw-day-trade-eligibility.service
  stockagent-tw-day-trade-eligibility.timer
  stockagent-tw-day-trade-margin-actions.service
  stockagent-tw-day-trade-margin-actions.timer
  stockagent-tw-public-publication-sweep.service
  stockagent-tw-public-publication-sweep.timer
  stockagent-tw-public-0830-check.service
  stockagent-tw-public-0830-check.timer
  stockagent-tw-public-cold-publish.service
  stockagent-tw-public-cold-publish.timer
  stockagent-tw-public-cold-publish.path
  stockagent-tw-trading-universe.service
  stockagent-tw-trading-universe.timer
)
if [[ "$universe_only" == true ]]; then
  units=(stockagent-tw-trading-universe.service stockagent-tw-trading-universe.timer)
fi
rendered_units=()
for unit in "${units[@]}"; do
  sed \
    -e "s|@REPO_ROOT@|$(escape_replacement "$repo_root")|g" \
    -e "s|@SERVICE_USER@|$(escape_replacement "$service_user")|g" \
    -e "s|@SERVICE_UID@|$(escape_replacement "$service_uid")|g" \
    -e "s|@SERVICE_GROUP@|$(escape_replacement "$service_group")|g" \
    -e "s|@SERVICE_HOME@|$(escape_replacement "$service_home")|g" \
    "$repo_root/deploy/systemd/${unit}.in" > "$temporary_dir/$unit"
  rendered_units+=("$temporary_dir/$unit")
done

systemd-analyze verify "${rendered_units[@]}"
install -m 0644 "$temporary_dir"/* /etc/systemd/system/
install -d -o "$service_user" -g "$service_group" -m 0750 "$repo_root/artifacts/live/tw_trading_universe"
install -d -o "$service_user" -g "$service_group" -m 0700 "/tmp/stockagent-rate-limits-$service_uid"
if [[ "$universe_only" == true ]]; then
  systemctl daemon-reload
  systemctl enable --now stockagent-tw-trading-universe.timer
  echo "[tw-public-publication] scoped_universe_install=true discord_restarted=false engine_restarted=false"
  exit 0
fi
chmod 0755 \
  "$repo_root/scripts/fetch_tw_day_trade_eligibility_on_publish.py" \
  "$repo_root/scripts/run_tw_day_trade_eligibility_watcher.sh" \
  "$repo_root/scripts/watch_tw_public_publication_group.py" \
  "$repo_root/scripts/run_tw_public_publication_sweep.sh" \
  "$repo_root/scripts/watch_tw_public_source_events.py" \
  "$repo_root/scripts/run_tw_public_source_event_monitor.sh" \
  "$repo_root/scripts/run_tw_public_0830_check.py" \
  "$repo_root/scripts/run_tw_public_0830_check.sh" \
  "$repo_root/scripts/finalize_tw_public_completed_session.py" \
  "$repo_root/scripts/publish_tw_public_cold_release.py" \
  "$repo_root/scripts/run_tw_public_cold_publish.sh"
systemctl daemon-reload
systemctl enable --now \
  stockagent-tw-public-source-events.service \
  stockagent-tw-day-trade-eligibility.timer \
  stockagent-tw-day-trade-margin-actions.timer \
  stockagent-tw-public-publication-sweep.timer \
  stockagent-tw-public-0830-check.timer \
  stockagent-tw-public-cold-publish.timer \
  stockagent-tw-public-cold-publish.path
systemctl enable --now stockagent-tw-trading-universe.timer

if [[ "${START_ELIGIBILITY_FETCH_NOW:-0}" == "1" ]]; then
  systemctl start stockagent-tw-day-trade-eligibility.service
fi
if [[ "${START_TW_PUBLIC_0830_CHECK_NOW:-0}" == "1" ]]; then
  systemctl start stockagent-tw-public-0830-check.service
fi
