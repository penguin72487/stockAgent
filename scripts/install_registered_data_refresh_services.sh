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
  stockagent-heavy-data.slice
  stockagent-finmind-free.service
  stockagent-finmind-complement.service
  stockagent-finmind-sponsor.service
  stockagent-finmind-quota-snapshot.service
  stockagent-finmind-quota-snapshot.timer
  stockagent-finmind-source-audit.service
  stockagent-finmind-source-audit.timer
  stockagent-crypto-training-refresh.service
  stockagent-crypto-training-refresh.timer
  stockagent-registered-data-daily.service
  stockagent-registered-data-daily.timer
  stockagent-registered-data-intraday.service
  stockagent-registered-data-intraday.timer
  stockagent-registered-data-features.service
  stockagent-registered-data-features.timer
  stockagent-registered-data-backfill.service
  stockagent-registered-data-backfill.timer
  stockagent-wsl-backfill-memory-reclaim.service
  stockagent-wsl-backfill-memory-reclaim.timer
  stockagent-binance-public-archive.service
  stockagent-binance-public-archive.timer
  stockagent-taifex-auxiliary-daily.service
  stockagent-taifex-auxiliary-daily.timer
  stockagent-taifex-public-history.service
  stockagent-taifex-public-history.timer
)
if [[ "${1:-}" == "features-only" ]]; then
  # A targeted deployment must not reinstall unrelated dirty service templates
  # or start the large daily/backfill jobs during a live market window.
  units=(
    stockagent-registered-data-features.service
    stockagent-registered-data-features.timer
  )
elif [[ "${1:-}" == "crypto-training-only" ]]; then
  units=(
    stockagent-crypto-training-refresh.service
    stockagent-crypto-training-refresh.timer
  )
elif [[ "${1:-}" == "intraday-timer-only" ]]; then
  # Repair the recurring trigger without replacing a running downloader or
  # reinstalling unrelated, possibly dirty service templates.
  units=(stockagent-registered-data-intraday.timer)
elif [[ "${1:-}" == "finlab-only" ]]; then
  # The quota observer is independent of the slow account download worker.
  units=(
    stockagent-finlab-local-refresh.service
    stockagent-finlab-local-refresh.timer
    stockagent-finlab-quota-snapshot.service
    stockagent-finlab-quota-snapshot.timer
  )
elif [[ "${1:-}" == "finmind-only" ]]; then
  units=(stockagent-finmind-free.service stockagent-finmind-complement.service stockagent-finmind-sponsor.service
         stockagent-finmind-quota-snapshot.service stockagent-finmind-quota-snapshot.timer
         stockagent-finmind-source-audit.service stockagent-finmind-source-audit.timer)
elif [[ $# -gt 0 ]]; then
  echo "usage: $0 [features-only|crypto-training-only|intraday-timer-only|finlab-only|finmind-only]" >&2
  exit 2
fi
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

verify_units=("$temporary_dir"/*)
if [[ "${1:-}" == "finlab-only" || $# -eq 0 ]]; then
  # Bash parses loops incrementally during execution. Catch a syntax error
  # before installing the timer or starting a multi-hour account download.
  bash -n "$repo_root/scripts/run_finlab_refresh.sh"
  bash -n "$repo_root/scripts/run_finlab_refresh_frozen.sh"
fi
if [[ "${1:-}" == "finmind-only" || $# -eq 0 ]]; then
  bash -n "$repo_root/scripts/run_finmind_free.sh"
  bash -n "$repo_root/scripts/run_finmind_complement.sh"
  bash -n "$repo_root/scripts/run_finmind_sponsor.sh"
fi
systemd-analyze verify "${verify_units[@]}"
install -m 0644 "$temporary_dir"/* /etc/systemd/system/
if [[ "${1:-}" != "intraday-timer-only" ]]; then
  chmod 0755 \
    "$repo_root/scripts/check_outside_tw_opening_resource_window.py" \
    "$repo_root/scripts/run_outside_tw_opening_resource_window.sh" \
    "$repo_root/scripts/run_registered_data_refresh.sh" \
    "$repo_root/scripts/run_finlab_refresh.sh" \
    "$repo_root/scripts/run_finlab_refresh_frozen.sh" \
    "$repo_root/scripts/run_downloader_with_release.sh" \
    "$repo_root/scripts/run_binance_public_archive.sh" \
    "$repo_root/scripts/run_taifex_auxiliary_daily.sh" \
    "$repo_root/scripts/run_taifex_public_history.sh"
fi
systemctl daemon-reload
if [[ "${1:-}" == "features-only" ]]; then
  systemctl enable --now stockagent-registered-data-features.timer
  echo "[registered-data] crypto feature timer enabled; existing jobs left untouched"
  exit 0
fi
if [[ "${1:-}" == "crypto-training-only" ]]; then
  systemctl enable --now stockagent-crypto-training-refresh.timer
  echo "[registered-data] crypto training refresh timer enabled; current writers left untouched"
  exit 0
fi
if [[ "${1:-}" == "intraday-timer-only" ]]; then
  systemctl enable stockagent-registered-data-intraday.timer
  # enable --now leaves an already-active but elapsed timer unchanged.
  systemctl restart stockagent-registered-data-intraday.timer
  timer_substate="$(systemctl show stockagent-registered-data-intraday.timer -p SubState --value)"
  if [[ "$timer_substate" == "elapsed" ]]; then
    echo "[registered-data] intraday timer remains elapsed after restart" >&2
    exit 1
  fi
  echo "[registered-data] intraday timer rearmed; other units and running jobs left untouched"
  exit 0
fi
if [[ "${1:-}" == "finlab-only" ]]; then
  systemctl enable --now stockagent-finlab-local-refresh.timer stockagent-finlab-quota-snapshot.timer
  echo "[registered-data] FinLab download and quota-observation timers enabled"
  exit 0
fi
if [[ "${1:-}" == "finmind-only" ]]; then
  systemctl enable --now stockagent-finmind-free.service stockagent-finmind-complement.service stockagent-finmind-sponsor.service stockagent-finmind-quota-snapshot.timer stockagent-finmind-source-audit.timer
  echo "[registered-data] FinMind Free and Sponsor history services enabled"
  exit 0
fi
systemctl enable --now \
  stockagent-finmind-free.service \
  stockagent-finmind-complement.service \
  stockagent-finmind-sponsor.service \
  stockagent-finmind-quota-snapshot.timer \
  stockagent-finmind-source-audit.timer \
  stockagent-crypto-training-refresh.timer \
  stockagent-registered-data-daily.timer \
  stockagent-registered-data-intraday.timer \
  stockagent-registered-data-features.timer \
  stockagent-registered-data-backfill.timer \
  stockagent-wsl-backfill-memory-reclaim.timer \
  stockagent-binance-public-archive.timer \
  stockagent-taifex-auxiliary-daily.timer \
  stockagent-taifex-public-history.timer

if [[ "${START_DATA_REFRESH_NOW:-1}" == "1" ]]; then
  systemctl start --no-block stockagent-registered-data-daily.service
  systemctl start --no-block stockagent-registered-data-intraday.service
  systemctl start --no-block stockagent-binance-public-archive.service
  systemctl start --no-block stockagent-taifex-auxiliary-daily.service
  systemctl start --no-block stockagent-taifex-public-history.service
  echo "[registered-data] timers enabled; current refreshes requested"
else
  echo "[registered-data] timers enabled; existing jobs left untouched"
fi
