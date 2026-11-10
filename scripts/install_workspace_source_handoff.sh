#!/usr/bin/env bash
# Install only this source handoff's reviewed unit files. Existing unrelated
# service configuration, running trading services and source payloads remain.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
unit_root=/etc/systemd/system
units=(stockagent-minute-source-handoff-boot.service stockagent-workspace-source-handoff-boot.service stockagent-workspace-source-handoff.service stockagent-workspace-source-handoff.timer stockagent-workspace-symlink-closure.service stockagent-workspace-symlink-closure.timer stockagent-futures-e-cold-inventory.service stockagent-futures-e-cold-inventory.timer stockagent-futures-725-index-projection.service stockagent-futures-725-index-projection.timer)
consumers=(stockagent-registered-data-daily.service stockagent-crypto-training-refresh.service stockagent-data-refresh-status-snapshot.service stockagent-shioaji-minute-backfill.service)

systemd-analyze verify "${units[@]/#/$repo_root/deploy/systemd/}"
for unit in "${units[@]}"; do
  destination="$unit_root/$unit"
  if [[ -e "$destination" || -L "$destination" ]]; then
    [[ -f "$destination" && ! -L "$destination" ]] && cmp -s "$repo_root/deploy/systemd/$unit" "$destination" || {
      printf 'refuse to replace an existing different unit: %s\n' "$destination" >&2
      exit 1
    }
  else
    install -m 0644 "$repo_root/deploy/systemd/$unit" "$destination"
  fi
done
for unit in "${consumers[@]}"; do
  if [[ -f "$unit_root/$unit" ]]; then
    directory="$unit_root/$unit.d"
    mkdir -p "$directory"
    destination="$directory/40-workspace-source-handoff.conf"
    if [[ -e "$destination" || -L "$destination" ]]; then
      [[ -f "$destination" && ! -L "$destination" ]] && cmp -s "$repo_root/deploy/systemd/stockagent-workspace-source-handoff-consumer.conf" "$destination" || {
        printf 'refuse to replace a different consumer dependency: %s\n' "$destination" >&2
        exit 1
      }
    else
      install -m 0644 "$repo_root/deploy/systemd/stockagent-workspace-source-handoff-consumer.conf" "$destination"
    fi
  fi
done
trading_unit="$unit_root/stockagent-tw-day-trade-simulation.service"
if [[ -f "$trading_unit" ]]; then
  directory="$trading_unit.d"
  mkdir -p "$directory"
  destination="$directory/41-minute-source-handoff.conf"
  if [[ -e "$destination" || -L "$destination" ]]; then
    [[ -f "$destination" && ! -L "$destination" ]] && cmp -s "$repo_root/deploy/systemd/stockagent-minute-source-handoff-consumer.conf" "$destination" || {
      printf 'refuse to replace a different minute dependency: %s\n' "$destination" >&2
      exit 1
    }
  else
    install -m 0644 "$repo_root/deploy/systemd/stockagent-minute-source-handoff-consumer.conf" "$destination"
  fi
fi
timer_directory="$unit_root/stockagent-futures-e-cold-inventory.timer.d"
mkdir -p "$timer_directory"
timer_override="$timer_directory/30-current-head-cadence.conf"
if [[ -e "$timer_override" || -L "$timer_override" ]]; then
  [[ -f "$timer_override" && ! -L "$timer_override" ]] && cmp -s "$repo_root/deploy/systemd/stockagent-futures-e-cold-inventory-cadence.conf" "$timer_override" || {
    printf 'refuse to replace a different futures comparison cadence: %s\n' "$timer_override" >&2
    exit 1
  }
else
  install -m 0644 "$repo_root/deploy/systemd/stockagent-futures-e-cold-inventory-cadence.conf" "$timer_override"
fi
systemctl daemon-reload
systemctl enable stockagent-minute-source-handoff-boot.service stockagent-workspace-source-handoff-boot.service stockagent-workspace-source-handoff.timer stockagent-workspace-symlink-closure.timer stockagent-futures-e-cold-inventory.timer stockagent-futures-725-index-projection.timer
systemctl start stockagent-workspace-source-handoff-boot.service
systemctl is-active --quiet stockagent-workspace-source-handoff-boot.service
systemctl start stockagent-workspace-source-handoff.timer
systemctl start stockagent-workspace-symlink-closure.timer
systemctl restart stockagent-futures-e-cold-inventory.timer
systemctl start stockagent-futures-725-index-projection.timer
