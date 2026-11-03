#!/usr/bin/env bash
set -euo pipefail

automation_only=false
if [[ "${1:-}" == --automation-only && $# == 1 ]]; then
  automation_only=true
elif (( $# != 0 )); then
  echo "Usage: $0 [--automation-only]" >&2
  exit 2
fi

if (( EUID != 0 )); then
  echo "Root privileges are required." >&2
  exit 2
fi
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
if [[ -f /var/lib/stockagent-packed-edge/state.json ]]; then
  echo "Refusing artifact retirement on an index-only edge." >&2
  exit 2
fi
if [[ ! -d /run/systemd/system ]]; then
  echo "This installer requires systemd." >&2
  exit 2
fi
temporary_dir="$(mktemp -d)"
trap 'rm -f "$temporary_dir"/*; rmdir "$temporary_dir"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
source "$repo_root/scripts/runtime_env.sh"
run_fintech_python - "$repo_root" <<'PY'
from pathlib import Path
import sys
from stockagent.data_sync.retirement_queue import load_configuration
root = Path(sys.argv[1])
load_configuration(root / 'configs/data_sync/wsl_retirement_queue.json', root)
PY
backup_env_file="${STOCKAGENT_BACKUP_STREAM_ENV_FILE:-/etc/stockagent/backup-stream.env}"
if [[ ! -f "$backup_env_file" ]]; then
  echo "Install the canonical backup role before enabling private automation." >&2
  exit 2
fi
escaped_backup_env="${backup_env_file//\\/\\\\}"
escaped_backup_env="${escaped_backup_env//&/\\&}"
units=(stockagent-wsl-retirement-queue.service stockagent-wsl-retirement-queue.timer
       stockagent-private-cold-backup.service stockagent-private-cold-backup.timer
       stockagent-backup-notify.service stockagent-backup-notify.timer)
timers=(stockagent-wsl-retirement-queue.timer stockagent-private-cold-backup.timer stockagent-backup-notify.timer)
if [[ "$automation_only" == false ]]; then
  units+=(stockagent-enrolled-artifact-retirement.service stockagent-enrolled-artifact-retirement.timer)
  timers+=(stockagent-enrolled-artifact-retirement.timer)
fi
for unit in "${units[@]}"; do
  sed -e "s&__REPO_ROOT__&$escaped_root&g" -e "s&__BACKUP_ENV_FILE__&$escaped_backup_env&g" \
    "$repo_root/deploy/systemd/${unit}.in" > "$temporary_dir/$unit"
done
systemd-analyze verify "$temporary_dir"/*.service "$temporary_dir"/*.timer
followup_dir=/etc/systemd/system/stockagent-wsl-retirement-queue.service.d
followup_path="$followup_dir/40-testing-disposition-20261103.conf"
sed -e "s&__REPO_ROOT__&$escaped_root&g" \
  "$repo_root/deploy/systemd/stockagent-wsl-retirement-20261103.conf.in" > "$temporary_dir/followup.conf"
if [[ -L "$followup_path" ]] || { [[ -e "$followup_path" ]] && ! cmp -s "$temporary_dir/followup.conf" "$followup_path"; }; then
  echo "Preserve the changed existing retirement follow-up: $followup_path" >&2
  exit 1
fi
install -d -m 0755 "$followup_dir"
install -m 0644 "$temporary_dir/followup.conf" "$followup_path"
rm "$temporary_dir/followup.conf"
install -m 0644 "$temporary_dir"/* /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now "${timers[@]}"
