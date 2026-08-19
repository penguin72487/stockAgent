#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT

escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
units=(
    stockagent-public-dashboards.service
    stockagent-data-refresh-status-snapshot.service
    stockagent-data-refresh-status-snapshot.timer
)
for unit in "${units[@]}"; do
    template="$repo_root/deploy/systemd/${unit}.in"
    sed "s&__REPO_ROOT__&$escaped_root&g" "$template" > "$temporary_dir/$unit"
done
mkdir -p "$repo_root/artifacts/live/data_monitor"
chmod 0755 \
    "$repo_root/scripts/run_data_refresh_status_snapshot.sh" \
    "$repo_root/scripts/snapshot_data_refresh_services.py"
systemd-analyze verify "$temporary_dir"/*.service "$temporary_dir"/*.timer
install -m 0644 "$temporary_dir"/* /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now stockagent-data-refresh-status-snapshot.timer
systemctl start stockagent-data-refresh-status-snapshot.service
systemctl enable stockagent-public-dashboards.service
systemctl restart stockagent-public-dashboards.service
ready=false
for _attempt in $(seq 1 10); do
    if systemctl is-active --quiet stockagent-public-dashboards.service \
        && curl --fail --silent --show-error --max-time 5 \
            http://127.0.0.1:8770/healthz >/dev/null; then
        ready=true
        break
    fi
    sleep 1
done

if [[ "$ready" != "true" ]]; then
    journalctl --no-pager --unit stockagent-public-dashboards.service --lines 50
    echo "Public dashboard gateway did not become healthy within 10 seconds." >&2
    exit 1
fi
echo "stockagent-public-dashboards.service active; gateway=http://127.0.0.1:8770"
