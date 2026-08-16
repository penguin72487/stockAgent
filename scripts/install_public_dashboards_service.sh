#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="$repo_root/deploy/systemd/stockagent-public-dashboards.service.in"
target="/etc/systemd/system/stockagent-public-dashboards.service"
temporary="$(mktemp)"
trap 'rm -f "$temporary"' EXIT

escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
sed "s&__REPO_ROOT__&$escaped_root&g" "$template" > "$temporary"
install -m 0644 "$temporary" "$target"
systemd-analyze verify "$target"
systemctl daemon-reload
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
