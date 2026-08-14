#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="$repo_root/deploy/systemd/stockagent-openbb-archive.service.in"
target="/etc/systemd/system/stockagent-openbb-archive.service"
temporary="$(mktemp)"
trap 'rm -f "$temporary"' EXIT

escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
sed "s&__REPO_ROOT__&$escaped_root&g" "$template" > "$temporary"
install -m 0644 "$temporary" "$target"
systemd-analyze verify "$target"

source "$repo_root/scripts/runtime_env.sh"
if [[ -s "$repo_root/data_openBB/_state/monitor_history.jsonl" ]]; then
  run_fintech_python "$repo_root/scripts/rebuild_openbb_dashboard_history.py" \
    --output-dir "$repo_root/data_openBB"
fi

systemctl daemon-reload
systemctl enable --now stockagent-openbb-archive.service

ready=false
for _attempt in $(seq 1 30); do
  if systemctl is-active --quiet stockagent-openbb-archive.service \
    && [[ -s "$repo_root/data_openBB/_state/supervisor.pid" ]]; then
    supervisor_pid="$(tr -d '[:space:]' < "$repo_root/data_openBB/_state/supervisor.pid")"
    if [[ "$supervisor_pid" =~ ^[1-9][0-9]*$ ]] \
      && kill -0 "$supervisor_pid" 2>/dev/null \
      && tr '\0' ' ' < "/proc/$supervisor_pid/cmdline" \
        | grep -q 'run_openbb_archive_supervisor.sh'; then
      ready=true
      break
    fi
  fi
  sleep 1
done
if [[ "$ready" != "true" ]]; then
  journalctl --no-pager --unit stockagent-openbb-archive.service --lines 80
  echo "OpenBB archive supervisor did not become live within 30 seconds." >&2
  exit 1
fi

echo "stockagent-openbb-archive.service enabled and active"
