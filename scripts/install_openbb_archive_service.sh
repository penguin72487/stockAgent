#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="$repo_root/deploy/systemd/stockagent-openbb-archive.service.in"
timer_template="$repo_root/deploy/systemd/stockagent-openbb-archive.timer.in"
target="/etc/systemd/system/stockagent-openbb-archive.service"
timer_target="/etc/systemd/system/stockagent-openbb-archive.timer"
temporary="$(mktemp)"
temporary_timer="$(mktemp)"
trap 'rm -f "$temporary" "$temporary_timer"' EXIT
run_now=false

usage() {
  echo "Usage: sudo bash scripts/install_openbb_archive_service.sh [--run-now|--no-start]" >&2
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
  echo "[openbb-archive] root privileges are required" >&2
  exit 2
fi
if [[ ! -f "$template" || ! -f "$timer_template" ]]; then
  echo "[openbb-archive] service or timer template is missing" >&2
  exit 2
fi

escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
sed "s&__REPO_ROOT__&$escaped_root&g" "$template" > "$temporary"
cp "$timer_template" "$temporary_timer"
install -m 0644 "$temporary" "$target"
install -m 0644 "$temporary_timer" "$timer_target"
systemd-analyze verify "$target" "$timer_target"

source "$repo_root/scripts/runtime_env.sh"
if [[ -s "$repo_root/data_openBB/_state/monitor_history.jsonl" ]]; then
  run_fintech_python "$repo_root/scripts/rebuild_openbb_dashboard_history.py" \
    --output-dir "$repo_root/data_openBB"
fi

systemctl daemon-reload
systemctl disable stockagent-openbb-archive.service >/dev/null 2>&1 || true
systemctl enable --now stockagent-openbb-archive.timer
if [[ "$run_now" == "true" ]]; then
  systemctl restart stockagent-openbb-archive.service
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
fi

echo "[openbb-archive] service_trigger=$(systemctl is-enabled stockagent-openbb-archive.service 2>/dev/null || true) active=$(systemctl is-active stockagent-openbb-archive.service 2>/dev/null || true)"
echo "[openbb-archive] timer=$timer_target enabled=$(systemctl is-enabled stockagent-openbb-archive.timer)"
