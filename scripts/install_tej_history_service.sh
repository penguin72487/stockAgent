#!/usr/bin/env bash
set -euo pipefail

tej_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tej_start=false
case "${1:-}" in
  --start) tej_start=true ;;
  ""|--no-start) ;;
  *) echo 'Usage: bash scripts/install_tej_history_service.sh [--start|--no-start]' >&2; exit 2 ;;
esac
if (( EUID != 0 )); then
  echo 'Root privileges are required to install the TEJ service.' >&2
  exit 2
fi
test -f "$tej_repo_root/data_tej/desktop_session.json"
test -f "$tej_repo_root/data_tej/queue.sqlite3"
tej_render_dir="$(mktemp -d)"
trap 'rmdir "$tej_render_dir" 2>/dev/null || true' EXIT
tej_escaped_root="${tej_repo_root//\\/\\\\}"
tej_escaped_root="${tej_escaped_root//&/\\&}"
sed "s&__REPO_ROOT__&$tej_escaped_root&g" \
  "$tej_repo_root/deploy/systemd/stockagent-tej-history.service.in" \
  > "$tej_render_dir/stockagent-tej-history.service"
systemd-analyze verify "$tej_render_dir/stockagent-tej-history.service"
install -m 0644 "$tej_render_dir/stockagent-tej-history.service" /etc/systemd/system/stockagent-tej-history.service
# Rendering scratch is not source data. Delete only this exact known file.
unlink "$tej_render_dir/stockagent-tej-history.service"
systemctl daemon-reload
systemctl enable stockagent-tej-history.service
if [[ "$tej_start" == true ]]; then
  systemctl start stockagent-tej-history.service
fi
echo 'Installed stockagent-tej-history.service; TEJ/Excel login and exact private session remain required.'
