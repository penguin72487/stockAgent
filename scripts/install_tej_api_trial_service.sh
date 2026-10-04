#!/usr/bin/env bash
set -euo pipefail
tej_api_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tej_api_start=false
case "${1:-}" in
  --start) tej_api_start=true ;;
  ""|--no-start) ;;
  *) echo 'Usage: bash scripts/install_tej_api_trial_service.sh [--start|--no-start]' >&2; exit 2 ;;
esac
test -f "$tej_api_repo_root/data_tej/api_trial_v1/queue.sqlite3"
tej_api_render_dir="$(mktemp -d)"
trap 'rmdir "$tej_api_render_dir" 2>/dev/null || true' EXIT
tej_api_escaped_root="${tej_api_repo_root//\\/\\\\}"
tej_api_escaped_root="${tej_api_escaped_root//&/\\&}"
for tej_api_extension in service timer; do
  tej_api_file="stockagent-tej-api-trial.$tej_api_extension"
  sed "s&__REPO_ROOT__&$tej_api_escaped_root&g" \
    "$tej_api_repo_root/deploy/systemd/$tej_api_file.in" > "$tej_api_render_dir/$tej_api_file"
done
systemd-analyze verify "$tej_api_render_dir/stockagent-tej-api-trial.service" "$tej_api_render_dir/stockagent-tej-api-trial.timer"
for tej_api_extension in service timer; do
  tej_api_file="stockagent-tej-api-trial.$tej_api_extension"
  install -m 0644 "$tej_api_render_dir/$tej_api_file" "/etc/systemd/system/$tej_api_file"
  unlink "$tej_api_render_dir/$tej_api_file"
done
systemctl daemon-reload
systemctl enable stockagent-tej-api-trial.timer
if [[ "$tej_api_start" == true ]]; then
  systemctl start stockagent-tej-api-trial.timer
  systemctl start --no-block stockagent-tej-api-trial.service
fi
