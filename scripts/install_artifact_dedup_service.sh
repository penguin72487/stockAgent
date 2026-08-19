#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT

escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
units=(
  stockagent-artifact-dedup.service
  stockagent-artifact-dedup.timer
)
for unit in "${units[@]}"; do
  sed "s&__REPO_ROOT__&$escaped_root&g" \
    "$repo_root/deploy/systemd/${unit}.in" >"$temporary_dir/$unit"
done

chmod 0755 \
  "$repo_root/scripts/deduplicate_artifacts.py" \
  "$repo_root/scripts/run_artifact_dedup.sh" \
  "$repo_root/scripts/install_artifact_dedup_service.sh"
systemd-analyze verify "$temporary_dir"/*.service "$temporary_dir"/*.timer
install -m 0644 "$temporary_dir"/* /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now stockagent-artifact-dedup.timer
systemctl restart stockagent-artifact-dedup.timer
systemctl status --no-pager stockagent-artifact-dedup.timer
