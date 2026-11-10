#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temporary="$(mktemp -d)"
trap 'rm -f -- "$temporary/stockagent-packed-object-placement.service" "$temporary/stockagent-packed-object-placement.timer"; rmdir -- "$temporary"' EXIT
escaped_root="${repo_root//\\/\\\\}"
escaped_root="${escaped_root//&/\\&}"
policy=/etc/stockagent/packed-object-placement.json
source_policy="$repo_root/configs/data_sync/packed_object_placement.json"
if [[ -e "$policy" ]] && ! cmp -s -- "$source_policy" "$policy"; then
  printf '%s\n' 'Existing operator placement policy differs; preserve it and reconcile first.' >&2
  exit 78
fi
for unit in stockagent-packed-object-placement.service stockagent-packed-object-placement.timer; do
  sed "s&__REPO_ROOT__&$escaped_root&g" "$repo_root/deploy/systemd/$unit.in" > "$temporary/$unit"
done
systemd-analyze verify "$temporary/stockagent-packed-object-placement.service" "$temporary/stockagent-packed-object-placement.timer"
install -d -m 0755 /etc/stockagent
install -m 0600 "$source_policy" "$policy"
install -m 0644 "$temporary/stockagent-packed-object-placement.service" /etc/systemd/system/stockagent-packed-object-placement.service
install -m 0644 "$temporary/stockagent-packed-object-placement.timer" /etc/systemd/system/stockagent-packed-object-placement.timer
systemctl daemon-reload
systemctl enable --now stockagent-packed-object-placement.timer
systemctl is-active --quiet stockagent-packed-object-placement.timer
printf '%s\n' 'E service-archive placement enabled; the original owners gate each exact source handoff.'
