#!/usr/bin/env bash
set -euo pipefail
stockagent_tier_repo="$(dirname "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")")"
cd "$stockagent_tier_repo"
source scripts/runtime_env.sh
run_fintech_python - <<'PY'
from pathlib import Path
from stockagent.data_sync.cold_tiers import load_policy
load_policy(Path('configs/data_sync/cold_tiers.json').absolute())
PY
install -d -m 0700 /var/lib/stockagent-cold-tiers
install -d -m 0755 /etc/stockagent
if [[ -e /etc/stockagent/cold-tiers.json ]]; then
  cmp --silent configs/data_sync/cold_tiers.json /etc/stockagent/cold-tiers.json || {
    echo 'Existing cold-tier policy differs; reconcile the reviewed local policy before enrollment.' >&2
    exit 1
  }
else
  install -m 0600 configs/data_sync/cold_tiers.json /etc/stockagent/cold-tiers.json
fi
for stockagent_tier_unit in stockagent-cold-tiers.service stockagent-cold-tiers.timer; do
  sed "s|__REPO_ROOT__|$stockagent_tier_repo|g" "deploy/systemd/$stockagent_tier_unit.in" > "/etc/systemd/system/$stockagent_tier_unit"
done
systemd-analyze verify /etc/systemd/system/stockagent-cold-tiers.service /etc/systemd/system/stockagent-cold-tiers.timer
systemctl daemon-reload
systemctl enable --now stockagent-cold-tiers.timer
