#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh

calendar_admitted() {
  local admission_rc=0
  run_fintech_python scripts/check_outside_tw_opening_resource_window.py \
    --calendar-refresh --official-calendar-root /srv/stockagent-live/data_tw_public/stocks \
    || admission_rc=$?
  if (( admission_rc == 1 )); then
    echo "[calendar] verified calendar retained; defer redundant writer until after opening"
    exit 0
  fi
  if (( admission_rc != 0 )); then
    return "$admission_rc"
  fi
}
calendar_admitted

# Reuse the canonical collector, immutable raw receipts and source-writer lock.
# Never overwrite the broad close/pre-open run's metadata or start cold sync.
mkdir -p /srv/stockagent-live/.locks
exec 9>/srv/stockagent-live/.locks/tw-public-refresh.lock
if ! flock -w 20 9; then
  echo "[calendar] canonical writer busy; retain last verified calendar, retry next timer"
  exit 0
fi
# Lock wait may cross the opening boundary; admission must remain current.
calendar_admitted
run_fintech_python downloader/download_tw_public_data.py \
  --mode daily --datasets twse_api_holidayschedule_holidayschedule \
  --output-dir /srv/stockagent-live/data_tw_public \
  --run-metadata-dir "$repo_root/artifacts/data_refresh/tw_public/calendar/latest" \
  --workers 1 --timeout 30 --retries 1
