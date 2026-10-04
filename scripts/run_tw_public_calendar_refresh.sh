#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# Reuse the canonical collector, immutable raw receipts and source-writer lock.
# Never overwrite the broad close/pre-open run's metadata or start cold sync.
mkdir -p /srv/stockagent-live/.locks
exec 9>/srv/stockagent-live/.locks/tw-public-refresh.lock
if ! flock -w 20 9; then
  echo "[calendar] canonical writer busy; retain last verified calendar, retry next timer"
  exit 0
fi
source scripts/runtime_env.sh
run_fintech_python downloader/download_tw_public_data.py \
  --mode daily --datasets twse_api_holidayschedule_holidayschedule \
  --output-dir /srv/stockagent-live/data_tw_public \
  --run-metadata-dir "$repo_root/artifacts/data_refresh/tw_public/calendar/latest" \
  --workers 1 --timeout 30 --retries 1
