#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
extra_args=()
if (( $# > 0 )); then
  if (( $# != 1 )) || [[ "$1" != "--refresh" ]]; then
    echo "usage: $0 [--refresh]" >&2
    exit 2
  fi
  extra_args=(--refresh)
fi

# Serialize with the canonical TW public producer. The core 159-source run
# metadata belongs to the close/pre-open pipeline and is intentionally left
# intact by this independent background source catch-up.
lock_path="/srv/stockagent-live/.locks/tw-public-refresh.lock"
mkdir -p "$(dirname "$lock_path")"
exec 9>"$lock_path"
flock -w 1800 9 || { echo "[tw-public-catalogs] canonical source writer stayed active for 30 minutes" >&2; exit 75; }

source scripts/runtime_env.sh
metadata_dir="$repo_root/artifacts/data_refresh/tw_public/catalogs/latest"
run_started="$(date +%s)"
download_rc=0
run_fintech_python downloader/download_tw_public_data.py \
  --mode daily \
  --datasets gcis fsc \
  --output-dir /srv/stockagent-live/data_tw_public \
  --run-metadata-dir "$metadata_dir" \
  --timeout 120 \
  --workers 2 \
  "${extra_args[@]}" || download_rc=$?

# The core 159-source receipt remains at the live root. Publish the exact
# audited source tree only after a successful catalog batch, outside the
# producer lock because the canonical publisher acquires that same lock.
new_versions=0
summary="$metadata_dir/download_summary.json"
if [[ -f "$summary" && "$(stat -c %Y "$summary")" -ge "$run_started" ]]; then
  new_versions="$(run_fintech_python -c 'import json,sys; print(int(json.load(open(sys.argv[1], encoding="utf-8"))["fetched_dates"]))' "$summary")"
fi
flock -u 9
if (( new_versions > 0 )); then
  if ! run_fintech_python scripts/publish_tw_public_cold_release.py --timeout-seconds 7200; then
    echo "[tw-public-catalogs] cold publication remains pending; see cold_publish/latest.json" >&2
  fi
fi
exit "$download_rc"
