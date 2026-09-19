#!/usr/bin/env bash
# One-shot, lock-serialized catch-up for the provisional TW macro sources.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
data_root="$(readlink -f data_tw_public)"
if [[ ! -d "$data_root" ]]; then
  echo "Taiwan public data root is missing: $data_root" >&2
  exit 1
fi
lock_path="$(dirname "$data_root")/.locks/tw-public-refresh.lock"
mkdir -p "$(dirname "$lock_path")"
exec 9>>"$lock_path"
flock -w 21600 9 || { echo "Timed out waiting for Taiwan public source lock" >&2; exit 1; }

if [[ "${1:-}" != "--finalize-only" ]]; then
  run_fintech_python downloader/download_tw_mof_macro_release_dates.py \
    --output-dir "$data_root" --request-interval 0.5 --recent-pages 0 \
    --source-update-lock-held
  run_fintech_python downloader/download_tw_mof_trade_release_values.py \
    --output-dir "$data_root" --request-interval 0.5 \
    --source-update-lock-held
fi
run_fintech_python scripts/build_tw_public_provisional_macro.py \
  --input-dir "$data_root" --source-update-lock-held
run_fintech_python downloader/download_tw_mof_release_archive.py \
  --output-dir "$data_root" --request-interval 0.5 \
  --source-update-lock-held
