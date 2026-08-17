#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source "$repo_root/scripts/runtime_env.sh"
selected_python="$(resolve_fintech_python)"

lock_path="$repo_root/artifacts/binance_public_archive/refresh.lock"
mkdir -p "$(dirname "$lock_path")"
exec 9>"$lock_path"
if ! flock -n 9; then
  echo "[binance-public-archive] another plan/download cycle owns the lock"
  exit 0
fi

exec "$selected_python" "$repo_root/downloader/download_binance_public_archive.py" \
  --output-root "$repo_root/data_binance_archive" \
  --mode download \
  --markets spot um cm \
  --workers "${BINANCE_PUBLIC_ARCHIVE_WORKERS:-32}" \
  --reserve-gib "${BINANCE_PUBLIC_ARCHIVE_RESERVE_GIB:-100}"
