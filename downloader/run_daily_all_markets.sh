#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || command -v python || true)}"
if [[ -z "$PYTHON_BIN" ]]; then
  echo "[daily] python not found in PATH" >&2
  exit 2
fi

TODAY="$(date +%F)"
WORKERS="${WORKERS:-8}"
ASSET_WORKERS="${ASSET_WORKERS:-2}"
RETRIES="${RETRIES:-2}"
REPAIR_OVERLAP_DAYS="${REPAIR_OVERLAP_DAYS:-7}"
FRANKFURTER_TIMEOUT="${FRANKFURTER_TIMEOUT:-30}"
FRANKFURTER_SYMBOLS_FILE="${FRANKFURTER_SYMBOLS_FILE:-configs/forex_all_pairs_frankfurter.txt}"
RUN_PEPPERSTONE_GROUPS="${RUN_PEPPERSTONE_GROUPS:-1}"
PEPPERSTONE_WORKERS="${PEPPERSTONE_WORKERS:-8}"

run_step() {
  local name="$1"
  shift
  echo "[daily] step=${name} start"
  "$@"
  echo "[daily] step=${name} done"
}

echo "[daily] date=${TODAY} root=${ROOT_DIR}"

run_step yahoo_all_repair \
  "$PYTHON_BIN" downloader/download_yahoo_ohlcv.py \
  --mode repair \
  --asset all \
  --end-date "$TODAY" \
  --workers "$WORKERS" \
  --asset-workers "$ASSET_WORKERS" \
  --retries "$RETRIES" \
  --repair-overlap-days "$REPAIR_OVERLAP_DAYS"

if [[ -f "$FRANKFURTER_SYMBOLS_FILE" ]]; then
  run_step frankfurter_forex_incremental \
    "$PYTHON_BIN" downloader/download_forex_frankfurter.py \
    --output-dir data_yahoo/forex \
    --symbols-file "$FRANKFURTER_SYMBOLS_FILE" \
    --end-date "$TODAY" \
    --workers "$WORKERS" \
    --timeout "$FRANKFURTER_TIMEOUT" \
    --incremental \
    --skip-manifest
else
  echo "[daily] skip=frankfurter_forex_incremental reason=missing_symbols_file file=${FRANKFURTER_SYMBOLS_FILE}" >&2
fi

if [[ "$RUN_PEPPERSTONE_GROUPS" == "1" ]]; then
  run_step pepperstone_groups_repair \
    "$PYTHON_BIN" downloader/download_pepperstone.py \
    --mode repair \
    --groups all \
    --end-date "$TODAY" \
    --workers "$PEPPERSTONE_WORKERS" \
    --retries "$RETRIES" \
    --repair-overlap-days "$REPAIR_OVERLAP_DAYS"
else
  echo "[daily] skip=pepperstone_groups_repair reason=RUN_PEPPERSTONE_GROUPS=${RUN_PEPPERSTONE_GROUPS}"
fi

echo "[daily] all markets completed"