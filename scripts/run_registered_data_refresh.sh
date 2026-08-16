#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source "$repo_root/scripts/runtime_env.sh"

refresh_scope="${1:-daily}"
common_env=(
  RUN_MODE=once
  MAX_CYCLES=1
  FAIL_FAST=0
  DAILY_PARALLEL_GROUPS=1
  RUN_TW_PUBLIC_DATA=0
  RUN_ALPACA_US=0
  RUN_DATA_QUALITY_AUDIT=0
  YAHOO_DAILY_DISCOVER_SYMBOLS=1
  YAHOO_INCLUDE_US_DELISTED=1
  YAHOO_STEP_TIMEOUT_SECONDS=0
)

case "$refresh_scope" in
  daily)
    exec env "${common_env[@]}" \
      RUN_ID="registered-daily-$(date -u +%Y%m%dT%H%M%SZ)" \
      LOCK_FILE="$repo_root/artifacts/daily_downloader/registered_daily.lock" \
      RUN_LOG_DIR="$repo_root/artifacts/daily_downloader/registered_daily" \
      RUN_RECORD_FILE="$repo_root/artifacts/daily_downloader/registered_daily_runs.tsv" \
      RUN_YAHOO=1 \
      YAHOO_ASSETS="us_stocks forex" \
      RUN_FRANKFURTER=1 \
      FRANKFURTER_OUTPUT_DIR="$repo_root/data_forex_frankfurter" \
      RUN_PEPPERSTONE_GROUPS=1 \
      RUN_CEX_PERP=0 \
      "$repo_root/downloader/run_daily_all_markets.sh"
    ;;
  intraday)
    exec env "${common_env[@]}" \
      RUN_ID="registered-intraday-$(date -u +%Y%m%dT%H%M%SZ)" \
      LOCK_FILE="$repo_root/artifacts/daily_downloader/registered_intraday.lock" \
      RUN_LOG_DIR="$repo_root/artifacts/daily_downloader/registered_intraday" \
      RUN_RECORD_FILE="$repo_root/artifacts/daily_downloader/registered_intraday_runs.tsv" \
      RUN_YAHOO=1 \
      YAHOO_ASSETS="crypto" \
      RUN_FRANKFURTER=0 \
      RUN_PEPPERSTONE_GROUPS=0 \
      RUN_CEX_PERP=1 \
      "$repo_root/downloader/run_daily_all_markets.sh"
    ;;
  *)
    echo "usage: $0 {daily|intraday}" >&2
    exit 2
    ;;
esac
