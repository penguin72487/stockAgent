#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
ENV_FILE="${SHIOAJI_TAIFEX_ENV_FILE:-$REPO_ROOT/.env.futures}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[shioaji-tx-history-runner] missing env file: $ENV_FILE" >&2
  exit 2
fi
set -a
source "$ENV_FILE"
set +a
source scripts/runtime_env.sh

RUN_ROOT="${SHIOAJI_FUTURES_HISTORY_RUN_ROOT:-artifacts/data_repair/shioaji_futures_history}"
LOG_FILE="$RUN_ROOT/run.log"
TARGET_FILE="$RUN_ROOT/target_end_date.txt"
ALIAS_FILE="${SHIOAJI_FUTURES_ALIAS_FILE:-data_tw_futures/shioaji_contracts/continuous_contracts.csv}"
mkdir -p "$RUN_ROOT"
exec 9>"$RUN_ROOT/runner.lock"
flock -n 9 || exit 3
exec > >(tee -a "$LOG_FILE") 2>&1

if [[ ! -s "$TARGET_FILE" ]]; then
  TZ=Asia/Taipei date -d yesterday +%F > "$TARGET_FILE"
fi
END_DATE="$(tr -d '[:space:]' < "$TARGET_FILE")"

market_priority_window_seconds() {
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
target_date = now.date()
target = datetime.combine(target_date, time(14, 31), tzinfo=tz)
if now >= target:
    target_date += timedelta(days=1)
    while target_date.weekday() >= 5:
        target_date += timedelta(days=1)
    target = datetime.combine(target_date, time(14, 31), tzinfo=tz)
print(max(60, int((target - now).total_seconds())))
PY
}

# Quota reset timing is an observed broker-side fact, not a trading-calendar
# rule.  Recheck api.usage() periodically so a weekend or otherwise unexpected
# counter drop resumes the receipt-backed download without waiting several days.
QUOTA_RECHECK_SECONDS="${SHIOAJI_FUTURES_HISTORY_QUOTA_RECHECK_SECONDS:-300}"
if [[ ! "$QUOTA_RECHECK_SECONDS" =~ ^[0-9]+$ ]] || \
   (( QUOTA_RECHECK_SECONDS < 60 || QUOTA_RECHECK_SECONDS > 3600 )); then
  echo "[shioaji-futures-history-runner] SHIOAJI_FUTURES_HISTORY_QUOTA_RECHECK_SECONDS must be an integer within 60..3600" >&2
  exit 2
fi

if [[ ! -f "$ALIAS_FILE" ]]; then
  echo "[shioaji-futures-history-runner] missing alias inventory: $ALIAS_FILE" >&2
  exit 2
fi
mapfile -t CONTRACTS < <(
  run_fintech_python - "$ALIAS_FILE" <<'PY'
import sys
import polars as pl
frame = pl.read_csv(sys.argv[1]).sort(["priority", "contract"])
codes = frame.get_column("contract").to_list()
codes = ["TXFR1", *[value for value in codes if value != "TXFR1"]]
print("\n".join(codes))
PY
)

echo "[shioaji-futures-history-runner] started=$(TZ=Asia/Taipei date --iso-8601=seconds) end_date=$END_DATE contracts=${#CONTRACTS[@]}"
for contract in "${CONTRACTS[@]}"; do
  if [[ "$contract" == "TXFR1" ]]; then
    output_dir="data_tw_index_futures/shioaji_history/TXFR1"
  else
    output_dir="data_tw_futures/shioaji_history/$contract"
  fi
  while true; do
    echo "[shioaji-futures-history-runner] contract_start=$contract output=$output_dir"
    set +e
    run_fintech_python -m downloader.download_shioaji_tx_futures_ticks \
      --simulation \
      --contract "$contract" \
      --output-dir "$output_dir" \
      --end-date "$END_DATE"
    rc=$?
    set -e
    if (( rc == 0 )); then
      echo "[shioaji-futures-history-runner] contract_complete=$contract"
      break
    fi
    if (( rc != 75 && rc != 76 )); then
      echo "[shioaji-futures-history-runner] failed contract=$contract rc=$rc" >&2
      exit "$rc"
    fi
    if (( rc == 76 )); then
      delay="$(market_priority_window_seconds)"
      reason="market_hours_priority_gate"
    else
      delay="$QUOTA_RECHECK_SECONDS"
      reason="next_quota_window"
    fi
    echo "[shioaji-futures-history-runner] waiting_seconds=$delay reason=$reason contract=$contract quota_recheck_seconds=$QUOTA_RECHECK_SECONDS"
    sleep "$delay"
  done
done
echo "[shioaji-futures-history-runner] all_contracts_complete=$(TZ=Asia/Taipei date --iso-8601=seconds)"
