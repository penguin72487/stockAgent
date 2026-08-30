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
BATCH_RECEIPT="$RUN_ROOT/latest_batch.json"
ALIAS_FILE="${SHIOAJI_FUTURES_ALIAS_FILE:-data_tw_futures/shioaji_contracts/continuous_contracts.csv}"
CALENDAR_FILE="${SHIOAJI_FUTURES_HISTORY_CALENDAR_FILE:-data_tw_index_futures/day_session_contracts.parquet}"
mkdir -p "$RUN_ROOT"
exec 9>"$RUN_ROOT/runner.lock"
flock -n 9 || exit 3
exec > >(tee -a "$LOG_FILE") 2>&1

market_priority_window_seconds() {
  run_fintech_python - <<'PY'
from stockagent.live.shioaji_schedule import historical_query_pause_seconds
print(historical_query_pause_seconds())
PY
}

quota_retry_seconds() {
  run_fintech_python - "$QUOTA_RECHECK_SECONDS" <<'PY'
from datetime import datetime, time, timedelta
import sys
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
if now.weekday() >= 5:
    print(int(sys.argv[1]))
    raise SystemExit
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

# Quota reset timing is an observed broker-side fact, not a wall-clock reset we
# manufacture.  Weekends retain periodic rechecks; on weekdays a depleted
# history budget cannot resume until the next post-close window.
QUOTA_RECHECK_SECONDS="${SHIOAJI_FUTURES_HISTORY_QUOTA_RECHECK_SECONDS:-300}"
if [[ ! "$QUOTA_RECHECK_SECONDS" =~ ^[0-9]+$ ]] || \
   (( QUOTA_RECHECK_SECONDS < 60 || QUOTA_RECHECK_SECONDS > 3600 )); then
  echo "[shioaji-futures-history-runner] SHIOAJI_FUTURES_HISTORY_QUOTA_RECHECK_SECONDS must be an integer within 60..3600" >&2
  exit 2
fi
TARGET_RECHECK_SECONDS="${SHIOAJI_FUTURES_HISTORY_TARGET_RECHECK_SECONDS:-300}"
if [[ ! "$TARGET_RECHECK_SECONDS" =~ ^[0-9]+$ ]] || \
   (( TARGET_RECHECK_SECONDS < 60 || TARGET_RECHECK_SECONDS > 3600 )); then
  echo "[shioaji-futures-history-runner] SHIOAJI_FUTURES_HISTORY_TARGET_RECHECK_SECONDS must be an integer within 60..3600" >&2
  exit 2
fi
CONNECTION_RETRY_SECONDS="${SHIOAJI_FUTURES_HISTORY_CONNECTION_RETRY_SECONDS:-60}"
CONNECTION_RELEASE_SECONDS="${SHIOAJI_FUTURES_HISTORY_CONNECTION_RELEASE_SECONDS:-2}"
for setting in CONNECTION_RETRY_SECONDS CONNECTION_RELEASE_SECONDS; do
  value="${!setting}"
  if [[ ! "$value" =~ ^[0-9]+$ ]] || (( value < 1 || value > 300 )); then
    echo "[shioaji-futures-history-runner] $setting must be an integer within 1..300" >&2
    exit 2
  fi
done
TARGET_CHECK_CONTRACTS="${SHIOAJI_FUTURES_HISTORY_TARGET_CHECK_CONTRACTS:-25}"
if [[ ! "$TARGET_CHECK_CONTRACTS" =~ ^[0-9]+$ ]] || \
   (( TARGET_CHECK_CONTRACTS < 1 || TARGET_CHECK_CONTRACTS > 743 )); then
  echo "[shioaji-futures-history-runner] SHIOAJI_FUTURES_HISTORY_TARGET_CHECK_CONTRACTS must be an integer within 1..743" >&2
  exit 2
fi
MAX_TRAFFIC_FRACTION="${SHIOAJI_FUTURES_HISTORY_MAX_TRAFFIC_FRACTION:-0.90}"
run_fintech_python - "$MAX_TRAFFIC_FRACTION" <<'PY'
import sys

fraction = float(sys.argv[1])
if not 0.0 < fraction < 1.0:
    raise SystemExit("SHIOAJI_FUTURES_HISTORY_MAX_TRAFFIC_FRACTION must be within 0..1")
PY

if [[ ! -f "$ALIAS_FILE" ]]; then
  echo "[shioaji-futures-history-runner] missing alias inventory: $ALIAS_FILE" >&2
  exit 2
fi
if [[ ! -f "$CALENDAR_FILE" ]]; then
  echo "[shioaji-futures-history-runner] missing official TX calendar: $CALENDAR_FILE" >&2
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

resolve_targets() {
  run_fintech_python - "$CALENDAR_FILE" <<'PY'
from pathlib import Path
import polars as pl

from stockagent.live.shioaji_schedule import previous_tw_stock_session

calendar_path = Path(__import__("sys").argv[1])
expected_latest = previous_tw_stock_session(parquet_root=Path("data_tw_public"))
calendar_latest = (
    pl.scan_parquet(calendar_path)
    .filter((pl.col("product") == "TX") & (pl.col("date") <= pl.lit(expected_latest)))
    .select(pl.col("date").max())
    .collect()
    .item()
)
if calendar_latest is None:
    raise SystemExit("official TX calendar contains no completed TX session")
print(calendar_latest.isoformat(), expected_latest.isoformat())
PY
}

batch_is_current() {
  run_fintech_python - "$BATCH_RECEIPT" "$1" "${#CONTRACTS[@]}" <<'PY'
import json
from pathlib import Path
import sys

path, target, total = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
raise SystemExit(
    0
    if payload.get("status") == "complete"
    and payload.get("target_end_date") == target
    and int(payload.get("terminal_contracts", -1)) == total
    else 1
)
PY
}

write_batch_receipt() {
  run_fintech_python - "$BATCH_RECEIPT" "$1" "$2" "$3" "${#CONTRACTS[@]}" <<'PY'
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

path = Path(sys.argv[1])
target = sys.argv[2]
complete, unavailable, total = map(int, sys.argv[3:])
payload = {
    "schema_version": 1,
    "status": "complete",
    "target_end_date": target,
    "complete_contracts": complete,
    "provider_unavailable_contracts": unavailable,
    "terminal_contracts": complete + unavailable,
    "total_contracts": total,
    "completed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
}
path.parent.mkdir(parents=True, exist_ok=True)
temporary = path.with_suffix(path.suffix + ".tmp")
temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.replace(temporary, path)
PY
}

contract_terminal_state() {
  run_fintech_python - "$1/manifest.json" "$2" <<'PY'
import json
from pathlib import Path
import sys

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    print("pending")
    raise SystemExit
if payload.get("history_end") != sys.argv[2]:
    print("pending")
elif payload.get("status") == "complete" and not payload.get("missing_trading_dates"):
    print("complete")
elif (
    payload.get("status") == "contract_unavailable"
    and payload.get("no_data_fabricated") is True
):
    print("contract_unavailable")
else:
    print("pending")
PY
}

echo "[shioaji-futures-history-runner] started=$(TZ=Asia/Taipei date --iso-8601=seconds) contracts=${#CONTRACTS[@]} max_traffic_fraction=$MAX_TRAFFIC_FRACTION cutoff=07:45 resume=14:31 calendar=$CALENDAR_FILE"
while true; do
  read -r END_DATE EXPECTED_LATEST_DATE <<< "$(resolve_targets)"
  temporary_target="$TARGET_FILE.tmp"
  printf '%s\n' "$END_DATE" > "$temporary_target"
  mv "$temporary_target" "$TARGET_FILE"
  if [[ "$END_DATE" != "$EXPECTED_LATEST_DATE" ]]; then
    echo "[shioaji-futures-history-runner] calendar_lag=true target_end_date=$END_DATE expected_latest_completed_session=$EXPECTED_LATEST_DATE source=$CALENDAR_FILE"
  fi
  if batch_is_current "$END_DATE"; then
    echo "[shioaji-futures-history-runner] target_current=true end_date=$END_DATE waiting_seconds=$TARGET_RECHECK_SECONDS reason=await_official_calendar_advance"
    sleep "$TARGET_RECHECK_SECONDS"
    continue
  fi

  echo "[shioaji-futures-history-runner] batch_start=$(TZ=Asia/Taipei date --iso-8601=seconds) end_date=$END_DATE contracts=${#CONTRACTS[@]}"
  completed_contracts=0
  unavailable_contracts=0
  target_advanced=false
  contract_index=0
  for contract in "${CONTRACTS[@]}"; do
    if (( contract_index % TARGET_CHECK_CONTRACTS == 0 )); then
      read -r OBSERVED_END_DATE OBSERVED_EXPECTED_LATEST <<< "$(resolve_targets)"
      if [[ "$OBSERVED_END_DATE" != "$END_DATE" ]]; then
        echo "[shioaji-futures-history-runner] target_advanced=true previous_end_date=$END_DATE new_end_date=$OBSERVED_END_DATE action=restart_batch"
        target_advanced=true
        break
      fi
    fi
    contract_index=$((contract_index + 1))
    if [[ "$contract" == "TXFR1" ]]; then
      output_dir="data_tw_index_futures/shioaji_history/TXFR1"
    else
      output_dir="data_tw_futures/shioaji_history/$contract"
    fi
    terminal_state="$(contract_terminal_state "$output_dir" "$END_DATE")"
    if [[ "$terminal_state" == "complete" ]]; then
      completed_contracts=$((completed_contracts + 1))
      echo "[shioaji-futures-history-runner] contract_reused=$contract target_end_date=$END_DATE evidence=terminal_manifest"
      continue
    fi
    if [[ "$terminal_state" == "contract_unavailable" ]]; then
      unavailable_contracts=$((unavailable_contracts + 1))
      echo "[shioaji-futures-history-runner] contract_reused=$contract target_end_date=$END_DATE evidence=provider_unavailable_manifest"
      continue
    fi
    while true; do
      priority_delay="$(market_priority_window_seconds)"
      if (( priority_delay > 0 )); then
        echo "[shioaji-futures-history-runner] waiting_seconds=$priority_delay reason=live_priority_window contract=$contract cutoff=07:45 resume=14:31"
        sleep "$priority_delay"
        continue
      fi
      echo "[shioaji-futures-history-runner] contract_start=$contract output=$output_dir target_end_date=$END_DATE"
      set +e
      run_fintech_python -m downloader.download_shioaji_tx_futures_ticks \
        --simulation \
        --contract "$contract" \
        --calendar-path "$CALENDAR_FILE" \
        --output-dir "$output_dir" \
        --end-date "$END_DATE" \
        --max-traffic-fraction "$MAX_TRAFFIC_FRACTION"
      rc=$?
      set -e
      if (( rc == 0 )); then
        echo "[shioaji-futures-history-runner] contract_complete=$contract target_end_date=$END_DATE"
        completed_contracts=$((completed_contracts + 1))
        sleep "$CONNECTION_RELEASE_SECONDS"
        break
      fi
      if (( rc == 78 )); then
        echo "[shioaji-futures-history-runner] contract_terminal=$contract status=contract_unavailable no_data_fabricated=true"
        unavailable_contracts=$((unavailable_contracts + 1))
        sleep "$CONNECTION_RELEASE_SECONDS"
        break
      fi
      if (( rc == 79 )); then
        echo "[shioaji-futures-history-runner] waiting_seconds=$CONNECTION_RETRY_SECONDS reason=connection_capacity contract=$contract broker_code=451"
        sleep "$CONNECTION_RETRY_SECONDS"
        continue
      fi
      if (( rc != 75 && rc != 76 )); then
        echo "[shioaji-futures-history-runner] failed contract=$contract rc=$rc" >&2
        exit "$rc"
      fi
      if (( rc == 76 )); then
        delay="$(market_priority_window_seconds)"
        reason="market_hours_priority_gate"
      else
        delay="$(quota_retry_seconds)"
        reason="next_safe_quota_window"
      fi
      echo "[shioaji-futures-history-runner] waiting_seconds=$delay reason=$reason contract=$contract quota_recheck_seconds=$QUOTA_RECHECK_SECONDS"
      sleep "$delay"
    done
  done
  if [[ "$target_advanced" == true ]]; then
    continue
  fi
  write_batch_receipt "$END_DATE" "$completed_contracts" "$unavailable_contracts"
  echo "[shioaji-futures-history-runner] batch_complete=$(TZ=Asia/Taipei date --iso-8601=seconds) end_date=$END_DATE completed_contracts=$completed_contracts unavailable_contracts=$unavailable_contracts total_contracts=${#CONTRACTS[@]}"
done
