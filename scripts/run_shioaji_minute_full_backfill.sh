#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ENV_FILE="${SHIOAJI_ENV_FILE:-$REPO_ROOT/.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[shioaji-minute-runner] missing env file: $ENV_FILE" >&2
  exit 2
fi
set -a
source "$ENV_FILE"
set +a
source scripts/runtime_env.sh

RUN_ROOT="${SHIOAJI_MINUTE_RUN_ROOT:-artifacts/data_repair/shioaji_minute_full}"
LOG_FILE="$RUN_ROOT/run.log"
LOCK_FILE="$RUN_ROOT/runner.lock"
TARGET_FILE="$RUN_ROOT/target_end_date.txt"
mkdir -p "$RUN_ROOT"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[shioaji-minute-runner] another runner holds $LOCK_FILE" >&2
  exit 3
fi
exec > >(tee -a "$LOG_FILE") 2>&1

resolve_backfill_end_date() {
  if [[ -n "${SHIOAJI_MINUTE_END_DATE:-}" ]]; then
    printf '%s\n' "$SHIOAJI_MINUTE_END_DATE"
    return
  fi
  run_fintech_python - <<'PY'
from pathlib import Path

from stockagent.live.shioaji_schedule import latest_completed_tw_stock_session

print(latest_completed_tw_stock_session(parquet_root=Path("data_tw_public")).isoformat())
PY
}

publish_target() {
  local target="$1"
  local temporary="$TARGET_FILE.tmp"
  printf '%s\n' "$target" > "$temporary"
  mv "$temporary" "$TARGET_FILE"
}

seconds_until_after_close() {
  run_fintech_python - <<'PY'
from stockagent.live.shioaji_schedule import historical_query_pause_seconds
print(historical_query_pause_seconds())
PY
}

top200_priority_state() {
  run_fintech_python - <<'PY'
from datetime import datetime, time
import json
from pathlib import Path
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
if now.weekday() >= 5:
    print("0 weekend_no_capture")
    raise SystemExit

trade_date = now.date().isoformat()


def audit_ok(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (
        isinstance(payload, dict)
        and payload.get("status") == "ok"
        and payload.get("trade_date") == trade_date
        and int(payload.get("symbols", 0)) == 200
    )


# Before 07:45 no stock live capture owns the quote connections or traffic, so
# historical work may run.  From 07:45 onward the live capture and its audits
# own the window through 14:31.
priority_start = datetime.combine(now.date(), time(7, 45), tzinfo=tz)
after_close = datetime.combine(now.date(), time(14, 31), tzinfo=tz)
if now < priority_start:
    print("0 preopen_offhours")
elif now < after_close:
    print(f"{max(60, int((after_close - now).total_seconds()))} top200_then_market_close")
elif audit_ok(Path(f"data_tw_microstructure/audits/{trade_date}.json")) and audit_ok(
    Path(f"data_tw_microstructure/audits/hft_{trade_date}.json")
):
    print("0 top200_audited")
else:
    print("60 top200_audit_pending")
PY
}

seconds_until_next_quota_window() {
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

tz = ZoneInfo("Asia/Taipei")
now = datetime.now(tz)
# A reset is accepted only after api.usage() actually drops; this function does
# not manufacture an 08:00 reset.  On weekdays the next eligible retry remains
# after the protected live window.  Weekends periodically recheck observed use.
if now.weekday() >= 5:
    print(300)
    raise SystemExit
target_date = now.date() + timedelta(days=1)
while target_date.weekday() >= 5:
    target_date += timedelta(days=1)
target = datetime.combine(target_date, time(14, 31), tzinfo=tz)
print(max(60, int((target - now).total_seconds())))
PY
}

futures_priority_state() {
  run_fintech_python - <<'PY'
import json
from pathlib import Path
import polars as pl

calendar = Path("data_tw_index_futures/day_session_contracts.parquet")
receipt_path = Path("artifacts/data_repair/shioaji_futures_history/latest_batch.json")
try:
    latest = (
        pl.scan_parquet(calendar)
        .filter(pl.col("product") == "TX")
        .select(pl.col("date").max())
        .collect()
        .item()
    )
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    print("60 futures_history_receipt_missing")
    raise SystemExit
ready = (
    latest is not None
    and receipt.get("status") == "complete"
    and receipt.get("target_end_date") == latest.isoformat()
    and int(receipt.get("terminal_contracts", -1))
        == int(receipt.get("total_contracts", -2))
)
print("0 futures_history_current" if ready else "60 futures_history_priority")
PY
}

resolve_worker_count() {
  local configured="${SHIOAJI_MINUTE_WORKERS:-4}"
  if [[ ! "$configured" =~ ^[0-9]+$ ]] || (( configured < 1 || configured > 4 )); then
    echo "[shioaji-minute-runner] SHIOAJI_MINUTE_WORKERS must be an integer within 1..4" >&2
    return 2
  fi
  local fop_workers history_workers available
  fop_workers="$(pgrep -fc 'python .*downloader\.stream_shioaji_taifex_bidask' || true)"
  history_workers="$(pgrep -fc 'python .*(downloader\.download_shioaji_tx_futures_ticks|downloader\.download_shioaji_historical_market_data)' || true)"
  available=$((5 - fop_workers - history_workers))
  if (( available < 1 )); then
    printf '0 %s %s\n' "$fop_workers" "$history_workers"
  elif (( configured < available )); then
    printf '%s %s %s\n' "$configured" "$fop_workers" "$history_workers"
  else
    printf '%s %s %s\n' "$available" "$fop_workers" "$history_workers"
  fi
}

echo "[shioaji-minute-runner] started_at=$(TZ=Asia/Taipei date --iso-8601=seconds) rolling_target=true"

while true; do
  BACKFILL_END_DATE="$(resolve_backfill_end_date)"
  publish_target "$BACKFILL_END_DATE"
  echo "[shioaji-minute-runner] target_end_date=$BACKFILL_END_DATE boundary=latest_completed_tw_stock_session"

  read -r futures_delay futures_reason <<< "$(futures_priority_state)"
  if (( futures_delay > 0 )); then
    echo "[shioaji-minute-runner] waiting_seconds=$futures_delay reason=$futures_reason"
    sleep "$futures_delay"
    continue
  fi

  read -r top200_delay top200_reason <<< "$(top200_priority_state)"
  if (( top200_delay > 0 )); then
    echo "[shioaji-minute-runner] waiting_seconds=$top200_delay reason=$top200_reason"
    sleep "$top200_delay"
    continue
  fi
  echo "[shioaji-minute-runner] priority_gate=$top200_reason"

  market_delay="$(seconds_until_after_close)"
  if (( market_delay > 0 )); then
    echo "[shioaji-minute-runner] waiting_seconds=$market_delay reason=taiwan_market_hours"
    sleep "$market_delay"
  fi

  read -r minute_workers fop_workers history_workers <<< "$(resolve_worker_count)"
  if (( minute_workers < 1 )); then
    echo "[shioaji-minute-runner] waiting_seconds=60 reason=shioaji_connection_capacity fop_workers=$fop_workers history_workers=$history_workers account_limit=5"
    sleep 60
    continue
  fi

  echo "[shioaji-minute-runner] download_start=$(TZ=Asia/Taipei date --iso-8601=seconds)"
  echo "[shioaji-minute-runner] connection_plan minute_workers=$minute_workers fop_workers=$fop_workers history_workers=$history_workers account_limit=5"
  set +e
  run_fintech_python -m downloader.download_shioaji_tw_minute_kbars \
    --simulation \
    --all-symbols \
    --workers "$minute_workers" \
    --requests-per-second "${SHIOAJI_MINUTE_REQUESTS_PER_SECOND:-10}" \
    --max-traffic-fraction "${SHIOAJI_MINUTE_MAX_TRAFFIC_FRACTION:-0.90}" \
    --start-date 2020-03-02 \
    --end-date "$BACKFILL_END_DATE"
  download_rc=$?
  set -e
  echo "[shioaji-minute-runner] download_exit=$download_rc at=$(TZ=Asia/Taipei date --iso-8601=seconds)"

  summary_state="$(run_fintech_python - "$BACKFILL_END_DATE" <<'PY'
import json
import sys
from pathlib import Path

target_end = sys.argv[1]
path = Path("data_tw_minute/shioaji_1m/download_summary.json")
if not path.is_file():
    print("missing")
else:
    payload = json.loads(path.read_text(encoding="utf-8"))
    run_payload = payload
    run_selected = int(payload.get("selected_symbols", 0))
    run_reported = int(payload.get("reported_symbols", 0))
    run_failed = int(payload.get("failed_symbols", 0))
    run_partial = int(payload.get("partial_symbols", 0))
    current_run_ready = (
        bool(run_payload.get("resumable_collection_complete"))
        and run_selected > 0
        and run_reported == run_selected
        and run_failed == 0
        and run_partial == 0
        and run_payload.get("end_date") == target_end
        and not bool(run_payload.get("stopped_for_traffic"))
        and not bool(run_payload.get("stopped_for_market_hours"))
    )
    print(
        "ready=" + str(current_run_ready).lower()
        + " complete=" + str(bool(payload.get("selected_coverage_complete"))).lower()
        + " collected=" + str(bool(payload.get("resumable_collection_complete"))).lower()
        + " selected=" + str(run_selected)
        + " reported=" + str(run_reported)
        + " done=" + str(payload.get("complete_symbols", 0))
        + " gap_symbols=" + str(payload.get("complete_with_source_gap_symbols", 0))
        + " unavailable=" + str(payload.get("contract_unavailable_symbols", 0))
        + " failed=" + str(run_failed)
        + " partial=" + str(run_partial)
        + " end_date=" + str(payload.get("end_date"))
        + " target_match=" + str(payload.get("end_date") == target_end).lower()
        + " traffic_stop=" + str(bool(run_payload.get("stopped_for_traffic"))).lower()
        + " market_stop=" + str(bool(run_payload.get("stopped_for_market_hours"))).lower()
        + " traffic=" + str(run_payload.get("traffic_used_bytes"))
        + "/" + str(run_payload.get("traffic_limit_bytes"))
    )
PY
)"
  echo "[shioaji-minute-runner] summary $summary_state"

  if (( download_rc == 0 )) \
    && [[ "$summary_state" == "ready=true "* ]]; then
    echo "[shioaji-minute-runner] building audited available-source research dataset"
    run_fintech_python scripts/build_shioaji_tw_minute_dataset.py
    latest_date="$(
      run_fintech_python - <<'PY'
import json
from pathlib import Path
payload = json.loads(Path("data_tw_minute/research_dataset/manifest.json").read_text())
print(payload["dates"][-1])
PY
    )"
    run_fintech_python scripts/audit_shioaji_tw_minute_dataset.py \
      --trade-date "$latest_date"
    run_fintech_python scripts/audit_shioaji_tw_minute_dataset.py \
      --all-partitions \
      --output data_tw_minute/audits/full_latest.json
    run_fintech_python -m pytest -q \
      test/test_shioaji_tw_minute_kbars.py \
      test/test_tw_minute_mode.py
    echo "[shioaji-minute-runner] materializing zero-traffic daily and hybrid datasets"
    run_fintech_python -m downloader.download_shioaji_tw_kbars \
      --local-only \
      --start-date 2020-03-02 \
      --end-date "$BACKFILL_END_DATE"
    run_fintech_python -m scripts.build_tw_shioaji_dataset
    run_fintech_python -m scripts.audit_tw_shioaji_dataset
    echo "[shioaji-minute-runner] research_ready=true contract=audited_available_source source_gaps_are_masked=true"
    echo "[shioaji-minute-runner] daily_ready=true materialization=verified_local_minute api_requests_started=0"
    echo "[shioaji-minute-runner] collection_completed_at=$(TZ=Asia/Taipei date --iso-8601=seconds)"
    exit 0
  fi

  if [[ "$summary_state" == *"market_stop=true"* ]]; then
    next_delay="$(seconds_until_after_close)"
    if (( next_delay <= 0 )); then
      next_delay=300
    fi
    echo "[shioaji-minute-runner] waiting_seconds=$next_delay reason=market_hours"
  else
    next_delay="$(seconds_until_next_quota_window)"
    echo "[shioaji-minute-runner] waiting_seconds=$next_delay reason=next_quota_window"
  fi
  sleep "$next_delay"
done
