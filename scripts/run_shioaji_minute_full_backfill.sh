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

if [[ -n "${SHIOAJI_MINUTE_END_DATE:-}" ]]; then
  BACKFILL_END_DATE="$SHIOAJI_MINUTE_END_DATE"
else
  BACKFILL_END_DATE="$(TZ=Asia/Taipei date -d yesterday +%F)"
fi
printf '%s\n' "$BACKFILL_END_DATE" > "$TARGET_FILE"

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


# Historical KBar backfill remains outside the conservative 07:45-14:31
# live-priority window even if the Top-200 capture finishes a little earlier.
after_close = datetime.combine(now.date(), time(14, 31), tzinfo=tz)
if now < after_close:
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
# Shioaji historical-data traffic resets at 08:00 on trading days. Top-200
# Tick/BidAsk capture owns the session first, so the next KBar window starts
# only after its capture/audit window and the market-hours safety buffer.
target_date = now.date() + timedelta(days=1)
while target_date.weekday() >= 5:
    target_date += timedelta(days=1)
target = datetime.combine(target_date, time(14, 31), tzinfo=tz)
print(max(60, int((target - now).total_seconds())))
PY
}

echo "[shioaji-minute-runner] started_at=$(TZ=Asia/Taipei date --iso-8601=seconds) end_date=$BACKFILL_END_DATE"

while true; do
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

  echo "[shioaji-minute-runner] download_start=$(TZ=Asia/Taipei date --iso-8601=seconds)"
  set +e
  run_fintech_python -m downloader.download_shioaji_tw_minute_kbars \
    --simulation \
    --all-symbols \
    --workers "${SHIOAJI_MINUTE_WORKERS:-5}" \
    --requests-per-second "${SHIOAJI_MINUTE_REQUESTS_PER_SECOND:-10}" \
    --max-traffic-fraction "${SHIOAJI_MINUTE_MAX_TRAFFIC_FRACTION:-0.90}" \
    --traffic-reserve-mb "${SHIOAJI_MINUTE_TRAFFIC_RESERVE_MB:-25}" \
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
run_path = Path("data_tw_minute/shioaji_1m/latest_run_summary.json")
if not path.is_file():
    print("missing")
else:
    payload = json.loads(path.read_text(encoding="utf-8"))
    run_payload = (
        json.loads(run_path.read_text(encoding="utf-8"))
        if run_path.is_file()
        else payload
    )
    print(
        "complete=" + str(bool(payload.get("selected_coverage_complete"))).lower()
        + " collected=" + str(bool(payload.get("resumable_collection_complete"))).lower()
        + " selected=" + str(run_payload.get("selected_symbols", 0))
        + " reported=" + str(run_payload.get("reported_symbols", 0))
        + " done=" + str(payload.get("complete_symbols", 0))
        + " gap_symbols=" + str(payload.get("complete_with_source_gap_symbols", 0))
        + " unavailable=" + str(payload.get("contract_unavailable_symbols", 0))
        + " failed=" + str(run_payload.get("failed_symbols", 0))
        + " partial=" + str(run_payload.get("partial_symbols", 0))
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
    && [[ "$summary_state" == *"collected=true"* ]] \
    && [[ "$summary_state" == *"target_match=true"* ]]; then
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
