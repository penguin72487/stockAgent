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
SOURCE_GAP_RETRY_MARKER="$RUN_ROOT/source_gap_retry_v1.completed"
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


def terminal_skip_ok(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (
        isinstance(payload, dict)
        and payload.get("trade_date") == trade_date
        and payload.get("status") == "skipped"
        and payload.get("reason") == "connection_budget"
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
elif terminal_skip_ok(
    Path("artifacts/data_capture/shioaji_top200/latest_capture_state.json")
):
    print("0 top200_terminal_skip")
else:
    # Capture ended at 13:30.  A missing audit is a data-quality failure, not
    # an actionable reason to hold unrelated historical stock downloads.
    print("0 top200_audit_missing")
PY
}

seconds_until_next_quota_window() {
  run_fintech_python - <<'PY'
from datetime import datetime
from pathlib import Path
from stockagent.live.shioaji_schedule import TAIPEI, next_postreset_historical_window

now = datetime.now(TAIPEI)
target = next_postreset_historical_window(now, parquet_root=Path("data_tw_public"))
print(max(60, int((target - now).total_seconds())))
PY
}

latest_run_stop_reason() {
  run_fintech_python - "$BACKFILL_END_DATE" "$download_started_ns" <<'PY'
import json
from pathlib import Path
import sys

path = Path("data_tw_minute/shioaji_1m/latest_run_summary.json")
try:
    if path.stat().st_mtime_ns < int(sys.argv[2]):
        raise ValueError("stale partial receipt")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("end_date") != sys.argv[1]:
        raise ValueError("different target")
except (OSError, ValueError, json.JSONDecodeError):
    print("other")
else:
    if payload.get("stopped_for_traffic"):
        print("traffic")
    elif payload.get("stopped_for_schedule"):
        print("schedule")
    elif payload.get("stopped_for_market_hours"):
        print("market")
    else:
        print("other")
PY
}

resolve_worker_count() {
  local configured="${SHIOAJI_MINUTE_WORKERS:-4}"
  if [[ ! "$configured" =~ ^[0-9]+$ ]] || (( configured < 1 || configured > 4 )); then
    echo "[shioaji-minute-runner] SHIOAJI_MINUTE_WORKERS must be an integer within 1..4" >&2
    return 2
  fi
  local fop_workers history_workers quote_clients
  fop_workers="$(pgrep -fc 'python .*downloader\.stream_shioaji_taifex_bidask' || true)"
  history_workers="$(pgrep -fc 'python .*(downloader\.download_shioaji_tx_futures_ticks|downloader\.download_shioaji_historical_market_data)' || true)"
  quote_clients="${SHIOAJI_RESERVED_STOCK_QUOTE_CLIENTS:-2}"
  if [[ ! "$quote_clients" =~ ^[0-9]+$ ]] || (( quote_clients > 4 )); then
    echo "[shioaji-minute-runner] SHIOAJI_RESERVED_STOCK_QUOTE_CLIENTS must be within 0..4" >&2
    return 2
  fi
  # The 14:31-14:45 frontier window precedes the 14:50 FOP pre-open.  The
  # downloader receives a hard resumable deadline, so it cannot keep those
  # temporary connections when the non-recoverable stream starts.
  run_fintech_python - "$fop_workers" "$history_workers" "$quote_clients" "$configured" <<'PY'
import sys
from stockagent.live.shioaji_schedule import minute_connection_plan

fop, history, quotes, configured = map(int, sys.argv[1:])
workers, reserved_fop, reserved_history, deadline = minute_connection_plan(
    active_fop_workers=fop,
    active_history_workers=history,
    reserved_stock_quotes=quotes,
    configured_workers=configured,
)
print(
    workers, fop, history, quotes, reserved_fop, reserved_history,
    deadline.isoformat() if deadline is not None else "-",
)
PY
}

connection_retry_seconds() {
  # With the planned three night collectors and reserved quote/history slots,
  # there cannot be a safe stock-minute login before the 05:00 night close.
  # Sleep to that boundary instead of re-reading calendars and receipts every
  # minute throughout the night.  Other capacity shortages retain a short retry.
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from stockagent.data.taifex_sessions import TAIPEI, taifex_session_kind

now = datetime.now(TAIPEI)
if taifex_session_kind(now, include_preopen=True) != "night":
    print(60)
else:
    wake = datetime.combine(now.date(), time(5, 0, 10), tzinfo=TAIPEI)
    if wake <= now:
        wake += timedelta(days=1)
    print(max(60, int((wake - now).total_seconds())))
PY
}

echo "[shioaji-minute-runner] started_at=$(TZ=Asia/Taipei date --iso-8601=seconds) rolling_target=true"

while true; do
  BACKFILL_END_DATE="$(resolve_backfill_end_date)"
  publish_target "$BACKFILL_END_DATE"
  echo "[shioaji-minute-runner] target_end_date=$BACKFILL_END_DATE boundary=latest_completed_tw_stock_session"

  # Futures history has its own publication calendar.  Its receipt is useful
  # downstream for joined curves, but must not gate independent stock K-bars.
  echo "[shioaji-minute-runner] futures_history=independent_downstream_gate"

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
    # The target may advance while sleeping across the close. Never download
    # yesterday's already-complete frontier in the new day's 14:31 slot.
    continue
  fi

  read -r minute_workers fop_workers history_workers quote_clients reserved_fop reserved_history minute_deadline <<< "$(resolve_worker_count)"
  if (( minute_workers < 1 )); then
    capacity_delay=60
    if (( reserved_fop >= 3 )); then capacity_delay="$(connection_retry_seconds)"; fi
    echo "[shioaji-minute-runner] waiting_seconds=$capacity_delay reason=shioaji_connection_capacity fop_workers=$fop_workers reserved_fop=$reserved_fop history_workers=$history_workers reserved_history=$reserved_history reserved_stock_quotes=$quote_clients account_limit=5"
    sleep "$capacity_delay"
    continue
  fi

  echo "[shioaji-minute-runner] download_start=$(TZ=Asia/Taipei date --iso-8601=seconds)"
  echo "[shioaji-minute-runner] connection_plan minute_workers=$minute_workers fop_workers=$fop_workers reserved_fop=$reserved_fop history_workers=$history_workers reserved_history=$reserved_history reserved_stock_quotes=$quote_clients stop_at=$minute_deadline account_limit=5"
  minute_deadline_args=()
  if [[ "$minute_deadline" != - ]]; then
    minute_deadline_args+=(--stop-at "$minute_deadline")
  fi
  # Compare exact symbol IDs, not only the last date or catalog size.
  read -r frontier_ready_before_run source_rows_before source_fingerprint_before <<< "$(
    run_fintech_python -m scripts.shioaji_minute_backfill_state frontier \
      --target-date "$BACKFILL_END_DATE"
  )"
  minute_retry_args=()
  deferred_source_gap_retry=0
  retry_source_gaps="${SHIOAJI_MINUTE_RETRY_SOURCE_GAPS:-auto}"
  if [[ "$retry_source_gaps" == "auto" ]]; then
    # A successful pass is one retry per completed target session, not a
    # permanent opt-out. Newly published sessions can reveal new source gaps.
    if [[ -f "$SOURCE_GAP_RETRY_MARKER" ]] \
      && grep -Fq "target=$BACKFILL_END_DATE" "$SOURCE_GAP_RETRY_MARKER"; then
      retry_source_gaps=0
    else
      retry_source_gaps=1
    fi
    if [[ "$retry_source_gaps" == "1" && "$frontier_ready_before_run" == "0" ]]; then
      # First publish the new completed session for every symbol. Old explicit
      # gaps get their own subsequent pass and cannot starve fresh coverage.
      retry_source_gaps=0
      deferred_source_gap_retry=1
    fi
  fi
  if [[ "$retry_source_gaps" == "1" ]]; then
    minute_retry_args+=(--retry-source-gaps --fallback-missing-kbars-to-ticks)
  fi
  echo "[shioaji-minute-runner] frontier_ready_before_run=$frontier_ready_before_run retry_source_gaps=$retry_source_gaps deferred_source_gap_retry=$deferred_source_gap_retry marker=$SOURCE_GAP_RETRY_MARKER"
  download_started_ns="$(date +%s%N)"
  set +e
  run_fintech_python -m downloader.download_shioaji_tw_minute_kbars \
    --simulation \
    --all-symbols \
    "${minute_retry_args[@]}" \
    "${minute_deadline_args[@]}" \
    --workers "$minute_workers" \
    --requests-per-second "${SHIOAJI_MINUTE_REQUESTS_PER_SECOND:-5}" \
    --max-traffic-fraction "${SHIOAJI_MINUTE_MAX_TRAFFIC_FRACTION:-0.90}" \
    --start-date 2020-03-02 \
    --end-date "$BACKFILL_END_DATE"
  download_rc=$?
  set -e
  echo "[shioaji-minute-runner] download_exit=$download_rc at=$(TZ=Asia/Taipei date --iso-8601=seconds)"

  summary_state="$(run_fintech_python - "$BACKFILL_END_DATE" "$download_started_ns" <<'PY'
import json
import sys
from pathlib import Path

target_end = sys.argv[1]
started_ns = int(sys.argv[2])
summary_root = Path("data_tw_minute/shioaji_1m")
fresh = [
    path for path in (
        summary_root / "download_summary.json",
        summary_root / "latest_run_summary.json",
    )
    if path.is_file() and path.stat().st_mtime_ns >= started_ns
]
if not fresh:
    print("missing")
else:
    path = max(fresh, key=lambda candidate: candidate.stat().st_mtime_ns)
    payload = json.loads(path.read_text(encoding="utf-8"))
    run_payload = payload
    run_selected = int(payload.get("selected_symbols", 0))
    run_reported = int(payload.get("reported_symbols", 0))
    run_failed = int(payload.get("failed_symbols", 0))
    run_partial = int(payload.get("partial_symbols", 0))
    current_run_ready = (
        path.name == "download_summary.json"
        and bool(run_payload.get("resumable_collection_complete"))
        and run_selected > 0
        and run_reported == run_selected
        and run_failed == 0
        and run_partial == 0
        and run_payload.get("end_date") == target_end
        and not bool(run_payload.get("stopped_for_traffic"))
        and not bool(run_payload.get("stopped_for_market_hours"))
        and not bool(run_payload.get("stopped_for_schedule"))
        and not bool(run_payload.get("fatal_error"))
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
        + " schedule_stop=" + str(bool(run_payload.get("stopped_for_schedule"))).lower()
        + " traffic=" + str(run_payload.get("traffic_used_bytes"))
        + "/" + str(run_payload.get("traffic_limit_bytes"))
    )
PY
)"
  echo "[shioaji-minute-runner] summary $summary_state"

  if (( download_rc == 0 )) \
    && [[ "$summary_state" == "ready=true "* ]]; then
    echo "[shioaji-minute-runner] auditing historical source coverage from 2020-03-02"
    if ! run_fintech_python -m scripts.audit_shioaji_minute_history \
      --start-date 2020-03-02 \
      --end-date "$BACKFILL_END_DATE" \
      --require-classified; then
      echo "[shioaji-minute-runner] source_history_audit=incomplete reason=unclassified_or_pending_history"
      sleep 300
      continue
    fi
    # A count-only shortcut is unsafe: a corrected K-bar can retain the same
    # row count.  The reuse gate compares every source chunk's SHA-256 and gap
    # classification with an already audited research manifest instead.
    skip_materialization="$(
      run_fintech_python -m scripts.shioaji_minute_backfill_state reuse \
        --target-date "$BACKFILL_END_DATE" \
        --frontier-was-ready "$frontier_ready_before_run" \
        --source-rows-before "$source_rows_before" \
        --source-fingerprint-before "$source_fingerprint_before"
    )"
    if [[ "$skip_materialization" == "1" ]]; then
      echo "[shioaji-minute-runner] materialization=unchanged_source_rows_and_verified_current_audit skip_rebuild=true"
    else
      echo "[shioaji-minute-runner] building audited available-source research dataset"
      # Run repository-owned programs as modules. Executing a scripts/ file
      # directly drops the repo root from sys.path during this expensive pass.
      run_fintech_python -m scripts.build_shioaji_tw_minute_dataset \
        --calendar-root data_tw_public
      latest_date="$(
        run_fintech_python - <<'PY'
import json
from pathlib import Path
payload = json.loads(Path("data_tw_minute/research_dataset/manifest.json").read_text())
print(payload["dates"][-1])
PY
      )"
      run_fintech_python -m scripts.audit_shioaji_tw_minute_dataset \
        --trade-date "$latest_date"
      run_fintech_python -m scripts.audit_shioaji_tw_minute_dataset \
        --all-partitions \
        --calendar-root data_tw_public \
        --output data_tw_minute/audits/full_latest.json
    fi
    # Code tests run at deployment/CI, not once per unchanged data refresh.
    # Source receipts and the full dataset audit are the runtime gates.
    echo "[shioaji-minute-runner] materializing zero-traffic daily and hybrid datasets"
    run_fintech_python -m downloader.download_shioaji_tw_kbars \
      --local-only \
      --start-date 2020-03-02 \
      --end-date "$BACKFILL_END_DATE"
    run_fintech_python -m scripts.build_tw_shioaji_dataset
    run_fintech_python -m scripts.audit_tw_shioaji_dataset
    echo "[shioaji-minute-runner] research_ready=true contract=audited_available_source source_gaps_are_masked=true"
    echo "[shioaji-minute-runner] daily_ready=true materialization=verified_local_minute api_requests_started=0"
    if [[ "$retry_source_gaps" == "1" ]]; then
      retry_marker_tmp="$SOURCE_GAP_RETRY_MARKER.tmp"
      printf '%s target=%s\n' "$(TZ=Asia/Taipei date --iso-8601=seconds)" "$BACKFILL_END_DATE" > "$retry_marker_tmp"
      mv "$retry_marker_tmp" "$SOURCE_GAP_RETRY_MARKER"
    fi
    echo "[shioaji-minute-runner] collection_completed_at=$(TZ=Asia/Taipei date --iso-8601=seconds)"
    if (( deferred_source_gap_retry == 1 )); then
      gap_symbols="$(run_fintech_python - <<'PY'
import json
from pathlib import Path
payload = json.loads(Path("data_tw_minute/shioaji_1m/download_summary.json").read_text())
print(int(payload.get("complete_with_source_gap_symbols") or 0))
PY
)"
      if (( gap_symbols > 0 )); then
        echo "[shioaji-minute-runner] next_phase=historical_source_gap_retry latest_session_already_audited=true gap_symbols=$gap_symbols"
        continue
      fi
      retry_marker_tmp="$SOURCE_GAP_RETRY_MARKER.tmp"
      printf '%s target=%s no_source_gaps=true\n' "$(TZ=Asia/Taipei date --iso-8601=seconds)" "$BACKFILL_END_DATE" > "$retry_marker_tmp"
      mv "$retry_marker_tmp" "$SOURCE_GAP_RETRY_MARKER"
    fi
    exit 0
  fi

  stop_reason="$(latest_run_stop_reason)"
  if [[ "$summary_state" == *"market_stop=true"* || "$stop_reason" == market ]]; then
    next_delay="$(seconds_until_after_close)"
    if (( next_delay <= 0 )); then
      next_delay=300
    fi
    echo "[shioaji-minute-runner] waiting_seconds=$next_delay reason=market_hours"
  elif [[ "$stop_reason" == traffic ]]; then
    next_delay="$(seconds_until_next_quota_window)"
    echo "[shioaji-minute-runner] waiting_seconds=$next_delay reason=observed_traffic_ceiling"
  elif [[ "$stop_reason" == schedule ]]; then
    echo "[shioaji-minute-runner] reason=scheduled_pre_night_yield rechecking_connection_plan=true"
    continue
  elif [[ "$minute_deadline" != - ]] && (( $(date +%s) < $(date -d "$minute_deadline" +%s) )); then
    # Transient login or source failures should not throw away the remaining
    # short post-close window. The downloader still enforces the hard stop.
    next_delay=60
    echo "[shioaji-minute-runner] waiting_seconds=$next_delay reason=pre_night_frontier_retry"
  else
    next_delay=300
    echo "[shioaji-minute-runner] waiting_seconds=$next_delay reason=retry_incomplete_run"
  fi
  sleep "$next_delay"
done
