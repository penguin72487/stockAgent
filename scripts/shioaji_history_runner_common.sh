#!/usr/bin/env bash
# Shared wait receipts and catalog publication for the existing history runners.
history_wait() {
  local delay="$1" reason="$2"
  run_fintech_python - "$run_root/scheduler.json" "$delay" "$reason" <<'PY'
from pathlib import Path
import sys
from downloader.shioaji_history_repair import record_schedule
record_schedule(Path(sys.argv[1]), reason=sys.argv[3], seconds=int(sys.argv[2]))
PY
  echo "[shioaji-history-runner] waiting_seconds=$delay reason=$reason"
  if (( delay > 0 )); then
    # The official futures calendar can gain today's session while this
    # runner is idle. Poll only its file signature, not the full 770-alias
    # inventory, and resume within a minute of that dependency changing.
    if [[ "$reason" == next_incremental_sweep && -n "${calendar:-}" ]]; then
      local initial_signature remaining interval current_signature
      initial_signature="$(stat -Lc '%Y:%s' "$calendar" 2>/dev/null || true)"
      remaining="$delay"
      while (( remaining > 0 )); do
        interval=$(( remaining < 60 ? remaining : 60 ))
        sleep "$interval"
        remaining=$(( remaining - interval ))
        current_signature="$(stat -Lc '%Y:%s' "$calendar" 2>/dev/null || true)"
        if [[ "$current_signature" != "$initial_signature" ]]; then
          echo '[shioaji-history-runner] official_calendar_changed=true resuming_now'
          break
        fi
      done
    else
      sleep "$delay"
    fi
  fi
}

history_protected_delay() {
  run_fintech_python - <<'PY'
from stockagent.live.shioaji_schedule import historical_query_pause_seconds
print(historical_query_pause_seconds())
PY
}

history_quota_delay() {
  run_fintech_python - <<'PY'
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
now = datetime.now(ZoneInfo('Asia/Taipei'))
if now.weekday() >= 5:
    print(300)
else:
    day = now.date()
    target = datetime.combine(day, time(14, 31), tzinfo=now.tzinfo)
    if now >= target:
        day += timedelta(days=1)
        while day.weekday() >= 5:
            day += timedelta(days=1)
        target = datetime.combine(day, time(14, 31), tzinfo=now.tzinfo)
    print(max(60, int((target-now).total_seconds())))
PY
}

history_publish() {
  local receipt="$1" kind="$2"
  shift 2
  if ! run_fintech_python - "$receipt" "$kind" <<'PY'
from pathlib import Path
import json, sys
from downloader.shioaji_history_repair import publication_ready
payload = json.loads(Path(sys.argv[1]).read_text())
raise SystemExit(0 if publication_ready(payload, continuous=sys.argv[2]=='continuous') else 1)
PY
  then
    echo '[shioaji-history-runner] publication=deferred reason=source_coverage_gaps'
    return 0
  fi
  local dataset
  for dataset in "$@"; do
    if ! bash scripts/run_data_release.sh publish "$dataset" --sync-root "${STOCKAGENT_PACKED_ROOT:-/srv/stockagent-packed}"; then
      echo "[shioaji-history-runner] publication=deferred dataset=$dataset reason=catalog_gate_or_publication_failure"
    fi
  done
}
