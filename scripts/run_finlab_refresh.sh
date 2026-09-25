#!/usr/bin/env bash
set -euo pipefail

finlab_repo_root="${FINLAB_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$finlab_repo_root"
source "$finlab_repo_root/scripts/runtime_env.sh"
finlab_python_bin="$(resolve_fintech_python)"
"$finlab_python_bin" -c '
from importlib.metadata import version
from packaging.version import Version
if Version(version("finlab")) < Version("2.1.1"):
    raise SystemExit("[finlab] SDK 2.0.22/2.0.23 were withdrawn; install requirements-finlab.txt before refreshing")
'
finlab_key_timeout_seconds="${FINLAB_SYNC_KEY_TIMEOUT_SECONDS:-600}"
finlab_max_timeouts="${FINLAB_SYNC_MAX_TIMEOUTS:-2}"
if ! [[ "$finlab_key_timeout_seconds" =~ ^[0-9]+$ ]] ||
   ! (( finlab_key_timeout_seconds >= 1 && finlab_key_timeout_seconds <= 3600 )); then
  echo "[finlab] FINLAB_SYNC_KEY_TIMEOUT_SECONDS must be 1..3600" >&2
  exit 2
fi
if ! [[ "$finlab_max_timeouts" =~ ^[0-9]+$ ]] || ! (( finlab_max_timeouts >= 1 )); then
  echo "[finlab] FINLAB_SYNC_MAX_TIMEOUTS must be positive" >&2
  exit 2
fi
command -v timeout >/dev/null || { echo "[finlab] GNU timeout is required" >&2; exit 2; }

# The quota resets at 08:00 Taipei time, but live Taiwan opening has a
# protected 08:20–09:10 resource window.  The first sweep only starts keys
# whose full configured timeout plus termination grace fits before 08:18.
finlab_preopen=0
finlab_preopen_deadline=0
finlab_reset_launch=0
finlab_local_clock="$(TZ=Asia/Taipei date +%H%M%S)"
if [[ "$finlab_local_clock" > "075959" && "$finlab_local_clock" < "082000" ]]; then
  finlab_reset_launch=1
  if "$finlab_python_bin" "$finlab_repo_root/scripts/check_outside_tw_opening_resource_window.py" \
      --official-calendar-root "$finlab_repo_root/data_tw_public" --session-status >/dev/null; then
    finlab_preopen=1
    finlab_preopen_deadline="$(TZ=Asia/Taipei date -d "$(TZ=Asia/Taipei date +%F) 08:18:00" +%s)"
  fi
fi

mkdir -p "$finlab_repo_root/data_finlab"
exec 9>"$finlab_repo_root/data_finlab/.sync.lock"
if ! flock -n 9; then
  echo "[finlab] another local sync is active; skipping this timer run"
  exit 0
fi

if (( finlab_preopen )) && (( $(date +%s) >= finlab_preopen_deadline )); then
  echo "[finlab] opening protection is too close; waiting for the 09:10 resume"
  exit 0
fi

if (( finlab_reset_launch )); then
  if ! timeout --signal=TERM --kill-after=10s 140s \
    "$finlab_python_bin" "$finlab_repo_root/scripts/snapshot_finlab_quota.py" \
      --confirm-daily-reset --wait-seconds 120; then
    echo "[finlab] 08:00 reset not yet confirmed by account quota; 09:10 resume remains scheduled"
    exit 0
  fi
fi

if (( finlab_preopen )) && (( $(date +%s) >= finlab_preopen_deadline )); then
  echo "[finlab] quota confirmation exhausted the pre-open runway; waiting for 09:10"
  exit 0
fi

# Refresh the denominator once per invocation. Catalog membership is not an
# entitlement or download receipt, but must not remain frozen after SDK updates.
run_fintech_python "$finlab_repo_root/scripts/download_finlab_history.py" discover

# Isolate every key in a fresh SDK process. Several wide FinLab tables retained
# gigabytes in the SDK cache when ten keys shared a process; this keeps peak
# memory independent of the number of catalog keys attempted that day.
# The SDK owns its delta handling and authoritative account quota.
# A single oversized key is already recorded and cooled down by sync_selection.
# Stop only after consecutive SDK timeouts: an intervening completed SDK call
# proves the worker can still advance to later catalog keys.
finlab_consecutive_timeouts=0
finlab_state="not_run"
for ((finlab_batch = 1; finlab_batch <= ${FINLAB_SYNC_MAX_BATCHES:-500}; finlab_batch++)); do
  if (( finlab_preopen )); then
    finlab_runway_seconds=$((finlab_preopen_deadline - $(date +%s)))
    if (( finlab_runway_seconds <= finlab_key_timeout_seconds + 30 )); then
      echo "[finlab] pre-open runway ended; next sweep begins at 09:10 Asia/Taipei"
      break
    fi
  fi
  finlab_attempt_id="${BASHPID}-$(date +%s%N)"
  if timeout --signal=TERM --kill-after=30s "${finlab_key_timeout_seconds}s" \
    "$finlab_python_bin" "$finlab_repo_root/scripts/download_finlab_history.py" sync \
      --attempt-id "$finlab_attempt_id" \
      --limit "${FINLAB_SYNC_BATCH_SIZE:-1}" \
      --refresh-days "${FINLAB_SYNC_REFRESH_DAYS:-1}" \
      --min-quota-remaining-mb "${FINLAB_SYNC_MIN_QUOTA_MB:-50}"; then
    finlab_consecutive_timeouts=0
  else
    finlab_sync_exit=$?
    if [[ "$finlab_sync_exit" != "124" ]]; then
      echo "[finlab] sync exited unexpectedly code=$finlab_sync_exit" >&2
      exit "$finlab_sync_exit"
    fi
    run_fintech_python "$finlab_repo_root/scripts/download_finlab_history.py" record-timeout \
      --attempt-id "$finlab_attempt_id" \
      --timeout-seconds "$finlab_key_timeout_seconds"
    finlab_consecutive_timeouts=$((finlab_consecutive_timeouts + 1))
  fi
  finlab_state="$(run_fintech_python -c 'import json; from pathlib import Path; print(json.loads(Path("data_finlab/runs/latest.json").read_text())["state"])')"
  if [[ "$finlab_state" != "partial" ]]; then
    break
  fi
  if (( finlab_consecutive_timeouts >= finlab_max_timeouts )); then
    echo "[finlab] consecutive SDK timeouts reached count=$finlab_consecutive_timeouts; remaining datasets stay partial"
    break
  fi
done

if (( finlab_preopen )); then
  echo "[finlab] pre-open acquisition complete; heavy research build deferred until 09:10"
  exit 0
fi

# Only build/package the derived research ABI after a complete current sweep.
# This avoids repeated ~million-row table work during a long quota backfill.
if [[ -f "$finlab_repo_root/artifacts/research_features/tw_public_research_all_2014_v3.parquet" ]]; then
  if run_fintech_python "$finlab_repo_root/scripts/finlab_release_gate.py"; then
    run_fintech_python "$finlab_repo_root/scripts/build_finlab_research_overlay.py"
    # Stage rechecks every source SHA-256; catalog publication requires the
    # resulting freshness receipt. Raw data_finlab stays publish:false.
    if [[ "${FINLAB_PRIVATE_RESEARCH_PUBLISH:-1}" == "1" ]]; then
      finlab_stage_result="$(run_fintech_python "$finlab_repo_root/scripts/stage_tw_public_finlab_research_release.py")"
      echo "$finlab_stage_result"
      finlab_should_publish=1
      if [[ "$finlab_stage_result" == *'"event": "finlab_research_stage_unchanged"'* ]]; then
        finlab_publish_ready="$(stockagent-data publish-status tw-public-research-finlab-2014-v4 | run_fintech_python -c 'import json,sys; rows=json.load(sys.stdin)["datasets"]; print(str(rows[0]["publish_ready"]).lower())')"
        if [[ "$finlab_publish_ready" != "true" ]]; then
          echo "[finlab] exact source content unchanged; old staged receipt not republished"
          finlab_should_publish=0
        else
          echo "[finlab] exact source content unchanged; publisher will reuse the identical release"
        fi
      else
        echo "[finlab] current source content changed; publishing audited private release"
      fi
      if [[ "$finlab_should_publish" == "1" ]]; then
        stockagent-data publish tw-public-research-finlab-2014-v4
        run_fintech_python "$finlab_repo_root/scripts/audit_finlab_research_cold.py"
      fi
    fi
  else
    echo "[finlab] catalog not current; no derived rebuild, private package, or cold publication"
  fi
else
  echo "[finlab] research base is absent; raw acquisition succeeded, overlay deferred"
fi

# Tick is the lowest-priority consumer of this FinLab account quota.  It may
# use only residual capacity after the general history sweep and its private
# research publication have had their turn.  A fresh local selection recheck
# closes the gap between the last sync receipt and this request boundary.
# The larger Tick-only reserve protects later high-priority revisions today;
# the general downloader keeps its independent 50 MB reserve.
if [[ "$finlab_state" == "pass_complete" ]]; then
  if finlab_general_status="$(
    run_fintech_python "$finlab_repo_root/scripts/download_finlab_history.py" pending \
      --refresh-days "${FINLAB_SYNC_REFRESH_DAYS:-1}"
  )"; then
    finlab_actionable_pending="$(run_fintech_python -c \
      'import json,sys; print(json.loads(sys.argv[1])["actionable_pending"])' \
      "$finlab_general_status")"
    if [[ "$finlab_actionable_pending" == "0" ]]; then
      finlab_tick_reserve_mb="$(run_fintech_python -c \
        'import os; from dotenv import dotenv_values; print(os.environ.get("FINLAB_TICK_QUOTA_RESERVE_MB") or dotenv_values(".env").get("FINLAB_TICK_QUOTA_RESERVE_MB") or "500")')"
      if ! timeout --signal=TERM --kill-after=10s 1200s \
          "$finlab_python_bin" "$finlab_repo_root/scripts/download_finlab_market_intraday.py" \
            --limit "${FINLAB_INTRADAY_PARTITIONS_PER_RUN:-256}" \
            --reserve-mb "$finlab_tick_reserve_mb"; then
        echo "[finlab] residual-quota Tick pass incomplete; general history remains higher priority" >&2
      fi
    else
      echo "[finlab] Tick deferred: $finlab_actionable_pending actionable general history keys remain"
    fi
  else
    echo "[finlab] Tick deferred: general-work inventory is unverified" >&2
  fi
else
  echo "[finlab] Tick deferred: general history sweep state=$finlab_state"
fi
