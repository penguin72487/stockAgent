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
finlab_policy_settings="$(run_fintech_python "$finlab_repo_root/scripts/finlab_refresh_policy.py")"
finlab_policy_fields="$(run_fintech_python -c \
  'import json,sys; p=json.loads(sys.argv[1]); print(p["minute_partitions_per_pass"], p["maximum_intraday_passes"], int(p["catalog_current"]), p["history_floor_mb"], p["general_batch_size"], p["processing_threads"])' \
  "$finlab_policy_settings")"
read -r finlab_minute_default_limit finlab_max_intraday_passes finlab_catalog_current finlab_default_history_floor finlab_general_default_limit finlab_processing_threads <<< "$finlab_policy_fields"
# Parallelism belongs to stock/range workers, not workers × all host cores.
# These variables affect this collector's children only, never other services.
export POLARS_MAX_THREADS="$finlab_processing_threads"
export OMP_NUM_THREADS="$finlab_processing_threads"
export OPENBLAS_NUM_THREADS="$finlab_processing_threads"
export MKL_NUM_THREADS="$finlab_processing_threads"
export NUMEXPR_NUM_THREADS="$finlab_processing_threads"
finlab_general_batch_size="${FINLAB_SYNC_BATCH_SIZE:-$finlab_general_default_limit}"
if ! [[ "$finlab_general_batch_size" =~ ^[0-9]+$ ]] || ! (( finlab_general_batch_size >= 1 && finlab_general_batch_size <= 16 )); then
  echo "[finlab] FINLAB_SYNC_BATCH_SIZE must be 1..16" >&2
  exit 2
fi

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

# Reuse a validated recent catalog across short preempted passes. Catalog
# membership is not entitlement/download evidence; refresh at least hourly.
if [[ "$finlab_catalog_current" != "1" ]]; then
  run_fintech_python "$finlab_repo_root/scripts/download_finlab_history.py" discover
fi

# Reuse one SDK process for a bounded group; release in-memory frames after
# each key, never clear FileStorage's source files. Per-key deadlines and the
# outer group watchdog retain timeout recovery without thousands of imports,
# full-catalog scans and repeated metadata calls on short incremental checks.
# The SDK owns its delta handling and authoritative account quota.
# Verified SDK incremental checks ignore FINLAB_SYNC_MIN_QUOTA_MB. That local
# reserve applies only to new history / whole-object streaming; quota-deferred
# prefixes are scanned past before the per-process actual-attempt limit.
# A single oversized key is already recorded and cooled down by sync_selection.
# Stop only after consecutive SDK timeouts: an intervening completed SDK call
# proves the worker can still advance to later catalog keys.
finlab_consecutive_timeouts=0
finlab_state="not_run"
if (( finlab_preopen )); then
  finlab_general_batch_size=1  # The original full-key opening runway stays binding.
fi
finlab_group_timeout_seconds=$((finlab_key_timeout_seconds * finlab_general_batch_size))
for ((finlab_batch = 1; finlab_batch <= ${FINLAB_SYNC_MAX_BATCHES:-500}; finlab_batch++)); do
  if (( finlab_preopen )); then
    finlab_runway_seconds=$((finlab_preopen_deadline - $(date +%s)))
    if (( finlab_runway_seconds <= finlab_key_timeout_seconds + 30 )); then
      echo "[finlab] pre-open runway ended; next sweep begins at 09:10 Asia/Taipei"
      break
    fi
  fi
  finlab_attempt_id="${BASHPID}-$(date +%s%N)"
  if timeout --signal=TERM --kill-after=30s "${finlab_group_timeout_seconds}s" \
    "$finlab_python_bin" "$finlab_repo_root/scripts/download_finlab_history.py" sync \
      --attempt-id "$finlab_attempt_id" \
      --limit "$finlab_general_batch_size" \
      --key-timeout-seconds "$finlab_key_timeout_seconds" \
      --maximum-consecutive-timeouts "$finlab_max_timeouts" \
      --refresh-days "${FINLAB_SYNC_REFRESH_DAYS:-1}" \
      --min-quota-remaining-mb "${FINLAB_SYNC_MIN_QUOTA_MB:-$finlab_default_history_floor}"; then
    finlab_consecutive_timeouts="$(run_fintech_python -c 'import json; from pathlib import Path; print(json.loads(Path("data_finlab/runs/latest.json").read_text()).get("consecutive_timeouts",0))')"
  else
    finlab_sync_exit=$?
    if [[ "$finlab_sync_exit" != "124" ]]; then
      echo "[finlab] sync exited unexpectedly code=$finlab_sync_exit" >&2
      exit "$finlab_sync_exit"
    fi
    run_fintech_python "$finlab_repo_root/scripts/download_finlab_history.py" record-timeout \
      --attempt-id "$finlab_attempt_id" \
      --timeout-seconds "$finlab_group_timeout_seconds"
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

# Minutes precede Tick. Both use the same account owner, quota and receipts;
# previously acquired Tick partitions supply local minutes without an API call.
# Tick is the lowest-priority consumer of this FinLab account quota.  It may
# use only residual capacity after the general history sweep and its private
# research publication have had their turn.  A fresh local selection recheck
# closes the gap between the last sync receipt and this request boundary.
# Minutes reserve 50 MB independently of Tick's future-work reserve. During
# the configured last hour before reset, both local floors become zero;
# official quota and due general work still own admission on every boundary.
if [[ "$finlab_state" == "pass_complete" ]]; then
  if finlab_general_status="$(
    run_fintech_python "$finlab_repo_root/scripts/download_finlab_history.py" pending \
      --refresh-days "${FINLAB_SYNC_REFRESH_DAYS:-1}"
  )"; then
    finlab_tick_allowed="$(run_fintech_python -c \
      'import json,sys; p=json.loads(sys.argv[1]); print(str(p.get("minute_residual_allowed", p.get("supplemental_allowed", False))).lower())' \
      "$finlab_general_status")"
    if [[ "$finlab_tick_allowed" == "true" ]]; then
      finlab_tick_reserve_mb="$(run_fintech_python -c \
        'import json,os,sys; from dotenv import dotenv_values; status=json.loads(sys.argv[1]); configured=os.environ.get("FINLAB_TICK_QUOTA_RESERVE_MB") or dotenv_values(".env").get("FINLAB_TICK_QUOTA_RESERVE_MB"); print(max(float(status.get("scheduled_reserve_mb",500)), float(configured) if configured else 50))' \
        "$finlab_general_status")"
      finlab_minute_succeeded=0
      finlab_minute_cleared=false
      for ((finlab_intraday_pass=1; finlab_intraday_pass<=finlab_max_intraday_passes; finlab_intraday_pass++)); do
        if timeout --signal=TERM --kill-after=10s 1200s \
          "$finlab_python_bin" "$finlab_repo_root/scripts/download_finlab_market_intraday.py" \
            --kind tw_minute \
            --limit "${FINLAB_MINUTE_PARTITIONS_PER_RUN:-$finlab_minute_default_limit}" \
            --sync-lock-fd 9 \
            --max-run-seconds 1140; then
          finlab_minute_succeeded=1
        else
          finlab_minute_succeeded=0
          echo "[finlab] minute pass incomplete; keeping its cursor for recovery; Tick deferred" >&2
          break
        fi
        finlab_minute_result="$(run_fintech_python -c \
          'import json; from pathlib import Path; p=json.loads(Path("data_finlab/intraday/market_status.json").read_text()); priority=p.get("minute_priority",{}); print(p.get("state", "unverified").split(";", 1)[0], str(p.get("active_kind")=="tw_minute" and priority.get("tick_residual_allowed") is True and p.get("state") not in {"running","interrupted","quota_unknown","authentication_failed","waiting_required_general_data","disk_reserve_reached","quota_margin_reached","quota_exhausted","opening_window_yield"}).lower(), str(priority.get("actionable_known_window_partitions", priority.get("pending_known_window_partitions",0))>0 or priority.get("due_publication_rechecks",0)>0).lower())')"
        read -r finlab_minute_state finlab_minute_cleared finlab_minute_actionable <<< "$finlab_minute_result"
        if [[ "$finlab_minute_cleared" == "true" ]]; then
          break
        fi
        if [[ "$finlab_minute_state" != "batch_limit" && "$finlab_minute_state" != "run_time_budget_reached" ]] &&
           ! [[ "$finlab_minute_state" == "waiting_older_publication" && "$finlab_minute_actionable" == "true" ]]; then
          break  # Due general work, real quota/source/disk waits must yield.
        fi
        echo "[finlab] continuing minute pass under the same account owner ($finlab_intraday_pass/$finlab_max_intraday_passes)"
      done
      if [[ "$finlab_minute_succeeded" == "1" && "$finlab_minute_cleared" == "true" ]]; then
        if ! timeout --signal=TERM --kill-after=10s 1200s \
            "$finlab_python_bin" "$finlab_repo_root/scripts/download_finlab_market_intraday.py" \
              --kind tw_tick --limit "${FINLAB_INTRADAY_PARTITIONS_PER_RUN:-256}" \
              --sync-lock-fd 9 --max-run-seconds 1140 --reserve-mb "$finlab_tick_reserve_mb"; then
          echo "[finlab] residual Tick pass incomplete; next invocation starts with minutes" >&2
        fi
      else
        echo "[finlab] minute history / publication rechecks have priority; Tick deferred"
      fi
    else
        echo "[finlab] minutes and Tick deferred: actionable general acquisition remains"
    fi
  else
    echo "[finlab] Tick deferred: general-work inventory is unverified" >&2
  fi
else
  echo "[finlab] Tick deferred: general history sweep state=$finlab_state"
fi
