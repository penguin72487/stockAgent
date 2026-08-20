#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

output_dir="${OPENBB_OUTPUT_DIR:-data_openBB}"
state_dir="$output_dir/_state"
log_dir="$output_dir/logs"
monitor_interval="${OPENBB_MONITOR_INTERVAL_SECONDS:-60}"
full_monitor_interval="${OPENBB_FULL_MONITOR_INTERVAL_SECONDS:-900}"
idle_full_monitor_interval="${OPENBB_IDLE_FULL_MONITOR_INTERVAL_SECONDS:-21600}"
full_monitor_timeout="${OPENBB_FULL_MONITOR_TIMEOUT_SECONDS:-600}"
restart_delay="${OPENBB_RESTART_DELAY_SECONDS:-60}"
stall_timeout="${OPENBB_STALL_TIMEOUT_SECONDS:-3600}"
terminate_grace="${OPENBB_TERMINATE_GRACE_SECONDS:-30}"
max_downloader_fds="${OPENBB_MAX_DOWNLOADER_FDS:-4096}"
max_downloader_rss_bytes="${OPENBB_MAX_DOWNLOADER_RSS_BYTES:-17179869184}"
min_free_bytes="${OPENBB_MIN_FREE_BYTES:-107374182400}"
download_batch_size="${OPENBB_DOWNLOAD_BATCH_SIZE:-1792}"
max_log_bytes="${OPENBB_SUPERVISOR_LOG_MAX_BYTES:-268435456}"
if [[ ! "$monitor_interval" =~ ^[1-9][0-9]*$ ]]; then
  echo "[openbb-supervisor] OPENBB_MONITOR_INTERVAL_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$full_monitor_interval" =~ ^[1-9][0-9]*$ ]]; then
  echo "[openbb-supervisor] OPENBB_FULL_MONITOR_INTERVAL_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$idle_full_monitor_interval" =~ ^[1-9][0-9]*$ ]]; then
  echo "[openbb-supervisor] OPENBB_IDLE_FULL_MONITOR_INTERVAL_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$full_monitor_timeout" =~ ^[1-9][0-9]*$ ]]; then
  echo "[openbb-supervisor] OPENBB_FULL_MONITOR_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$restart_delay" =~ ^[1-9][0-9]*$ ]]; then
  echo "[openbb-supervisor] OPENBB_RESTART_DELAY_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$stall_timeout" =~ ^[1-9][0-9]*$ ]]; then
  echo "[openbb-supervisor] OPENBB_STALL_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$terminate_grace" =~ ^[1-9][0-9]*$ ]]; then
  echo "[openbb-supervisor] OPENBB_TERMINATE_GRACE_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$max_downloader_fds" =~ ^[0-9]+$ ]]; then
  echo "[openbb-supervisor] OPENBB_MAX_DOWNLOADER_FDS must be a non-negative integer" >&2
  exit 2
fi
if [[ ! "$max_downloader_rss_bytes" =~ ^[0-9]+$ ]]; then
  echo "[openbb-supervisor] OPENBB_MAX_DOWNLOADER_RSS_BYTES must be a non-negative integer" >&2
  exit 2
fi
if [[ ! "$min_free_bytes" =~ ^[0-9]+$ ]]; then
  echo "[openbb-supervisor] OPENBB_MIN_FREE_BYTES must be a non-negative integer" >&2
  exit 2
fi
if [[ ! "$download_batch_size" =~ ^[1-9][0-9]*$ ]]; then
  echo "[openbb-supervisor] OPENBB_DOWNLOAD_BATCH_SIZE must be a positive integer" >&2
  exit 2
fi
if [[ ! "$max_log_bytes" =~ ^[0-9]+$ ]]; then
  echo "[openbb-supervisor] OPENBB_SUPERVISOR_LOG_MAX_BYTES must be a non-negative integer" >&2
  exit 2
fi
mkdir -p "$state_dir" "$log_dir"
if ! command -v timeout >/dev/null 2>&1; then
  echo "[openbb-supervisor] GNU timeout is required for bounded monitor scans" >&2
  exit 2
fi
source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)" || {
  echo "[openbb-supervisor] unable to resolve the fintech Python runtime" >&2
  exit 2
}

# A multi-week archive must not silently move its right boundary every time
# the supervisor restarts after midnight.  Pin the first run date for this
# output directory. A different boundary belongs to a different output
# directory, so an accidental environment override cannot change plan identity.
archive_end_date_path="$state_dir/archive_end_date.txt"
requested_archive_end_date="${OPENBB_ARCHIVE_END_DATE:-}"
pinned_archive_end_date=""
if [[ -s "$archive_end_date_path" ]]; then
  pinned_archive_end_date="$(tr -d '[:space:]' <"$archive_end_date_path")"
fi
if [[ -n "$pinned_archive_end_date" ]]; then
  if [[ -n "$requested_archive_end_date" \
    && "$requested_archive_end_date" != "$pinned_archive_end_date" ]]; then
    echo "[openbb-supervisor] archive end date is already pinned to $pinned_archive_end_date for $output_dir; use a different output directory for $requested_archive_end_date" >&2
    exit 2
  fi
  archive_end_date="$pinned_archive_end_date"
else
  archive_end_date="${requested_archive_end_date:-$(date +%F)}"
fi
if [[ ! "$archive_end_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] \
  || [[ "$(date -d "$archive_end_date" +%F 2>/dev/null || true)" != "$archive_end_date" ]]; then
  echo "[openbb-supervisor] invalid archive end date: $archive_end_date" >&2
  exit 2
fi
archive_end_date_tmp="$archive_end_date_path.tmp.$$"
printf '%s\n' "$archive_end_date" >"$archive_end_date_tmp"
mv -f "$archive_end_date_tmp" "$archive_end_date_path"

log_path="$log_dir/supervisor.log"
exec > >(tee -a "$log_path") 2>&1

# Open the lock only after creating the tee process. Otherwise tee inherits the
# locked descriptor and can keep the archive permanently locked after the
# supervisor exits while a background monitor sleep still holds its pipe open.
exec 9>"$state_dir/supervisor.lock"
if ! flock -n 9; then
  echo "[openbb-supervisor] another supervisor already holds $state_dir/supervisor.lock" >&2
  exit 3
fi
echo "$$" >"$state_dir/supervisor.pid"
echo "[openbb-supervisor] pinned archive_end_date=$archive_end_date state=$archive_end_date_path"
echo "[openbb-supervisor] watchdog_interval=${monitor_interval}s full_monitor_interval=${full_monitor_interval}s idle_full_monitor_interval=${idle_full_monitor_interval}s full_monitor_timeout=${full_monitor_timeout}s stall_timeout=${stall_timeout}s terminate_grace=${terminate_grace}s min_free_bytes=$min_free_bytes"

rotate_log_if_needed() {
  ((max_log_bytes > 0)) || return 0
  local size
  size="$(stat -c '%s' "$log_path" 2>/dev/null || echo 0)"
  ((size >= max_log_bytes)) || return 0
  if cp -f "$log_path" "$log_path.1.tmp"; then
    mv -f "$log_path.1.tmp" "$log_path.1"
    # `tee -a` holds the original inode with O_APPEND, so truncation keeps the
    # live pipe attached and subsequent writes resume at the new end-of-file.
    : >"$log_path"
    echo "[openbb-supervisor] rotated log at ${size} bytes; backup=$log_path.1"
  else
    rm -f "$log_path.1.tmp"
    echo "[openbb-supervisor] unable to rotate $log_path" >&2
  fi
}

process_tree_rss_kib() {
  local root_pid="$1"
  ps -eo pid=,ppid=,rss= 2>/dev/null \
    | awk -v root="$root_pid" '
      { pid[NR]=$1; parent[$1]=$2; rss[$1]=$3 }
      END {
        total=0
        for (i=1; i<=NR; i++) {
          current=pid[i]
          depth=0
          while (current > 1 && depth < 128) {
            if (current == root) {
              total += rss[pid[i]]
              break
            }
            current=parent[current]
            depth++
          }
        }
        printf "%.0f\n", total
      }
    '
}

monitor_pid=""
run_snapshot_monitor() {
  timeout --signal=TERM --kill-after=30s "${full_monitor_timeout}s" \
    "$python_bin" scripts/monitor_openbb_archive.py \
    --output-dir "$output_dir" --write-snapshot --append-history --no-progress &
  monitor_pid=$!
  local monitor_rc=0
  wait "$monitor_pid" || monitor_rc=$?
  monitor_pid=""
  return "$monitor_rc"
}

refresh_watchdog_state() {
  local -a fields=()
  mapfile -t fields < <(
    "$python_bin" scripts/openbb_archive_watchdog.py \
      --state-dir "$state_dir" --shell-fields
  )
  scheduler_phase="${fields[0]:-missing}"
  scheduler_attempted="${fields[1]:-0}"
  scheduler_active="${fields[2]:-0}"
  scheduler_completed="${fields[3]:-0}"
  scheduler_backpressure="${fields[4]:-0}"
  scheduler_updated="${fields[5]:--}"
  has_cooldown="${fields[6]:-0}"
  pending_eligible="${fields[7]:-1}"
  running_tasks="${fields[8]:-0}"
  current_manifest_task_update="${fields[9]:--}"
  next_cooldown_until_epoch="${fields[11]:-0}"
  scheduler_wait_reason="${fields[12]:--}"
  scheduler_wait_until_epoch="${fields[13]:-0}"
}

download_pid=""
sleep_pid=""
terminate_downloader() {
  local pid="${1:-}"
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 0
  kill -0 "$pid" 2>/dev/null || return 0
  kill -TERM "$pid" 2>/dev/null || true
  local deadline=$((SECONDS + terminate_grace))
  while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "[openbb-supervisor] downloader PID $pid ignored TERM for ${terminate_grace}s; sending KILL" >&2
    kill -KILL "$pid" 2>/dev/null || true
  fi
}

cleanup() {
  if [[ -n "$sleep_pid" ]] && kill -0 "$sleep_pid" 2>/dev/null; then
    kill -TERM "$sleep_pid" 2>/dev/null || true
    wait "$sleep_pid" 2>/dev/null || true
  fi
  if [[ -n "$monitor_pid" ]] && kill -0 "$monitor_pid" 2>/dev/null; then
    pkill -TERM -P "$monitor_pid" 2>/dev/null || true
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
  fi
  if [[ -n "$download_pid" ]] && kill -0 "$download_pid" 2>/dev/null; then
    terminate_downloader "$download_pid"
    wait "$download_pid" 2>/dev/null || true
  fi
  rm -f "$state_dir/downloader.pid" "$state_dir/supervisor.pid"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

round=0
download_progress_args=(--no-progress)
if [[ "${OPENBB_SUPERVISOR_TQDM:-0}" == "1" ]]; then
  download_progress_args=()
fi
while true; do
  round=$((round + 1))
  echo "[openbb-supervisor] round=$round start=$(date --iso-8601=seconds)"
  set +e
  # User arguments come first; supervisor-owned routing and date boundaries
  # are appended last so a stray duplicate cannot make the monitor watch a
  # different output directory or plan than the downloader actually uses.
  ./scripts/run_openbb_archive_download.sh \
    "$@" \
    --output-dir "$output_dir" \
    --end-date "$archive_end_date" \
    --resume-existing-plan \
    --batch-size "$download_batch_size" \
    "${download_progress_args[@]}" &
  download_pid=$!
  echo "$download_pid" >"$state_dir/downloader.pid"
  last_progress_marker=""
  last_manifest_task_update=""
  last_progress_epoch="$(date +%s)"
  # Reusing a fresh durable snapshot prevents every harmless supervisor
  # recycle from immediately launching another multi-million-row audit.
  last_full_monitor_epoch="$(
    stat -c '%Y' "$state_dir/monitor_latest.json" 2>/dev/null || echo 0
  )"
  has_cooldown=0
  pending_eligible=1
  running_tasks=0
  stop_reason=""
  while kill -0 "$download_pid" 2>/dev/null; do
    sleep "$monitor_interval" &
    sleep_pid=$!
    wait "$sleep_pid" 2>/dev/null || true
    sleep_pid=""
    if kill -0 "$download_pid" 2>/dev/null; then
      rotate_log_if_needed
      if ((min_free_bytes > 0)); then
        free_bytes="$(
          df --output=avail -B1 "$output_dir" 2>/dev/null \
            | tail -n 1 \
            | tr -d '[:space:]'
        )"
        if [[ "$free_bytes" =~ ^[0-9]+$ ]] && ((free_bytes < min_free_bytes)); then
          stop_reason="disk free bytes $free_bytes below safety floor $min_free_bytes"
          echo "[openbb-supervisor] $stop_reason; stopping without restart" >&2
          terminate_downloader "$download_pid"
          break
        fi
      fi
      if ((max_downloader_fds > 0)) && [[ -d "/proc/$download_pid/fd" ]]; then
        fd_count="$(
          find "/proc/$download_pid/fd" -mindepth 1 -maxdepth 1 2>/dev/null \
            | wc -l \
            || true
        )"
        if ((fd_count >= max_downloader_fds)); then
          echo "[openbb-supervisor] downloader fd_count=$fd_count reached limit=$max_downloader_fds; restarting to release provider sockets" >&2
          terminate_downloader "$download_pid"
          break
        fi
      fi
      if ((max_downloader_rss_bytes > 0)) && [[ -r "/proc/$download_pid/status" ]]; then
        # SEC projections run in spawned processes to escape the Python GIL.
        # Guard the complete downloader process tree, not only its parent RSS.
        rss_kib="$(process_tree_rss_kib "$download_pid")"
        rss_kib="${rss_kib:-0}"
        if [[ "$rss_kib" =~ ^[0-9]+$ ]]; then
          rss_bytes=$((rss_kib * 1024))
          if ((rss_bytes >= max_downloader_rss_bytes)); then
            echo "[openbb-supervisor] downloader process_tree_rss_bytes=$rss_bytes reached limit=$max_downloader_rss_bytes; restarting to release provider/Arrow/Python working sets" >&2
            terminate_downloader "$download_pid"
            break
          fi
        fi
      fi
      if [[ ! -f "$state_dir/openbb_archive.sqlite3" ]]; then
        now_epoch="$(date +%s)"
        stall_seconds=$((now_epoch - last_progress_epoch))
        echo "[openbb-watchdog] phase=initializing manifest=missing stall_seconds=$stall_seconds"
        if ((stall_seconds >= stall_timeout)); then
          echo "[openbb-supervisor] manifest was not created within ${stall_timeout}s; restarting downloader" >&2
          terminate_downloader "$download_pid"
          break
        fi
        continue
      fi
      phase_path="$state_dir/downloader_phase.json"
      downloader_phase=""
      if [[ -s "$phase_path" ]]; then
        downloader_phase="$(
          sed -n 's/.*"phase"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
            "$phase_path"
        )"
      fi
      if [[ "$downloader_phase" == "planning" \
        || "$downloader_phase" == "manifest_maintenance" ]]; then
        phase_update="$(stat -c '%Y' "$phase_path" 2>/dev/null || echo 0)"
        phase_marker="phase:${downloader_phase}:${phase_update}"
        now_epoch="$(date +%s)"
        if [[ "$phase_marker" != "$last_progress_marker" ]]; then
          last_progress_marker="$phase_marker"
          last_progress_epoch="$now_epoch"
        fi
        stall_seconds=$((now_epoch - last_progress_epoch))
        phase_summary="$(tr -d '\n' <"$phase_path")"
        echo "[openbb-planner] $phase_summary stall_seconds=$stall_seconds"
        if ((stall_seconds >= stall_timeout)); then
          echo "[openbb-supervisor] no planner progress for ${stall_timeout}s; restarting downloader" >&2
          terminate_downloader "$download_pid"
          break
        fi
        # A multi-million-row manifest build is already I/O-bound. Running the
        # full monitor scan concurrently once per minute competes for the same
        # SQLite pages and reports partial-plan false positives. The atomic
        # phase state above is the authoritative progress source until task
        # execution starts.
        continue
      fi
      # The scheduler publishes a small atomic state file whenever completion
      # persistence advances. Read that every minute for stall/resource
      # protection instead of scanning the multi-million-row manifest. The
      # full contract/pagination/quota audit runs on its own slower cadence
      # below, so monitoring cannot consume half of every provider's CPU time.
      refresh_watchdog_state
      now_epoch="$(date +%s)"
      progress_marker="download:${scheduler_attempted}"
      if [[ "$progress_marker" != "$last_progress_marker" ]]; then
        last_progress_marker="$progress_marker"
        last_progress_epoch="$now_epoch"
      fi
      stall_seconds=$((now_epoch - last_progress_epoch))
      echo "[openbb-watchdog] phase=$scheduler_phase attempted=$scheduler_attempted active=$scheduler_active completed_pending=$scheduler_completed backpressure=$scheduler_backpressure updated_at=$scheduler_updated stall_seconds=$stall_seconds"

      # A full audit scans the complete multi-million-row manifest. While the
      # scheduler is deliberately asleep with no in-flight work, repeating
      # that scan every 15 minutes cannot reveal download progress and only
      # churns page cache. The public status already merges the live scheduler
      # heartbeat with the latest complete snapshot, so audit at a much lower
      # idle cadence and restore the normal cadence as soon as work runs.
      effective_full_monitor_interval="$full_monitor_interval"
      if [[ "$scheduler_phase" == "waiting" \
        && "$scheduler_active" == "0" \
        && "$scheduler_completed" == "0" \
        && "$scheduler_wait_until_epoch" =~ ^[0-9]+$ \
        && "$scheduler_wait_until_epoch" -gt "$now_epoch" ]]; then
        effective_full_monitor_interval="$idle_full_monitor_interval"
      fi
      if ((last_full_monitor_epoch == 0 \
        || now_epoch - last_full_monitor_epoch >= effective_full_monitor_interval)); then
        monitor_ok=0
        if run_snapshot_monitor; then
          monitor_ok=1
        else
          echo "[openbb-supervisor] full monitor snapshot failed or exceeded ${full_monitor_timeout}s; lightweight stall watchdog remains active" >&2
        fi
        last_full_monitor_epoch="$(date +%s)"

        # A provider-wide quota cooldown is expected backpressure, not a stuck
        # downloader. Do not restart merely because every eligible task waits
        # for a persisted reset deadline. Refresh only the small atomic files
        # written above; this never repeats the expensive manifest audit.
        if [[ "$monitor_ok" == "1" ]]; then
          refresh_watchdog_state
          now_epoch="$(date +%s)"
          progress_marker="download:${scheduler_attempted}"
          if [[ "$progress_marker" != "$last_progress_marker" ]]; then
            last_progress_marker="$progress_marker"
            last_progress_epoch="$now_epoch"
          fi
          if [[ "$current_manifest_task_update" != "-" \
            && "$current_manifest_task_update" != "$last_manifest_task_update" ]]; then
            last_manifest_task_update="$current_manifest_task_update"
            last_progress_epoch="$now_epoch"
          fi
        fi
      fi

      if [[ "$has_cooldown" == "1" && "$pending_eligible" == "0" \
        && "$running_tasks" == "0" ]]; then
        last_progress_epoch="$(date +%s)"
        echo "[openbb-supervisor] waiting for provider cooldown; stall watchdog paused"
      fi
      # Execution eligibility belongs to the scheduler.  Its zero-refill
      # decision is more precise than a periodic full-manifest monitor count:
      # a task can look generally eligible while every remaining provider is
      # currently behind a durable cooldown or task retry deadline.
      now_epoch="$(date +%s)"
      if [[ "$scheduler_phase" == "waiting" \
        && "$scheduler_active" == "0" \
        && "$scheduler_completed" == "0" \
        && "$scheduler_wait_until_epoch" =~ ^[0-9]+$ \
        && "$scheduler_wait_until_epoch" -gt "$now_epoch" ]]; then
        last_progress_epoch="$now_epoch"
        echo "[openbb-supervisor] scheduler waiting reason=$scheduler_wait_reason until_epoch=$scheduler_wait_until_epoch; stall watchdog paused"
      fi
      now_epoch="$(date +%s)"
      stall_seconds=$((now_epoch - last_progress_epoch))
      if ((stall_seconds >= stall_timeout)); then
        echo "[openbb-supervisor] no manifest progress for ${stall_timeout}s; restarting downloader" >&2
        terminate_downloader "$download_pid"
        break
      fi
    fi
  done
  wait "$download_pid"
  download_rc=$?
  set -e
  download_pid=""
  rm -f "$state_dir/downloader.pid"
  rotate_log_if_needed

  if [[ -n "$stop_reason" ]]; then
    echo "[openbb-supervisor] stopped: $stop_reason" >&2
    exit 2
  fi

  post_monitor_ok=0
  if run_snapshot_monitor; then
    post_monitor_ok=1
    refresh_watchdog_state
  else
    echo "[openbb-supervisor] post-exit monitor failed or timed out; forcing another resumable round" >&2
  fi
  # The full monitor scan above already persisted an atomic snapshot. Reading
  # one field must not repeat the same multi-million-row SQLite audit and add
  # another minute of dead time before every restart.
  if [[ "$post_monitor_ok" == "1" ]]; then
    retryable="$(
      OPENBB_MONITOR_OUTPUT_DIR="$output_dir" "$python_bin" - <<'PY'
import json
import os
from pathlib import Path

snapshot = Path(os.environ["OPENBB_MONITOR_OUTPUT_DIR"]) / "_state" / "monitor_latest.json"
try:
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    print(max(0, int(payload["retryable_tasks"])))
except (OSError, ValueError, TypeError, json.JSONDecodeError, KeyError):
    # Fail open toward another supervised round if the snapshot itself is
    # unavailable; the next monitor pass will preserve the real terminal gate.
    print(1)
PY
    )"
  else
    # A stale earlier snapshot might say zero even though the interrupted
    # downloader just left retryable work. Never enter terminal audit unless
    # the post-exit manifest scan itself succeeded.
    retryable=1
  fi
  echo "[openbb-supervisor] round=$round exit=$download_rc retryable=$retryable at=$(date --iso-8601=seconds)"

  if [[ "$retryable" == "0" ]]; then
    break
  fi
  restart_wait="$restart_delay"
  restart_reason="retryable work remains"
  now_epoch="$(date +%s)"
  if [[ "$post_monitor_ok" == "1" && "$has_cooldown" == "1" \
    && "$pending_eligible" == "0" && "$running_tasks" == "0" \
    && "$next_cooldown_until_epoch" =~ ^[0-9]+$ \
    && "$next_cooldown_until_epoch" -gt "$now_epoch" ]]; then
    cooldown_wait=$((next_cooldown_until_epoch - now_epoch))
    if ((cooldown_wait > restart_wait)); then
      restart_wait="$cooldown_wait"
    fi
    restart_reason="all remaining work is quota-blocked until epoch $next_cooldown_until_epoch"
  fi
  echo "[openbb-supervisor] restarting in ${restart_wait}s; reason=$restart_reason"
  sleep "$restart_wait" &
  sleep_pid=$!
  wait "$sleep_pid" 2>/dev/null || true
  sleep_pid=""
done

echo "[openbb-supervisor] no retryable task remains; running full Parquet audit"
if ! "$python_bin" scripts/monitor_openbb_archive.py \
  --output-dir "$output_dir" --audit-files --write-snapshot --append-history --fail-on-incomplete; then
  echo "[openbb-supervisor] terminal unresolved tasks or file-integrity failures remain" >&2
  exit 2
fi

./scripts/run_openbb_archive_compaction.sh --output-dir "$output_dir"
echo "[openbb-supervisor] compaction written; running independent compact/view audit"
./scripts/run_openbb_archive_compaction.sh \
  --output-dir "$output_dir" --audit-only

echo "[openbb-supervisor] compact audit passed; re-running final source/catalog audit"
if ! "$python_bin" scripts/monitor_openbb_archive.py \
  --output-dir "$output_dir" --audit-files --write-snapshot --append-history --fail-on-incomplete; then
  echo "[openbb-supervisor] final post-compaction source/catalog audit failed" >&2
  exit 2
fi

echo "[openbb-supervisor] download, source/catalog audits, compaction, and compact audit completed at $(date --iso-8601=seconds)"
