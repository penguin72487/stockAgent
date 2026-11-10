#!/usr/bin/env bash
# The singleton covers this runner's lifetime. FD 9 remains the existing
# publisher-compatible source lock, but is released during idle waits.

minute_runner_acquire_locks() {
  local run_root="$1"
  exec 8>>"$run_root/runner.owner.lock"
  if ! flock -n 8; then
    echo "[shioaji-minute-runner] another minute runner owns the singleton" >&2
    return 3
  fi
  exec 9>>"$run_root/runner.lock"
  if ! flock -n 9; then
    echo "[shioaji-minute-runner] waiting for the existing source owner" >&2
    flock -x 9
  fi
}

minute_runner_sleep_unlocked() {
  local delay="$1"
  if [[ ! "$delay" =~ ^[0-9]+$ ]]; then
    echo "[shioaji-minute-runner] invalid idle delay" >&2
    return 2
  fi
  flock -u 9
  # Do not let the sleep process retain the singleton after a runner exit.
  sleep "$delay" 8>&- 9>&-
  # When another owner is copying or publishing, wait instead of racing it.
  # The caller returns to the top of the loop and recalculates all clocks.
  flock -x 9
}
