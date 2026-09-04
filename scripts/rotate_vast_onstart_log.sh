#!/usr/bin/env bash
set -euo pipefail

log_path=/var/log/onstart.log
first_archive=/var/log/onstart.log.1.gz
second_archive=/var/log/onstart.log.2.gz
minimum_bytes="${VAST_ONSTART_ROTATE_BYTES:-104857600}"

exec 9>/run/lock/vast-onstart-log-rotate.lock
flock -n 9 || exit 0

[[ -f "$log_path" && ! -L "$log_path" ]] || exit 0
current_bytes="$(stat -c %s "$log_path")"
(( current_bytes >= minimum_bytes )) || exit 0

temporary_archive="$(mktemp /var/log/.onstart.log.XXXXXX.gz)"
trap 'rm -f "$temporary_archive"' EXIT

# This is the copytruncate model: the long-running tee keeps its inode and
# file descriptor, while the complete pre-rotation log remains compressed.
nice -n 19 ionice -c 3 gzip -1 -c -- "$log_path" >"$temporary_archive"
gzip -t -- "$temporary_archive"

if [[ -f "$first_archive" && ! -L "$first_archive" ]]; then
  mv -f -- "$first_archive" "$second_archive"
fi
mv -- "$temporary_archive" "$first_archive"
truncate -s 0 -- "$log_path"
trap - EXIT
