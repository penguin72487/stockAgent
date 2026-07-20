#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

source scripts/runtime_env.sh

malloc_arena_max="${MALLOC_ARENA_MAX:-${OPENBB_MALLOC_ARENA_MAX:-4}}"
if [[ ! "$malloc_arena_max" =~ ^[1-9][0-9]*$ ]]; then
  echo "MALLOC_ARENA_MAX/OPENBB_MALLOC_ARENA_MAX must be a positive integer." >&2
  exit 2
fi
export MALLOC_ARENA_MAX="$malloc_arena_max"

python_bin="$(resolve_fintech_python)" || {
  echo "Unable to resolve the fintech Python runtime." >&2
  exit 2
}

# Pin the archive right boundary for this output directory.  Otherwise a
# direct rerun after midnight changes the plan identity and re-enumerates the
# entire universe even though every earlier checkpoint is still valid.
output_dir="${OPENBB_OUTPUT_DIR:-data_openBB}"
archive_end_date="${OPENBB_ARCHIVE_END_DATE:-}"
explicit_end_date=0
for ((arg_index=1; arg_index<=$#; arg_index++)); do
  arg="${!arg_index}"
  if [[ "$arg" == "--output-dir" && $((arg_index + 1)) -le $# ]]; then
    next_index=$((arg_index + 1))
    output_dir="${!next_index}"
  elif [[ "$arg" == --output-dir=* ]]; then
    output_dir="${arg#--output-dir=}"
  fi
  if [[ "$arg" == "--end-date" || "$arg" == --end-date=* ]]; then
    explicit_end_date=1
    if [[ "$arg" == "--end-date" && $((arg_index + 1)) -le $# ]]; then
      next_index=$((arg_index + 1))
      archive_end_date="${!next_index}"
    elif [[ "$arg" == --end-date=* ]]; then
      archive_end_date="${arg#--end-date=}"
    fi
  fi
done
archive_end_date_path="$output_dir/_state/archive_end_date.txt"
pinned_archive_end_date=""
if [[ -s "$archive_end_date_path" ]]; then
  pinned_archive_end_date="$(tr -d '[:space:]' <"$archive_end_date_path")"
fi
if [[ -n "$pinned_archive_end_date" ]]; then
  if [[ -n "$archive_end_date" && "$archive_end_date" != "$pinned_archive_end_date" ]]; then
    echo "Archive end date is already pinned to $pinned_archive_end_date for $output_dir; use a different output directory for $archive_end_date." >&2
    exit 2
  fi
  archive_end_date="$pinned_archive_end_date"
elif [[ -z "$archive_end_date" ]]; then
  archive_end_date="$(date +%F)"
fi
if [[ ! "$archive_end_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] \
  || [[ "$(date -d "$archive_end_date" +%F 2>/dev/null || true)" != "$archive_end_date" ]]; then
  echo "Invalid archive end date: $archive_end_date" >&2
  exit 2
fi
mkdir -p "$(dirname "$archive_end_date_path")"
archive_end_date_tmp="$archive_end_date_path.tmp.$$"
printf '%s\n' "$archive_end_date" >"$archive_end_date_tmp"
mv -f "$archive_end_date_tmp" "$archive_end_date_path"

archive_args=(
  --output-dir "$output_dir"
  --start-date 2000-01-01
  --resume-existing-plan
)
if ((explicit_end_date == 0)); then
  archive_args+=(--end-date "$archive_end_date")
fi
exec "$python_bin" downloader/download_openbb_archive.py \
  "${archive_args[@]}" \
  "$@"
