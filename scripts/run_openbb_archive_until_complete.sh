#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

output_dir="${OPENBB_OUTPUT_DIR:-data_openBB}"
forward_args=()
while (($# > 0)); do
  case "$1" in
    --output-dir)
      if (($# < 2)); then
        echo "--output-dir requires a value" >&2
        exit 2
      fi
      output_dir="$2"
      shift 2
      ;;
    --output-dir=*)
      output_dir="${1#--output-dir=}"
      shift
      ;;
    --start-date|--start-date=*)
      echo "The archive start date is fixed at 2000-01-01." >&2
      exit 2
      ;;
    --end-date|--end-date=*)
      echo "Use OPENBB_ARCHIVE_END_DATE=YYYY-MM-DD before the first run; the archive then pins that boundary." >&2
      exit 2
      ;;
    *)
      forward_args+=("$1")
      shift
      ;;
  esac
done

export OPENBB_OUTPUT_DIR="$output_dir"
# This foreground entry point is intended for a human terminal. Keep detailed
# downloader progress by default; set OPENBB_SUPERVISOR_TQDM=0 for quiet logs.
export OPENBB_SUPERVISOR_TQDM="${OPENBB_SUPERVISOR_TQDM:-1}"

state_dir="$output_dir/_state"
mkdir -p "$state_dir"
for pid_name in supervisor downloader; do
  pid_path="$state_dir/$pid_name.pid"
  pid=""
  if [[ -s "$pid_path" ]]; then
    pid="$(tr -d '[:space:]' <"$pid_path")"
  fi
  if [[ "$pid" =~ ^[1-9][0-9]*$ ]] && kill -0 "$pid" 2>/dev/null; then
    echo "An OpenBB $pid_name is already active with PID $pid; refusing a duplicate run." >&2
    exit 3
  fi
done

source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)" || {
  echo "Unable to resolve the fintech Python runtime." >&2
  exit 2
}

"$python_bin" - <<'PY'
import importlib.util

required = ("duckdb", "openbb", "polars", "pyarrow")
missing = [name for name in required if importlib.util.find_spec(name) is None]
if missing:
    raise SystemExit("Missing fintech runtime modules: " + ", ".join(missing))
PY

min_free_bytes="${OPENBB_MIN_FREE_BYTES:-107374182400}"
if [[ ! "$min_free_bytes" =~ ^[0-9]+$ ]]; then
  echo "OPENBB_MIN_FREE_BYTES must be a non-negative integer." >&2
  exit 2
fi
free_bytes="$(
  df --output=avail -B1 "$output_dir" 2>/dev/null \
    | tail -n 1 \
    | tr -d '[:space:]'
)"
if [[ ! "$free_bytes" =~ ^[0-9]+$ ]]; then
  echo "Unable to determine free space for $output_dir." >&2
  exit 2
fi
if ((min_free_bytes > 0 && free_bytes < min_free_bytes)); then
  echo "Free space $free_bytes bytes is below the safety floor $min_free_bytes; not starting." >&2
  exit 2
fi

archive_end_date=""
if [[ -s "$state_dir/archive_end_date.txt" ]]; then
  archive_end_date="$(tr -d '[:space:]' <"$state_dir/archive_end_date.txt")"
fi
requested_archive_end_date="${OPENBB_ARCHIVE_END_DATE:-}"
if [[ -n "$archive_end_date" && -n "$requested_archive_end_date" \
  && "$archive_end_date" != "$requested_archive_end_date" ]]; then
  echo "Archive end date is already pinned to $archive_end_date for $output_dir; use a different output directory for $requested_archive_end_date." >&2
  exit 2
fi
if [[ -z "$archive_end_date" ]]; then
  archive_end_date="${requested_archive_end_date:-$(date +%F)}"
fi
echo "[openbb-run] output_dir=$output_dir start=2000-01-01 end=$archive_end_date free_bytes=$free_bytes"
echo "[openbb-run] Ctrl-C is safe; rerun this same script to resume without redownloading completed tasks."
if [[ "${OPENBB_PREFLIGHT_ONLY:-0}" == "1" ]]; then
  echo "[openbb-run] preflight completed; downloader was not started"
  exit 0
fi

exec bash scripts/run_openbb_archive_supervisor.sh \
  "${forward_args[@]}" \
  --start-date 2000-01-01
