#!/usr/bin/env bash
set -euo pipefail

if (($# < 4)); then
  echo "Usage: $0 DATASET SYNC_ROOT -- COMMAND [ARG ...]" >&2
  exit 2
fi

dataset="$1"
sync_root="$2"
shift 2
if [[ "${1:-}" != "--" ]]; then
  echo "Expected -- before the downloader command." >&2
  exit 2
fi
shift
if (($# == 0)); then
  echo "A downloader/build/audit command is required." >&2
  exit 2
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# Publication runs only after the complete downloader/build/audit command exits
# successfully. A failed or interrupted producer therefore cannot advance a head.
"$@"

publish_args=(publish "${dataset}" --sync-root "${sync_root}")
if [[ -n "${STOCKAGENT_SYNC_NODE_ID:-}" ]]; then
  publish_args+=(--node-id "${STOCKAGENT_SYNC_NODE_ID}")
fi
exec "${SCRIPT_DIR}/run_data_release.sh" "${publish_args[@]}"
