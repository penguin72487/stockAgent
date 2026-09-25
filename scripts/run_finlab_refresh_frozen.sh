#!/usr/bin/env bash
# Validate and execute exactly one immutable copy of a long-running Bash job.
set -euo pipefail

finlab_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
finlab_source_script="$finlab_repo_root/scripts/run_finlab_refresh.sh"
finlab_snapshot_dir="$(mktemp -d)"
finlab_frozen_script="$finlab_snapshot_dir/run_finlab_refresh.sh"
trap 'rm -f -- "$finlab_frozen_script"; rmdir -- "$finlab_snapshot_dir"' EXIT

cp -- "$finlab_source_script" "$finlab_frozen_script"
if ! cmp -s -- "$finlab_source_script" "$finlab_frozen_script"; then
  echo "[finlab] refresh script changed while snapshotting; aborting before download" >&2
  exit 2
fi
bash -n "$finlab_frozen_script"
FINLAB_REPO_ROOT="$finlab_repo_root" bash "$finlab_frozen_script"
