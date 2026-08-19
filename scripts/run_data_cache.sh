#!/usr/bin/env bash
set -euo pipefail

script_path="$(readlink -f -- "${BASH_SOURCE[0]}")"
script_dir="$(cd -- "$(dirname -- "${script_path}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

case "${1:-}" in
  publish)
    shift
    packed_sync_root="${STOCKAGENT_PACKED_SYNC_ROOT:-/srv/stockagent-packed}"
    exec "${script_dir}/run_data_release.sh" publish "$@" \
      --sync-root "${packed_sync_root}"
    ;;
  publish-status)
    shift
    packed_sync_root="${STOCKAGENT_PACKED_SYNC_ROOT:-/srv/stockagent-packed}"
    exec "${script_dir}/run_data_release.sh" status "$@" \
      --sync-root "${packed_sync_root}"
    ;;
esac

# shellcheck source=scripts/runtime_env.sh
source "${repo_root}/scripts/runtime_env.sh"
run_fintech_python "${repo_root}/scripts/data_cache.py" "$@"
