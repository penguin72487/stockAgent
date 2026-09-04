#!/usr/bin/env bash
set -euo pipefail

script_path="$(readlink -f -- "${BASH_SOURCE[0]}")"
script_dir="$(cd -- "$(dirname -- "${script_path}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

edge_state="${STOCKAGENT_PACKED_EDGE_STATE:-/var/lib/stockagent-packed-edge/state.json}"
if [ -f "${edge_state}" ]; then
  case "${1:-}" in
    use|gc|evict)
      # shellcheck source=scripts/runtime_env.sh
      source "${repo_root}/scripts/runtime_env.sh"
      if [ -r /etc/environment ]; then
        set -a
        # shellcheck disable=SC1091
        source /etc/environment
        set +a
      fi
      run_fintech_python "${repo_root}/scripts/manage_packed_edge.py" "$@"
      exit $?
      ;;
  esac
fi

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
