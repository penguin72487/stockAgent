#!/usr/bin/env bash
set -euo pipefail

script_path="$(readlink -f -- "${BASH_SOURCE[0]}")"
script_dir="$(cd -- "$(dirname -- "${script_path}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

edge_state="${STOCKAGENT_PACKED_EDGE_STATE:-/var/lib/stockagent-packed-edge/state.json}"
# Observe the authoritative cold mount in its owner's namespace. Checking a
# different namespace and then reading from this one would give a false guard
# failure (or an unrelated tree) in WSL IDE shells.
case "${1:-}" in
  status|automation-status)
    if [[ ! -f "$edge_state" && -d /run/systemd/system \
          && "${STOCKAGENT_PACKED_SYNC_ROOT:-/srv/stockagent-packed}" == /srv/stockagent-packed \
          && "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
      exec nsenter --mount=/proc/1/ns/mnt -- /usr/bin/bash "$script_path" "$@"
    fi
    ;;
esac
if [ -f "${edge_state}" ]; then
  case "${1:-}" in
    use|gc|evict|reader-status)
      # shellcheck source=scripts/runtime_env.sh
      source "${repo_root}/scripts/runtime_env.sh"
      if [ -r /etc/environment ]; then
        set -a
        # shellcheck disable=SC1091
        source /etc/environment
        set +a
      fi
      # An independently installed, pinned reader can support newer packed
      # schemas without rewriting a running training checkout. The original
      # edge state, lock, cache and live policy remain the owners.
      edge_reader_control="${STOCKAGENT_PACKED_EDGE_READER_CONTROL:-/var/lib/stockagent-packed-edge/reader-release.control}"
      edge_reader_script="${repo_root}/scripts/manage_packed_edge.py"
      if [[ -e "$edge_reader_control" || -L "$edge_reader_control" ]]; then
        if [[ ! -f "$edge_reader_control" || -L "$edge_reader_control" \
              || "$(stat -c '%u' -- "$edge_reader_control")" != "$(id -u)" \
              || "$(stat -c '%a' -- "$edge_reader_control")" != 600 ]]; then
          printf '%s\n' 'ERROR: packed edge reader control is unsafe' >&2
          exit 1
        fi
        IFS=' ' read -r edge_reader_root edge_reader_manifest edge_reader_repo edge_reader_extra < "$edge_reader_control" || true
        if [[ -z "$edge_reader_root" || ! "$edge_reader_manifest" =~ ^[0-9a-f]{64}$ \
              || -z "$edge_reader_repo" || -n "${edge_reader_extra:-}" \
              || "$edge_reader_root" != /* || "$edge_reader_repo" != /* \
              || ! -d "$edge_reader_root" || -L "$edge_reader_root" \
              || "$(realpath -e -- "$edge_reader_root")" != "$edge_reader_root" \
              || "$(stat -c '%u' -- "$edge_reader_root")" != "$(id -u)" \
              || "$(stat -c '%a' -- "$edge_reader_root")" != 700 \
              || ! -L "$edge_reader_root/configs" \
              || "$(realpath -e -- "$edge_reader_root/configs")" != "$edge_reader_repo/configs" \
              || ! -f "$edge_reader_root/reader-manifest.sha256" \
              || -L "$edge_reader_root/reader-manifest.sha256" \
              || ! -f "$edge_reader_root/scripts/manage_packed_edge.py" \
              || ! -d "$edge_reader_root/vendor" ]]; then
          printf '%s\n' 'ERROR: packed edge reader release is unavailable or invalid' >&2
          exit 1
        fi
        edge_observed_manifest="$(sha256sum -- "$edge_reader_root/reader-manifest.sha256")"
        if [[ "${edge_observed_manifest%% *}" != "$edge_reader_manifest" ]] \
            || ! (cd -- "$edge_reader_root" && sha256sum --quiet --check reader-manifest.sha256); then
          printf '%s\n' 'ERROR: packed edge reader release failed identity verification' >&2
          exit 1
        fi
        edge_reader_script="$edge_reader_root/scripts/manage_packed_edge.py"
        export PYTHONPATH="$edge_reader_root/vendor${PYTHONPATH:+:$PYTHONPATH}"
      fi
      run_fintech_python "$edge_reader_script" "$@"
      exit $?
      ;;
  esac
fi

case "${1:-}" in
  layout)
    shift
    packed_sync_root="${STOCKAGENT_PACKED_SYNC_ROOT:-/srv/stockagent-packed}"
    exec "${script_dir}/run_data_release.sh" layout "$@" \
      --sync-root "${packed_sync_root}"
    ;;
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
