#!/usr/bin/env bash
set -euo pipefail

stockagent_release_main() {
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

# The systemd D: mount owner and WSL IDE shells can have different mount
# namespaces. Use the enrolled owner's namespace, never relax its mount guard.
release_sync_root="${STOCKAGENT_PACKED_SYNC_ROOT:-/srv/stockagent-packed}"
release_completed_forward=false
release_argv=("$@")
for ((i=0; i<${#release_argv[@]}; i++)); do
  if [[ "${release_argv[$i]}" == --sync-root && $((i+1)) -lt ${#release_argv[@]} ]]; then
    release_sync_root="${release_argv[$((i+1))]}"
  elif [[ "${release_argv[$i]}" == --sync-root=* ]]; then
    release_sync_root="${release_argv[$i]#--sync-root=}"
  elif [[ "${release_argv[$i]}" == --completed-forward-key || "${release_argv[$i]}" == --completed-forward-key=* ]]; then
    release_completed_forward=true
  fi
done
if [[ $EUID == 0 && "$release_sync_root" == /srv/stockagent-packed \
      && -d /run/systemd/system && -f /srv/stockagent-packed/.stockagent-d-primary \
      && "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
  exec nsenter --mount=/proc/1/ns/mnt -- /usr/bin/bash "$REPO_ROOT/scripts/run_data_release.sh" "$@"
fi
cd "$REPO_ROOT"

# shellcheck source=scripts/runtime_env.sh
source "${REPO_ROOT}/scripts/runtime_env.sh"
ensure_enrolled_wsl_identity "$REPO_ROOT"
run_fintech_python "${REPO_ROOT}/scripts/publish_data_releases.py" "$@"
if [[ "${1:-}" == publish && "$release_completed_forward" == true ]]; then
  # The CLI has accepted one registered completed-output key. It changes no
  # training source or recipe input: do not rebuild/publish the same cookbook
  # once for every output. Invalid selections still propagate the child error.
  echo "[data-release] completed_forward_only=true training_sources_changed=false recipe_refresh=not_needed" >&2
  return 0
fi
if [[ "${1:-}" == publish ]]; then
  recipe_sync_root="/srv/stockagent-packed"
  release_argv=("$@")
  for ((i=0; i<${#release_argv[@]}; i++)); do
    if [[ "${release_argv[$i]}" == --sync-root && $((i+1)) -lt ${#release_argv[@]} ]]; then
      recipe_sync_root="${release_argv[$((i+1))]}"
    elif [[ "${release_argv[$i]}" == --sync-root=* ]]; then
      recipe_sync_root="${release_argv[$i]#--sync-root=}"
    fi
  done
  # A head may already be committed. Recipe metadata failure is reported as
  # such, not as a failed download; the metadata-only reconciliation timer
  # resumes it. Neither path starts a GPU job or assembles an unsolicited panel.
  if [[ "${2:-}" != training-dataset-recipes ]] && ! bash \
    "${REPO_ROOT}/scripts/run_training_dataset_recipes.sh" --sync-root "$recipe_sync_root" >&2; then
    echo "[data-release] release_command_completed=true recipe_refresh=deferred owner=stockagent-training-recipes.timer" >&2
  fi
fi
}
# Parse the final exit together with the call. A function alone does not stop
# a resumed top-level reader from reading the new tail of a growing script.
{
  stockagent_release_main "$@"
  exit "$?"
}
