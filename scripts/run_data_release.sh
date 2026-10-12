#!/usr/bin/env bash
set -euo pipefail

stockagent_release_main() {
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

# The systemd D: mount owner and WSL IDE shells can have different mount
# namespaces. Use the enrolled owner's namespace, never relax its mount guard.
release_sync_root="${STOCKAGENT_PACKED_SYNC_ROOT:-/srv/stockagent-packed}"
release_argv=("$@")
for ((i=0; i<${#release_argv[@]}; i++)); do
  if [[ "${release_argv[$i]}" == --sync-root && $((i+1)) -lt ${#release_argv[@]} ]]; then
    release_sync_root="${release_argv[$((i+1))]}"
  elif [[ "${release_argv[$i]}" == --sync-root=* ]]; then
    release_sync_root="${release_argv[$i]#--sync-root=}"
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
run_fintech_python "${REPO_ROOT}/scripts/publish_data_releases.py" "$@"
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
# Parse the whole body before starting a long child; an in-place rollout must
# not make a resumed shell read unrelated offsets from a newly edited script.
stockagent_release_main "$@"
