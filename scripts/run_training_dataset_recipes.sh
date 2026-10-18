#!/usr/bin/env bash
set -euo pipefail
stockagent_recipe_main() {
recipe_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
recipe_sync_root="${STOCKAGENT_PACKED_ROOT:-/srv/stockagent-packed}"
recipe_argv=("$@")
for ((i=0; i<${#recipe_argv[@]}; i++)); do
  if [[ "${recipe_argv[$i]}" == --sync-root && $((i+1)) -lt ${#recipe_argv[@]} ]]; then
    recipe_sync_root="${recipe_argv[$((i+1))]}"
  elif [[ "${recipe_argv[$i]}" == --sync-root=* ]]; then
    recipe_sync_root="${recipe_argv[$i]#--sync-root=}"
  fi
done
if [[ $EUID == 0 && "$recipe_sync_root" == /srv/stockagent-packed \
      && -d /run/systemd/system && -f /srv/stockagent-packed/.stockagent-d-primary \
      && "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
  exec nsenter --mount=/proc/1/ns/mnt -- /usr/bin/bash "$recipe_repo_root/scripts/run_training_dataset_recipes.sh" "$@"
fi
cd "$recipe_repo_root"
source scripts/runtime_env.sh
mkdir -p artifacts/operations/training_dataset_recipes
exec 8>artifacts/operations/training_dataset_recipes/refresh.lock
flock -n 8 || exit 0
recipe_remote_report="artifacts/data_quality/minute_recipes_20261007/vastai1t_methods.json"
if [[ -f "$recipe_remote_report" ]]; then
  run_fintech_python -m scripts.maintain_training_dataset_recipes refresh \
    --source-lock-fd 8 --remote-run-report "$recipe_remote_report" "$@"
else
  run_fintech_python -m scripts.maintain_training_dataset_recipes refresh --source-lock-fd 8 "$@"
fi
# Publish metadata through the same registered publisher without recursively
# invoking the downloader's post-publication hook. Status/heartbeat is excluded.
recipe_notification=()
if [[ -f "$recipe_sync_root/.stockagent-d-primary" ]]; then
  mkdir -p artifacts/operations/training-recipe-publications
  recipe_publication_stage="$(mktemp -d "$recipe_repo_root/artifacts/operations/training-recipe-publications/run.XXXXXXXX")"
  recipe_notification=(--defer-scan --result-receipt "$recipe_publication_stage/publication.json")
fi
run_fintech_python -m scripts.publish_data_releases publish training-dataset-recipes \
  --sync-root "$recipe_sync_root" --source-lock-fd 8 "${recipe_notification[@]}"
}
# Keep termination in the same parsed command as the long-running function;
# no top-level read may resume at the old EOF after an in-place rollout.
{
  stockagent_recipe_main "$@"
  exit "$?"
}
