#!/usr/bin/env bash
set -euo pipefail
recipe_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
recipe_service_user="${STOCKAGENT_SERVICE_USER:-$(stat -c '%U' "$recipe_repo_root")}"
recipe_service_group="$(id -gn "$recipe_service_user")"
recipe_service_home="$(getent passwd "$recipe_service_user" | cut -d: -f6)"
if ((EUID != 0)); then echo 'root is required to install systemd units' >&2; exit 2; fi
recipe_stage="$(mktemp -d /tmp/stockagent-recipe-units.XXXXXXXX)"
cleanup_recipe_stage() {
  for recipe_staged_unit in stockagent-training-recipes.service stockagent-training-recipes.timer; do
    if [[ -f "$recipe_stage/$recipe_staged_unit" && ! -L "$recipe_stage/$recipe_staged_unit" ]]; then
      unlink "$recipe_stage/$recipe_staged_unit"
    fi
  done
  rmdir "$recipe_stage" 2>/dev/null || true
}
trap cleanup_recipe_stage EXIT
for recipe_unit in stockagent-training-recipes.service stockagent-training-recipes.timer; do
  sed -e "s|@REPO_ROOT@|$recipe_repo_root|g" \
      -e "s|@SERVICE_USER@|$recipe_service_user|g" \
      -e "s|@SERVICE_GROUP@|$recipe_service_group|g" \
      -e "s|@SERVICE_HOME@|$recipe_service_home|g" \
      "$recipe_repo_root/deploy/systemd/$recipe_unit.in" > "$recipe_stage/$recipe_unit"
done
systemd-analyze verify "$recipe_stage/stockagent-training-recipes.service" "$recipe_stage/stockagent-training-recipes.timer"
for recipe_unit in stockagent-training-recipes.service stockagent-training-recipes.timer; do
  install -m 0644 "$recipe_stage/$recipe_unit" "/etc/systemd/system/$recipe_unit"
  unlink "$recipe_stage/$recipe_unit"
done
systemctl daemon-reload
systemctl enable --now stockagent-training-recipes.timer
