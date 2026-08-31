#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

set -a
if [[ -f "$REPO_ROOT/.env" ]]; then
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
fi
if [[ -f "$REPO_ROOT/services/discord_bot/.env" ]]; then
  # shellcheck disable=SC1091
  source "$REPO_ROOT/services/discord_bot/.env"
fi
set +a

# shellcheck disable=SC1091
source "$REPO_ROOT/scripts/runtime_env.sh"
run_fintech_python "$REPO_ROOT/scripts/run_discord_artifact_maintenance.py"
