#!/usr/bin/env bash
set -euo pipefail

control_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$control_repo_root"
control_environment="${STOCKAGENT_CONTROL_ENV_FILE:-/etc/stockagent/control-plane.env}"
if [[ -r "$control_environment" ]]; then
  set -a
  # root-owned private role configuration, following the existing service env convention.
  source "$control_environment"
  set +a
fi
if [[ -n "${CONTROL_PLANE_ENV_PATH:-}" ]]; then
  export FINTECH_ENV_PATH="$CONTROL_PLANE_ENV_PATH"
  export PYTHON_BIN="$FINTECH_ENV_PATH/bin/python"
fi
source scripts/runtime_env.sh
exec "$(resolve_fintech_python)" scripts/manage_control_work.py "$@"
