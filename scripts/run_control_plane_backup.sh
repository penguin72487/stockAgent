#!/usr/bin/env bash
set -euo pipefail
control_backup_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$control_backup_root"
set -a
source "${STOCKAGENT_CONTROL_ENV_FILE:-/etc/stockagent/control-plane.env}"
set +a
export FINTECH_ENV_PATH="$CONTROL_PLANE_ENV_PATH"
unset PYTHON_BIN
source scripts/runtime_env.sh
run_fintech_python scripts/backup_control_plane.py "$@"
