#!/usr/bin/env bash
set -euo pipefail
control_queue_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$control_queue_repo"
control_queue_env="${STOCKAGENT_CONTROL_ENV_FILE:-/etc/stockagent/control-plane.env}"
[[ -f "$control_queue_env" && ! -L "$control_queue_env" \
   && "$(stat -c '%a:%u' "$control_queue_env")" == 600:0 ]] || exit 78
set -a
source "$control_queue_env"
set +a
unset PYTHON_BIN
export FINTECH_ENV_PATH="${CONTROL_PLANE_ENV_PATH:?Missing accepted control role}"
source scripts/runtime_env.sh
exec "$(resolve_fintech_python)" scripts/manage_control_release_queue.py "$@"
