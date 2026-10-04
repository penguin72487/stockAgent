#!/usr/bin/env bash
set -euo pipefail
finlab_due_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$finlab_due_repo_root"
source scripts/runtime_env.sh
finlab_due_python="$(resolve_fintech_python)"
exec "$finlab_due_python" scripts/check_finlab_refresh_due.py "$@"
