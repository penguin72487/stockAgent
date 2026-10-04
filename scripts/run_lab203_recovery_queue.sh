#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
set -a
source /etc/lab203-backup/recovery-queue.env
set +a
unset PYTHON_BIN
cd -- "$REPO_ROOT"
source "$REPO_ROOT/scripts/runtime_env.sh"
run_fintech_python "$REPO_ROOT/scripts/run_lab203_recovery_queue.py" \
  --configuration "$STOCKAGENT_RECOVERY_QUEUE_CONFIGURATION"
