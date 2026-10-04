#!/usr/bin/env bash
set -euo pipefail
backup_stream_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$backup_stream_repo"
set -a
source "${STOCKAGENT_BACKUP_STREAM_ENV_FILE:-/etc/stockagent/backup-stream.env}"
set +a
unset PYTHON_BIN
source scripts/runtime_env.sh
run_fintech_python - <<'PY'
import json,os
from pathlib import Path
from stockagent.runtime_identity import runtime_identity,validate_runtime_lock
lock=Path(os.environ['STOCKAGENT_BACKUP_RUNTIME_LOCK'])
if validate_runtime_lock(json.loads(lock.read_bytes()),runtime_identity()):
    raise SystemExit('backup role differs from its accepted Mamba runtime lock')
PY
bash scripts/mount_backup_transport.sh --check
if [[ "${1:-}" == local-recovery ]]; then
  shift
  run_fintech_python scripts/verify_backup_stream_local.py "$@"
elif [[ "${1:-}" == publish-handoff ]]; then
  shift
  run_fintech_python scripts/publish_backup_handoff.py "$@"
else
  run_fintech_python scripts/manage_backup_stream.py "$@"
fi
