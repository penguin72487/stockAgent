#!/usr/bin/env bash
set -euo pipefail
stockagent_lakehouse_script="$(readlink -f "${BASH_SOURCE[0]}")"
if [[ "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
  exec nsenter --mount=/proc/1/ns/mnt -- bash "$stockagent_lakehouse_script" "$@"
fi
cd "$(dirname "$(dirname "$stockagent_lakehouse_script")")"
source scripts/runtime_env.sh
stockagent_lakehouse_prefix="$(run_fintech_python - <<'PY'
import json,os
from pathlib import Path
p=Path('/etc/stockagent/lakehouse-control.json')
if p.is_symlink() or p.stat().st_mode & 0o077 or p.stat().st_uid != os.geteuid():
    raise SystemExit('private lakehouse configuration ownership/mode differs')
print(json.loads(p.read_bytes())['role_prefix'])
PY
)"
export FINTECH_ENV_PATH="$stockagent_lakehouse_prefix"
unset PYTHON_BIN
source scripts/runtime_env.sh
if [[ "${1:-}" == python ]]; then
  shift
  exec_fintech_python "$@"
else
  exec_fintech_python scripts/manage_lakehouse.py "$@"
fi
