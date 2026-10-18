#!/usr/bin/env bash
set -euo pipefail
stockagent_lakehouse_script="$(readlink -f "${BASH_SOURCE[0]}")"
if [[ "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
  exec nsenter --mount=/proc/1/ns/mnt -- bash "$stockagent_lakehouse_script" "$@"
fi
cd "$(dirname "$(dirname "$stockagent_lakehouse_script")")"
if [[ -S /run/WSL/1_interop && ! -S "${WSL_INTEROP:-/missing}" ]]; then
  export WSL_INTEROP=/run/WSL/1_interop
fi
if ! command -v powershell.exe >/dev/null 2>&1 && [[ -f /mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe ]]; then
  export PATH="$PATH:/mnt/c/Windows/System32/WindowsPowerShell/v1.0"
fi
source scripts/runtime_env.sh
if [[ -z "${WSL_DISTRO_NAME:-}" && "$(uname -r)" == *microsoft* ]]; then
  # systemd does not inherit WSL login variables. Reuse the node's existing
  # rescue enrollment, never a guessed/default distribution. The native
  # capacity probe still verifies that exact distribution's registered VHDX.
  export WSL_DISTRO_NAME="$(run_fintech_python - <<'PY'
import json, os, re
from pathlib import Path
p = Path('configs/training_rescue.json')
if p.is_symlink() or p.resolve().parent != Path('configs').resolve() or p.stat().st_uid != os.geteuid():
    raise SystemExit('node WSL enrollment ownership/path differs')
value = json.loads(p.read_bytes())
name = value.get('windows_distribution')
if (value.get('contract') != 'stockagent.training_rescue.v1' or value.get('schema_version') != 1
        or not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', name)):
    raise SystemExit('node WSL enrollment is missing or invalid')
print(name)
PY
)"
  [[ -n "$WSL_DISTRO_NAME" ]] || exit 1
fi
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
