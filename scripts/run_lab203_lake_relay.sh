#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")")"
stockagent_lake_relay_prefix="$(python3 - <<'PY'
import json,os
from pathlib import Path
p=Path('/etc/lab203-backup/lake-relay.json')
if p.is_symlink() or p.stat().st_mode & 0o077 or p.stat().st_uid != os.geteuid():
    raise SystemExit('relay private configuration ownership/mode differs')
print(json.loads(p.read_bytes())['role_prefix'])
PY
)"
export FINTECH_ENV_PATH="$stockagent_lake_relay_prefix"
unset PYTHON_BIN
source scripts/runtime_env.sh
run_fintech_python scripts/run_lab203_lake_relay.py
