#!/usr/bin/env bash
# D operations must observe exactly the production authority mount namespace.
set -euo pipefail
stockagent_rescue_script="$(readlink -f "${BASH_SOURCE[0]}")"
stockagent_rescue_repo="$(dirname "$(dirname "$stockagent_rescue_script")")"
if [[ "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
  exec nsenter -t 1 -m -- /bin/bash "$stockagent_rescue_script" "$@"
fi
cd "$stockagent_rescue_repo"
if [[ -S /run/WSL/1_interop && ! -S "${WSL_INTEROP:-/missing}" ]]; then
  export WSL_INTEROP=/run/WSL/1_interop
fi
if ! command -v powershell.exe >/dev/null 2>&1 && [[ -f /mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe ]]; then
  export PATH="$PATH:/mnt/c/Windows/System32/WindowsPowerShell/v1.0"
fi
source scripts/runtime_env.sh
if [[ -z "${WSL_DISTRO_NAME:-}" && "$(uname -r)" == *microsoft* ]]; then
  # systemd drops WSL's login variables. This public node-specific enrollment
  # is validated by the installer; never guess a different VHDX distribution.
  export WSL_DISTRO_NAME="$(run_fintech_python -c 'import json; print(json.load(open("configs/training_rescue.json"))["windows_distribution"])')"
fi
exec_fintech_python scripts/training_rescue.py "$@"
