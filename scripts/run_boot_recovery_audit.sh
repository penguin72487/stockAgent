#!/usr/bin/env bash
set -euo pipefail
boot_audit_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# WSL interactive sessions can restore DrvFs into a distinct mount namespace.
# Observe the whole workflow in PID 1's namespace, including markers and file
# identities; checking only a mount guard there would misrepresent caller I/O.
if [[ -d /run/systemd/system &&
      "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
    exec nsenter --mount=/proc/1/ns/mnt -- /usr/bin/bash \
        "$boot_audit_root/scripts/run_boot_recovery_audit.sh" "$@"
fi
cd "$boot_audit_root"
source scripts/runtime_env.sh
if [[ ! -S "${WSL_INTEROP:-}" && -S /run/WSL/1_interop ]]; then
    export WSL_INTEROP=/run/WSL/1_interop
fi
# systemd's PATH does not inherit Windows directories from an IDE shell.
if ! command -v powershell.exe >/dev/null 2>&1 &&
   [[ -x /mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe ]]; then
    export PATH="$PATH:/mnt/c/Windows/System32/WindowsPowerShell/v1.0"
fi
boot_audit_python="$(resolve_fintech_python)"
exec "$boot_audit_python" scripts/audit_boot_recovery.py "$@"
