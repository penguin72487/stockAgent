#!/usr/bin/env bash
# Cold operations must observe the same mount namespace as their systemd owner.
set -euo pipefail
stockagent_operation_script="$(readlink -f "${BASH_SOURCE[0]}")"
stockagent_operation_root="$(dirname "$(dirname "$stockagent_operation_script")")"
case "${1:-}" in
  scripts/audit_node_storage_roles.py|scripts/organize_vast_bulk_archives.py|scripts/receive_vast_bulk_archives.py|scripts/install_vast_bulk_return.py|scripts/enroll_vast_legacy_cache_capture.py|scripts/benchmark_remote_cold_transport.py|scripts/benchmark_cold_pack_read.py|scripts/benchmark_legacy_archive_workflow.py|scripts/benchmark_cold_blob_read.py|scripts/handoff_remote_legacy_archive_worker.py) ;;
  *) printf '%s\n' 'Use an explicitly supported authority storage operation.' >&2; exit 64 ;;
esac
if [[ "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
  exec nsenter -t 1 -m -- /bin/bash "$stockagent_operation_script" "$@"
fi
cd "$stockagent_operation_root"
stockagent_owner_environment=/etc/stockagent/remote-cold-artifact-ingress.env
if [[ -f "$stockagent_owner_environment" && ! -L "$stockagent_owner_environment" ]]; then
  [[ "$(stat -c '%a:%u' "$stockagent_owner_environment")" == 600:0 ]] || exit 78
  set -a
  source "$stockagent_owner_environment"
  set +a
fi
source scripts/runtime_env.sh
if [[ -S /run/WSL/1_interop && ! -S "${WSL_INTEROP:-/missing}" ]]; then
  export WSL_INTEROP=/run/WSL/1_interop
fi
if ! command -v powershell.exe >/dev/null 2>&1 && [[ -f /mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe ]]; then
  export PATH="$PATH:/mnt/c/Windows/System32/WindowsPowerShell/v1.0"
fi
run_fintech_python "$@"
