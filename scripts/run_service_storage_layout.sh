#!/usr/bin/env bash
# Runs once before service owners start; never stops an active service.
set -euo pipefail
stockagent_layout_script="$(readlink -f "${BASH_SOURCE[0]}")"
if [[ "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
  exec nsenter --mount=/proc/1/ns/mnt -- bash "$stockagent_layout_script" "$@"
fi
cd "$(dirname "$(dirname "$stockagent_layout_script")")"
source scripts/runtime_env.sh
exec_fintech_python scripts/organize_service_storage.py --apply --boot-handoff "$@"
