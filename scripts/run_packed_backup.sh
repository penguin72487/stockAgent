#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
source "${REPO_ROOT}/scripts/runtime_env.sh"
run_fintech_python "${REPO_ROOT}/scripts/packed_backup.py" "$@"
