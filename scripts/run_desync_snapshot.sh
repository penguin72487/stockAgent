#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)

# shellcheck source=scripts/runtime_env.sh
source "${SCRIPT_DIR}/runtime_env.sh"
run_fintech_python "${REPO_ROOT}/scripts/desync_snapshot.py" "$@"
