#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$REPO_ROOT/scripts/runtime_env.sh"

run_fintech_python "$REPO_ROOT/scripts/run_live_artifact_sync.py" "$@"
