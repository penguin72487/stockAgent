#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
exec_fintech_python scripts/maintain_shioaji_sdk.py "${1:-maintain}" "${@:2}"
