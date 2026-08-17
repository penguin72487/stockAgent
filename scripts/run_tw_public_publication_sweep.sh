#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)"
exec "$python_bin" scripts/watch_tw_public_publication_group.py "$@"
