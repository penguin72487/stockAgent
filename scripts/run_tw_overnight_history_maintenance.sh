#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
source scripts/runtime_env.sh
FINTECH_PYTHON="$(resolve_fintech_python)" || {
  printf 'Unable to resolve the fintech Python runtime.\n' >&2
  exit 2
}
exec "$FINTECH_PYTHON" scripts/maintain_tw_overnight_history.py "$@"
