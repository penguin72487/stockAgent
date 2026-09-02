#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh

shioaji_env_file="${SHIOAJI_ENV_FILE:-$repo_root/.env}"
if [[ ! -r "$shioaji_env_file" ]]; then
  echo "[tw-day-trade-minute-curves] unreadable Shioaji environment file: $shioaji_env_file" >&2
  exit 2
fi
set -a
# shellcheck disable=SC1090
source "$shioaji_env_file"
set +a
if [[ -z "${SHIOAJI_API_KEY:-}" || -z "${SHIOAJI_SECRET_KEY:-}" ]]; then
  echo "[tw-day-trade-minute-curves] SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required" >&2
  exit 2
fi

python_bin="$(resolve_fintech_python)"
exec "$python_bin" scripts/maintain_tw_day_trade_minute_curves.py "$@"
