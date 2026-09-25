#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)"
"$python_bin" scripts/refresh_tw_day_trade_margin_actions.py --start-year 2026 "$@"
# Cold publication is independent from opening readiness. If another registered
# writer is active, this records a successful deferred attempt instead of
# repeating the expensive action rebuild every five minutes.
exec "$python_bin" scripts/publish_tw_public_cold_release.py --defer-stale-derived-receipts
