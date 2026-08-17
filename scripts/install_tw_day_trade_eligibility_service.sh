#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${TW_DAY_TRADE_ELIGIBILITY_SERVICE_USER:-}" ]]; then
  export TW_PUBLIC_SERVICE_USER="$TW_DAY_TRADE_ELIGIBILITY_SERVICE_USER"
fi
exec "$repo_root/scripts/install_tw_public_publication_services.sh" "$@"
