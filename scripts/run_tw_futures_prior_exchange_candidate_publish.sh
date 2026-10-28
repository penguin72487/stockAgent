#!/usr/bin/env bash
# Publish an independently compared research candidate with its frozen code.
set -euo pipefail

if [[ $# -ne 6 ]]; then
  printf 'Usage: %s CODE_RECEIPT CANDIDATE AUDIT AUDIT_SCRIPT PUBLISH_SCRIPT REPORT\n' "$0" >&2
  exit 2
fi
receipt="$1"
candidate="$2"
audit="$3"
audit_script="$4"
publisher="$5"
report="$6"
workspace="${STOCKAGENT_WORKSPACE:-$(git rev-parse --show-toplevel)}"
cd "$workspace"
if [[ -z "${FINTECH_ENV_PATH:-}" && -f /etc/supervisor/conf.d/stockagent-futures-halt345-v9-complete.conf ]]; then
  FINTECH_ENV_PATH="$(sed -n 's/^environment=FINTECH_ENV_PATH="\([^"]*\)".*/\1/p' /etc/supervisor/conf.d/stockagent-futures-halt345-v9-complete.conf)"
  [[ -n "$FINTECH_ENV_PATH" ]] || { printf '%s\n' 'Existing futures runtime selection is missing' >&2; exit 1; }
  export FINTECH_ENV_PATH
fi
code="$(dirname "$receipt")/build-source"
source "$code/scripts/runtime_env.sh"
export PYTHONPATH="$code" PYTHONDONTWRITEBYTECODE=1
exec_fintech_python "$publisher" --receipt "$receipt" --candidate "$candidate" \
  --audit "$audit" --audit-script "$audit_script" \
  --final-settlement "$workspace/data_tw_index_futures/preparation_sources/margin_sources/final/futures_final_settlement_history.parquet" \
  --output "$candidate/release" --report "$report"
