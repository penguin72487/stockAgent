#!/usr/bin/env bash
set -euo pipefail
if [[ "$(hostname)" == penguin && "$(readlink /proc/self/ns/mnt)" != "$(readlink /proc/1/ns/mnt)" ]]; then
  exec nsenter -t 1 -m -- /usr/bin/bash "$(readlink -f "${BASH_SOURCE[0]}")" "$@"
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
source scripts/runtime_env.sh
python_bin="$(resolve_fintech_python)"

cohort_args=()
if [[ "${1:-}" == "--cohort" ]]; then
  readiness_args=()
  case "${2:-}" in
    main) cohort_root=/var/lib/stockagent-vast-legacy-return ;;
    partitions) cohort_root=/var/lib/stockagent-vast-legacy-return-partitions ;;
    reviewed-main-20261006) cohort_root=/var/lib/stockagent-vast-reviewed-return-20261006 ;;
    reviewed-panel-20261006)
      cohort_root=/var/lib/stockagent-vast-reviewed-partitions-20261006
      readiness_args=(--selection "$cohort_root/scope-selection.json")
      ;;
    *) exit 64 ;;
  esac
  shift 2
  cohort_args=(--state-root "$cohort_root")
  if [[ "$cohort_root" == /var/lib/stockagent-vast-reviewed-* || "${STOCKAGENT_LEGACY_REVIEWED_20261006:-0}" == 1 ]]; then
    cohort_args+=(--policy configs/data_sync/vastai_reviewed_legacy_return_20261006.json)
  fi
  cohort_args+=("${readiness_args[@]}")
  if [[ "${1:-}" == "--check-ready" ]]; then
    [[ "$#" == 1 ]] || exit 64
    exec "$python_bin" scripts/check_legacy_return_ready.py "$cohort_root" "${readiness_args[@]}"
  fi
fi

# Share the canonical cohort/ingress owners; keep bulk preservation behind
# the data, web and execution services on penguin.
exec ionice -c 3 nice -n 19 /usr/bin/bash \
  scripts/run_authority_storage_operation.sh \
  scripts/return_remote_legacy_archives.py "$@" "${cohort_args[@]}"
