#!/usr/bin/env bash
set -euo pipefail

# A symlink installation still discovers the checkout that owns this script.
agent_script="$(readlink -f -- "${BASH_SOURCE[0]}")"
agent_cwd_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -n "$agent_cwd_root" && -f "$agent_cwd_root/scripts/runtime_env.sh" ]]; then
  agent_default_repo="$agent_cwd_root"
else
  agent_default_repo="$(cd -- "$(dirname -- "$agent_script")/.." && pwd -P)"
fi
agent_repo="${STOCKAGENT_REPO_ROOT:-$agent_default_repo}"
if [[ ! -f "$agent_repo/scripts/agent_workflow.py" ]]; then
  printf 'Agent workflow not found in %s\n' "$agent_repo" >&2
  exit 1
fi
export STOCKAGENT_REPO_ROOT="$agent_repo"
if [[ "${1:-}" == "work" ]]; then
  shift
  exec /bin/bash "$agent_repo/scripts/stockagent-control.sh" "$@"
fi
source "$agent_repo/scripts/runtime_env.sh"
run_fintech_python "$agent_repo/scripts/agent_workflow.py" "$@"
