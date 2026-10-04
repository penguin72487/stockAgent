#!/usr/bin/env bash
set -euo pipefail

agent_repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
agent_bin_dir=/usr/local/bin
agent_install_packages=true
while (($#)); do
  case "$1" in
    --bin-dir) agent_bin_dir="$2"; shift 2 ;;
    --skip-packages) agent_install_packages=false; shift ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
done

agent_missing=()
for agent_pair in 'tmux:tmux' 'rg:ripgrep' 'jq:jq' 'sqlite3:sqlite3'; do
  agent_command="${agent_pair%%:*}"
  if ! command -v "$agent_command" >/dev/null 2>&1 || [[ ! -x "/usr/bin/$agent_command" ]]; then
    agent_missing+=("${agent_pair#*:}")
  fi
done
if $agent_install_packages && ((${#agent_missing[@]})); then
  if [[ "$(id -u)" != 0 ]] || ! command -v apt-get >/dev/null 2>&1; then
    printf 'Install missing packages first: %s\n' "${agent_missing[*]}" >&2
    exit 1
  fi
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${agent_missing[@]}"
fi

agent_target="$agent_bin_dir/stockagent-agent"
if [[ -e "$agent_target" || -L "$agent_target" ]]; then
  if [[ ! -L "$agent_target" || "$(readlink -f -- "$agent_target")" != "$agent_repo/scripts/stockagent-agent.sh" ]]; then
    printf 'Existing launcher belongs to another installation: %s\n' "$agent_target" >&2
    exit 1
  fi
else
  mkdir -p -- "$agent_bin_dir"
  ln -s -- "$agent_repo/scripts/stockagent-agent.sh" "$agent_target"
fi
chmod 0755 "$agent_repo/scripts/stockagent-agent.sh" "$agent_repo/scripts/install_agent_workflow.sh"
mkdir -p -- "$agent_repo/artifacts/operations/agent-workflow"
chmod 0700 "$agent_repo/artifacts/operations/agent-workflow"
STOCKAGENT_REPO_ROOT="$agent_repo" "$agent_target" doctor --output "$agent_repo/artifacts/operations/agent-workflow/installation.json"
