# Interactive shells in the dedicated StockAgent tmux server.
if [[ -n "${STOCKAGENT_REPO_ROOT:-}" && -f "$STOCKAGENT_REPO_ROOT/scripts/runtime_env.sh" ]]; then
  source "$STOCKAGENT_REPO_ROOT/scripts/runtime_env.sh"
fi
PS1='[stockagent \W] \$ '
