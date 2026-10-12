#!/usr/bin/env bash
# Adapter only; accounting, DDP, logging and checkpoints remain canonical.
set -euo pipefail
: "${STOCKAGENT_ABLATION_FROZEN_RUNNER:?use the annual ablation entrypoint}"
exec bash "$STOCKAGENT_ABLATION_FROZEN_RUNNER" "$@" \
  --resume --no-retrain-completed-folds --no-profile-timing \
  --no-debug-timing-sync --no-isolate-train-folds
