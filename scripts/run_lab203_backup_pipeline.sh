#!/usr/bin/env bash
set -euo pipefail
relay_pipeline_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$relay_pipeline_repo"
set -a
source /etc/lab203-backup/backup-pipeline-v10.env
set +a
unset PYTHON_BIN
source scripts/runtime_env.sh
run_fintech_python scripts/run_lab203_backup_pipeline.py --configuration /etc/lab203-backup/backup-pipeline-v10.json
