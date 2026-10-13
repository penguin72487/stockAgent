#!/usr/bin/env bash
set -euo pipefail
task_root=${STOCKAGENT_FRONTIER_PACKAGE_ROOT:?}
code_root=${STOCKAGENT_FRONTIER_CODE_ROOT:?}
cd "$code_root"
source scripts/runtime_env.sh
finish() {
  local probe_exit=$?
  run_fintech_python - "$probe_exit" <<'PY'
import json, os, sys
from pathlib import Path
p = Path(os.environ["STOCKAGENT_FRONTIER_PACKAGE_ROOT"])
rc = int(sys.argv[1])
(p / "layernorm-probes-v17-exit.json").write_text(json.dumps({"completed": rc == 0, "returncode": rc}))
PY
}
trap finish EXIT
run_fintech_python scripts/run_frontier_operator_queue.py \
  --output "$task_root/operators-v17-layernorm" \
  --after-exit "$task_root/full-fold-phase2/queue-exit.json" \
  --rows 4096 --width 32 --cases layernorm_quack
run_fintech_python scripts/run_frontier_operator_queue.py \
  --output "$task_root/operators-v17-wide-layernorm" \
  --rows 4096 --width 14726 --cases layernorm_torch,layernorm_compile,layernorm_quack
