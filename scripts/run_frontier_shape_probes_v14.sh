#!/usr/bin/env bash
# Finish corrected packages and representative dimensions before resuming folds.
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
(p / "shape-probes-v14-exit.json").write_text(json.dumps({"completed": rc == 0, "returncode": rc}))
PY
}
trap finish EXIT
run_fintech_python scripts/run_frontier_operator_queue.py \
  --output "$task_root/operators-v14-cross-softmax" \
  --after-exit "$task_root/operators-v11/queue-exit.json" \
  --rows 2048 --width 2757 --cases softmax_quack_padded
run_fintech_python scripts/run_frontier_operator_queue.py \
  --output "$task_root/operators-v14-temporal-attention" \
  --rows 4096 --cases attention_reduce_compile,attention_flash4_nomask
run_fintech_python scripts/run_frontier_operator_queue.py \
  --output "$task_root/operators-v14-compact-gemm" \
  --rows 16384 --width 1024 --output-width 64 \
  --cases gemm_torch,gemm_autotune,gemm_quack_tuned,gemm_torchao_fp8_compile,gemm_deepgemm,gemm_te_fp8
