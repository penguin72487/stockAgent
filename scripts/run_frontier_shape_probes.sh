#!/usr/bin/env bash
# Serialized representative shapes. Parent full-fold queue stays held until
# probes-exit.json exists; each operator queue owns the canonical GPU lease.
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
(p / "shape-probes-v8-exit.json").write_text(json.dumps({"completed": rc == 0, "returncode": rc}))
PY
}
trap finish EXIT
run_fintech_python scripts/run_frontier_operator_queue.py \
  --output "$task_root/operators-v8" \
  --after-exit "$task_root/full-fold-phase1/bf16_temporal_portfolio/exit.json" \
  --cases attention_flash4,gemm_te_fp8,gemm_te_mxfp8,gemm_te_nvfp4,gemm_cublasdx,gemm_cusparselt,gemm_quack_tuned,gemm_torchao_fp8_compile,swiglu_thunderkittens,softmax_torch,softmax_compile,softmax_quack
run_fintech_python scripts/run_frontier_operator_queue.py \
  --output "$task_root/operators-v8-cross-softmax" --rows 2048 --width 2757 \
  --cases softmax_torch,softmax_compile,softmax_quack
run_fintech_python scripts/run_frontier_operator_queue.py \
  --output "$task_root/operators-v8-temporal-attention" --rows 4096 \
  --cases attention_math,attention_compile,attention_flash4,attention_cudnn,attention_flex_padded
run_fintech_python scripts/run_frontier_operator_queue.py \
  --output "$task_root/operators-v8-portfolio-attention" --rows 32 --query-steps 16 --key-steps 2757 \
  --cases attention_math,attention_compile,attention_flash4,attention_cudnn,attention_flex_padded
