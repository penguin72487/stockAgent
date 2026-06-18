#!/usr/bin/env bash
set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$ROOT_DIR/scripts/runtime_env.sh" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/scripts/runtime_env.sh"
  prepend_fintech_path
fi

PYTHON_BIN="${PYTHON_BIN:-$(resolve_fintech_python 2>/dev/null || true)}"
if [[ -x "$FINTECH_MAMBA_BIN" ]]; then
  CONDA_LIST=("$FINTECH_MAMBA_BIN" list -p "$FINTECH_ENV_PATH")
elif command -v mamba >/dev/null 2>&1; then
  CONDA_LIST=(mamba list -p "$FINTECH_ENV_PATH")
elif command -v conda >/dev/null 2>&1; then
  CONDA_LIST=(conda list -p "$FINTECH_ENV_PATH")
else
  echo "Neither micromamba, mamba, nor conda found." >&2
  exit 2
fi

echo "========== CUDA-X Package Check =========="

need=(
  cuda-toolkit
  cuda-libraries
  cuda-libraries-dev
  cuda-command-line-tools
  cuda-compiler
  cuda-nvcc
  cuda-nvrtc
  cuda-cupti
  cuda-nvtx
  cudnn
  libcudnn-dev
  nccl
  libcublas
  libcublas-dev
  libcufft
  libcufft-dev
  libcurand
  libcurand-dev
  libcusolver
  libcusolver-dev
  libcusparse
  libcusparse-dev
)

missing=()

for p in "${need[@]}"; do
  if "${CONDA_LIST[@]}" | awk 'NR>3 {print $1}' | grep -qx "$p"; then
    echo "[OK]      $p"
  else
    echo "[MISSING] $p"
    missing+=("$p")
  fi
done

echo
echo "========== Python Import Check =========="

"$PYTHON_BIN" - <<'PY'
import importlib.util

def has_module(name):
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        return False

for pkg in ["torch", "triton", "cupy", "tensorrt"]:
    print(f"{pkg:12s}:", "FOUND" if has_module(pkg) else "NOT FOUND")
PY

echo
if [ "${#missing[@]}" -eq 0 ]; then
  echo "All core CUDA-X conda packages are installed."
else
  echo "Install missing packages with:"
  echo "mamba install -c conda-forge ${missing[*]}"
fi

echo
echo "If TensorRT is missing, try:"
echo "mamba install -c conda-forge tensorrt"
echo "or:"
echo "mamba install -c nvidia -c conda-forge tensorrt"
