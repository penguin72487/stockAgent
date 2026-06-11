#!/usr/bin/env bash
set -e

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
  if conda list "$p" | awk 'NR>3 {print $1}' | grep -qx "$p"; then
    echo "[OK]      $p"
  else
    echo "[MISSING] $p"
    missing+=("$p")
  fi
done

echo
echo "========== Python Import Check =========="

python - <<'PY'
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
