#!/usr/bin/env bash
set -euo pipefail

# Build xFormers from source for current environment:
# - Python 3.12
# - CUDA toolkit 13.2
# - GPU arch sm_89 (RTX 4070 Ti SUPER)

if ! command -v python >/dev/null 2>&1; then
  echo "python not found"
  exit 1
fi
if ! command -v nvcc >/dev/null 2>&1; then
  echo "nvcc not found. Please install CUDA toolkit 13.2 and ensure PATH includes nvcc."
  exit 1
fi

echo "[env] python: $(python -V)"
echo "[env] nvcc:"
nvcc --version | tail -n 1

# Prefer the active env's nvcc location.
NVCC_PATH="$(command -v nvcc)"
CUDA_HOME="$(dirname "$(dirname "$NVCC_PATH")")"
export CUDA_HOME
export PATH="$CUDA_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$CUDA_HOME/lib64:${LD_LIBRARY_PATH:-}"

# Build flags tuned for Ada Lovelace (sm_89)
export TORCH_CUDA_ARCH_LIST="8.9"
export FORCE_CUDA=1
export MAX_JOBS="${MAX_JOBS:-$(nproc)}"

# Some toolchains still require this with newer CUDA + gcc combos.
export NVCC_FLAGS="-allow-unsupported-compiler"

# Make sure build-time deps are present in this env.
python -m pip install -U pip setuptools wheel ninja cmake packaging

# Remove incompatible prebuilt wheel if present.
python -m pip uninstall -y xformers || true

# Clone and build from source without build isolation so torch is visible.
WORKDIR="${WORKDIR:-$PWD/.build/xformers-src}"
mkdir -p "$WORKDIR"
if [[ ! -d "$WORKDIR/.git" ]]; then
  git clone --recursive https://github.com/facebookresearch/xformers.git "$WORKDIR"
fi

pushd "$WORKDIR" >/dev/null
git fetch --tags --prune
# Use latest stable tag known to work with torch 2.10 series.
git checkout v0.0.35
git submodule sync --recursive
git submodule update --init --recursive

if [[ ! -f "third_party/cutlass/include/cutlass/cutlass.h" ]]; then
  echo "[error] CUTLASS submodule is still missing after init/update"
  exit 1
fi

python -m pip install -v --no-build-isolation --no-deps .
popd >/dev/null

# Validation
python -m xformers.info
python - <<'PY'
import torch
import xformers.ops as xops

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
q = torch.randn(2, 64, 4, 32, device=device, dtype=torch.bfloat16)
k = torch.randn(2, 64, 4, 32, device=device, dtype=torch.bfloat16)
v = torch.randn(2, 64, 4, 32, device=device, dtype=torch.bfloat16)
out = xops.memory_efficient_attention(q, k, v, p=0.0)
print('xformers attention ok:', tuple(out.shape))
PY

echo "[done] xFormers source build and validation complete"
