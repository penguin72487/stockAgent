#!/usr/bin/env bash
set -euo pipefail

REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-/workspace/stockAgent}"
REMOTE_ENV_DIR="${REMOTE_ENV_DIR:-/home/user/miniforge3/envs/fintech}"
ENV_ARCHIVE="${ENV_ARCHIVE:-/workspace/fintech-env.tar.gz}"

usage() {
  cat <<'EOF'
Usage: bash scripts/setup_vast.sh [options]

Options:
  --project-dir <path>    Project directory on Vast (default: /workspace/stockAgent)
  --env-dir <path>        fintech env install path (default: /home/user/miniforge3/envs/fintech)
  --env-archive <path>    conda-pack archive path (default: /workspace/fintech-env.tar.gz)
  -h, --help              Show this help

Environment overrides:
  REMOTE_PROJECT_DIR, REMOTE_ENV_DIR, ENV_ARCHIVE
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      REMOTE_PROJECT_DIR="$2"
      shift 2
      ;;
    --env-dir)
      REMOTE_ENV_DIR="$2"
      shift 2
      ;;
    --env-archive)
      ENV_ARCHIVE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "$REMOTE_PROJECT_DIR" ]]; then
  echo "Project directory not found: $REMOTE_PROJECT_DIR" >&2
  exit 2
fi

mkdir -p "$REMOTE_ENV_DIR"

if [[ ! -x "$REMOTE_ENV_DIR/bin/python" && ! -f "$ENV_ARCHIVE" ]]; then
  echo "Neither existing env python nor env archive found." >&2
  echo "Missing python: $REMOTE_ENV_DIR/bin/python" >&2
  echo "Missing archive: $ENV_ARCHIVE" >&2
  exit 2
fi

if [[ ! -x "$REMOTE_ENV_DIR/bin/python" ]]; then
  echo "[setup-vast] extracting env: $ENV_ARCHIVE -> $REMOTE_ENV_DIR"
  tar -xzf "$ENV_ARCHIVE" -C "$REMOTE_ENV_DIR"
else
  echo "[setup-vast] existing env found: $REMOTE_ENV_DIR"
fi

if [[ -x "$REMOTE_ENV_DIR/bin/conda-unpack" ]]; then
  echo "[setup-vast] running conda-unpack"
  "$REMOTE_ENV_DIR/bin/conda-unpack"
fi

# Keep compatibility with project docs/configs and older absolute paths.
mkdir -p /root /home/user
if [[ "$REMOTE_PROJECT_DIR" != "/root/stockAgent" && ! -e /root/stockAgent ]]; then
  ln -s "$REMOTE_PROJECT_DIR" /root/stockAgent
fi
if [[ "$REMOTE_PROJECT_DIR" != "/home/user/stockAgent" && ! -e /home/user/stockAgent ]]; then
  ln -s "$REMOTE_PROJECT_DIR" /home/user/stockAgent
fi

export FINTECH_ENV_PATH="$REMOTE_ENV_DIR"
export PYTHON_BIN="$REMOTE_ENV_DIR/bin/python"
export PATH="$REMOTE_ENV_DIR/bin:$PATH"

if [[ -f "$REMOTE_PROJECT_DIR/scripts/runtime_env.sh" ]]; then
  # shellcheck disable=SC1091
  source "$REMOTE_PROJECT_DIR/scripts/runtime_env.sh"
  prepend_fintech_path
fi

cd "$REMOTE_PROJECT_DIR"

echo "[setup-vast] python: $("$PYTHON_BIN" -V)"
if command -v nvidia-smi >/dev/null 2>&1; then
  nvidia-smi
else
  echo "[setup-vast] nvidia-smi not found"
fi

"$PYTHON_BIN" - <<'PY'
import shutil
import torch

print(f"[setup-vast] torch={torch.__version__} torch_cuda={torch.version.cuda}")
print(f"[setup-vast] cuda_available={torch.cuda.is_available()} device_count={torch.cuda.device_count()}")
if torch.cuda.is_available():
    print(f"[setup-vast] cuda_device={torch.cuda.get_device_name(0)}")
ptxas = shutil.which("ptxas")
print(f"[setup-vast] ptxas={ptxas}")
PY

"$PYTHON_BIN" -m py_compile \
  stockagent/config.py \
  stockagent/training/trainer.py \
  stockagent/training/loss.py \
  stockagent/backtest/simulator.py

echo "[setup-vast] ready"
