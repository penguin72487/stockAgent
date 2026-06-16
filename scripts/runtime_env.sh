#!/usr/bin/env bash

: "${FINTECH_ENV_PATH:=/home/user/miniforge3/envs/fintech}"
: "${FINTECH_MAMBA_BIN:=/home/user/miniforge3/micromamba}"

resolve_fintech_python() {
  if [[ -n "${PYTHON_BIN:-}" && -x "${PYTHON_BIN:-}" ]]; then
    printf "%s\n" "$PYTHON_BIN"
    return 0
  fi
  if [[ -x "$FINTECH_ENV_PATH/bin/python" ]]; then
    printf "%s\n" "$FINTECH_ENV_PATH/bin/python"
    return 0
  fi
  command -v python3 || command -v python
}

prepend_fintech_path() {
  if [[ -d "$FINTECH_ENV_PATH/bin" ]]; then
    export PATH="$FINTECH_ENV_PATH/bin:$PATH"
  fi
}

normalize_fintech_cuda_env() {
  local cuda_target="$FINTECH_ENV_PATH/targets/x86_64-linux"
  if [[ -f "$cuda_target/include/cuda_runtime.h" ]]; then
    export CUDA_PATH="$cuda_target"
    export CUDA_HOME="$cuda_target"
  elif [[ -f "$FINTECH_ENV_PATH/include/cuda_runtime.h" ]]; then
    export CUDA_PATH="$FINTECH_ENV_PATH"
    export CUDA_HOME="$FINTECH_ENV_PATH"
  elif [[ -n "${CUDA_PATH:-}" ]]; then
    export CUDA_HOME="$CUDA_PATH"
  fi
}

normalize_fintech_cuda_env
