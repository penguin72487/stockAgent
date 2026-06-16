#!/usr/bin/env bash

detect_fintech_env_path() {
  if [[ -n "${FINTECH_ENV_PATH:-}" && -x "$FINTECH_ENV_PATH/bin/python" ]]; then
    printf "%s\n" "$FINTECH_ENV_PATH"
    return 0
  fi
  if [[ -n "${CONDA_PREFIX:-}" && -x "$CONDA_PREFIX/bin/python" ]]; then
    printf "%s\n" "$CONDA_PREFIX"
    return 0
  fi

  local candidate
  for candidate in \
    "$HOME/miniforge3/envs/fintech" \
    "$HOME/mambaforge/envs/fintech" \
    "$HOME/miniconda3/envs/fintech" \
    "$HOME/anaconda3/envs/fintech" \
    "/root/miniforge3/envs/fintech" \
    "/home/user/miniforge3/envs/fintech"; do
    if [[ -x "$candidate/bin/python" ]]; then
      printf "%s\n" "$candidate"
      return 0
    fi
  done

  return 1
}

detect_mamba_or_conda_bin() {
  local candidate
  for candidate in "${FINTECH_MAMBA_BIN:-}" micromamba mamba conda; do
    if [[ -z "$candidate" ]]; then
      continue
    fi
    if [[ -x "$candidate" ]]; then
      printf "%s\n" "$candidate"
      return 0
    fi
    if command -v "$candidate" >/dev/null 2>&1; then
      command -v "$candidate"
      return 0
    fi
  done
  return 1
}

FINTECH_ENV_PATH="${FINTECH_ENV_PATH:-$(detect_fintech_env_path 2>/dev/null || true)}"
FINTECH_MAMBA_BIN="${FINTECH_MAMBA_BIN:-$(detect_mamba_or_conda_bin 2>/dev/null || true)}"

resolve_fintech_python() {
  if [[ -n "${PYTHON_BIN:-}" && -x "${PYTHON_BIN:-}" ]]; then
    printf "%s\n" "$PYTHON_BIN"
    return 0
  fi
  if [[ -n "${FINTECH_ENV_PATH:-}" && -x "$FINTECH_ENV_PATH/bin/python" ]]; then
    printf "%s\n" "$FINTECH_ENV_PATH/bin/python"
    return 0
  fi
  command -v python3 || command -v python
}

prepend_fintech_path() {
  if [[ -n "${FINTECH_ENV_PATH:-}" && -d "$FINTECH_ENV_PATH/bin" ]]; then
    export PATH="$FINTECH_ENV_PATH/bin:$PATH"
  fi
}
