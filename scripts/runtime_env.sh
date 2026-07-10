#!/usr/bin/env bash

# Portable runtime discovery for every shell entrypoint in this repository.
# Source this file before invoking Python.  Sourcing is intentionally
# idempotent and makes the runtime selected by PYTHON_BIN/FINTECH_ENV_PATH the
# source of truth for CONDA_PREFIX, PATH, and CUDA discovery.

_runtime_resolve_executable() {
  local candidate="${1:-}"
  [[ -n "$candidate" ]] || return 1
  if [[ "$candidate" == */* ]]; then
    [[ -x "$candidate" ]] || return 1
    printf "%s\n" "$candidate"
    return 0
  fi
  command -v "$candidate" 2>/dev/null
}

_runtime_python_prefix() {
  local python_path
  python_path="$(_runtime_resolve_executable "${1:-}")" || return 1
  if command -v readlink >/dev/null 2>&1; then
    python_path="$(readlink -f "$python_path" 2>/dev/null || printf "%s" "$python_path")"
  fi
  [[ "$(basename "$(dirname "$python_path")")" == "bin" ]] || return 1
  dirname "$(dirname "$python_path")"
}

detect_mamba_or_conda_bin() {
  local candidate
  for candidate in "${FINTECH_MAMBA_BIN:-}" micromamba mamba conda; do
    [[ -n "$candidate" ]] || continue
    if _runtime_resolve_executable "$candidate" >/dev/null 2>&1; then
      _runtime_resolve_executable "$candidate"
      return 0
    fi
  done
  return 1
}

detect_fintech_env_path() {
  if [[ -n "${FINTECH_ENV_PATH:-}" && -x "$FINTECH_ENV_PATH/bin/python" ]]; then
    printf "%s\n" "$FINTECH_ENV_PATH"
    return 0
  fi

  # An arbitrary active environment (for example /venv/main in CI or an IDE)
  # is not the project's runtime merely because CONDA_PREFIX is populated.
  if [[ -n "${CONDA_PREFIX:-}" \
      && ( "${CONDA_DEFAULT_ENV:-}" == "fintech" || "${CONDA_PREFIX##*/}" == "fintech" ) \
      && -x "$CONDA_PREFIX/bin/python" ]]; then
    printf "%s\n" "$CONDA_PREFIX"
    return 0
  fi

  local candidate env_root
  local -a dynamic_candidates=()
  IFS=':' read -r -a _fintech_env_roots <<< "${CONDA_ENVS_PATH:-}"
  for env_root in "${_fintech_env_roots[@]}"; do
    [[ -n "$env_root" ]] && dynamic_candidates+=("$env_root/fintech")
  done
  for candidate in \
    "$HOME/miniforge3/envs/fintech" \
    "$HOME/mambaforge/envs/fintech" \
    "$HOME/miniconda3/envs/fintech" \
    "$HOME/anaconda3/envs/fintech" \
    "/venv/fintech" \
    "/root/miniforge3/envs/fintech" \
    "/home/user/miniforge3/envs/fintech" \
    "/opt/conda/envs/fintech" \
    "${dynamic_candidates[@]}"; do
    if [[ -x "$candidate/bin/python" ]]; then
      printf "%s\n" "$candidate"
      return 0
    fi
  done

  local manager
  manager="$(detect_mamba_or_conda_bin 2>/dev/null || true)"
  if [[ -n "$manager" ]]; then
    candidate="$("$manager" env list --json 2>/dev/null | sed -n 's/.*"\([^" ]*\/fintech\)".*/\1/p' | head -n 1)"
    if [[ -n "$candidate" && -x "$candidate/bin/python" ]]; then
      printf "%s\n" "$candidate"
      return 0
    fi
  fi
  return 1
}

resolve_fintech_python() {
  local candidate
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    candidate="$(_runtime_resolve_executable "$PYTHON_BIN" 2>/dev/null || true)"
    if [[ -n "$candidate" ]]; then
      printf "%s\n" "$candidate"
      return 0
    fi
  fi
  if [[ -n "${FINTECH_ENV_PATH:-}" && -x "$FINTECH_ENV_PATH/bin/python" ]]; then
    printf "%s\n" "$FINTECH_ENV_PATH/bin/python"
    return 0
  fi
  command -v python3 2>/dev/null || command -v python 2>/dev/null
}

_runtime_remove_prefix_from_path_var() {
  local var_name="$1" prefix="${2:-}" current part joined=""
  [[ -n "$prefix" ]] || return 0
  current="${!var_name:-}"
  IFS=':' read -r -a _runtime_path_parts <<< "$current"
  for part in "${_runtime_path_parts[@]}"; do
    [[ -n "$part" ]] || continue
    [[ "$part" == "$prefix" || "$part" == "$prefix/"* ]] && continue
    joined="${joined:+$joined:}$part"
  done
  printf -v "$var_name" '%s' "$joined"
  export "$var_name"
}

_runtime_prepend_path() {
  local value="${1:-}" current part joined=""
  [[ -n "$value" && -d "$value" ]] || return 0
  current="${PATH:-}"
  IFS=':' read -r -a _runtime_path_parts <<< "$current"
  for part in "${_runtime_path_parts[@]}"; do
    [[ -n "$part" && "$part" != "$value" ]] || continue
    joined="${joined:+$joined:}$part"
  done
  export PATH="$value${joined:+:$joined}"
}

_runtime_cuda_root_usable() {
  [[ -n "${1:-}" && -f "$1/include/cuda_runtime.h" ]]
}

resolve_fintech_cuda_root() {
  local env_path="${1:-${FINTECH_ENV_PATH:-}}" candidate
  if [[ -n "${STOCKAGENT_CUDA_ROOT:-}" ]]; then
    _runtime_cuda_root_usable "$STOCKAGENT_CUDA_ROOT" || return 1
    printf "%s\n" "$STOCKAGENT_CUDA_ROOT"
    return 0
  fi
  for candidate in \
    "$env_path/targets/x86_64-linux" \
    "$env_path" \
    "${CUDA_PATH:-}" \
    "${CUDA_HOME:-}" \
    "${CUDA_ROOT:-}" \
    "${CUDAToolkit_ROOT:-}" \
    "/usr/local/cuda"; do
    if _runtime_cuda_root_usable "$candidate"; then
      printf "%s\n" "$candidate"
      return 0
    fi
  done
  return 1
}

normalize_fintech_cuda_env() {
  local cuda_root
  cuda_root="$(resolve_fintech_cuda_root "${FINTECH_ENV_PATH:-}" 2>/dev/null || true)"
  if [[ -z "$cuda_root" ]]; then
    unset CUDA_PATH CUDA_HOME CUDA_ROOT CUDAToolkit_ROOT
    return 0
  fi
  export CUDA_PATH="$cuda_root"
  export CUDA_HOME="$cuda_root"
  export CUDA_ROOT="$cuda_root"
  export CUDAToolkit_ROOT="$cuda_root"
  if [[ -x "${FINTECH_ENV_PATH:-}/bin/nvcc" ]]; then
    export CUDACXX="$FINTECH_ENV_PATH/bin/nvcc"
  elif [[ -x "$cuda_root/bin/nvcc" ]]; then
    export CUDACXX="$cuda_root/bin/nvcc"
  fi
  _runtime_prepend_path "$cuda_root/bin"
}

activate_fintech_runtime() {
  local selected_python selected_prefix old_prefix stack_index
  selected_python="$(resolve_fintech_python 2>/dev/null || true)"
  [[ -n "$selected_python" ]] || return 1
  selected_prefix="$(_runtime_python_prefix "$selected_python" 2>/dev/null || true)"
  [[ -n "$selected_prefix" ]] || return 1

  old_prefix="${CONDA_PREFIX:-}"
  if [[ -n "$old_prefix" && "$old_prefix" != "$selected_prefix" ]]; then
    _runtime_remove_prefix_from_path_var PATH "$old_prefix"
    _runtime_remove_prefix_from_path_var LD_LIBRARY_PATH "$old_prefix"
    _runtime_remove_prefix_from_path_var LIBRARY_PATH "$old_prefix"
    _runtime_remove_prefix_from_path_var CPATH "$old_prefix"
  fi

  # Preserve PYTHON_BIN only when the caller supplied it.  FINTECH_ENV_PATH can
  # then still be changed after sourcing (for example by a daemon env file)
  # without an auto-populated PYTHON_BIN pinning the previous selection.
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    export PYTHON_BIN="$selected_python"
  fi
  export FINTECH_ENV_PATH="$selected_prefix"
  if [[ -d "$selected_prefix/conda-meta" || "${selected_prefix##*/}" == "fintech" ]]; then
    export CONDA_PREFIX="$selected_prefix"
    export CONDA_DEFAULT_ENV="${selected_prefix##*/}"
    export CONDA_SHLVL=1
  else
    unset CONDA_PREFIX CONDA_DEFAULT_ENV
    export CONDA_SHLVL=0
  fi
  for stack_index in {1..9}; do
    unset "CONDA_PREFIX_${stack_index}" 2>/dev/null || true
  done

  normalize_fintech_cuda_env
  # The selected environment wins over both an inherited env and a system CUDA.
  _runtime_prepend_path "$selected_prefix/bin"
}

# Backward-compatible name used by the existing shell entrypoints.
prepend_fintech_path() {
  activate_fintech_runtime
}

# Consistent function for interactive commands after sourcing this file:
#   run_fintech_python -m pytest -q -s test
run_fintech_python() {
  local selected_python
  activate_fintech_runtime || {
    printf "Unable to resolve a Python runtime; set FINTECH_ENV_PATH or PYTHON_BIN.\n" >&2
    return 2
  }
  selected_python="$(resolve_fintech_python)" || return 2
  "$selected_python" "$@"
}

FINTECH_ENV_PATH="${FINTECH_ENV_PATH:-$(detect_fintech_env_path 2>/dev/null || true)}"
FINTECH_MAMBA_BIN="${FINTECH_MAMBA_BIN:-$(detect_mamba_or_conda_bin 2>/dev/null || true)}"

# Sourcing this file is sufficient; callers do not need a second activation
# step.  Keep failure non-fatal so diagnostic scripts can explain a missing env.
activate_fintech_runtime 2>/dev/null || true
