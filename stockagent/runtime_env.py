from __future__ import annotations

import os
import sys
from pathlib import Path


def _prepend_path(path: Path) -> None:
    if not path.exists():
        return
    value = str(path)
    parts = [part for part in os.environ.get("PATH", "").split(os.pathsep) if part]
    parts = [part for part in parts if part != value]
    os.environ["PATH"] = os.pathsep.join([value, *parts])


def _remove_prefix_from_path_var(name: str, prefix: Path) -> None:
    raw = os.environ.get(name, "")
    if not raw:
        return
    prefix_value = str(prefix)
    kept = [
        part
        for part in raw.split(os.pathsep)
        if part
        and part != prefix_value
        and not part.startswith(prefix_value + os.sep)
    ]
    os.environ[name] = os.pathsep.join(kept)


def active_python_prefix() -> Path:
    """Return the prefix belonging to the interpreter that is actually running."""

    return Path(sys.prefix).expanduser().resolve()


def normalize_python_env() -> Path:
    """Remove inherited environment metadata that disagrees with this Python.

    Calling an environment's Python by absolute path does not activate that
    environment: ``CONDA_PREFIX`` and ``PATH`` may still identify an IDE, CI,
    or parent-shell environment.  The executable interpreter is the only
    reliable source of truth once the process has started.
    """

    prefix = active_python_prefix()
    old_prefix_raw = os.environ.get("CONDA_PREFIX")
    if old_prefix_raw:
        old_prefix = Path(old_prefix_raw).expanduser().resolve()
        if old_prefix != prefix:
            for name in ("PATH", "LD_LIBRARY_PATH", "LIBRARY_PATH", "CPATH"):
                _remove_prefix_from_path_var(name, old_prefix)

    if (prefix / "conda-meta").is_dir():
        os.environ["CONDA_PREFIX"] = str(prefix)
        os.environ["CONDA_DEFAULT_ENV"] = prefix.name
        os.environ["CONDA_SHLVL"] = "1"
    else:
        os.environ.pop("CONDA_PREFIX", None)
        os.environ.pop("CONDA_DEFAULT_ENV", None)
        os.environ["CONDA_SHLVL"] = "0"
    for index in range(1, 10):
        os.environ.pop(f"CONDA_PREFIX_{index}", None)

    _prepend_path(Path(sys.executable).resolve().parent)
    return prefix


def _cuda_root_is_usable(path: Path) -> bool:
    return bool(path and (path / "include" / "cuda_runtime.h").exists())


def _cuda_root_candidates() -> list[Path]:
    candidates: list[Path] = []
    explicit_root = os.environ.get("STOCKAGENT_CUDA_ROOT")
    if explicit_root:
        candidates.append(Path(os.path.expandvars(explicit_root)).expanduser())

    # Prefer the runtime selected by sys.executable over inherited CUDA_HOME or
    # CONDA_PREFIX.  The latter commonly belong to a different parent shell.
    python_prefix = active_python_prefix()
    executable_prefix = Path(sys.executable).resolve().parent.parent
    for prefix in (python_prefix, executable_prefix):
        candidates.extend([prefix / "targets" / "x86_64-linux", prefix])

    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        prefix = Path(conda_prefix).expanduser().resolve()
        if prefix == python_prefix:
            candidates.extend([prefix / "targets" / "x86_64-linux", prefix])

    for env_name in ("CUDA_PATH", "CUDA_HOME", "CUDA_ROOT", "CUDAToolkit_ROOT"):
        raw = os.environ.get(env_name)
        if raw:
            candidates.append(Path(os.path.expandvars(raw)).expanduser())

    candidates.append(Path("/usr/local/cuda"))

    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        try:
            resolved = str(candidate.resolve())
        except Exception:
            resolved = str(candidate)
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(candidate)
    return unique


def normalize_cuda_env() -> Path | None:
    """Make CUDA-related env vars consistent across machines.

    RAPIDS/cuda-pathfinder warns when CUDA_PATH and CUDA_HOME disagree. The
    project should not depend on whether a machine installs the env under
    /root, /home/user, or another prefix, so we derive the CUDA root from the
    active Python/conda environment and then set both variables to the same
    usable root.
    """

    prefix = normalize_python_env()

    for candidate in _cuda_root_candidates():
        if not _cuda_root_is_usable(candidate):
            continue
        candidate = candidate.resolve()
        value = str(candidate)
        os.environ["CUDA_PATH"] = value
        os.environ["CUDA_HOME"] = value
        os.environ["CUDA_ROOT"] = value
        os.environ["CUDAToolkit_ROOT"] = value
        _prepend_path(candidate / "bin")
        if (prefix / "bin" / "nvcc").is_file():
            os.environ["CUDACXX"] = str(prefix / "bin" / "nvcc")
        elif (candidate / "bin" / "nvcc").is_file():
            os.environ["CUDACXX"] = str(candidate / "bin" / "nvcc")
        # Keep the environment's ptxas/nvcc ahead of a system toolkit when the
        # selected conda environment provides them.
        _prepend_path(Path(sys.executable).resolve().parent)
        return candidate
    for name in ("CUDA_PATH", "CUDA_HOME", "CUDA_ROOT", "CUDAToolkit_ROOT"):
        os.environ.pop(name, None)
    return None


def normalize_runtime_env() -> tuple[Path, Path | None]:
    """Normalize Python/Conda and CUDA state, returning both selected roots."""

    prefix = normalize_python_env()
    return prefix, normalize_cuda_env()
