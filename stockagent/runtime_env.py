from __future__ import annotations

import os
import sys
from pathlib import Path


def _prepend_path(path: Path) -> None:
    if not path.exists():
        return
    value = str(path)
    parts = [part for part in os.environ.get("PATH", "").split(os.pathsep) if part]
    if value not in parts:
        os.environ["PATH"] = os.pathsep.join([value, *parts])


def _cuda_root_is_usable(path: Path) -> bool:
    return bool(path and (path / "include" / "cuda_runtime.h").exists())


def _cuda_root_candidates() -> list[Path]:
    candidates: list[Path] = []
    for env_name in ("STOCKAGENT_CUDA_ROOT", "CUDA_PATH", "CUDA_HOME", "CUDAToolkit_ROOT"):
        raw = os.environ.get(env_name)
        if raw:
            candidates.append(Path(os.path.expandvars(raw)).expanduser())

    for raw_prefix in (os.environ.get("CONDA_PREFIX"), sys.prefix):
        if not raw_prefix:
            continue
        prefix = Path(raw_prefix).expanduser()
        candidates.extend([prefix / "targets" / "x86_64-linux", prefix])

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

    env_bin = Path(sys.executable).resolve().parent
    _prepend_path(env_bin)

    for candidate in _cuda_root_candidates():
        if not _cuda_root_is_usable(candidate):
            continue
        value = str(candidate)
        os.environ["CUDA_PATH"] = value
        os.environ["CUDA_HOME"] = value
        os.environ["CUDAToolkit_ROOT"] = value
        _prepend_path(candidate / "bin")
        return candidate
    return None
