from __future__ import annotations

import argparse
import importlib
import json
import os
import platform
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.runtime_env import normalize_runtime_env
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock


REQUIRED = ("numpy", "pyarrow", "yaml", "torch", "polars")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate and describe the selected stockAgent runtime.")
    parser.add_argument(
        "--require-cuda",
        action="store_true",
        help="fail when torch cannot use CUDA",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="treat runtime consistency warnings as failures",
    )
    parser.add_argument("--runtime-lock-output", type=Path,
                        help="write an exact installed-runtime identity after successful validation")
    parser.add_argument("--expected-runtime-lock", type=Path,
                        help="fail when installed packages or the platform differ from this lock")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    before = {
        name: os.environ.get(name)
        for name in (
            "CONDA_PREFIX",
            "CONDA_DEFAULT_ENV",
            "CUDA_PATH",
            "CUDA_HOME",
            "CUDA_ROOT",
            "CUDAToolkit_ROOT",
        )
    }
    python_prefix, cuda_root = normalize_runtime_env()

    modules: dict[str, str | None] = {}
    failures: list[str] = []
    warnings: list[str] = []
    for name in REQUIRED:
        try:
            module = importlib.import_module(name)
            modules[name] = str(getattr(module, "__version__", "installed"))
        except Exception as exc:
            modules[name] = None
            failures.append(f"{name}: {exc}")

    torch_info: dict[str, object] = {}
    if modules.get("torch") is not None:
        import torch

        torch_info = {
            "cuda_available": bool(torch.cuda.is_available()),
            "cuda_version": torch.version.cuda,
            "device_count": int(torch.cuda.device_count()),
            "devices": [torch.cuda.get_device_name(i) for i in range(torch.cuda.device_count())],
        }
        if args.require_cuda and not torch_info["cuda_available"]:
            failures.append("CUDA is required but torch.cuda.is_available() is false")

    conda_prefix = os.environ.get("CONDA_PREFIX")
    conda_prefix_matches = conda_prefix is None or Path(conda_prefix).resolve() == python_prefix
    if not conda_prefix_matches:
        failures.append(
            f"CONDA_PREFIX={conda_prefix!r} does not match Python prefix {str(python_prefix)!r}"
        )
    if python_prefix.name != "fintech":
        warnings.append(
            f"selected Python prefix is named {python_prefix.name!r}, not the preferred 'fintech'"
        )
    cuda_values = {
        name: os.environ.get(name)
        for name in ("CUDA_PATH", "CUDA_HOME", "CUDA_ROOT", "CUDAToolkit_ROOT")
    }
    nonempty_cuda_values = {value for value in cuda_values.values() if value}
    cuda_vars_consistent = len(nonempty_cuda_values) <= 1
    if not cuda_vars_consistent:
        failures.append("CUDA_PATH/CUDA_HOME/CUDA_ROOT/CUDAToolkit_ROOT disagree")
    if cuda_root is None:
        warnings.append("no CUDA toolkit root containing include/cuda_runtime.h was found")
    if args.strict and warnings:
        failures.extend(f"warning: {warning}" for warning in warnings)

    identity = runtime_identity()
    if identity.get("metadata_errors"):
        failures.append("runtime distribution metadata observation is incomplete")
    if args.expected_runtime_lock:
        try:
            expected = json.loads(args.expected_runtime_lock.read_text(encoding="utf-8"))
            if not isinstance(expected, dict):
                raise ValueError("runtime lock must be a JSON object")
            failures.extend(validate_runtime_lock(expected, identity))
        except (OSError, ValueError) as exc:
            failures.append(f"runtime lock: {exc}")

    report = {
        "python": sys.executable,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "conda_prefix": conda_prefix,
        "modules": modules,
        "torch": torch_info,
        "tools": {name: shutil.which(name) for name in ("ptxas", "nvcc", "git")},
        "repo": str(REPO_ROOT),
        "runtime": {
            "before_normalization": before,
            "python_prefix": str(python_prefix),
            "conda_prefix_matches_python": conda_prefix_matches,
            "cuda_root": str(cuda_root) if cuda_root is not None else None,
            "cuda_variables": cuda_values,
            "cuda_variables_consistent": cuda_vars_consistent,
            "path_head": os.environ.get("PATH", "").split(os.pathsep)[:5],
            "shell_selection": {
                name: os.environ.get(name)
                for name in ("PYTHON_BIN", "FINTECH_ENV_PATH", "STOCKAGENT_CUDA_ROOT")
                if os.environ.get(name)
            },
        },
        "warnings": warnings,
        "failures": failures,
        "runtime_identity": identity,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if failures:
        print("Environment check failed:\n- " + "\n- ".join(failures), file=sys.stderr)
        return 1
    if args.runtime_lock_output:
        from downloader.artifact_io import atomic_write_json
        atomic_write_json(args.runtime_lock_output, identity)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
