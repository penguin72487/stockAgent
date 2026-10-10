from __future__ import annotations

import argparse
import csv
import importlib
import json
import os
import platform
import shutil
import stat
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.runtime_env import normalize_runtime_env
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock


REQUIRED = ("numpy", "pyarrow", "yaml", "torch", "polars")


def _torch_cuda_report(torch, *, require_cuda: bool, minimum_devices: int = 1) -> tuple[dict, list[str]]:
    """Separate NVML discovery from initialized, executable CUDA devices.

    CUDA visibility is inherited before this process imports Torch and is never
    changed here. This process exits after preflight; its CUDA/RNG state cannot
    contaminate a trainer or the long-lived ablation scheduler.
    """
    info = {
        "cuda_available": False,
        "cuda_version": torch.version.cuda,
        "device_count": 0,
        "devices": [],
        "cuda_runtime_initialized": False,
        "cuda_compute_verified": False,
        "verified_device_indices": [],
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "minimum_cuda_devices": minimum_devices if require_cuda else None,
        "probe_errors": [],
        "probe_contract": "initialized_compute_cuda_preflight_v2",
    }

    def error(stage: str, exc: Exception) -> None:
        info["probe_errors"].append({"stage": stage, "type": type(exc).__name__, "message": str(exc)})

    available = False
    try:
        available = bool(torch.cuda.is_available())
    except Exception as exc:
        error("availability", exc)
    try:
        info["device_count"] = int(torch.cuda.device_count())
    except Exception as exc:
        error("discovery", exc)
    count = info["device_count"]
    if not available or count <= 0 or info["probe_errors"]:
        reasons = [f"CUDA {item['stage']} failed: {item['type']}: {item['message']}"
                   for item in info["probe_errors"]]
        if require_cuda or count > 0:
            reasons.append(f"CUDA compute is unavailable; discovered devices={count}. "
                           "NVML device discovery is not proof of CUDA initialization.")
        return info, reasons
    if require_cuda and count < minimum_devices:
        return info, [f"CUDA requires at least {minimum_devices} devices; discovered {count}"]
    try:
        torch.cuda.init()
        info["cuda_runtime_initialized"] = True
    except Exception as exc:
        error("initialization", exc)
    if info["cuda_runtime_initialized"]:
        for index in range(count):
            try:
                name = torch.cuda.get_device_name(index)
                info["devices"].append(name)
                if require_cuda:
                    value = torch.ones(1, device=f"cuda:{index}")
                    torch.cuda.synchronize(index)
                    if float(value.item()) != 1.0:
                        raise RuntimeError("one-element CUDA allocation/kernel verification failed")
                    info["verified_device_indices"].append(index)
                    del value
            except Exception as exc:
                error(f"device:{index}", exc)
                break
    info["cuda_compute_verified"] = bool(require_cuda and len(info["verified_device_indices"]) == count)
    info["cuda_available"] = bool(info["cuda_runtime_initialized"] and not info["probe_errors"])
    reasons = [f"CUDA {item['stage']} failed: {item['type']}: {item['message']}"
               for item in info["probe_errors"]]
    return info, reasons


def _gpu_recovery_diagnostics() -> dict:
    """NVML recovery flags explain host ownership; they never prove compute health."""
    info = {"status": "unavailable", "gpus": [], "host_os_reboot_required": None}
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,uuid,gpu_recovery_action", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        if result.returncode:
            raise RuntimeError(f"nvidia-smi exit {result.returncode}: {result.stderr.strip()[:1024]}")
        gpus = []
        for row in csv.reader(result.stdout.splitlines()):
            if not row:
                continue
            if len(row) != 3 or not row[0].strip().isdigit() or not row[1].strip().startswith("GPU-"):
                raise ValueError("unexpected nvidia-smi GPU recovery query row")
            gpus.append({"index": int(row[0]), "uuid": row[1].strip(), "recovery_action": row[2].strip()})
        if not gpus:
            raise ValueError("nvidia-smi GPU recovery query returned no rows")
        info.update(status="observed", gpus=gpus,
                    host_os_reboot_required=any(gpu["recovery_action"].lower() == "reboot" for gpu in gpus))
    except (OSError, subprocess.SubprocessError, ValueError, RuntimeError) as exc:
        info["error"] = {"type": type(exc).__name__, "message": str(exc)}
    return info


def _cuda_device_diagnostics() -> dict:
    """Read/open only: never chmod, recreate devices, or reset a host driver."""
    nodes = []
    for name in ("/dev/nvidiactl", "/dev/nvidia-uvm", "/dev/nvidia-uvm-tools"):
        node = {"path": name}
        try:
            value = Path(name).stat()
            node["character_device"] = stat.S_ISCHR(value.st_mode)
            if node["character_device"]:
                node.update(major=os.major(value.st_rdev), minor=os.minor(value.st_rdev))
                descriptor = os.open(name, os.O_RDWR | os.O_CLOEXEC)
                os.close(descriptor)
                node["open"] = "ok"
        except OSError as exc:
            node["error"] = {"errno": exc.errno, "message": str(exc)}
        nodes.append(node)
    recovery = _gpu_recovery_diagnostics()
    hint = "If UVM has EIO or permission errors, the host/container GPU owner must restore device access."
    if recovery["host_os_reboot_required"]:
        hint = ("NVIDIA reports GPU Recovery Action=Reboot: the physical host OS requires recovery/reboot "
                "by the host owner. Restarting only the Docker instance is not a host OS reboot.")
    return {"device_nodes": nodes, "gpu_recovery": recovery,
            "recovery_hint": hint + " Do not reinstall the injected driver, hide the error with "
                             "NVML-only checks, or fall back to CPU."}


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
    parser.add_argument("--minimum-cuda-devices", type=int, default=1,
                        help="required device count with --require-cuda (DDP launchers use 2)")
    parser.add_argument("--runtime-lock-output", type=Path,
                        help="write an exact installed-runtime identity after successful validation")
    parser.add_argument("--expected-runtime-lock", type=Path,
                        help="fail when installed packages or the platform differ from this lock")
    args = parser.parse_args()
    if args.minimum_cuda_devices < 1:
        parser.error("--minimum-cuda-devices must be positive")
    if args.minimum_cuda_devices != 1 and not args.require_cuda:
        parser.error("--minimum-cuda-devices requires --require-cuda")
    return args


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

        torch_info, cuda_errors = _torch_cuda_report(
            torch, require_cuda=args.require_cuda, minimum_devices=args.minimum_cuda_devices,
        )
        (failures if args.require_cuda else warnings).extend(cuda_errors)
        if not torch_info["cuda_available"] and torch.version.cuda is not None:
            torch_info["device_diagnostics"] = _cuda_device_diagnostics()

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
