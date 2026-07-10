from __future__ import annotations

import importlib
import json
import os
import platform
import shutil
import sys
from pathlib import Path


REQUIRED = ("numpy", "pyarrow", "yaml", "torch", "polars")


def main() -> int:
    modules: dict[str, str | None] = {}
    failures: list[str] = []
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

    report = {
        "python": sys.executable,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "conda_prefix": os.environ.get("CONDA_PREFIX"),
        "modules": modules,
        "torch": torch_info,
        "tools": {name: shutil.which(name) for name in ("ptxas", "nvcc", "git")},
        "repo": str(Path(__file__).resolve().parents[1]),
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if failures:
        print("Environment check failed:\n- " + "\n- ".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
