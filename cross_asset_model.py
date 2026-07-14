from __future__ import annotations

"""All-fold, first-test-calendar-year cross-asset explainability entry point."""

import os
import sys
from pathlib import Path


def _visible_gpu_count() -> int:
    try:
        import torch

        return int(torch.cuda.device_count())
    except Exception:
        return 0


def _configure_preimport_threads(rank_count: int) -> None:
    try:
        logical_cpus = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        logical_cpus = max(1, int(os.cpu_count() or 1))
    # This host has SMT2; the runner later discovers exact physical-core
    # topology, pins each child, and reapplies the precise value.
    threads = max(1, logical_cpus // max(1, 2 * int(rank_count)))
    for name in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "POLARS_MAX_THREADS",
    ):
        os.environ[name] = str(threads)


def _maybe_relaunch_torchrun() -> None:
    if "--help" in sys.argv or "-h" in sys.argv:
        _configure_preimport_threads(1)
        return
    if int(os.environ.get("WORLD_SIZE", "1")) > 1:
        _configure_preimport_threads(int(os.environ.get("LOCAL_WORLD_SIZE", "1")))
        return
    if os.environ.get("STOCKAGENT_CROSS_ASSET_TORCHRUN_CHILD") == "1":
        return
    if "--device" in sys.argv:
        index = sys.argv.index("--device")
        if index + 1 < len(sys.argv) and str(sys.argv[index + 1]).lower().startswith("cpu"):
            return
    gpu_count = _visible_gpu_count()
    if gpu_count <= 1:
        _configure_preimport_threads(1)
        return
    _configure_preimport_threads(gpu_count)
    environment = dict(os.environ)
    environment["STOCKAGENT_CROSS_ASSET_TORCHRUN_CHILD"] = "1"
    command = [
        sys.executable,
        "-m",
        "torch.distributed.run",
        "--standalone",
        f"--nproc-per-node={gpu_count}",
        str(Path(__file__).resolve()),
        *sys.argv[1:],
    ]
    print(
        f"[cross-asset] auto-launching {gpu_count} rank-local GPU workers",
        flush=True,
    )
    os.execvpe(sys.executable, command, environment)


if __name__ == "__main__":
    _maybe_relaunch_torchrun()

from stockagent.cross_asset_runner import main  # noqa: E402


if __name__ == "__main__":
    main()
