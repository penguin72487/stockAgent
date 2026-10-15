"""Scoped CPU-source pool setup, called before importing numeric libraries.

This is not a CPU quota, training policy or global service configuration. The
factorized source CLI and isolated comparison workers opt in; importing this
module never changes another caller's environment or initializes CUDA.
"""
from __future__ import annotations

import os

THREAD_ENV_KEYS = ("POLARS_MAX_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")


def configure_cpu_source_runtime(requested: int | None = None) -> dict:
    if requested is None:
        requested = int(os.environ.get("STOCKAGENT_FACTORIZED_PREP_THREADS", "2"))
    if type(requested) is not int or requested <= 0:
        raise ValueError("source processing threads must be a positive integer")
    affinity = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else (os.cpu_count() or 1)
    effective = min(requested, max(1, affinity))
    settings = {key: str(effective) for key in THREAD_ENV_KEYS}
    settings["CUDA_VISIBLE_DEVICES"] = ""  # CPU preparation, not the inference/training owner.
    os.environ.update(settings)
    return {"contract": "scoped_cpu_source_pool_profile_v1", "requested_threads": requested,
            "effective_threads": effective, "environment": settings,
            "process_cpu_quota": False, "cuda_initialized": False}
