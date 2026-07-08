from __future__ import annotations

import os
from contextlib import ExitStack, nullcontext

import torch
from torch.profiler import record_function


def _env_truthy(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "on", "yes"}


_PROFILE_RANGES = _env_truthy("STOCKAGENT_PROFILE_RANGES", "0")
_NVTX_RANGES = _env_truthy("STOCKAGENT_NVTX_RANGES", "0")
PROFILE_RANGES_ENABLED = bool(_PROFILE_RANGES or _NVTX_RANGES)


def _torch_is_compiling() -> bool:
    compiler = getattr(torch, "compiler", None)
    is_compiling = getattr(compiler, "is_compiling", None)
    if callable(is_compiling):
        try:
            if bool(is_compiling()):
                return True
        except Exception:
            pass
    dynamo = getattr(torch, "_dynamo", None)
    is_compiling = getattr(dynamo, "is_compiling", None)
    if callable(is_compiling):
        try:
            return bool(is_compiling())
        except Exception:
            return False
    return False


def profile_range(name: str):
    if not PROFILE_RANGES_ENABLED or _torch_is_compiling():
        return nullcontext()
    stack = ExitStack()
    if _PROFILE_RANGES:
        stack.enter_context(record_function(name))
    if _NVTX_RANGES and torch.cuda.is_available():
        stack.enter_context(torch.cuda.nvtx.range(name))
    return stack
