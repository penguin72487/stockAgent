"""Replay canonical futures tensor ledgers without changing their arithmetic.

This cache owns CUDA launch capture only. Accounting, recurrence and backward
equations remain in the existing daily and minute executors.
"""
from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import fields
import gc
import os
from typing import Callable, TypeVar
import warnings

import torch

Result = TypeVar("Result")
_CACHE: OrderedDict[tuple, object] = OrderedDict()
_STATS = {"constructors": 0, "calls": 0, "busy_eager_calls": 0, "evictions": 0}
_MAX_ENTRIES = 8


@contextmanager
def _capture_gc_guard():
    """Keep cyclic CUDA-graph destruction outside another graph's capture.

    A retired callable can own a Python cycle. Collect it before capture and
    suspend automatic cyclic GC until capture finishes: CUDA graph reset/free
    is illegal on the capturing stream. Preserve an already disabled caller.
    """
    enabled = gc.isenabled()
    if enabled:
        gc.collect()
        gc.disable()
    try:
        yield
    finally:
        if enabled:
            gc.enable()


def futures_cuda_graph_enabled(weights: torch.Tensor) -> bool:
    return (
        os.environ.get("STOCKAGENT_FUTURES_CUDA_GRAPH", "0").lower()
        in {"1", "true", "yes", "on"}
        and weights.device.type == "cuda"
        and not torch.compiler.is_compiling()
        and not torch.cuda.is_current_stream_capturing()
    )


def get_futures_cuda_graph_stats(*, reset: bool = False) -> dict[str, int]:
    result = dict(_STATS, cache_entries=len(_CACHE))
    if reset:
        for key in _STATS:
            _STATS[key] = 0
    return result


def clear_futures_cuda_graph_cache() -> None:
    """Release inactive captured shapes; pending backward graphs remain owned."""
    for key, runner in list(_CACHE.items()):
        if not runner.busy:
            del _CACHE[key]


class _Runner:
    def __init__(self, function: Callable[..., Result], args: tuple, kwargs: dict):
        self.function = function
        self.tensor_keys = tuple(k for k, v in kwargs.items() if isinstance(v, torch.Tensor))
        self.constants = {k: v for k, v in kwargs.items() if k not in self.tensor_keys}
        self.positional_count = len(args)
        self.names: tuple[str, ...] = ()
        self.differentiable: tuple[bool, ...] = ()
        self.busy = False
        self.result_type = None
        samples = tuple(x.detach().clone().requires_grad_(x.requires_grad)
                        for x in (*args, *(kwargs[k] for k in self.tensor_keys)))

        def tensor_call(*values):
            options = dict(self.constants)
            options.update(zip(self.tensor_keys, values[self.positional_count:], strict=True))
            result = function(*values[:self.positional_count], **options)
            self.result_type = type(result)
            self.names = tuple(f.name for f in fields(result)
                               if isinstance(getattr(result, f.name), torch.Tensor))
            outputs = tuple(getattr(result, name) for name in self.names)
            self.differentiable = tuple(x.requires_grad for x in outputs)
            return outputs

        # Same AMP dtype, without an unstable autocast weight-cache pointer.
        with _capture_gc_guard(), warnings.catch_warnings(), torch.autocast(
            "cuda", dtype=torch.get_autocast_dtype("cuda"),
            enabled=torch.is_autocast_enabled("cuda"), cache_enabled=False,
        ):
            # The public PyTorch helper warms isolated leaves on its side
            # stream before capture, as in the canonical dual-session runner.
            warnings.filterwarnings(
                "ignore", message="The AccumulateGrad node's stream does not match.*",
                category=UserWarning,
            )
            self.call = torch.cuda.make_graphed_callables(
                tensor_call, samples, num_warmup_iters=3, allow_unused_input=True)

    def run(self, args: tuple, kwargs: dict):
        inputs = (*args, *(kwargs[k] for k in self.tensor_keys))
        values = self.call(*inputs)
        # Returned buffers and recurrent state must survive subsequent replay.
        outputs = tuple(v.clone() if differentiable else v.detach().clone()
                        for v, differentiable in zip(values, self.differentiable, strict=True))
        if torch.is_grad_enabled() and any(self.differentiable):
            self.busy = True
            differentiable_inputs = tuple(x for x in inputs if x.requires_grad)
            handles = []
            last_backward = None
            versions = tuple(x._version for x in inputs)

            def prepare_backward(grad):
                nonlocal last_backward
                task = torch._C._current_graph_task_id()
                if last_backward is not None and task != last_backward:
                    if tuple(x._version for x in inputs) != versions:
                        raise RuntimeError("futures CUDA graph input modified before retained backward")
                    # PyTorch's captured backward may reuse forward workspace.
                    # Reconstruct it before a second backward of a retained
                    # graph; the normal single-backward trainer pays no replay.
                    with torch.no_grad():
                        self.call(*inputs)
                last_backward = task
                return grad

            for output, differentiable in zip(outputs, self.differentiable, strict=True):
                if differentiable:
                    handles.append(output.register_hook(prepare_backward))

            def release(grad):
                # A retained autograd graph may be used again. Keep its static
                # buffers reserved; subsequent calls use the exact eager path.
                keep = getattr(torch._C._autograd, "_get_current_graph_task_keep_graph", lambda: True)()
                if not keep:
                    self.busy = False
                    for handle in handles:
                        handle.remove()
                # autograd.grad() may return this buffer directly. Preserve its
                # value when a later captured backward reuses the static buffer.
                return grad.clone()

            # The canonical ledgers differentiate actions only. This hook runs
            # after the captured backward has consumed its saved static state.
            handles.append(differentiable_inputs[0].register_hook(release))
        return self.result_type(**dict(zip(self.names, outputs, strict=True)))


def run_futures_cuda_graph(function: Callable[..., Result], *args: torch.Tensor, **kwargs) -> Result:
    """Capture one exact call shape; never overwrite a pending backward graph."""
    tensors = (*args, *(value for value in kwargs.values() if isinstance(value, torch.Tensor)))
    key = (function, torch.is_grad_enabled(), torch.is_inference_mode_enabled(),
           torch.are_deterministic_algorithms_enabled(),
           os.environ.get("STOCKAGENT_FUTURES_FUNDING_COMPILE", "0"),
           os.environ.get("STOCKAGENT_FUTURES_POSITION_COMPILE", "1"),
           torch.is_autocast_enabled("cuda"), torch.get_autocast_dtype("cuda"),
           tuple((tuple(x.shape), tuple(x.stride()), x.dtype, x.device, x.requires_grad) for x in tensors),
           tuple((k, None if isinstance(v, torch.Tensor) else v) for k, v in kwargs.items()))
    runner = _CACHE.get(key)
    if runner is not None and runner.busy:
        _STATS["busy_eager_calls"] += 1
        return function(*args, **kwargs)
    if runner is None:
        # Evict only graphs with no live backward reference. Failures propagate:
        # requested acceleration must not silently masquerade as active.
        if len(_CACHE) >= _MAX_ENTRIES:
            for old_key, old_runner in list(_CACHE.items()):
                if not old_runner.busy:
                    del _CACHE[old_key]
                    _STATS["evictions"] += 1
                    break
            else:
                _STATS["busy_eager_calls"] += 1
                return function(*args, **kwargs)
        runner = _Runner(function, args, kwargs)
        _CACHE[key] = runner
        _STATS["constructors"] += 1
    _CACHE.move_to_end(key)
    _STATS["calls"] += 1
    return runner.run(args, kwargs)
