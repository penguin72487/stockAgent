"""Idle ownership includes CPU caches and a deferred allocator obligation."""
from collections import OrderedDict
import gc

import pytest

from stockagent.live import signal_engine as engine


CACHES = (
    "_LIVE_MODEL_CACHE", "_LIVE_PANEL_CACHE", "_LIVE_PANEL_SOURCE_KEY_CACHE",
    "_LIVE_FACTORIZED_PANEL_CACHE", "_LIVE_CHECKPOINT_CACHE",
    "_LIVE_MODEL_INPUT_CACHE", "_LIVE_ALIGNED_PANEL_CACHE",
)


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    for name in CACHES:
        monkeypatch.setattr(engine, name, OrderedDict())
    monkeypatch.setattr(engine, "_LIVE_RAW_TAIL_SOURCE_CACHE", None)
    monkeypatch.setattr(engine, "_LIVE_CUDA_CACHE_RELEASE_PENDING", False)


def forbid_cuda(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("empty/CPU-only idle work must not call CUDA")
    for name in ("is_initialized", "is_available", "init", "synchronize", "empty_cache"):
        monkeypatch.setattr(engine.torch.cuda, name, forbidden)
    monkeypatch.setattr(engine.torch.accelerator, "empty_host_cache", forbidden)


def fake_cuda(monkeypatch, events):
    monkeypatch.setattr(engine.torch.cuda, "is_initialized", lambda: True)
    monkeypatch.setattr(engine.torch.cuda, "synchronize", lambda: events.append("synchronize"))
    monkeypatch.setattr(engine.torch._C, "_cuda_clearCublasWorkspaces",
                        lambda: events.append("blas"))
    monkeypatch.setattr(engine.torch.cuda, "empty_cache", lambda: events.append("device"))
    monkeypatch.setattr(engine.torch.accelerator, "empty_host_cache", lambda: events.append("host"))


def test_empty_idle_never_collects_or_probes(monkeypatch):
    forbid_cuda(monkeypatch)
    monkeypatch.setattr(gc, "collect", lambda: pytest.fail("empty idle must not collect"))
    for _ in range(5):
        assert engine.release_idle_live_cuda_cache() is False


@pytest.mark.parametrize("name", CACHES[1:])
def test_cpu_only_cache_is_released_once_without_cuda(monkeypatch, name):
    forbid_cuda(monkeypatch)
    collects = []
    monkeypatch.setattr(gc, "collect", lambda: collects.append(True))
    getattr(engine, name)["fixture"] = object()
    assert engine.release_idle_live_cuda_cache() is True
    assert all(not getattr(engine, cache) for cache in CACHES)
    assert engine._LIVE_CUDA_CACHE_RELEASE_PENDING is False
    assert engine.release_idle_live_cuda_cache() is False
    assert collects == [True]


def test_raw_tail_only_clear_is_once_and_does_not_construct_a_cache(monkeypatch):
    forbid_cuda(monkeypatch)
    class Raw:
        entries = 1
        clears = 0
        def stats(self):
            return {"entries": self.entries}
        def clear(self):
            self.entries = 0
            self.clears += 1
    raw = Raw()
    monkeypatch.setattr(engine, "_LIVE_RAW_TAIL_SOURCE_CACHE", raw)
    assert engine.release_idle_live_cuda_cache() is True
    assert engine.release_idle_live_cuda_cache() is False
    assert engine._LIVE_RAW_TAIL_SOURCE_CACHE is raw
    assert raw.clears == 1


def test_model_invalidation_keeps_allocator_release_pending(monkeypatch):
    events = []
    fake_cuda(monkeypatch, events)
    engine._LIVE_MODEL_CACHE["model"] = object()
    engine.clear_live_inference_memory_cache()
    assert not engine._LIVE_MODEL_CACHE
    assert engine._LIVE_CUDA_CACHE_RELEASE_PENDING is True
    assert engine.release_idle_live_cuda_cache() is True
    assert events == ["synchronize", "blas", "device", "host"]
    assert engine._LIVE_CUDA_CACHE_RELEASE_PENDING is False
    forbid_cuda(monkeypatch)
    assert engine.release_idle_live_cuda_cache() is False


def test_failed_host_flush_keeps_pending_obligation_for_next_idle(monkeypatch):
    events = []
    fake_cuda(monkeypatch, events)
    engine._LIVE_MODEL_CACHE["model"] = object()
    def failed():
        raise RuntimeError("host allocator transient")
    monkeypatch.setattr(engine.torch.accelerator, "empty_host_cache", failed)
    with pytest.raises(RuntimeError, match="host allocator transient"):
        engine.release_idle_live_cuda_cache()
    assert not engine._LIVE_MODEL_CACHE
    assert engine._LIVE_CUDA_CACHE_RELEASE_PENDING is True
    fake_cuda(monkeypatch, events)
    assert engine.release_idle_live_cuda_cache() is True
    assert events == ["synchronize", "blas", "device", "synchronize", "blas", "device", "host"]
    assert engine._LIVE_CUDA_CACHE_RELEASE_PENDING is False


def test_cpu_model_invalidation_does_not_initialize_cuda(monkeypatch):
    engine._LIVE_MODEL_CACHE["cpu-model"] = object()
    engine.clear_live_inference_memory_cache()
    monkeypatch.setattr(engine.torch.cuda, "is_initialized", lambda: False)
    for name in ("is_available", "init", "synchronize", "empty_cache"):
        monkeypatch.setattr(engine.torch.cuda, name,
                            lambda *_a, **_kw: pytest.fail("do not initialize CUDA to release"))
    assert engine.release_idle_live_cuda_cache() is True
    assert engine.release_idle_live_cuda_cache() is False


def test_older_runtime_without_public_host_api_preserves_device_release(monkeypatch):
    events = []
    fake_cuda(monkeypatch, events)
    monkeypatch.delattr(engine.torch.accelerator, "empty_host_cache")
    engine._LIVE_MODEL_CACHE["model"] = object()
    assert engine.release_idle_live_cuda_cache() is True
    assert events == ["synchronize", "blas", "device"]
