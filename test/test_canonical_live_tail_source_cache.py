"""Raw-tail sharing must preserve every canonical field and safe idle release."""
from dataclasses import fields

import numpy as np
import polars as pl
import pytest

from downloader.live_tail_source_cache import LiveTailSourceCache
from scripts.benchmark_ohlcv_tail_reader import _frame_identity
from stockagent.data import panel as panels
from stockagent.data.panel_cache import array_content_fingerprint
from stockagent.live import signal_engine as engine
from test_live_tail_source_cache import source


def identity(value):
    return {field.name: (array_content_fingerprint(item)
                        if isinstance(item := getattr(value, field.name), np.ndarray) else item)
            for field in fields(value)}


@pytest.fixture(autouse=True)
def clean_cache(monkeypatch):
    monkeypatch.delenv("STOCKAGENT_LIVE_RAW_TAIL_CACHE_BYTES", raising=False)
    engine.clear_live_panel_memory_cache()
    yield
    engine.clear_live_panel_memory_cache()


@pytest.mark.parametrize("raw", [False, True])
def test_real_symbol_reader_callback_preserves_all_arrays_and_bits(tmp_path, raw):
    path = source(tmp_path)
    cache = LiveTailSourceCache()
    kwargs = dict(tail_rows=48, tradable_mode="tw_limit_guard",
                  trading_volume_policy="required", raw_input_features=raw)
    expected = identity(panels._load_symbol_arrays_pyarrow_tail(path, **kwargs))
    for _ in range(2):
        assert identity(panels._load_symbol_arrays_pyarrow_tail(
            path, **kwargs, raw_tail_reader=cache.read)) == expected
    assert cache.stats()["misses"] == cache.stats()["hits"] == 1


@pytest.mark.parametrize("raw", [False, True])
def test_real_complete_builder_callback_preserves_every_panel_field(tmp_path, raw):
    path = source(tmp_path)
    kwargs = dict(tail_rows=48, tradable_mode="tw_limit_guard",
                  trading_volume_policy="required", strict_no_fallback=True,
                  panel_load_workers=2, external_include_features=False,
                  external_include_rules=False, feature_include=["*"] if raw else [])
    expected = identity(panels.build_tail_panel(path.parent, **kwargs))
    cache = LiveTailSourceCache()
    assert identity(panels.build_tail_panel(path.parent, **kwargs,
                                          raw_tail_reader=cache.read)) == expected
    assert cache.stats()["misses"] == 1


def test_process_owner_lazily_shares_cache_and_idle_clear_releases_buffers(tmp_path, monkeypatch):
    path = source(tmp_path)
    for name in ("is_available", "init", "synchronize", "empty_cache"):
        monkeypatch.setattr(engine.torch.cuda, name,
                            lambda *_a, **_kw: pytest.fail("raw cache must not call CUDA"))
    cache = engine._live_raw_tail_source_cache()
    assert cache.max_bytes == 64 * 1024**2
    cache.read(path, tail_rows=48)
    assert engine._live_raw_tail_source_cache() is cache
    cache.read(path, tail_rows=48)
    assert cache.stats()["hits"] == 1
    assert 0 < cache.stats()["resident_buffer_bytes"] <= cache.max_bytes
    epoch = cache.stats()["epoch"]
    engine.clear_live_inference_memory_cache()
    assert cache.stats()["entries"] == cache.stats()["resident_buffer_bytes"] == 0
    assert cache.stats()["epoch"] == epoch + 1


def test_idle_clear_fences_a_real_inflight_reader(tmp_path):
    path = source(tmp_path)
    cache = engine._live_raw_tail_source_cache()
    original = cache._loader
    def loader(path, rows):
        engine.clear_live_panel_memory_cache()
        return original(path, rows)
    cache._loader = loader
    try:
        assert cache.read(path, tail_rows=48).num_rows == 48
        assert cache.stats()["entries"] == cache.stats()["resident_buffer_bytes"] == 0
    finally:
        cache._loader = original


def test_disable_releases_existing_buffers_without_changing_rows(tmp_path, monkeypatch):
    path = source(tmp_path)
    cache = engine._live_raw_tail_source_cache()
    table = cache.read(path, tail_rows=48)
    monkeypatch.setenv("STOCKAGENT_LIVE_RAW_TAIL_CACHE_BYTES", "0")
    disabled = engine._live_raw_tail_source_cache()
    assert disabled is not cache and disabled.max_bytes == 0
    assert cache.stats()["resident_buffer_bytes"] == 0
    actual = disabled.read(path, tail_rows=48)
    assert _frame_identity(pl.from_arrow(actual)) == _frame_identity(pl.from_arrow(table))
    assert disabled.stats()["entries"] == disabled.stats()["resident_buffer_bytes"] == 0


@pytest.mark.parametrize("capacity", ["-1", "True", "0.5", str(64 * 1024**2 + 1)])
def test_bad_capacity_never_becomes_an_unbounded_resident_cache(monkeypatch, capacity):
    monkeypatch.setenv("STOCKAGENT_LIVE_RAW_TAIL_CACHE_BYTES", capacity)
    with pytest.raises(ValueError, match="raw tail cache"):
        engine._live_raw_tail_source_cache()
