from dataclasses import fields
from concurrent.futures import ThreadPoolExecutor
import threading
import time

import numpy as np
import polars as pl
import pytest

from downloader.live_tail_source_cache import LiveTailSourceCache
from downloader.ohlcv_hot_tail import read_logical_parquet
from scripts.benchmark_ohlcv_tail_reader import _frame_identity
from stockagent.data.panel import _symbol_arrays_from_arrow_table
from stockagent.data.panel_cache import array_content_fingerprint


def source(tmp_path, name="2330_features.parquet", rows=1000):
    path = tmp_path / "data_tw_public/stocks" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    bits = np.resize(np.array([0, 0x8000000000000000, 0x7FF8000000000017], dtype=np.uint64), rows)
    frame = pl.DataFrame({"date": pl.date_range(pl.date(2020, 1, 1), pl.date(2030, 1, 1),
        interval="1d", eager=True).head(rows), "open": [10.0]*rows, "max": [15.0]*rows,
        "min": [5.0]*rows, "close": [12.0]*rows, "Trading_Volume": bits.view(np.float64),
        "note": ["原始來源"]*rows}).with_columns(
            pl.when(pl.int_range(pl.len())%17==0).then(None).otherwise(pl.col("Trading_Volume")).alias("Trading_Volume"))
    frame.write_parquet(path)
    return path


@pytest.mark.parametrize("raw", [False, True])
def test_raw_rows_and_all_derived_arrays_match_before_and_after_shared_hit(tmp_path, raw):
    path = source(tmp_path)
    original = read_logical_parquet(path, tail_rows=48).to_arrow()
    cache = LiveTailSourceCache()
    for table in [cache.read(path, tail_rows=48), cache.read(path, tail_rows=48)]:
        assert _frame_identity(pl.from_arrow(table)) == _frame_identity(pl.from_arrow(original))
        expected = _symbol_arrays_from_arrow_table(original, path, tradable_mode="tw_limit_guard",
                                                   trading_volume_policy="required", raw_input_features=raw)
        actual = _symbol_arrays_from_arrow_table(table, path, tradable_mode="tw_limit_guard",
                                                trading_volume_policy="required", raw_input_features=raw)
        assert {field.name: array_content_fingerprint(getattr(actual, field.name))
                for field in fields(actual) if field.name != "symbol"} == {
                    field.name: array_content_fingerprint(getattr(expected, field.name))
                    for field in fields(expected) if field.name != "symbol"}
    assert cache.stats()["hits"] == cache.stats()["misses"] == 1


def test_compact_tail_does_not_hold_a_full_historical_rowgroup(tmp_path):
    path = source(tmp_path)
    cache = LiveTailSourceCache()
    table = cache.read(path, tail_rows=48)
    assert table.num_rows == 48
    assert table.get_total_buffer_size() < 10_000
    assert cache.stats()["resident_buffer_bytes"] == table.get_total_buffer_size()


@pytest.mark.parametrize("change", ["replace", "hot_tail", "bounds"])
def test_source_generation_parts_and_requested_length_never_share_stale_rows(tmp_path, change):
    path = source(tmp_path)
    cache = LiveTailSourceCache()
    cache.read(path, tail_rows=48)
    if change == "replace":
        replacement = path.with_name("replacement.parquet")
        pl.read_parquet(path).with_columns(pl.lit(20.0).alias("close")).write_parquet(replacement)
        replacement.replace(path)
        actual = cache.read(path, tail_rows=48)
        assert pl.from_arrow(actual)["close"].to_list() == [20.0]*48
    elif change == "hot_tail":
        tail = path.parent / "_hot_tail" / path.name
        tail.parent.mkdir()
        pl.read_parquet(path).tail(1).with_columns(pl.lit(20.0).alias("close")).write_parquet(tail)
        assert pl.from_arrow(cache.read(path, tail_rows=48))["close"][-1] == 20.0
    else:
        assert cache.read(path, tail_rows=32).num_rows == 32
    assert cache.stats()["misses"] == 2


def test_transient_decode_failure_is_not_memoized(tmp_path):
    path = source(tmp_path)
    count = 0
    def loader(path, rows):
        nonlocal count
        count += 1
        if count == 1:
            raise OSError("transient source failure")
        return read_logical_parquet(path, tail_rows=rows).to_arrow()
    cache = LiveTailSourceCache(loader=loader)
    with pytest.raises(OSError):
        cache.read(path, tail_rows=48)
    assert cache.stats()["entries"] == 0
    assert cache.read(path, tail_rows=48).num_rows == 48
    assert count == 2


def test_generation_change_during_decode_cannot_enter_the_cache(tmp_path):
    path = source(tmp_path)
    def loader(path, rows):
        table = read_logical_parquet(path, tail_rows=rows).to_arrow()
        path.touch()
        return table
    cache = LiveTailSourceCache(loader=loader)
    with pytest.raises(OSError, match="source changed"):
        cache.read(path, tail_rows=48)
    assert cache.stats()["entries"] == 0


def test_clear_fences_inflight_population_and_releases_resident_buffers(tmp_path):
    path = source(tmp_path)
    def loader(path, rows):
        cache.clear()
        return read_logical_parquet(path, tail_rows=rows).to_arrow()
    cache = LiveTailSourceCache(loader=loader)
    assert cache.read(path, tail_rows=48).num_rows == 48
    assert cache.stats()["entries"] == cache.stats()["resident_buffer_bytes"] == 0


def test_bounded_lru_and_zero_capacity_do_not_change_values(tmp_path):
    first, second = source(tmp_path), source(tmp_path, "1101_features.parquet")
    size = LiveTailSourceCache().read(first, tail_rows=48).get_total_buffer_size()
    cache = LiveTailSourceCache(max_bytes=size)
    cache.read(first, tail_rows=48)
    cache.read(second, tail_rows=48)
    assert cache.stats()["entries"] == cache.stats()["evictions"] == 1
    assert cache.stats()["resident_buffer_bytes"] <= size
    disabled = LiveTailSourceCache(max_bytes=0)
    assert disabled.read(first, tail_rows=48).num_rows == 48
    assert disabled.stats()["entries"] == disabled.stats()["resident_buffer_bytes"] == 0


@pytest.mark.parametrize("capacity", [-1, True, 0.5])
def test_invalid_capacity_does_not_silently_become_unbounded(capacity):
    with pytest.raises(ValueError):
        LiveTailSourceCache(max_bytes=capacity)


def test_unknown_loader_type_is_not_a_success_receipt(tmp_path):
    path = source(tmp_path)
    cache = LiveTailSourceCache(loader=lambda *_args: [])
    with pytest.raises(TypeError):
        cache.read(path, tail_rows=48)
    assert cache.stats()["entries"] == 0


def test_concurrent_feature_policies_share_one_physical_decode(tmp_path):
    path = source(tmp_path)
    started, release = threading.Event(), threading.Event()
    calls = []

    def load(path, rows):
        calls.append(1)
        started.set()
        assert release.wait(3)
        return read_logical_parquet(path, tail_rows=rows).to_arrow()

    cache = LiveTailSourceCache(loader=load)
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = [pool.submit(cache.read, path, tail_rows=48) for _ in range(3)]
        assert started.wait(3)
        deadline = time.monotonic() + 3
        while cache.stats()["joined_reads"] < 2 and time.monotonic() < deadline:
            time.sleep(0.005)
        assert cache.stats()["joined_reads"] == 2
        release.set()
        tables = [job.result(3) for job in jobs]
    assert calls == [1]
    assert all(table is tables[0] for table in tables)
    assert not cache._inflight


def test_concurrent_decode_failure_releases_waiters_and_retries(tmp_path):
    path = source(tmp_path)
    started, release = threading.Event(), threading.Event()
    error = OSError("source failure")

    def fail(*_):
        started.set()
        assert release.wait(3)
        raise error

    cache = LiveTailSourceCache(loader=fail)
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(cache.read, path, tail_rows=48) for _ in range(2)]
        assert started.wait(3)
        deadline = time.monotonic() + 3
        while cache.stats()["joined_reads"] < 1 and time.monotonic() < deadline:
            time.sleep(0.005)
        assert cache.stats()["joined_reads"] == 1
        release.set()
        for job in jobs:
            with pytest.raises(OSError) as caught:
                job.result(3)
            assert caught.value is error
    assert not cache._inflight
    assert cache.stats()["entries"] == 0
    cache._loader = lambda path, rows: read_logical_parquet(path, tail_rows=rows).to_arrow()
    assert cache.read(path, tail_rows=48).num_rows == 48
