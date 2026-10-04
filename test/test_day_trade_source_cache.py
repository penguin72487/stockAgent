"""Cache retention and concurrency must not change immutable session evidence."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
import threading
import time

import pytest
import torch

from stockagent.data.tw_day_trade_carry_source import (
    _PhysicalSessionCache, _cgroup_memory_headroom,
    _physical_source_cache_budget_bytes, _session_tensor_storages,
)
from test_day_trade_inference_source import physical_panel


@dataclass
class Tensors:
    value: torch.Tensor
    alias: torch.Tensor | None = None


def test_shared_views_count_backing_storage_once_and_not_logical_elements():
    value = torch.arange(8, dtype=torch.float64)
    item = Tensors(value[2:3], value[4:])
    assert sum(_session_tensor_storages(item).values()) == 64
    cache = _PhysicalSessionCache(64)
    for kind in ("packed", "dense", "compact"):
        assert cache.get_or_load(kind, 0, lambda: item) is item
    snapshot = cache.snapshot()
    assert snapshot["entries"] == 3
    assert snapshot["retained_tensor_bytes"] == 64
    snapshot["representations"]["packed"]["loads"] = 999
    assert cache.snapshot()["representations"]["packed"]["loads"] == 1
    tiny = _PhysicalSessionCache(8)
    assert tiny.get_or_load("compact", 0, lambda: item) is item
    assert tiny.snapshot()["entries"] == 0


def test_one_budget_evicts_lru_across_all_formats_and_active_views_survive():
    cache = _PhysicalSessionCache(128)
    calls = []

    def load(kind, row):
        def decode():
            calls.append((kind, row))
            return Tensors(torch.full((8,), float(row), dtype=torch.float64))
        return cache.get_or_load(kind, row, decode)

    first = load("packed", 1)
    active = load("compact", 2)
    assert load("packed", 1) is first
    load("dense", 3)
    assert calls == [("packed", 1), ("compact", 2), ("dense", 3)]
    assert cache.snapshot()["representations"]["compact"]["evictions"] == 1
    torch.testing.assert_close(active.value, torch.full((8,), 2.0, dtype=torch.float64))
    assert load("compact", 2) is not active
    assert cache.snapshot()["retained_tensor_bytes"] == 128
    assert cache.snapshot()["peak_retained_tensor_bytes"] == 128


def test_zero_disables_every_format_and_negative_budget_is_rejected():
    cache = _PhysicalSessionCache(0)
    for kind in ("packed", "dense", "compact"):
        for _ in range(2):
            cache.get_or_load(kind, 0, lambda: Tensors(torch.ones(2)))
    snapshot = cache.snapshot()
    assert snapshot["entries"] == snapshot["retained_tensor_bytes"] == 0
    assert all(stats["loads"] == stats["bypasses"] == 2
               for stats in snapshot["representations"].values())
    with pytest.raises(ValueError, match="nonnegative"):
        _PhysicalSessionCache(-1)


@pytest.mark.parametrize("budget", [0, 128])
@pytest.mark.parametrize("failure", [False, True])
def test_same_key_parallel_misses_share_result_or_error_and_recover(budget, failure):
    cache = _PhysicalSessionCache(budget)
    gate = threading.Barrier(5)
    started, release = threading.Event(), threading.Event()
    calls = []

    def loader():
        calls.append(1)
        started.set()
        if not release.wait(5):
            raise TimeoutError("test did not release its decoder")
        if failure:
            raise ValueError("source hash rejected")
        return Tensors(torch.ones(8, dtype=torch.float64))

    def request():
        gate.wait(timeout=5)
        return cache.get_or_load("packed", 0, loader)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(request) for _ in range(4)]
        try:
            gate.wait(timeout=5)
            assert started.wait(5)
            deadline = time.monotonic() + 3
            while cache.snapshot()["representations"]["packed"]["coalesced"] < 3:
                if time.monotonic() >= deadline:
                    pytest.fail("same-key callers failed to join the pending decode")
                release.wait(0.001)
        finally:
            release.set()
        if failure:
            for future in futures:
                with pytest.raises(ValueError, match="source hash rejected"):
                    future.result(timeout=5)
        else:
            results = [future.result(timeout=5) for future in futures]
            assert all(value is results[0] for value in results)
    assert calls == [1]
    assert cache.snapshot()["inflight"] == 0
    if failure:
        assert cache.snapshot()["representations"]["packed"]["errors"] == 1
        recovered = cache.get_or_load("packed", 0, lambda: Tensors(torch.zeros(8)))
        assert not recovered.value.any()


def test_independent_rows_still_decode_in_parallel_without_global_io_lock():
    cache = _PhysicalSessionCache(128)
    gate = threading.Barrier(2)

    def load(row):
        def decode():
            gate.wait(timeout=3)
            return Tensors(torch.full((8,), float(row), dtype=torch.float64))
        return cache.get_or_load("packed", row, decode)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(load, [0, 1]))
    assert [value.value[0].item() for value in results] == [0, 1]
    assert cache.snapshot()["peak_retained_tensor_bytes"] == 128


def test_nested_representations_share_one_pending_packed_decode_without_deadlock():
    cache = _PhysicalSessionCache(64)
    started, release = threading.Event(), threading.Event()
    calls = []

    def decode():
        calls.append(1)
        started.set()
        if not release.wait(5):
            raise TimeoutError("test decoder was not released")
        return Tensors(torch.ones(8, dtype=torch.float64))

    def packed():
        return cache.get_or_load("packed", 0, decode)

    def request(kind):
        return packed() if kind == "packed" else cache.get_or_load(kind, 0, packed)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(request, kind) for kind in ("packed", "dense", "compact")]
        try:
            assert started.wait(5)
            deadline = time.monotonic() + 3
            while cache.snapshot()["representations"]["packed"]["coalesced"] < 2:
                if time.monotonic() >= deadline:
                    pytest.fail("nested source representations did not share their decode")
                release.wait(0.001)
        finally:
            release.set()
        values = [future.result(timeout=5) for future in futures]
    assert all(value is values[0] for value in values)
    assert calls == [1]
    assert cache.snapshot()["entries"] == 3
    assert cache.snapshot()["retained_tensor_bytes"] == 64
    assert cache.snapshot()["inflight"] == 0


@pytest.mark.parametrize("setting", ["nan", "inf", "-inf", "1e309", "1e300", "-0.1", ""])
def test_invalid_budget_fails_explicitly(monkeypatch, setting):
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_SOURCE_CACHE_GIB", setting)
    with pytest.raises(ValueError, match="finite nonnegative"):
        _physical_source_cache_budget_bytes()


@pytest.mark.parametrize("setting, expected", [("0", 0), ("0.5", 512 * 1024**2)])
def test_explicit_budget_is_respected(monkeypatch, setting, expected):
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_SOURCE_CACHE_GIB", setting)
    assert _physical_source_cache_budget_bytes() == expected


def test_auto_budget_honors_v2_ancestor_headroom_and_rank_reserve(tmp_path, monkeypatch):
    import stockagent.data.tw_day_trade_carry_source as module

    gib = 1024**3
    root = tmp_path / "cgroups"
    leaf = root / "service/rank"
    leaf.mkdir(parents=True)
    membership = tmp_path / "membership"
    membership.write_text("0::/service/rank\n", encoding="utf-8")
    for folder, current, high, maximum in (
        (root / "service", 4 * gib, 13 * gib, 16 * gib),
        (leaf, gib, "max", 32 * gib),
    ):
        for name, value in (("memory.current", current), ("memory.high", high), ("memory.max", maximum)):
            (folder / name).write_text(str(value), encoding="ascii")
    assert _cgroup_memory_headroom(100 * gib, membership=membership, root=root) == 9 * gib
    original = Path.read_text

    def read(path, *args, **kwargs):
        if str(path) == "/proc/meminfo":
            return f"MemAvailable: {100 * gib // 1024} kB\n"
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    monkeypatch.setattr(module, "_cgroup_memory_headroom", lambda value: 9 * gib)
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_SOURCE_CACHE_GIB", "auto")
    monkeypatch.setenv("WORLD_SIZE", "2")
    assert _physical_source_cache_budget_bytes() == gib // 2
    monkeypatch.setattr(module, "_cgroup_memory_headroom", lambda value: gib)
    assert _physical_source_cache_budget_bytes() == 0


def test_projection_shares_nonsemantic_telemetry_not_the_source_audit():
    source = physical_panel().day_trade_carry_source
    cache = _PhysicalSessionCache(64)
    source = replace(source, audit_receipt={"test": "immutable"}, runtime_cache_info=cache.snapshot)
    projected = source.project_universe(("0050", "1101"))
    assert projected.runtime_cache_info == source.runtime_cache_info
    cache.get_or_load("packed", 0, lambda: Tensors(torch.ones(8)))
    assert projected.runtime_cache_info()["entries"] == 1
    assert source.audit_receipt == {"test": "immutable"}
