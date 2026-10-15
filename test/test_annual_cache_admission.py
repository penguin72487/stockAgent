"""A warm seed does no extra capacity work; a deferred miss keeps its proof."""
from pathlib import Path

import polars as pl
import pytest

from stockagent.data.annual_cache_admission import (
    AnnualCacheBuildDeferred, MemoryAdmittedObservationCache,
)
from test_annual_event_cache import original_coordinates, same_frames, source_at


def cache(tmp_path, source: Path, headroom: int, *, seed: bool = True):
    def capacity(_source, _scratch):
        return {"limits": {"memory_headroom_bytes": headroom}}
    return MemoryAdmittedObservationCache(tmp_path / "cache", seed_sources=[source] if seed else [],
                                         capacity_observer=capacity)


def prepare(cache, source, builder=original_coordinates):
    return cache.prepare(source, ["a", "b"], ["2330"], builder, prepare_identity="fixed-causal-source")


def test_seed_miss_with_gateway_capacity_cannot_call_the_heavy_builder(tmp_path):
    source = source_at(tmp_path)
    current = cache(tmp_path, source, 24 << 30)
    def forbidden(*_a):
        pytest.fail("denied cold seed must not run heavy preparation")
    with pytest.raises(AnnualCacheBuildDeferred) as error:
        prepare(current, source, forbidden)
    assert error.value.details["immutable_seed"] is True
    assert error.value.details["reason"] == "insufficient_memory_headroom"
    assert error.value.details["required_headroom_bytes"] == 26 << 30
    assert not list((tmp_path / "cache").glob("*/current.json"))
    assert current.stats()["builds"] == 0


def test_warmed_exact_seed_never_observes_capacity_or_rebuilds(tmp_path):
    source = source_at(tmp_path)
    prepared = cache(tmp_path, source, 64 << 30)
    generation, metadata = prepare(prepared, source)
    actual = pl.read_parquet(generation / metadata["payload"])
    def forbidden(*_a):
        pytest.fail("warm verified cache must not probe resources or build")
    warm = MemoryAdmittedObservationCache(tmp_path / "cache", seed_sources=[source],
                                          capacity_observer=forbidden)
    reread, proof = prepare(warm, source, forbidden)
    assert reread == generation and proof == metadata
    same_frames(pl.read_parquet(reread / proof["payload"]), actual)
    assert warm.stats()["hits"] == 1 and warm.stats()["builds"] == 0


def test_small_delta_is_not_held_to_full_seed_budget(tmp_path):
    source = source_at(tmp_path)
    delta = cache(tmp_path, source, 3 << 30, seed=False)
    _generation, proof = prepare(delta, source)
    assert proof["identity"]["prepare_identity"] == "fixed-causal-source"
    assert delta.stats()["builds"] == 1


def test_rejected_seed_is_retained_then_recovered_once_in_the_larger_owner(tmp_path):
    source = source_at(tmp_path)
    initial = cache(tmp_path, source, 64 << 30)
    generation, metadata = prepare(initial, source)
    damaged = generation / metadata["payload"]
    damaged.chmod(0o600)
    damaged.write_bytes(b"corrupted derived cache, not original source")
    damaged.chmod(0o400)
    denied = cache(tmp_path, source, 24 << 30)
    with pytest.raises(AnnualCacheBuildDeferred):
        prepare(denied, source)
    assert damaged.is_file()
    assert denied.stats()["rejections"] == 1 and denied.stats()["builds"] == 0
    assert list(generation.parent.glob("rejected-*.json"))
    recovery = cache(tmp_path, source, 64 << 30)
    restored, proof = prepare(recovery, source)
    assert restored != generation and damaged.is_file()
    same_frames(pl.read_parquet(restored / proof["payload"]),
                original_coordinates(source, ["a", "b"], ["2330"]))
    assert recovery.stats()["builds"] == recovery.stats()["rejections"] == 1


@pytest.mark.parametrize("bad", [None, -1, True, "107374182400"])
def test_unproved_capacity_cannot_be_assumed_unlimited(tmp_path, bad):
    source = source_at(tmp_path)
    current = cache(tmp_path, source, bad)
    with pytest.raises(AnnualCacheBuildDeferred) as error:
        prepare(current, source)
    assert error.value.details["reason"] == "capacity_unverified"
    assert current.stats()["builds"] == 0
