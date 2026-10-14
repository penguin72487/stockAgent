"""Canonical opt-in must preserve the uncached release stream and raw bits."""
from datetime import date

import numpy as np
import polars as pl
import pytest

from scripts import prepare_tw_day_trade_factorized_panel as sweep


def sources(tmp_path, *, legacy_order=False):
    rows = [
        (date(2024, 12, 27), "a", 1., "2024Q3"),
        (date(2024, 12, 28), "a", 3., "2024Q4"),
        (date(2024, 12, 28), "b", None, "2024Q4"),
        (date(2024, 12, 29), "b", 999., "2024Q3"),
        (date(2025, 1, 1), "a", 998., "2024Q3"),
        (date(2025, 1, 2), "b", 4., "2024Q4"),
        (date(2025, 1, 3), "a", -0., "2025Q1"),
        (date(2025, 1, 4), "b", 997., "2024Q4"),
        (date(2025, 1, 4), "daily", -5., None),
    ]
    table = pl.DataFrame([
        {"date": day, "symbol": "2330", "feature": feature, "value": value, "period": period}
        for day, feature, value, period in rows
    ], schema=sweep.OBS_SCHEMA).with_columns(pl.lit(1, dtype=pl.Int32).alias("_source_part"))
    if legacy_order:
        table = table.select("date", "symbol", "feature", "period", "value", "_source_part")
    paths = []
    for year in (2024, 2025):
        path = tmp_path / f"{year}.parquet"
        table.filter(pl.col("date").dt.year() == year).write_parquet(path)
        paths.append(path)
    definitions = [
        {"feature": feature, "report_group": "report" if feature in {"a", "b"} else ""}
        for feature in ("a", "b", "daily")
    ]
    return paths, definitions


def compare(cached, original, definitions):
    expected = original if isinstance(original, list) else list(original.iter_date_groups(definitions, ["2330"]))
    actual = list(cached.iter_date_groups(definitions, ["2330"]))
    assert [day for day, _ in actual] == [day for day, _ in expected]
    for (_, candidate), (_, reference) in zip(actual, expected, strict=True):
        candidate = candidate.sort("symbol", "feature")
        reference = reference.sort("symbol", "feature")
        assert candidate.schema == reference.schema and candidate.equals(reference)
        assert (candidate["value"].drop_nulls().to_numpy().view(np.uint64).tobytes()
                == reference["value"].drop_nulls().to_numpy().view(np.uint64).tobytes())


@pytest.mark.parametrize("legacy_order", [False, True])
def test_canonical_cold_warm_and_new_delta_remap_original_floors(tmp_path, monkeypatch, legacy_order):
    paths, definitions = sources(tmp_path, legacy_order=legacy_order)
    root = tmp_path / "cache"
    original = sweep.AnnualObservationEvents(paths)
    assert original.cache_stats() is None
    cold = sweep.AnnualObservationEvents(paths, cache_root=root)
    compare(cold, original, definitions)
    assert cold.cache_stats()["builds"] == 2 and cold.cache_stats()["hits"] == 0

    reader = sweep.prepare_annual_coordinates

    def no_read(*_args):
        raise AssertionError("warm canonical reader repeated annual preparation")

    expected = list(original.iter_date_groups(definitions, ["2330"]))
    with monkeypatch.context() as patch:
        patch.setattr(sweep, "prepare_annual_coordinates", no_read)
        warm = sweep.AnnualObservationEvents(paths, cache_root=root)
        compare(warm, expected, definitions)
        assert warm.cache_stats()["hits"] == 2 and warm.cache_stats()["builds"] == 0

    delta = tmp_path / "delta.parquet"
    pl.DataFrame([
        {"date": date(2025, 1, 5), "symbol": "2330", "feature": "daily", "value": 997., "period": "2024-12-31"},
        {"date": date(2025, 1, 5), "symbol": "2330", "feature": "b", "value": 8., "period": "2025Q1"},
    ], schema=sweep.OBS_SCHEMA).with_columns(pl.lit(2, dtype=pl.Int32).alias("_source_part")).write_parquet(delta)
    calls = []

    def record(path, *args):
        calls.append(path)
        return reader(path, *args)

    monkeypatch.setattr(sweep, "prepare_annual_coordinates", record)
    fresh = sweep.AnnualObservationEvents([*paths, delta], cache_root=root)
    compare(fresh, sweep.AnnualObservationEvents([*paths, delta]), definitions)
    # The uncached reference also reads all paths; only the cached caller's
    # statistics distinguish actual reused years from an unmeasured claim.
    assert fresh.cache_stats()["hits"] == 2 and fresh.cache_stats()["builds"] == 1
    assert calls.count(delta.resolve()) == 2


def test_canonical_policy_identity_change_invalidates_derived_years(tmp_path, monkeypatch):
    paths, definitions = sources(tmp_path)
    root = tmp_path / "cache"
    first = sweep.AnnualObservationEvents(paths, cache_root=root)
    compare(first, sweep.AnnualObservationEvents(paths), definitions)
    identity = sweep.annual_preparation_identity()
    monkeypatch.setattr(sweep, "annual_preparation_identity", lambda: {**identity, "policy_revision_test": "changed"})
    changed = sweep.AnnualObservationEvents(paths, cache_root=root)
    compare(changed, sweep.AnnualObservationEvents(paths), definitions)
    assert changed.cache_stats()["builds"] == 2 and changed.cache_stats()["hits"] == 0


def test_canonical_identity_binds_policy_cache_and_runtime():
    identity = sweep.annual_preparation_identity()
    assert identity["contract"]
    assert identity["runtime"]["polars"] == pl.__version__
    assert {"sweep", "mixed_frequency", "release_schedule", "cache"} == set(identity["code_sha256"])
    assert all(len(digest) == 64 for digest in identity["code_sha256"].values())


@pytest.mark.parametrize("legacy_order", [False, True])
def test_live_resource_adapter_preserves_canonical_keys_and_delta_floors(tmp_path, monkeypatch, legacy_order):
    from scripts import prepare_tw_day_trade_factorized_live as owner
    from stockagent.data.annual_cache_admission import MemoryAdmittedObservationCache

    paths, definitions = sources(tmp_path, legacy_order=legacy_order)
    root = tmp_path / "cache"
    expected = list(sweep.AnnualObservationEvents(paths).iter_date_groups(definitions, ["2330"]))
    # Existing exact entries are shared, not re-keyed by operational RAM policy.
    seeded = sweep.AnnualObservationEvents(paths, cache_root=root)
    compare(seeded, expected, definitions)

    def forbid(*_args):
        pytest.fail("a verified warm seed must neither probe capacity nor rebuild")

    def denied_gateway(cache_root, *, seed_sources):
        return MemoryAdmittedObservationCache(cache_root, seed_sources=seed_sources,
                                              capacity_observer=forbid)

    monkeypatch.setattr(owner, "MemoryAdmittedObservationCache", denied_gateway)
    with monkeypatch.context() as patch:
        patch.setattr(sweep, "prepare_annual_coordinates", forbid)
        warm = owner.AdmittedAnnualObservationEvents(paths, cache_root=root, seed_paths=paths)
        assert isinstance(warm, sweep.AnnualObservationEvents)
        compare(warm, expected, definitions)
        assert warm.cache_stats()["hits"] == 2 and warm.cache_stats()["builds"] == 0

    delta = tmp_path / "delta.parquet"
    pl.DataFrame([
        {"date": date(2025, 1, 5), "symbol": "2330", "feature": "daily", "value": -0., "period": "2024-12-31"},
        {"date": date(2025, 1, 5), "symbol": "2330", "feature": "b", "value": 8., "period": "2025Q1"},
    ], schema=sweep.OBS_SCHEMA).with_columns(pl.lit(2, dtype=pl.Int32).alias("_source_part")).write_parquet(delta)
    checked = []

    def capacity(source, _scratch):
        checked.append(source)
        assert source == delta.resolve(), "unchanged seeds must remain hits"
        return {"limits": {"memory_headroom_bytes": 3 << 30}}

    monkeypatch.setattr(owner, "MemoryAdmittedObservationCache", lambda cache_root, *, seed_sources:
        MemoryAdmittedObservationCache(cache_root, seed_sources=seed_sources, capacity_observer=capacity))
    fresh = owner.AdmittedAnnualObservationEvents([*paths, delta], cache_root=root, seed_paths=paths)
    compare(fresh, sweep.AnnualObservationEvents([*paths, delta]), definitions)
    assert checked == [delta.resolve()]
    assert fresh.cache_stats()["hits"] == 2 and fresh.cache_stats()["builds"] == 1
