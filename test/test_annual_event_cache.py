"""Raw coordinate fidelity, bounded reads and recovery of derived caches."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
import os
from pathlib import Path
import threading
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest

from scripts.benchmark_factorized_live_projection import SOURCE, WRITER, candidate_sources
from scripts.benchmark_project_data_processing import load_baseline
from stockagent.data.annual_event_cache import (
    PERIOD_SCHEMA, PreparedAnnualCoordinates, PreparedObservationCache, SCHEMA,
)


def original_coordinates(path, features, symbols):
    table = pl.read_parquet(path).filter(pl.col("feature").is_in(features) & pl.col("symbol").is_in(symbols))
    table = table.sort("_source_part").unique(["date", "symbol", "feature", "period"],
                                            keep="last", maintain_order=True).drop("_source_part")
    return table.sort("date", "symbol", "feature", "period").unique(
        ["date", "symbol", "feature"], keep="last", maintain_order=True)


def source_at(tmp_path, *, year=2025):
    path = tmp_path / f"{year}.parquet"
    bits = np.asarray([0x8000000000000000, 0x7FF8000000000001,
                       0x7FF0000000000000, 0xBFF0000000000000], dtype=np.uint64)
    table = pl.DataFrame({"date": [date(year, 1, d) for d in (2, 1, 2, 3)],
                          "symbol": ["2330"] * 4, "feature": ["a", "a", "b", "b"],
                          "value": bits.view(np.float64), "period": [None, "2024Q4", "2024Q4", "2025Q1"],
                          "_source_part": ["a"] * 4})
    extra = table[:1].with_columns(pl.lit(None, dtype=pl.Float64).alias("value"),
                                  pl.lit("b").alias("feature"), pl.lit("b").alias("_source_part"))
    pl.concat([table, extra]).write_parquet(path)
    return path


def same_frames(actual, expected):
    assert actual.schema == expected.schema and actual.equals(expected)
    assert (actual["value"].drop_nulls().to_numpy().view(np.uint64).tobytes()
            == expected["value"].drop_nulls().to_numpy().view(np.uint64).tobytes())


@pytest.mark.parametrize("order", ["native_delta", "legacy_annual"])
def test_cold_and_warm_all_columns_nulls_and_float_bits(tmp_path, order):
    source = source_at(tmp_path)
    if order == "legacy_annual":
        pl.read_parquet(source).select("date", "symbol", "feature", "period", "value", "_source_part").write_parquet(source)
    cache = PreparedObservationCache(tmp_path / "cache")
    expected = original_coordinates(source, ["a", "b"], ["2330"])
    actual = list(cache.iter_dates(source, ["a", "b"], ["2330"], original_coordinates, prepare_identity="original"))
    assert [d for d, _ in actual] == [date(2025, 1, d) for d in (1, 2, 3)]
    same_frames(pl.concat([part for _, part in actual]), expected)
    assert cache.builds == 1 and cache.hits == 0

    def no_rebuild(*_args):
        raise AssertionError("warm cache recomputed the whole year")

    again = list(cache.iter_dates(source, ["a", "b"], ["2330"], no_rebuild, prepare_identity="original"))
    same_frames(pl.concat([part for _, part in again]), expected)
    assert cache.builds == 1 and cache.hits == 1


def test_one_complete_date_even_with_multiple_row_groups(tmp_path, monkeypatch):
    import stockagent.data.annual_event_cache as module

    monkeypatch.setattr(module, "ROW_GROUP_ROWS", 1)
    source = source_at(tmp_path)
    cache = PreparedObservationCache(tmp_path / "cache")
    generation, metadata = cache.prepare(source, ["a", "b"], ["2330"], original_coordinates,
                                         prepare_identity="original")
    assert metadata["groups"][1]["row_groups"] == [1, 2]
    actual = list(cache.iter_dates(source, ["a", "b"], ["2330"], original_coordinates, prepare_identity="original"))
    assert len(actual) == 3 and actual[1][1].height == 2
    assert (generation / "events.parquet").stat().st_mode & 0o777 == 0o400


@pytest.mark.parametrize("damage", ["bytes", "missing", "manifest", "escape", "json"])
def test_bad_generation_is_retained_and_rebuilt_once(tmp_path, damage):
    source = source_at(tmp_path)
    cache = PreparedObservationCache(tmp_path / "cache")
    generation, metadata = cache.prepare(source, ["a"], ["2330"], original_coordinates, prepare_identity="original")
    head = generation.parent / "current.json"
    if damage == "bytes":
        payload = generation / "events.parquet"
        payload.chmod(0o600)
        with payload.open("ab") as stream:
            stream.write(b"bad")
    elif damage == "missing":
        (generation / "events.parquet").rename(generation / "retained-missing.parquet")
    elif damage == "manifest":
        (generation / "manifest.json").write_text("{}")
    elif damage == "escape":
        metadata["generation"] = "../../outside"
        head.write_text(json.dumps(metadata))
    else:
        head.write_text("{")
    rebuilt, _ = cache.prepare(source, ["a"], ["2330"], original_coordinates, prepare_identity="original")
    assert rebuilt != generation and generation.exists()
    assert cache.builds == 2 and cache.rejections == 1
    assert len(list(generation.parent.glob("rejected-*.json"))) == 1


def test_axes_preparation_and_source_bytes_are_content_keys(tmp_path):
    source = source_at(tmp_path)
    cache = PreparedObservationCache(tmp_path / "cache")
    paths = {cache.prepare(source, features, symbols, original_coordinates, prepare_identity=identity)[0]
             for features, symbols, identity in ((["a"], ["2330"], "v1"), (["a", "b"], ["2330"], "v1"),
                                                (["a"], ["none"], "v1"), (["a"], ["2330"], "v2"))}
    assert len(paths) == 4 and cache.builds == 4
    stamp = source.stat()
    pl.read_parquet(source).with_columns((pl.col("value") + 3).alias("value")).write_parquet(source)
    os.utime(source, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    changed, _ = cache.prepare(source, ["a"], ["2330"], original_coordinates, prepare_identity="v1")
    assert changed not in paths and cache.builds == 5


def test_single_flight_and_no_partial_head(tmp_path):
    source = source_at(tmp_path)
    started, release = threading.Event(), threading.Event()
    calls = []

    def slow(*args):
        calls.append(True)
        started.set()
        assert release.wait(timeout=5)
        return original_coordinates(*args)

    def execute():
        cache = PreparedObservationCache(tmp_path / "cache")
        return cache.prepare(source, ["a"], ["2330"], slow, prepare_identity="same")[0]

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(execute)
        assert started.wait(timeout=5)
        second = pool.submit(execute)
        assert not list((tmp_path / "cache").glob("*/current.json"))
        release.set()
        assert first.result(timeout=5) == second.result(timeout=5)
    assert len(calls) == 1


@pytest.mark.parametrize("failure", ["changed_source", "admission", "interrupted", "unsorted", "null_date"])
def test_failed_preparation_never_publishes_head(tmp_path, monkeypatch, failure):
    source = source_at(tmp_path)
    cache = PreparedObservationCache(tmp_path / "cache")

    def prepare(*args):
        table = original_coordinates(*args)
        if failure == "changed_source":
            with source.open("ab") as stream:
                stream.write(b"source changed")
        elif failure == "interrupted":
            raise RuntimeError("interrupted")
        elif failure == "unsorted":
            return table.reverse()
        elif failure == "null_date":
            return table.with_columns(pl.lit(None, dtype=pl.Date).alias("date"))
        return table

    if failure == "admission":
        monkeypatch.setattr("stockagent.data.annual_event_cache.shutil.disk_usage", lambda _path: SimpleNamespace(free=0))
    with pytest.raises((ValueError, RuntimeError)):
        cache.prepare(source, ["a", "b"], ["2330"], prepare, prepare_identity="original")
    assert not list((tmp_path / "cache").glob("*/current.json"))


def test_empty_coordinates_are_explicit_not_invented(tmp_path):
    source = source_at(tmp_path)
    cache = PreparedObservationCache(tmp_path / "cache")
    assert list(cache.iter_dates(source, ["absent"], ["2330"], original_coordinates, prepare_identity="v1")) == []
    assert cache.builds == 1


@pytest.mark.parametrize("variant", ["prepared", "prepared_causal"])
def test_streamed_original_report_floors_equal_whole_year_across_years(tmp_path, variant):
    baseline = load_baseline(SOURCE)
    candidate, unchanged_writer = candidate_sources(SOURCE.read_text(), WRITER.read_text(), variant)
    assert unchanged_writer == WRITER.read_text()
    path = tmp_path / "candidate.py"
    path.write_text(candidate)
    module = load_baseline(path)
    module.PREPARED_ANNUAL_CACHE_ROOT = tmp_path / "cache"
    rows = [
        (date(2024, 12, 27), "q1", 1., "2024Q3"),
        (date(2024, 12, 28), "q1", 3., "2024Q4"),
        (date(2024, 12, 28), "q2", None, "2024Q4"),
        (date(2024, 12, 29), "q2", 999., "2024Q3"),
        (date(2025, 1, 1), "q1", 998., "2024Q3"),
        (date(2025, 1, 2), "q2", 4., "2024Q4"),
        (date(2025, 1, 3), "q1", -0., "2025Q1"),
        (date(2025, 1, 4), "q2", 997., "2024Q4"),
        (date(2025, 1, 4), "daily", -5., None),
    ]
    table = pl.DataFrame([{"date": day, "symbol": "2330", "feature": feature, "value": value,
                           "period": period} for day, feature, value, period in rows], schema=SCHEMA).with_columns(
        pl.lit("source").alias("_source_part"))
    paths = []
    for year in (2024, 2025):
        member = tmp_path / f"{year}.parquet"
        table.filter(pl.col("date").dt.year() == year).write_parquet(member)
        paths.append(member)
    definitions = [{"feature": f, "report_group": "report" if f.startswith("q") else ""}
                   for f in ("q1", "q2", "daily")]
    expected = list(baseline.AnnualObservationEvents(paths).iter_date_groups(definitions, ["2330"]))
    actual = list(module.AnnualObservationEvents(paths).iter_date_groups(definitions, ["2330"]))
    # Whole-year filtering can reorder independent coordinates, but the set,
    # release boundaries, NULL events and all bits must be exactly identical.
    assert [day for day, _ in actual] == [day for day, _ in expected]
    for (_, part), (_, original) in zip(actual, expected, strict=True):
        same_frames(part.sort("symbol", "feature"), original.sort("symbol", "feature"))
    warm = list(module.AnnualObservationEvents(paths).iter_date_groups(definitions, ["2330"]))
    assert sum(cache.builds for cache in module.PREPARED_ANNUAL_CACHE_INSTANCES) == 2
    for (_, part), (_, original) in zip(warm, expected, strict=True):
        same_frames(part.sort("symbol", "feature"), original.sort("symbol", "feature"))
    if variant == "prepared_causal":
        # A later delta inserts a period *inside* the global rank ordering.
        # Cached terminal subjects must be remapped, not reuse old integers.
        delta = tmp_path / "delta.parquet"
        pl.DataFrame([
            {"date": date(2025, 1, 5), "symbol": "2330", "feature": "daily", "value": 997., "period": "2024-12-31"},
            {"date": date(2025, 1, 5), "symbol": "2330", "feature": "q2", "value": 8., "period": "2025Q1"},
        ], schema=SCHEMA).with_columns(pl.lit("delta").alias("_source_part")).write_parquet(delta)
        all_paths = [*paths, delta]

        def compare(definitions):
            reference = list(baseline.AnnualObservationEvents(all_paths).iter_date_groups(definitions, ["2330"]))
            result = list(module.AnnualObservationEvents(all_paths).iter_date_groups(definitions, ["2330"]))
            assert [day for day, _ in result] == [day for day, _ in reference]
            for (_, part), (_, original) in zip(result, reference, strict=True):
                same_frames(part.sort("symbol", "feature"), original.sort("symbol", "feature"))
            return module.PREPARED_ANNUAL_CACHE_INSTANCES[-1].stats()

        stats = compare(definitions)
        assert stats["hits"] == 2 and stats["builds"] == 1
        # A changed prefix invalidates every dependent year, even if this
        # year's own bytes are unchanged. Old cache generations remain intact.
        pl.read_parquet(paths[0]).with_columns((pl.col("value") + 1).alias("value")).write_parquet(paths[0])
        stats = compare(definitions)
        assert stats["hits"] == 0 and stats["builds"] == 3
        changed = [{**definition, "report_group": "separate" if definition["feature"] == "q2"
                    else definition["report_group"]} for definition in definitions]
        stats = compare(changed)
        assert stats["hits"] == 0 and stats["builds"] == 3


def test_terminal_subjects_are_verified_and_corruption_rebuilt(tmp_path):
    source = source_at(tmp_path)
    cache = PreparedObservationCache(tmp_path / "cache")

    def prepare(*args):
        table = original_coordinates(*args)
        floors = pl.DataFrame([("2330", "report", "2025Q1")], schema=PERIOD_SCHEMA, orient="row")
        return PreparedAnnualCoordinates(table, floors)

    generation, metadata = cache.prepare(source, ["a"], ["2330"], prepare, prepare_identity="source-prefix-and-rules")
    assert cache.read_terminal_periods(generation, metadata).rows() == [("2330", "report", "2025Q1")]
    path = generation / "terminal_periods.parquet"
    path.chmod(0o600)
    with path.open("ab") as stream:
        stream.write(b"bad")
    new_generation, _ = cache.prepare(source, ["a"], ["2330"], prepare, prepare_identity="source-prefix-and-rules")
    assert generation != new_generation and generation.exists() and cache.rejections == 1


@pytest.mark.parametrize("invalid", ["duplicate", "null", "wrong_schema"])
def test_invalid_terminal_state_cannot_publish(tmp_path, invalid):
    source = source_at(tmp_path)
    cache = PreparedObservationCache(tmp_path / "cache")

    def prepare(*args):
        floors = pl.DataFrame([("2330", "report", "2025Q1")], schema=PERIOD_SCHEMA, orient="row")
        if invalid == "duplicate":
            floors = pl.concat([floors, floors])
        elif invalid == "null":
            floors = floors.with_columns(pl.lit(None, dtype=pl.String).alias("subject"))
        else:
            floors = floors.rename({"subject": "wrong"})
        return PreparedAnnualCoordinates(original_coordinates(*args), floors)

    with pytest.raises(ValueError, match="terminal"):
        cache.prepare(source, ["a"], ["2330"], prepare, prepare_identity="prefix-and-rules")
    assert not list((tmp_path / "cache").glob("*/current.json"))


def test_terminal_sidecar_cannot_cross_reserve_after_payload_check(tmp_path, monkeypatch):
    import stockagent.data.annual_event_cache as module

    source = source_at(tmp_path)
    cache = PreparedObservationCache(tmp_path / "cache", reserve_bytes=1024)
    checks = []

    def space(_root):
        checks.append(True)
        return SimpleNamespace(free=(1 << 40) if len(checks) < 3 else 0)

    def prepared(*args):
        return PreparedAnnualCoordinates(original_coordinates(*args), pl.DataFrame(schema=PERIOD_SCHEMA))

    monkeypatch.setattr(module.shutil, "disk_usage", space)
    with pytest.raises(ValueError, match="terminal-state storage"):
        cache.prepare(source, ["a"], ["2330"], prepared, prepare_identity="reserve after sidecar")
    assert len(checks) == 3 and cache.builds == 0
    assert not list((tmp_path / "cache").glob("*/current.json"))
    assert list((tmp_path / "cache").glob("*/.attempt-*"))
