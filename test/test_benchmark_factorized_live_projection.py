"""Guard exact candidate construction and complete release-group semantics."""
from datetime import date
import io

import numpy as np
import polars as pl
import pytest

from scripts.benchmark_factorized_live_projection import (
    SLICE_HELPER, candidate_sources, new_private, verify_fixture,
)


def groups(table):
    namespace = {}
    exec(SLICE_HELPER, namespace)
    return list(namespace["chronological_date_groups"](table))


@pytest.mark.parametrize("order", ["sorted", "shuffled", "multi_chunk"])
def test_date_slices_match_original_partition_groups_all_fields(order):
    table = pl.DataFrame({"date": [date(2025, 1, day) for day in (3, 1, 3, 2, 1)],
                          "symbol": ["2330", "2317", "*", "0", "2317"],
                          "feature": ["a", "b", "x", "a", "b"],
                          "period": ["2024Q4", "2024Q3", None, "", "2024Q4"],
                          "value": [0., -0., None, float("nan"), -7.]})
    if order == "sorted":
        table = table.sort("date", maintain_order=True)
    elif order == "multi_chunk":
        table = pl.concat([table[:2], table[2:]], rechunk=False)
    before = table.clone()
    expected = sorted((key[0], value) for key, value in table.partition_by("date", as_dict=True).items())
    actual = groups(table)
    assert [day for day, _ in actual] == [day for day, _ in expected]
    for (_, part), (_, original) in zip(actual, expected, strict=True):
        assert part.equals(original)
        np.testing.assert_array_equal(part["value"].to_numpy().view(np.uint64),
                                      original["value"].to_numpy().view(np.uint64))
    assert table.equals(before)


def test_empty_and_null_dates_are_not_invented():
    assert groups(pl.DataFrame(schema={"date": pl.Date, "value": pl.Float64})) == []
    with pytest.raises(ValueError, match="NULL"):
        groups(pl.DataFrame({"date": [None], "value": [1.]}, schema={"date": pl.Date, "value": pl.Float64}))


@pytest.mark.parametrize("shape", [(1, 1, 0), (2, 3, 4), (32, 128, 17)])
@pytest.mark.parametrize("layout", ["contiguous", "strided", "fortran"])
def test_single_take_retains_every_raw_bit_and_npz_header(shape, layout):
    generator = np.random.default_rng(719)
    bits = generator.integers(0, 2**32, size=shape, dtype=np.uint32)
    if shape[-1]:
        bits[..., 0] = 0
        bits.flat[-1] = 0x80000000  # Signed zero is retained, not elided.
    values = bits.view(np.float32)
    if layout == "strided":
        values = values[:, :, ::-1]
    elif layout == "fortran":
        values = np.asfortranarray(values)
    values = np.ascontiguousarray(values, dtype=np.float32)
    before = values.tobytes()
    columns = np.flatnonzero(np.any(values.view(np.uint32) != 0, axis=(0, 1))).astype(np.uint32)
    original = np.ascontiguousarray(values[..., columns])
    candidate = np.take(values, columns, axis=-1)
    assert candidate.flags.c_contiguous and candidate.dtype == np.float32
    assert candidate.tobytes() == original.tobytes() and values.tobytes() == before
    expected, actual = io.BytesIO(), io.BytesIO()
    np.savez(expected, values=original, columns=columns)
    np.savez(actual, values=candidate, columns=columns)
    assert actual.getvalue() == expected.getvalue()


@pytest.mark.parametrize("variant", ["baseline", "take", "slices", "slices_take"])
def test_candidates_replace_only_frozen_hotspot(variant):
    sweep = 'prefix\nfor (day,),part in sorted(table.partition_by("date",as_dict=True).items()):yield day,part\nsuffix'
    writer = 'prefix\nstored = np.ascontiguousarray(values[..., columns])\nsuffix'
    actual_sweep, actual_writer = candidate_sources(sweep, writer, variant)
    assert (actual_sweep != sweep) == ("slices" in variant)
    assert (actual_writer != writer) == ("take" in variant)
    if variant == "baseline":
        assert (actual_sweep, actual_writer) == (sweep, writer)
    assert actual_sweep.startswith("prefix\n") and actual_writer.startswith("prefix\n")


@pytest.mark.parametrize("sweep,writer,variant", [
    ("changed", "changed", "take"), ("changed", "changed", "slices"),
    ("changed", "changed", "other"),
])
def test_unknown_original_or_candidate_is_rejected(sweep, writer, variant):
    with pytest.raises(ValueError):
        candidate_sources(sweep, writer, variant)


def test_benchmark_cannot_overwrite_existing_or_live_destination(tmp_path):
    with pytest.raises(ValueError, match="private"):
        new_private(tmp_path / "arbitrary")
    with pytest.raises(ValueError, match="private"):
        new_private(tmp_path)
    with pytest.raises(FileNotFoundError):
        verify_fixture(tmp_path)
