from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import ohlcv_hot_tail as reader
from scripts.benchmark_ohlcv_tail_reader import _frame_identity


@pytest.mark.parametrize("count", [0, 1, 32, 8193, 20_000])
@pytest.mark.parametrize("columns", [None, ["close", "date"]])
def test_bounded_reader_exact_values_and_dtypes(tmp_path: Path, count, columns) -> None:
    frame = pl.DataFrame({
        "date": pl.datetime_range(pl.datetime(2026, 1, 1), pl.datetime(2026, 1, 8),
                                   interval="1m", eager=True),
    }).with_row_index("index").with_columns(
        pl.when(pl.col("index") % 7 == 0).then(None)
        .otherwise(pl.col("index").cast(pl.Float64)).alias("close"),
        (pl.col("index") % 2 == 0).alias("flag"),
    )
    path = tmp_path / "physical[tail].parquet"
    frame.write_parquet(path, row_group_size=50_000)
    schema = columns or frame.columns
    reference = frame.select(schema).tail(max(1, count))
    actual = pl.from_arrow(reader._read_tail_row_groups(path, count, columns=columns))
    assert _frame_identity(actual) == _frame_identity(reference)


def test_projection_precedes_native_slice_and_missing_columns_remain_null(tmp_path, monkeypatch):
    path = tmp_path / "base.parquet"
    pl.DataFrame({"date": ["2026-01-01 00:00:00"] * 9000,
                  "close": range(9000), "unused": ["large"] * 9000}).write_parquet(path)
    scan = pl.scan_parquet
    plans = []

    def observed_scan(*args, **kwargs):
        query = scan(*args, **kwargs)

        class Observed:
            def select(self, columns):
                return ObservedSelected(query.select(columns))

        class ObservedSelected:
            def __init__(self, selected):
                self.query = selected

            def tail(self, count):
                limited = self.query.tail(count)
                plans.append(limited.explain())
                return limited

        return Observed()

    monkeypatch.setattr(pl, "scan_parquet", observed_scan)
    result = reader.read_logical_parquet(path, columns=["close", "absent"], tail_rows=32)
    assert result.columns == ["close", "absent"]
    assert result["close"].to_list() == list(range(8968, 9000))
    assert result["absent"].null_count() == 32
    assert any("PROJECT 1/3 COLUMNS" in plan and "SLICE" in plan for plan in plans)


@pytest.mark.parametrize("nested", [True, False])
def test_nested_and_dictionary_types_keep_arrow_reader(tmp_path, monkeypatch, nested):
    path = tmp_path / "fallback.parquet"
    values = (pa.array([[1, 2]] * 9000) if nested
              else pa.array(["a", "b"] * 4500).dictionary_encode())
    table = pa.table({"value": values})
    pq.write_table(table, path)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Unsupported logical dtype must keep Arrow's decoder")

    monkeypatch.setattr(pl, "scan_parquet", forbidden)
    result = reader._read_tail_row_groups(path, 32)
    assert pl.from_arrow(result).equals(pl.from_arrow(table.slice(8968, 32)))


def test_native_slice_retains_nan_payload_signed_zero_and_null_mask(tmp_path):
    path = tmp_path / "bits.parquet"
    bits = np.array([0, 0x8000000000000000, 0x7FF8000000000017,
                     0x7FF0000000000000, 0xFFF0000000000000], dtype=np.uint64)
    values = np.tile(bits.view(np.float64), 2000)
    frame = pl.DataFrame({"value": values}).with_columns(
        pl.when(pl.int_range(pl.len()) % 17 == 0).then(None)
        .otherwise(pl.col("value")).alias("value"))
    frame.write_parquet(path)
    reference = pl.from_arrow(pq.read_table(path)).tail(32)
    actual = pl.from_arrow(reader._read_tail_row_groups(path, 32))
    assert _frame_identity(actual) == _frame_identity(reference)


def test_corrupt_requested_column_does_not_become_empty_or_ready(tmp_path):
    path = tmp_path / "broken.parquet"
    pl.DataFrame({"value": range(9000)}).write_parquet(path)
    data = bytearray(path.read_bytes())
    # Preserve the footer, corrupt the first compressed column page.
    data[20:120] = b"\xff" * 100
    path.write_bytes(data)
    with pytest.raises((pl.exceptions.PolarsError, pa.ArrowException)):
        reader._read_tail_row_groups(path, 32)


def test_small_file_keeps_arrow_without_native_startup(tmp_path, monkeypatch):
    path = tmp_path / "small.parquet"
    pl.DataFrame({"value": range(100)}).write_parquet(path)
    monkeypatch.setattr(pl, "scan_parquet", lambda *_args, **_kwargs:
                        pytest.fail("Small row groups must not invoke native scan"))
    assert pl.from_arrow(reader._read_tail_row_groups(path, 32))["value"].to_list() == list(range(68, 100))


def test_atomic_source_replacement_during_native_read_is_rejected(tmp_path, monkeypatch):
    path, replacement = tmp_path / "source.parquet", tmp_path / "replacement.parquet"
    pl.DataFrame({"value": range(9000)}).write_parquet(path)
    pl.DataFrame({"value": range(1, 9001)}).write_parquet(replacement)
    scan = pl.scan_parquet

    class ReplacedRead:
        def __init__(self, query):
            self.query = query

        def tail(self, count):
            self.query = self.query.tail(count)
            return self

        def collect(self):
            result = self.query.collect()
            replacement.replace(path)
            return result

    monkeypatch.setattr(pl, "scan_parquet", lambda *args, **kwargs:
                        ReplacedRead(scan(*args, **kwargs)))
    with pytest.raises(OSError, match="generation changed"):
        reader._read_tail_row_groups(path, 32)


def test_complete_identity_distinguishes_values_schema_and_nulls():
    base = pl.DataFrame({"x": [0.0, -0.0, float("nan"), None]})
    assert _frame_identity(base) == _frame_identity(base.clone())
    assert _frame_identity(base) != _frame_identity(base.reverse())
    assert _frame_identity(base) != _frame_identity(base.fill_null(0.0))
    assert _frame_identity(base) != _frame_identity(base.cast({"x": pl.Float32}))
    assert _frame_identity(base) != _frame_identity(pl.DataFrame({"x": [0.0, 0.0, float("nan"), None]}))
