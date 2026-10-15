from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.benchmark_ohlcv_tail_reader import (
    READER, _frame_identity, child, select_tw_sources, threshold_candidate,
)
from scripts.benchmark_project_data_processing import load_baseline


@pytest.mark.parametrize("threshold", [-1, True, 1.2, None])
def test_invalid_cutoff_is_not_an_implicit_policy_change(threshold):
    with pytest.raises(ValueError):
        threshold_candidate(SimpleNamespace(), threshold)


def test_unknown_reference_cutoff_cannot_be_silently_rewritten(monkeypatch):
    monkeypatch.setattr("inspect.getsource", lambda _value: "def _read_tail_row_groups():\n    return 1\n")
    with pytest.raises(ValueError, match="single measured"):
        threshold_candidate(SimpleNamespace(_read_tail_row_groups=lambda: None), 256)


@pytest.mark.parametrize("rows", [100, 257, 1000, 5580, 9000])
@pytest.mark.parametrize("columns", [None, ["value", "date"]])
def test_isolated_cutoff_preserves_entire_requested_bits_and_current_reader(tmp_path, rows, columns):
    from downloader import ohlcv_hot_tail as current
    original = current._read_tail_row_groups
    module = load_baseline(READER)
    threshold_candidate(module, 256)
    path = tmp_path / "source[0].parquet"
    bits = np.resize(np.array([0, 0x8000000000000000, 0x7FF8000000000017], dtype=np.uint64), rows)
    source = pl.DataFrame({"date": pl.datetime_range(pl.datetime(2000, 1, 1),
        pl.datetime(2030, 1, 1), interval="1d", eager=True).head(rows),
        "value": bits.view(np.float64), "unused": ["台股"] * rows}).with_columns(
            pl.when(pl.int_range(pl.len()) % 17 == 0).then(None).otherwise(pl.col("value")).alias("value"))
    source.write_parquet(path, row_group_size=rows)
    baseline = pl.from_arrow(original(path, 48, columns=columns))
    candidate = pl.from_arrow(module._read_tail_row_groups(path, 48, columns=columns))
    assert _frame_identity(candidate) == _frame_identity(baseline)
    assert current._read_tail_row_groups is original


def test_nested_candidate_still_preserves_the_arrow_fallback(tmp_path, monkeypatch):
    module = load_baseline(READER)
    threshold_candidate(module, 256)
    path = tmp_path / "nested.parquet"
    table = pa.table({"x": pa.array([[1, 2]] * 1000)})
    pq.write_table(table, path)
    monkeypatch.setattr(pl, "scan_parquet", lambda *_args, **_kwargs: pytest.fail("nested native scan"))
    assert pl.from_arrow(module._read_tail_row_groups(path, 48)).equals(pl.from_arrow(table.slice(952, 48)))


def test_source_sample_is_deterministic_stratified_and_not_all_market(tmp_path):
    for index in range(10):
        path = tmp_path / f"{index}_features.parquet"
        path.write_bytes(b"a" * (index + 1))
    selected = select_tw_sources(tmp_path, 4)
    assert [path.name for path in selected.values()] == [f"{index}_features.parquet" for index in (0, 3, 6, 9)]
    assert select_tw_sources(tmp_path, 4) == selected
    with pytest.raises(ValueError):
        select_tw_sources(tmp_path, 11)


def test_child_cutoff_applies_to_the_frozen_baseline_and_complete_read(tmp_path):
    fixture = tmp_path / "fixture"
    fixture.mkdir()
    (fixture / "baseline.py").write_bytes(READER.read_bytes())
    source = fixture / "tiny.parquet"
    pl.DataFrame({"date": ["2026-10-13"] * 1000, "close": range(1000)}).write_parquet(source)
    result = child({"fixture": str(fixture), "base": str(source), "mode": "cutoff_candidate",
                    "tail_threshold": 256, "case": "all48"})
    assert result["result"]["rows"] == 48
    assert result["result"]["columns"] == 2
    assert result["workflow_seconds"] > 0
