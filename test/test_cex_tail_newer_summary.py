from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import asdict
from pathlib import Path
import shutil
from types import SimpleNamespace
from unittest.mock import patch

import polars as pl
import pytest

from downloader import download_binance_perp_15m as binance
from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx
from downloader.ohlcv_hot_tail import canonical_date_frame, tail_newer_summary
from scripts.benchmark_cex_tail_overlap import benchmark


BASE_MS = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def reference(frame, normalize, base_ms=BASE_MS):
    dates = normalize(frame).select(pl.col("date").str.to_datetime(strict=False).alias("__ts"))
    cutoff = datetime.fromtimestamp(base_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
    newer = dates.filter(pl.col("__ts") > cutoff)
    return newer.height, newer.select(pl.col("__ts").min()).item() if newer.height else None


@pytest.mark.parametrize("module", [binance, bybit, okx])
@pytest.mark.parametrize("values", [
    ["2026-01-01 00:02:00", "2026-01-01 00:00:00", "2026-01-01 00:01:00"],
    ["2026-01-01 00:01:00", "2026-01-01 00:01:00"],
    ["2025-12-31 23:59:00"],
    ["2026-01-01 00:00:00"],
    ["2026-01-01 00:00:59.999999", "2026-01-01 00:01:00.100000"],
    ["2026-01-01T00:01:00", "2026-01-01T00:02:00"],
    ["2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"],
    ["2026-01-01T08:01:00+08:00"],
    ["2026-01-01 00:01:00", None],
    ["2026-01-01 00:01:00", "2026-01-01 00:99:00"],
    ["2026-01-01 00:01:00", "bad"],
    ["bad"],
    [None],
    [],
])
def test_same_result_or_exception_as_complete_legacy_path(module, values):
    frame = pl.DataFrame({"date": pl.Series(values, dtype=pl.String)})
    before = frame.clone()
    normalize = module._normalize_date_frame
    try:
        expected = reference(frame, normalize)
    except Exception as error:
        with pytest.raises(type(error)):
            tail_newer_summary(frame, base_latest_ms=BASE_MS, normalize=normalize)
    else:
        assert tail_newer_summary(frame, base_latest_ms=BASE_MS, normalize=normalize) == expected
    assert frame.equals(before)


@pytest.mark.parametrize("module", [binance, bybit, okx])
@pytest.mark.parametrize("dtype", [pl.Datetime("us"), pl.Datetime("ns"), pl.Date, pl.Int64])
def test_non_string_keeps_legacy_cast_and_precision(module, dtype):
    dates = pl.Series([datetime(2026, 1, 1, 0, 1, 0, 999999), datetime(2025, 12, 31, 23, 59)])
    frame = pl.DataFrame({"date": dates.cast(dtype)})
    assert tail_newer_summary(frame, base_latest_ms=BASE_MS, normalize=module._normalize_date_frame) == reference(
        frame, module._normalize_date_frame)


def test_canonical_writer_dates_use_native_path_without_sort_or_string_format():
    frame = pl.DataFrame({"date": ["2026-01-01 00:02:00", "2026-01-01 00:01:00"]})

    def forbidden(_):
        raise AssertionError("complete normalization is unused for exact writer strings")

    assert tail_newer_summary(frame, base_latest_ms=BASE_MS, normalize=forbidden) == (
        2, datetime(2026, 1, 1, 0, 1))


def test_subsecond_base_cutoff_and_duplicate_count_are_not_rounded_or_deduplicated():
    frame = pl.DataFrame({"date": ["2026-01-01 00:00:00", "2026-01-01 00:00:01", "2026-01-01 00:00:01"]})
    assert tail_newer_summary(frame, base_latest_ms=BASE_MS + 999, normalize=bybit._normalize_date_frame) == (
        2, datetime(2026, 1, 1, 0, 0, 1))


def test_missing_column_keeps_reference_exception():
    frame = pl.DataFrame({"other": [1]})
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        tail_newer_summary(frame, base_latest_ms=BASE_MS, normalize=bybit._normalize_date_frame)


@pytest.mark.parametrize("module", [binance, bybit, okx])
@pytest.mark.parametrize("tail_values", [
    ["2026-01-01 00:01:00", "2026-01-01 00:02:00"],
    ["2026-01-01 00:04:00"],
    ["2026-01-01 00:01:00", "2026-01-01 00:01:00"],
    ["2025-12-31 23:59:00"],
    ["2026-01-01 00:02:00", None],
    ["2026-01-01 00:02:00", "bad"],
    ["2026-01-01T00:02:00Z"],
    ["2026-01-01 00:00:00.123456"],
    ["bad"],
    [],
])
def test_complete_information_loader_matches_legacy_overlap(module, tail_values, tmp_path):
    base = tmp_path / "TEST_features.parquet"
    pl.DataFrame({"date": ["2026-01-01 00:00:00", "2026-01-01 00:01:00"]}).write_parquet(
        base, statistics=True)
    tail = tmp_path / "_hot_tail" / base.name
    tail.parent.mkdir()
    pl.DataFrame({"date": pl.Series(tail_values, dtype=pl.String)}).write_parquet(tail, statistics=False)
    name = "_load_logical_existing_info" if module is binance else "_load_logical_existing_candle_info"
    loader = getattr(module, name)

    def legacy_overlap(frame, *, base_latest_ms, normalize):
        return reference(frame, normalize, base_ms=base_latest_ms)

    before = base.read_bytes(), tail.read_bytes()
    kwargs = {"require_contiguous": False} if module is bybit else {}
    try:
        with patch.object(module, "tail_newer_summary", legacy_overlap):
            expected = asdict(loader(base, **kwargs))
    except Exception as error:
        with pytest.raises(type(error)):
            loader(base, **kwargs)
    else:
        assert asdict(loader(base, **kwargs)) == expected
    assert (base.read_bytes(), tail.read_bytes()) == before


def test_benchmark_rejects_existing_output_without_source_reads(tmp_path):
    with pytest.raises(ValueError, match="fresh output"):
        benchmark(tmp_path, tmp_path)


def test_benchmark_rejects_source_overlap_before_creating_private_files(tmp_path, monkeypatch):
    from scripts import benchmark_cex_tail_overlap as helper

    monkeypatch.setattr(helper, "ROOT", tmp_path)
    root = tmp_path / "data_bybit" / "1m"
    root.mkdir(parents=True)
    output = root / "candidate"
    with pytest.raises(ValueError, match="overlap source"):
        benchmark(tmp_path, output)
    assert not output.exists()


@pytest.mark.parametrize("values", [
    ["2026-01-01 00:00:00", "2026-01-01 00:01:00"],
    ["2000-02-29 23:59:59"],
    ["0000-01-01 00:00:00"],
])
def test_canonical_date_frame_reuses_exact_complete_values(values):
    frame = pl.DataFrame({"date": values, "value": list(range(len(values)))})
    before = frame.clone()
    result = canonical_date_frame(frame)
    assert result is not None
    assert result.equals(bybit._normalize_date_frame(frame))
    assert result.schema == frame.schema
    assert frame.equals(before)
    if frame["date"].is_sorted():
        assert result is frame


@pytest.mark.parametrize("values", [
    [], [None], ["bad"], ["2026-01-01 00:00:00", None],
    ["2026-02-29 00:00:00"], ["2026-01-01 24:00:00"],
    ["2026-01-01 00:00:60"], ["2026-01-01 00:00:00.000000"],
    ["2026-01-01T00:00:00"], ["2026-01-01 00:00:00Z"],
    ["2026-01-01 00:00:00\n"], ["2026-1-01 00:00:00"],
    ["20260-1-01 00:00:00"], ["2026-01-01 00:00:00", "bad"],
    ["2026-01-01 00:02:00", "2026-01-01 00:00:00"],
    ["2026-01-01 00:01:00", "2026-01-01 00:01:00"],
])
def test_canonical_date_frame_declines_without_repair_or_dropping_rows(values):
    frame = pl.DataFrame({"date": pl.Series(values, dtype=pl.String)})
    assert canonical_date_frame(frame) is None


@pytest.mark.parametrize("dtype", [pl.Datetime("us"), pl.Datetime("ns"), pl.Date, pl.Int64])
def test_canonical_date_frame_declines_non_string_semantics(dtype):
    frame = pl.DataFrame({"date": pl.Series([datetime(2026, 1, 1)]).cast(dtype)})
    assert canonical_date_frame(frame) is None


def test_canonical_date_frame_does_not_trust_sorted_metadata():
    frame = pl.DataFrame({"date": ["2026-01-01 00:02:00", "2026-01-01 00:00:00"]}).set_sorted("date")
    assert canonical_date_frame(frame) is None


def _legacy_date_normalizer(frame):
    if frame.is_empty() or "date" not in frame.columns:
        return frame
    expr = (pl.col("date").str.to_datetime(strict=False)
            if frame.schema["date"] == pl.String
            else pl.col("date").cast(pl.Datetime("us"), strict=False))
    return frame.with_columns(expr.dt.strftime("%Y-%m-%d %H:%M:%S").alias("date")).drop_nulls("date").sort("date")


@pytest.mark.parametrize("values", [
    ["2026-01-01 00:00:00", "2026-01-01 00:01:00"],
    ["2026-01-01 00:01:00", "2026-01-01 00:01:00"],
    ["2026-01-01 00:01:00", "2026-01-01 00:00:00"],
    ["2026-01-01 00:00:59.999999", "2026-01-01 00:01:00.000000"],
    ["2026-01-01 00:00:00", "bad"], [None], [],
    ["2026-01-01T08:00:00+08:00", "2026-01-01T08:01:00+08:00"],
    ["2000-02-29 23:59:59"], ["2026-02-29 23:59:59"],
])
@pytest.mark.parametrize("provider", ["bybit", "okx"])
def test_normalizer_prototype_keeps_full_values_and_legacy_exceptions(provider, values):
    from scripts.benchmark_cex_tail_overlap import normalization_prototype

    frame = pl.DataFrame({"date": pl.Series(values, dtype=pl.String),
                          "negative": [-0.0] * len(values),
                          "sparse": pl.Series([None] * len(values), dtype=pl.Float64)})
    module = SimpleNamespace(_normalize_date_frame=_legacy_date_normalizer)
    candidate = normalization_prototype(module, provider)
    try:
        expected = _legacy_date_normalizer(frame)
    except Exception as error:
        with pytest.raises(type(error)):
            candidate(frame)
    else:
        assert candidate(frame).equals(expected)
        assert candidate(frame).schema == expected.schema


def test_normalizer_prototype_never_bypasses_binance_close_time_repair():
    from scripts.benchmark_cex_tail_overlap import normalization_prototype

    frame = pl.DataFrame({"date": ["2026-01-01 00:00:00"],
                          "binance_close_time_ms": [BASE_MS + 119999]})
    result = normalization_prototype(binance, "binance")(frame)
    assert result["date"].to_list() == ["2026-01-01 00:01:00"]


def test_complete_merge_benchmark_preserves_all_sources_and_output_schema(tmp_path, monkeypatch):
    from scripts import benchmark_cex_tail_overlap as helper

    code = tmp_path / "downloader"
    code.mkdir()
    reference = tmp_path / "reference"
    reference.mkdir()
    source_root = Path(helper.__file__).resolve().parents[1]
    shutil.copyfile(source_root / "downloader/ohlcv_hot_tail.py", code / "ohlcv_hot_tail.py")
    for provider, module in (("bybit", bybit), ("okx", okx), ("binance", binance)):
        shutil.copyfile(Path(module.__file__), reference / f"reference_{provider}.py")
        root = tmp_path / f"data_{provider}" / "1m"
        (root / "_hot_tail").mkdir(parents=True)
        pl.DataFrame({"date": ["2025-12-31 23:59:00"], "close": [1.0]}).write_parquet(root / "TEST_features.parquet")
        pl.DataFrame({"date": ["2026-01-01 00:00:00", "2026-01-01 00:01:00"],
                      "close": [-1.0, 0.0], "sparse": [None, 2.0]}).write_parquet(root / "_hot_tail" / "TEST_features.parquet")
    monkeypatch.setattr(helper, "ROOT", tmp_path)
    result = helper.benchmark_normalization(reference, tmp_path / "benchmark")
    assert result["state"] == "accepted_output_parity"
    assert result["symbols_by_provider"] == {"bybit": 1, "okx": 1, "binance": 1}
    assert len({row["output_sha256"] for row in result["measurements"]}) == 1
    assert result["provider_calls"] == result["source_writes"] == 0
    reused_output = tmp_path / "reused-benchmark"
    reused = helper.benchmark_normalization(reference, reused_output, input_fixture=tmp_path / "benchmark")
    assert reused["output_sha256"] == result["output_sha256"]
    assert reused["input_receipt_sha256"] == result["input_receipt_sha256"]
    assert not (reused_output / "bybit").exists()


def test_complete_value_digest_ignores_string_view_storage_not_data():
    from scripts.benchmark_cex_tail_overlap import frame_digest

    frame = pl.DataFrame({"date": ["2026-01-01 00:00:00", "2026-01-01 00:01:00"],
                          "value": [-0.0, None]})
    normalized = _legacy_date_normalizer(frame)
    assert frame_digest(frame) == frame_digest(normalized)
    assert frame_digest(frame) != frame_digest(frame.reverse())
    assert frame_digest(frame) != frame_digest(frame.with_columns(pl.lit(0.0).alias("value")))
    assert frame_digest(frame) != frame_digest(frame.with_columns(pl.col("value").cast(pl.Float32)))


def test_full_value_digest_preserves_valid_nan_payload_and_signed_zero():
    import numpy as np
    from scripts.benchmark_cex_tail_overlap import frame_digest

    nan_a = np.array([0x7FF8000000000001], dtype=np.uint64).view(np.float64)
    nan_b = np.array([0x7FF8000000000002], dtype=np.uint64).view(np.float64)
    assert frame_digest(pl.DataFrame({"value": nan_a})) != frame_digest(pl.DataFrame({"value": nan_b}))
    assert frame_digest(pl.DataFrame({"value": [-0.0]})) != frame_digest(pl.DataFrame({"value": [0.0]}))


def test_normalization_benchmark_rejects_redirected_source_overlap(tmp_path, monkeypatch):
    from scripts import benchmark_cex_tail_overlap as helper

    monkeypatch.setattr(helper, "ROOT", tmp_path)
    source = tmp_path / "data_bybit" / "1m"
    source.mkdir(parents=True)
    alias = tmp_path / "redirected"
    alias.symlink_to(source, target_is_directory=True)
    with pytest.raises(ValueError, match="overlap source"):
        helper.benchmark_normalization(tmp_path, alias / "case")
    assert not (source / "case").exists()


def test_value_digest_ignores_only_undefined_null_and_unused_bitmap_bits():
    import numpy as np
    import pyarrow as pa
    from scripts.benchmark_cex_tail_overlap import frame_digest

    def make(bitmap, hidden):
        array = pa.Array.from_buffers(pa.float64(), 2, [pa.py_buffer(bytes([bitmap])),
            pa.py_buffer(np.array([-0.0, hidden], dtype=np.float64).tobytes())])
        return pl.from_arrow(pa.table({"value": array}))

    left, right = make(1, 123.0), make(253, -999.0)
    assert left.equals(right)
    assert frame_digest(left) == frame_digest(right)
    assert frame_digest(left) != frame_digest(left.fill_null(123.0))


@pytest.mark.parametrize("values,dtype", [
    ([], pl.Float64), ([None, None], pl.Float64), ([], pl.String),
    ([True, None, False], pl.Boolean), ([b"ab", None, b"c"], pl.Binary),
    ([2**64 - 1, None, 0], pl.UInt64),
    ([datetime(2026, 1, 1), None], pl.Datetime("ns")),
])
def test_value_digest_preserves_empty_null_boolean_binary_and_temporal_columns(values, dtype):
    from scripts.benchmark_cex_tail_overlap import frame_digest

    frame = pl.DataFrame({"value": pl.Series(values, dtype=dtype)})
    assert frame_digest(frame) == frame_digest(frame.clone())


def test_value_digest_keeps_string_boundaries_and_chunk_independence():
    from scripts.benchmark_cex_tail_overlap import frame_digest

    left = pl.DataFrame({"s": ["ab", "c"]})
    right = pl.DataFrame({"s": ["a", "bc"]})
    assert frame_digest(left) != frame_digest(right)
    chunked = pl.concat([left.head(1), left.tail(1)], rechunk=False)
    assert frame_digest(left) == frame_digest(chunked)
