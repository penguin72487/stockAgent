"""Independent legacy references live in tests, never in the collectors."""
import ast
from datetime import date, time
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import numpy as np
import polars as pl
import pyarrow as pa
import pytest

from scripts.download_finlab_history import serialize_provider_frame, _event_bounds
from scripts.download_finlab_intraday import _validate_frame, fetch_partitions
from scripts.derive_finlab_minute import ticks_to_minutes
from scripts.finlab_columnar import canonical_arrow, to_polars, source_attrs, write_parquet, timestamp_bounds
from scripts.finlab_volume_units import with_verified_volume_shares
from scripts.finlab_incremental_refresh import checked_get
from test_finlab_stage_schedule import sdk_mock


@pytest.mark.parametrize('period,expected', [
    ('year', [('2024', 0, 2), ('2025', 2, 1)]),
    ('month', [('2024-01', 0, 1), ('2024-12', 1, 1), ('2025-01', 2, 1)]),
])
def test_period_slices_preserve_literal_index_order_and_raw_table(period, expected):
    from scripts.finlab_columnar import history_period_slices

    original = pa.table({'source_index': ['2024-01-01 00:00:00.123456789+08:00',
        '2024-12-31', '2025-01-01T00:00:00+00:00'], '0050': [-0., None, -13.25]})
    slices = history_period_slices(original, period)
    assert slices == expected
    assert pa.concat_tables([original.slice(begin, count) for _, begin, count in slices]).equals(
        original, check_metadata=True)


@pytest.mark.parametrize('labels', [
    ['2025-01-01', '2024-01-01'], ['2024-Q1'], ['2024-02-30'],
    ['2024-01-01', None], ['2024-01-01 not a timestamp'],
])
def test_period_slices_reject_unknown_null_or_reordered_clocks(labels):
    from scripts.finlab_columnar import history_period_slices

    original = pa.table({'source_index': pa.array(labels, type=pa.string())})
    with pytest.raises(ValueError):
        history_period_slices(original, 'year')


def test_native_period_append_fully_recovers_both_raw_versions_with_all_readers(tmp_path):
    import pyarrow.parquet as pq
    from scripts.benchmark_finlab_incremental_parquet import trial

    observed = pa.table({'source_index': ['2024-01-01', '2025-01-01', '2025-01-02'],
        '0050': [-0., None, -13.25], '2330': pa.array([2**63-1, -1, None], type=pa.int64())})
    fixture = tmp_path / 'observed.parquet'
    write_parquet(observed, fixture, attrs={'volume_units': 'shares', 'clock': 'source_label'})
    observed = pq.read_table(fixture)
    scratch = tmp_path / 'comparison'
    scratch.mkdir()
    result = trial(observed, scratch, None, partition_period='year', verify_other_readers=True)
    assert result['previous_version_readback_after_append']
    assert result['readers_verified'] == ['pyarrow', 'polars', 'duckdb']
    assert result['cold_versions_fully_reconstructed'] == 2
    assert result['stages'][1]['source_reused_files'] == 1
    assert result['stages'][1]['source_written_files'] == 1
    assert result['stages'][1]['source_written_bytes'] < result['stages'][1]['file_bytes']


@pytest.mark.parametrize("index", [pd.Index([10, 2, 3]),
    pd.Index([float("nan"), 2., 3.]),
    pd.Index(["2020-M01", "2020-M03", "2020-M02"]),
    pd.date_range("2020-01-01", periods=3, tz="Asia/Taipei"),
    pd.MultiIndex.from_tuples([("2330", 1), ("0050", 2), ("2330", 0)])])
def test_source_serialization_matches_independent_legacy_reference(index):
    source = pd.DataFrame({"a": [-3.0, float("nan"), 0.0], "b": [None, None, None],
                           "flag": pd.array([False, None, True], dtype="boolean")}, index=index)
    expected_index = source.index.to_frame(index=False)
    expected_index.columns = (["source_index"] if index.nlevels == 1 else
                              [f"source_index_{n}" for n in range(index.nlevels)])
    expected = pd.concat([expected_index.map(str), source.reset_index(drop=True)], axis=1)
    actual, stats = serialize_provider_frame(source)
    assert actual.equals(pa.Table.from_pandas(expected, preserve_index=False), check_metadata=False)
    valid = source.notna().any(axis=1)
    observed = source.index[valid.to_numpy()]
    assert stats["rows_with_values"] == int(valid.sum())
    assert stats["non_null_values"] == int(source.notna().sum().sum())
    assert stats["all_null_source_columns"] == ["b"]
    assert stats["first_non_null_source_index"] == str(observed.min())
    assert stats["last_non_null_source_index"] == str(observed.max())


def test_empty_dates_and_invalid_event_dates_are_unknown_not_fabricated():
    assert _event_bounds(pa.array([], type=pa.timestamp("ns")))["event_date_rows"] == 0
    assert _event_bounds(pa.array(["2020-01-02", None, "bad"]))["event_date_rows"] == 1
    assert _event_bounds(pa.array(["bad", None]))["event_date_rows"] == 0


@pytest.mark.parametrize("content_hash,valid", [("abc1234", True), ("abc12345", True),
                                               ("abc123", False), ("no-hash", False)])
def test_installed_sdk_seven_character_hash_is_a_valid_revision(content_hash, valid):
    sdk, _, _ = sdk_mock(content_hash=content_hash)
    _, evidence = checked_get("price:a", sdk=sdk)
    assert evidence["provider_content_hash"] == (content_hash if valid else None)
    assert evidence["normalization_contract_version"] == 2


def test_nanosecond_precision_timezone_raw_values_and_attrs_survive_boundary(tmp_path):
    source = pd.DataFrame({"timestamp": pd.to_datetime(["2026-06-01T09:00:00.000000001+08:00"]),
                           "raw": [-123.5], "missing": [float("nan")]})
    source.attrs["closed_dates"] = ["2026-06-02"]
    frame = to_polars(source)
    assert frame.schema["timestamp"].time_unit == "ns"
    assert timestamp_bounds(frame)[0] == "2026-06-01 09:00:00.000000001+08:00"
    assert frame[0, "raw"] == -123.5 and frame[0, "missing"] is None
    path = tmp_path / "data.parquet"
    write_parquet(frame, path, attrs=source_attrs(source))
    assert canonical_arrow(pl.read_parquet(path)).equals(canonical_arrow(frame), check_metadata=False)
    assert source.attrs["closed_dates"] == ["2026-06-02"]


@pytest.mark.parametrize("size", [True, 0, -1, 1.5, 1048577])
def test_parquet_row_group_size_rejected_before_writing(tmp_path, size):
    path = tmp_path / 'invalid.parquet'
    with pytest.raises(ValueError, match='row group size'):
        write_parquet(pa.table({'raw': [-1., None]}), path, row_group_size=size)
    assert not path.exists()


def test_explicit_row_groups_preserve_complete_raw_table_and_metadata(tmp_path):
    import pyarrow.parquet as pq

    table = pa.table({'symbol': ['0050', '2330', None, '2330', '0050'],
                      'raw': [-3.25, None, 0., 17., -1.]})
    baseline, candidate = tmp_path / 'baseline.parquet', tmp_path / 'candidate.parquet'
    write_parquet(table, baseline, attrs={'source_role': 'unverified_observation'})
    write_parquet(table, candidate, attrs={'source_role': 'unverified_observation'}, row_group_size=2)
    assert pq.ParquetFile(candidate).metadata.num_row_groups == 3
    assert pq.read_table(candidate).equals(pq.read_table(baseline), check_metadata=True)


@pytest.mark.parametrize('option,value', [
    ('use_dictionary', 1), ('use_dictionary', None),
    ('use_byte_stream_split', 1), ('use_byte_stream_split', 'true'),
])
def test_parquet_encoding_policy_rejects_non_boolean_values_before_writing(tmp_path, option, value):
    path = tmp_path / 'invalid.parquet'
    with pytest.raises(ValueError, match='encoding policies must be boolean'):
        write_parquet(pa.table({'raw': [-1., None]}), path, **{option: value})
    assert not path.exists()


def test_byte_plane_storage_preserves_raw_bits_nulls_types_and_all_three_readers(tmp_path):
    import duckdb
    import pyarrow.parquet as pq

    raw64 = np.array([0x8000000000000000, 0, 0x7ff8000000001234,
        0xfff0000000000000, 0x0010000000000000, 0x7fefffffffffffff], dtype=np.uint64).view(np.float64)
    raw32 = np.array([0x80000000, 0, 0x7fc01234, 0xff800000,
        0x00800000, 0x7f7fffff], dtype=np.uint32).view(np.float32)
    table = pa.table({
        'source_index': ['0050', '2330', '0010', None, '2026-01-01', '2026-01-02'],
        'raw64': pa.array(raw64, mask=np.array([False, True, False, False, False, False])),
        'raw32': pa.array(raw32, mask=np.array([False, True, False, False, False, False])),
        'integer': pa.array([-(2**63), None, 2**63-1, 0, -1, 1], type=pa.int64()),
    })
    path = tmp_path / 'byte-planes.parquet'
    write_parquet(table, path, attrs={'unit': 'shares'}, row_group_size=2,
                  use_dictionary=False, use_byte_stream_split=True)
    with duckdb.connect() as connection:
        sql_readback = connection.execute('SELECT * FROM read_parquet(?)', [str(path)]).to_arrow_table()
    readers = [pq.read_table(path), canonical_arrow(pl.read_parquet(path)), sql_readback]
    for actual in readers:
        assert actual.schema.equals(table.schema, check_metadata=False)
        assert actual.select(['source_index', 'integer']).equals(
            table.select(['source_index', 'integer']), check_metadata=False)
        for name, bits in (('raw64', np.uint64), ('raw32', np.uint32)):
            expected_column, actual_column = table[name].combine_chunks(), actual[name].combine_chunks()
            assert actual_column.is_null().equals(expected_column.is_null())
            valid = expected_column.is_valid().to_numpy(zero_copy_only=False)
            np.testing.assert_array_equal(actual_column.to_numpy(zero_copy_only=False).view(bits)[valid],
                                          expected_column.to_numpy(zero_copy_only=False).view(bits)[valid])
    metadata = pq.read_schema(path).metadata
    assert metadata[b'finlab_processing_contract_version'] == b'1'
    assert metadata[b'PANDAS_ATTRS'] == b'{"unit": "shares"}'


@pytest.mark.parametrize('key,rows,columns,expected', [
    ('price:成交股數', 4096, 256,
     {'row_group_size': 4096, 'use_dictionary': False, 'use_byte_stream_split': True}),
    ('price:成交股數', 4095, 256, {}),
    ('price:成交股數', 4096, 255, {}),
    ('price:收盤價', 4096, 256, {}),
    ('financial_statement:每股盈餘', 4096, 256, {}),
    ('tw_minute:2330', 4096, 256, {}),
])
def test_native_history_policy_is_confined_to_measured_source_grain(key, rows, columns, expected):
    from scripts.finlab_columnar import history_parquet_options

    table = pa.table({'source_index': pa.nulls(rows, type=pa.string()),
                      **{f'c{n}': pa.nulls(rows, type=pa.float64()) for n in range(columns-1)}})
    assert history_parquet_options(key, table) == expected


@pytest.mark.parametrize('changed_type', [pa.int64(), pa.float32(), pa.string()])
def test_native_volume_policy_defers_unmeasured_source_types(changed_type):
    from scripts.finlab_columnar import history_parquet_options

    table = pa.table({'source_index': pa.nulls(4096, type=pa.string()),
        'changed_source': pa.nulls(4096, type=changed_type),
        **{f'c{n}': pa.nulls(4096, type=pa.float64()) for n in range(254)}})
    assert history_parquet_options('price:成交股數', table) == {}


def test_general_collector_commits_qualified_volume_layout_without_rewriting_old_history(tmp_path):
    import pyarrow.parquet as pq
    from scripts.download_finlab_history import fetch_one, safe_stem

    key = 'price:成交股數'
    source = pd.DataFrame({f'{n:04d}': np.arange(4097, dtype='float64')
                           for n in range(256)}, index=pd.date_range('2014-01-01', periods=4097))
    with patch('finlab.data.get', return_value=source):
        first = fetch_one(key, tmp_path)
        unchanged = fetch_one(key, tmp_path)
    path = tmp_path / first['parquet_path']
    assert pq.ParquetFile(path).metadata.num_row_groups == 2
    assert first['parquet_storage_layout'] == {
        'contract': 'finlab_observed_wide_history_byte_planes_v1',
        'row_group_size': 4096, 'use_dictionary': False, 'use_byte_stream_split': True,
        'raw_values_or_processing_contract_changed': False,
    }
    assert first['volume_units']['canonical_unit'] == 'shares'
    assert unchanged['sha256'] == first['sha256'] and unchanged['last_check_result'] == 'unchanged'
    original_bytes = path.read_bytes()
    with patch('finlab.data.get', return_value=source.iloc[:-1]):
        previous = fetch_one(key, tmp_path)
    assert pq.ParquetFile(tmp_path / previous['parquet_path']).metadata.num_row_groups == 1
    assert (tmp_path / 'versions' / safe_stem(key) / (first['sha256'] + '.json')).is_file()
    assert path.read_bytes() == original_bytes
    expected, _ = serialize_provider_frame(source)
    assert pq.read_table(path).equals(expected, check_metadata=False)


def test_minute_validator_rejects_sub_microsecond_alignment():
    frame = pd.DataFrame({"stock_id": ["2330"], "trade_date": ["2026-06-01"],
        "timestamp": pd.to_datetime(["2026-06-01T09:00:00.000000001+08:00"]),
        "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "volume": [1]})
    with pytest.raises(ValueError, match="minute aligned"):
        _validate_frame(frame, "tw_minute:2330", date(2026, 6, 1))


def test_range_split_keeps_order_and_per_day_closed_evidence(tmp_path):
    days = [date(2026, 6, 1), date(2026, 6, 2)]
    frame = pd.DataFrame({"stock_id": ["2330", "2330"], "trade_date": [str(days[0])]*2,
        "timestamp": pd.to_datetime(["2026-06-01T09:00:01+08:00"]*2),
        "sequence": [2, 1], "close": [2., 1.], "volume": [1, 1]})
    frame.attrs["no_trade_dates"] = [str(days[1])]
    with patch("finlab.data.get", return_value=frame):
        result = fetch_partitions(tmp_path, "tw_tick:2330", days,
                                  now=pd.Timestamp("2026-06-03", tz="UTC").to_pydatetime())
    assert result["sdk_calls"] == 1
    assert result["outcomes"][1]["receipt"]["status"] == "verified_no_trade"
    receipt = result["outcomes"][0]["receipt"]
    assert pl.read_parquet(tmp_path / receipt["parquet_path"])["sequence"].to_list() == [2, 1]


def legacy_tick_reference(source):
    """Small, independent pandas oracle for equivalence/benchmarks only."""
    source = source[source.session.eq("regular") & source.volume.gt(0)].copy()
    source = source.sort_values(["timestamp", "sequence"], kind="stable")
    source["minute"] = source.timestamp.dt.floor("min")
    source["notional"] = source.close * source.volume
    return source.groupby("minute", sort=True).agg(
        open=("close", "first"), high=("close", "max"), low=("close", "min"), close=("close", "last"),
        volume=("volume", "sum"), tick_count=("close", "size"),
        volume_shares=("volume_shares", lambda x: x.sum(min_count=len(x))),
        notional=("notional", "sum")).assign(vwap=lambda x: x.notional / x.volume).drop(columns="notional").reset_index()


def legacy_tick_workflow(source, symbol, day):
    """The old full validation+aggregation path, isolated as a test oracle."""
    def validate(frame, *, minute=False):
        assert frame.stock_id.notna().all() and frame.stock_id.astype(str).eq(symbol).all()
        assert pd.to_datetime(frame.trade_date, errors="coerce").dt.date.eq(day).all()
        assert isinstance(frame.timestamp.dtype, pd.DatetimeTZDtype) and frame.timestamp.notna().all()
        local = frame.timestamp.dt.tz_convert("Asia/Taipei")
        assert local.dt.date.eq(day).all()
        volume = pd.to_numeric(frame.volume, errors="coerce")
        assert volume.notna().all() and np.isfinite(volume.to_numpy(dtype=float)).all() and volume.ge(0).all()
        if minute:
            prices = frame[["open", "high", "low", "close"]].apply(pd.to_numeric, errors="coerce")
            assert np.isfinite(prices.to_numpy(dtype=float)).all() and prices.gt(0).all().all()
            assert local.dt.second.eq(0).all() and local.dt.microsecond.eq(0).all()
            assert not frame.timestamp.duplicated().any()
            assert prices.high.ge(prices[["open", "low", "close"]].max(axis=1)).all()
            assert prices.low.le(prices[["open", "high", "close"]].min(axis=1)).all()
        else:
            assert not frame.duplicated(["timestamp", "sequence"]).any()
    validate(source)
    assert source.session.notna().all()
    regular = source[source.session.eq("regular") & source.volume.gt(0)]
    assert regular.close.notna().all() and np.isfinite(regular.close.to_numpy(dtype=float)).all() and regular.close.gt(0).all()
    clock = regular.timestamp.dt.tz_convert("Asia/Taipei").dt.floor("min").dt.time
    allowed = clock.between(time(9), time(13, 30))
    if day >= date(2012, 2, 20):
        allowed |= clock.eq(time(13, 33))
    assert allowed.all()
    bars = legacy_tick_reference(source).rename(columns={"minute": "timestamp"})
    bars["stock_id"], bars["trade_date"], bars["session"] = symbol, day.isoformat(), "regular"
    validate(bars, minute=True)
    return bars


def test_native_aggregation_matches_independent_reference_for_whole_input():
    # Repeated timestamps and gaps; interleaved after-hours data; partial unit
    # proofs. No interpolation or filling, and no loss of source sequence.
    count = 3000
    clock = pd.Timestamp("2026-06-01T09:00:00+08:00")
    frame = pd.DataFrame({"stock_id": ["2330"]*count, "trade_date": ["2026-06-01"]*count,
        "timestamp": [clock + pd.Timedelta(seconds=n//2) for n in range(count)],
        "sequence": list(reversed(range(count))), "close": [float(50+n%17) for n in range(count)],
        "volume": [n%9 for n in range(count)],
        "session": ["after_hours_fixed" if n%20 == 0 else "regular" for n in range(count)],
        "volume_shares": pd.array([None if n%23 == 0 else (n%9)*1000 for n in range(count)], dtype="Int64")})
    expected = legacy_tick_reference(frame)
    actual = ticks_to_minutes(frame, "2330", date(2026, 6, 1)).to_pandas().rename(columns={"timestamp": "minute"})
    # Only VWAP reductions may differ in the last few bits between engines.
    for name in expected.columns:
        if name != "vwap":
            pd.testing.assert_series_equal(actual[name], expected[name], check_dtype=False)
    assert actual.vwap.to_list() == pytest.approx(expected.vwap.to_list(), rel=1e-12, abs=1e-12)


def test_same_timestamp_null_sequence_keeps_legacy_stable_nulls_last_order():
    frame = pd.DataFrame({"stock_id": ["2330"]*3, "trade_date": ["2026-06-01"]*3,
        "timestamp": pd.to_datetime(["2026-06-01T09:00:01+08:00"]*3),
        "sequence": [None, 2, 1], "close": [99., 102., 101.], "volume": [1, 1, 1],
        "session": ["regular"]*3, "volume_shares": pd.array([None]*3, dtype="Int64")})
    bars = ticks_to_minutes(frame, "2330", date(2026, 6, 1))
    assert bars[0, "open"] == 101. and bars[0, "close"] == 99.


def test_collectors_have_no_dataframe_processing_or_reverse_conversion():
    root = Path(__file__).resolve().parents[1]
    files = ["download_finlab_history", "download_finlab_intraday", "derive_finlab_minute",
             "finlab_volume_units", "finlab_fixed_price_shares", "audit_finlab_runtime", "finlab_arrow_history"]
    for name in files:
        tree = ast.parse((root / "scripts" / f"{name}.py").read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not any(alias.name == "pandas" for alias in node.names), name
            if isinstance(node, ast.ImportFrom):
                assert not node.module or not node.module.startswith("pandas"), name
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in {"to_pandas", "from_pandas"}, name
