from datetime import date, datetime
import hashlib
import json

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from downloader.download_shioaji_tw_kbars import (
    UniverseRow,
    _cached_minute_chunk,
    _verified_minute_symbol_frame,
    normalize_kbars,
)
from downloader.download_shioaji_tw_minute_kbars import merge_retried_source_gap_chunk
from downloader.stock_volume_units import STOCK_MINUTE_RAW_COLUMNS, scan_stock_minute_sources
from scripts.build_shioaji_tw_minute_dataset import (
    EXECUTOR_ONLY_COLUMNS, MODEL_FEATURE_COLUMNS, build_research_frame,
)


VOLUME_COLUMNS = ["source_volume_multiplier", "volume_shares", "volume_unit_proof"]


def _minute(day: date) -> pl.DataFrame:
    return normalize_kbars(
        {"ts": [datetime.combine(day, datetime.min.time()).replace(hour=9, minute=1)],
         "Open": [100.0], "High": [100.0], "Low": [100.0], "Close": [100.0],
         "Volume": [2], "Amount": [200_000.0]},
        symbol="2330", market="twse", contract_unit=1000.0,
    )


@pytest.mark.parametrize("changed_source", [False, True])
def test_source_gap_merge_handles_volume_schema_but_rejects_raw_revision(changed_source):
    retained = _minute(date(2026, 6, 1)).drop(VOLUME_COLUMNS)
    refreshed = pl.concat([_minute(date(2026, 6, 1)), _minute(date(2026, 6, 2))])
    # A previous derived interpretation is not an immutable provider value.
    refreshed = refreshed.with_columns(pl.lit(2.0).alias("volume_shares"))
    if changed_source:
        refreshed = refreshed.with_columns(pl.lit(3.0).alias("Volume"))
        with pytest.raises(RuntimeError, match="conflicts"):
            merge_retried_source_gap_chunk(retained, refreshed)
        return
    merged = merge_retried_source_gap_chunk(retained, refreshed)
    assert merged["volume_shares"].to_list() == [2000.0, 2000.0]
    assert_frame_equal(merged.head(1).select(retained.columns), retained)
    assert VOLUME_COLUMNS[0] not in retained.columns


@pytest.mark.parametrize("reader", ["cached", "verified"])
def test_minute_readers_merge_old_new_schemas_without_rewriting_sources(tmp_path, reader):
    days = [date(2026, 6, 1), date(2026, 6, 2)]
    (tmp_path / "symbols").mkdir()
    base = tmp_path / "official.parquet"
    pl.DataFrame({"date": days, "Trading_Volume": [2000.0, 2000.0]}).write_parquet(base)
    row = UniverseRow(symbol="2330", name="TSMC", market="twse", security_type="stock", base_path=base)
    chunks = []
    originals = {}
    for index, day in enumerate(days):
        frame = _minute(day)
        if index == 0:
            frame = frame.drop(VOLUME_COLUMNS)
        path = tmp_path / f"{day}.parquet"
        frame.write_parquet(path)
        originals[path] = path.read_bytes()
        chunks.append({
            "start_date": str(day), "end_date": str(day), "status": "ok",
            "rows": 1, "data_path": str(path),
            "data_sha256": hashlib.sha256(originals[path]).hexdigest(),
        })
    manifest = tmp_path / "symbols/2330.manifest.json"
    manifest.write_text(json.dumps({
        "source": "shioaji_kbars_1m", "storage_frequency": "minute", "symbol": "2330",
        "requested_start": str(days[0]), "requested_end": str(days[1]), "chunks": chunks,
    }))
    if reader == "cached":
        merged = _cached_minute_chunk(
            tmp_path, row, start=days[0], end=days[1], expected_dates=set(days),
        )
    else:
        merged, _, _, count = _verified_minute_symbol_frame(
            tmp_path, row, requested_start=days[0], requested_end=days[1],
        )
        assert count == 2
    assert merged is not None
    assert merged["Volume"].to_list() == [2.0, 2.0]
    assert merged["volume_shares"].to_list() == [2000.0, 2000.0]
    assert merged["volume_unit_proof"].to_list() == ["amount_ohlc", "amount_ohlc"]
    assert all(path.read_bytes() == original for path, original in originals.items())


@pytest.mark.parametrize("new_first", [False, True])
def test_research_scan_preserves_mixed_source_metadata_and_causal_values(tmp_path, new_first):
    day = date(2026, 6, 1)
    source = _minute(day)
    second = source.with_columns(
        pl.col("ts") + pl.duration(minutes=1),
        pl.lit("actual-source-receipt").alias("source_provenance"),
        pl.lit(-7.).alias("source_volume_multiplier"),
        pl.lit(-123.).alias("volume_shares"),
    )
    legacy = source.drop(VOLUME_COLUMNS)
    frames = [legacy, second] if not new_first else [second, legacy]
    paths = []
    for index, frame in enumerate(frames):
        path = tmp_path/f"chunk-{index}.parquet";frame.write_parquet(path);paths.append(path)
    originals = {p:p.read_bytes() for p in paths}
    actual_source = scan_stock_minute_sources(paths).collect().sort("ts")
    assert actual_source["source_provenance"].to_list() == [None, "actual-source-receipt"]
    assert actual_source["Volume"].to_list() == [2., 2.]
    actual = build_research_frame(actual_source.lazy()).collect()
    reference_raw = pl.concat([
        frame.with_columns(pl.col("ts").cast(pl.Datetime("ns")))
        for frame in [legacy, second.select(legacy.columns)]
    ])
    reference = build_research_frame(reference_raw.lazy()).collect()
    columns = [*STOCK_MINUTE_RAW_COLUMNS, *MODEL_FEATURE_COLUMNS, *EXECUTOR_ONLY_COLUMNS,
               "source_volume_multiplier", "volume_shares", "source_volume_unit_valid"]
    assert_frame_equal(actual.select(columns), reference.select(columns))
    assert actual["source_volume_multiplier"].to_list() == [1000., 1000.]
    assert actual["volume_shares"].to_list() == [2000., 2000.]
    assert actual["execution_open_next_1m"].to_list() == [100., None]
    assert all(p.read_bytes() == original for p, original in originals.items())


def test_source_scan_rejects_a_missing_provider_field_before_null_insertion(tmp_path):
    first = tmp_path/'first.parquet';second=tmp_path/'second.parquet'
    _minute(date(2026, 6, 1)).write_parquet(first)
    _minute(date(2026, 6, 2)).drop("Amount").write_parquet(second)
    with pytest.raises(ValueError, match="required raw fields.*Amount"):
        scan_stock_minute_sources([first, second])


def test_source_scan_rejects_changed_field_types_and_retains_unknown_null_metadata(tmp_path):
    first=tmp_path/'first.parquet';second=tmp_path/'second.parquet'
    _minute(date(2026, 6, 1)).with_columns(pl.lit(None).alias("source_note")).write_parquet(first)
    _minute(date(2026, 6, 2)).with_columns(pl.lit("recorded").alias("source_note")).write_parquet(second)
    assert scan_stock_minute_sources([first,second]).collect()["source_note"].to_list() == [None,"recorded"]
    _minute(date(2026, 6, 2)).with_columns(pl.col("Volume").cast(pl.String)).write_parquet(second)
    with pytest.raises(pl.exceptions.SchemaError,match="Volume.*incompatible types"):
        scan_stock_minute_sources([first,second])
