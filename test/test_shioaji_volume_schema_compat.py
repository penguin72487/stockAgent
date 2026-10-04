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
