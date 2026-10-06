"""Source-shaped, read-only quality evidence; never synthesize observations.

Storage integrity, an observed interval's theoretical grid, and upstream
availability are different proofs. This module deliberately keeps them apart.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
import math
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

from downloader.audit_ohlcv_data import _audit_frame, _read_audit_frame
from downloader.ohlcv_hot_tail import invalid_candle_values, logical_parts
from downloader.parquet_integrity import signature

CONTRACT = "source_anomaly_evidence_v1"
OHLC = ("open", "max", "min", "close")


def file_binding(path: Path, *, logical: bool = False) -> list[dict]:
    parts = logical_parts(path) if logical else (path,)
    return [{"path": str(p.resolve()), "signature": list(signature(p))} for p in parts]


def footer_profile(path: Path) -> dict[str, Any]:
    """Inspect all row groups, not a sample; unknown statistics stay unknown.

    A non-finite numeric extremum is direct stored-value evidence. Negative
    economic values and ordinary NULLs are not corruption. Nested statistics
    are not coerced into a scalar column's row denominator.
    """
    before = file_binding(path)
    parquet = pq.ParquetFile(path)
    metadata = parquet.metadata
    rows = metadata.num_rows
    numeric_inf, missing_stats, nulls = set(), set(), 0
    for group in range(metadata.num_row_groups):
        row_group = metadata.row_group(group)
        for index in range(row_group.num_columns):
            column = row_group.column(index)
            name, stats = column.path_in_schema, column.statistics
            if stats is None:
                missing_stats.add(name)
                continue
            if "." not in name and stats.null_count is not None:
                nulls += stats.null_count
            if stats.has_min_max:
                for value in (stats.min, stats.max):
                    if isinstance(value, float) and math.isinf(value):
                        numeric_inf.add(name)
    # Count exact offending values only in columns whose extrema prove a risk.
    infinity_values = 0
    for name in sorted(numeric_inf):
        if "." in name or name not in parquet.schema_arrow.names:
            continue
        for batch in parquet.iter_batches(columns=[name], batch_size=65536, use_threads=False):
            infinity_values += int(pl.from_arrow(batch).select(
                pl.col(name).is_infinite().fill_null(False).sum()).item())
    result = {"contract": CONTRACT, "path": str(path), "rows": rows,
              "columns": len(parquet.schema_arrow.names), "row_groups": metadata.num_row_groups,
              "null_slots_from_footer": nulls, "statistics_missing_columns": sorted(missing_stats),
              "infinite_columns": sorted(numeric_inf), "infinite_values": infinity_values,
              "validation": "all_row_group_footers; exact_scan_of_infinite_extrema_columns",
              "keys_checked": False, "expected_rows": None,
              "expectedness": "native_grain_release_and_applicability_contract_required",
              "file_binding": before, "status": "invalid_values" if numeric_inf else "footer_checked"}
    if file_binding(path) != before:
        result.update(status="changed_during_check", infinite_values=None)
    return result


def merge_windows(windows: list[tuple[int, int]], *, interval_ms: int = 60000) -> list[list[int]]:
    """Coalesce adjacent exact windows; do not expand across a healthy middle."""
    merged: list[list[int]] = []
    for start, end in sorted(windows):
        if start > end:
            raise ValueError("reversed repair interval")
        if merged and start <= merged[-1][1] + interval_ms:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def candle_grid(frame: pl.DataFrame, *, completed_end_ms: int) -> dict:
    """A 24/7 1m grid bounded by actual first/last completed observations.

    Missing instants are finite upstream rechecks, not proven published bars.
    No listing-before-first assumption; no open candle in the denominator.
    """
    dates = frame.select(
        (pl.col("date").str.to_datetime(strict=False, time_zone="UTC", time_unit="ns")
         if frame.schema["date"] == pl.String else
         pl.col("date").cast(pl.Datetime("ns", "UTC"), strict=False)).alias("date"))
    invalid = dates["date"].null_count()
    valid = dates.drop_nulls().select(pl.col("date").dt.epoch("ns").alias("ns"))
    unfinished = valid.filter(pl.col("ns") > completed_end_ms * 1_000_000).height
    valid = valid.filter(pl.col("ns") <= completed_end_ms * 1_000_000)
    off_grid = valid.filter(pl.col("ns") % 60_000_000_000 != 0).height
    valid = valid.select((pl.col("ns") // 1_000_000).alias("ms"))
    unique = valid.unique("ms").sort("ms")
    duplicate_excess = valid.height - unique.height
    result = {"invalid_timestamps": invalid, "off_grid_rows": off_grid,
              "unfinished_or_future_rows": unfinished, "duplicate_excess_rows": duplicate_excess,
              "observed_unique_completed_rows": unique.height, "theoretical_rows_in_observed_span": None,
              "missing_minutes_in_observed_span": None, "repair_windows_ms": [],
              "grid_basis": "24x7_1m_actual_completed_observed_span; not_listing_or_API_availability_proof"}
    if unique.is_empty() or off_grid or invalid:
        return result
    first, last = unique["ms"][0], unique["ms"][-1]
    result["theoretical_rows_in_observed_span"] = (last - first) // 60000 + 1
    result["missing_minutes_in_observed_span"] = result["theoretical_rows_in_observed_span"] - unique.height
    pairs = unique.with_columns(pl.col("ms").shift(1).alias("previous")).filter(
        pl.col("ms") - pl.col("previous") > 60000)
    result["repair_windows_ms"] = [[r["previous"] + 60000, r["ms"] - 60000] for r in pairs.to_dicts()]
    return result


def _full_market_profile(path: Path, args, *, crypto_1m: bool = False) -> dict:
    """Reuse canonical logical hot-tail precedence and OHLC validation."""
    before = file_binding(path, logical=True)
    frame, columns = _read_audit_frame(path)
    evidence = asdict(_audit_frame(path.parent, path, args, frame, columns))
    evidence.update(contract=CONTRACT, file_binding=before, validation="logical_OHLCV_values_and_timestamp_keys")
    if evidence["status"] == "failed" or evidence["missing_columns"]:
        return evidence
    frame = frame.with_columns(*(pl.col(c).cast(pl.Float64, strict=False) for c in
                                (*OHLC, "Trading_Volume") if c in frame.columns))
    if "Trading_Volume" not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("Trading_Volume"))
    # Infinity was missed by the old <=0 / fill_nan(None) checks.
    values = [c for c in (*OHLC, "Trading_Volume") if c in frame.columns]
    evidence["infinite_values"] = int(frame.select(pl.sum_horizontal(
        *(pl.col(c).cast(pl.Float64, strict=False).is_infinite().fill_null(False) for c in values)
    ).sum()).item() or 0)
    # Zero/NULL price on no-trade source rows is not evidence of a lost trade.
    bad_price = pl.any_horizontal(*(
        (~pl.col(c).cast(pl.Float64, strict=False).is_finite() | (pl.col(c) <= 0)).fill_null(True)
        for c in OHLC))
    geometry = (pl.col("max") < pl.max_horizontal("open", "min", "close")) | (
        pl.col("min") > pl.min_horizontal("open", "max", "close"))
    traded = (pl.col("Trading_Volume") > 0).fill_null(False)
    invalid = ((bad_price & traded) | geometry.fill_null(False)
               | (pl.col("Trading_Volume") < 0).fill_null(False))
    if crypto_1m or "Trading_Volume" not in columns:
        # Exchange candles have positive quote prices, including zero-volume candles.
        invalid |= bad_price
    if crypto_1m:
        invalid |= ~pl.col('Trading_Volume').is_finite().fill_null(False)
    evidence["invalid_observed_rows"] = frame.filter(invalid).height
    if not crypto_1m:
        date_dtype = frame.schema['date']
        dates = (pl.col('date').str.to_datetime(strict=False) if date_dtype == pl.String
                 else pl.col('date').cast(pl.Datetime, strict=False))
        bad_dates = frame.filter(invalid).select(dates.dt.date().min().alias('first'))
        evidence['source_recheck_start_date'] = str(bad_dates['first'][0]) if bad_dates['first'][0] else None
    if crypto_1m:
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        grid = candle_grid(frame, completed_end_ms=now_ms // 60000 * 60000 - 60000)
        evidence.update(grid)
        bad = frame.filter(invalid).select(
            (pl.col("date").str.to_datetime(strict=False, time_zone="UTC", time_unit="ms")
             if frame.schema["date"] == pl.String else
             pl.col("date").cast(pl.Datetime("ms", "UTC"), strict=False)).dt.epoch("ms").alias("ms"))
        windows = [tuple(w) for w in grid["repair_windows_ms"]]
        windows.extend((t, t) for t in bad["ms"].drop_nulls().to_list())
        evidence["repair_windows_ms"] = merge_windows(windows)
    evidence["confirmed_value_error"] = bool(evidence["invalid_observed_rows"] or evidence["infinite_values"])
    evidence["status"] = (
        "invalid_values" if evidence["confirmed_value_error"] else
        "invalid_timestamp_keys" if evidence.get('invalid_timestamps') or evidence.get('off_grid_rows') else
        "duplicate_timestamp_keys" if evidence.get('duplicate_excess_rows') else
        "gap_candidate" if evidence.get("missing_minutes_in_observed_span") else "values_checked")
    if file_binding(path, logical=True) != before:
        evidence.update(status="changed_during_check", confirmed_value_error=False, repair_windows_ms=[])
    return evidence


def _keyed(frame: pl.DataFrame) -> pl.DataFrame:
    dtype = frame.schema['date']
    dates = (pl.col('date').str.to_datetime(strict=False, time_zone='UTC', time_unit='ns')
             if dtype == pl.String else pl.col('date').cast(pl.Datetime('ns', 'UTC'), strict=False))
    return frame.with_columns(dates.dt.epoch('ns').alias('__ns'))


def _stream_crypto_profile(path: Path) -> dict:
    """Linear scan of sorted canonical bars; bounded batches plus recent tail.

    Only batches touched by tail revisions are grouped. Noncanonical order or
    keys use the existing full logical reader, never a guessed sort/dedup rule.
    Every value/key is checked; this is not footer sampling.
    """
    before = file_binding(path, logical=True)
    parts = logical_parts(path)
    columns = ['date', *OHLC, 'Trading_Volume']
    parquet = pq.ParquetFile(parts[0])
    if any(c not in parquet.schema_arrow.names for c in columns):
        raise ValueError('requires canonical candle schema')
    tail = pl.DataFrame()
    if len(parts) > 1:
        tail_names = pq.read_schema(parts[1]).names
        tail = pl.from_arrow(pq.read_table(parts[1], columns=[c for c in columns if c in tail_names], use_threads=False))
        for c in columns:
            if c not in tail.columns:
                tail = tail.with_columns(pl.lit(None).alias(c))
        tail = _keyed(tail)
        if tail['__ns'].null_count():
            raise ValueError('invalid tail timestamp')
        tail = tail.group_by('__ns').agg(
            pl.col('date').last(), *(pl.col(c).drop_nulls().last() for c in columns[1:])).sort('__ns')
    ended = int(datetime.now(UTC).timestamp() // 60) * 60_000_000_000 - 60_000_000_000
    previous_base = previous_completed = first = last = None
    rows = observed = future = off_grid = invalid_rows = infinities = missing = 0
    windows: list[tuple[int, int]] = []

    def consume(frame):
        nonlocal previous_completed, first, last, rows, observed, future, off_grid, invalid_rows, infinities, missing
        if frame.is_empty():
            return
        rows += frame.height
        frame = frame.with_columns(*(pl.col(c).cast(pl.Float64, strict=False) for c in columns[1:]))
        invalid = frame.filter(invalid_candle_values())
        invalid_rows += invalid.height
        infinities += int(frame.select(pl.sum_horizontal(*(
            pl.col(c).is_infinite().fill_null(False) for c in columns[1:])).sum()).item() or 0)
        windows.extend((ns // 1_000_000, ns // 1_000_000) for ns in invalid['__ns']
                       if ns <= ended and ns % 60_000_000_000 == 0)
        future += frame.filter(pl.col('__ns') > ended).height
        completed = frame.filter(pl.col('__ns') <= ended)
        if completed.is_empty():
            return
        observed += completed.height
        off_grid += completed.filter(pl.col('__ns') % 60_000_000_000 != 0).height
        first = completed['__ns'][0] if first is None else first
        last = completed['__ns'][-1]
        previous = pl.col('__ns').shift(1).fill_null(previous_completed) if previous_completed is not None else pl.col('__ns').shift(1)
        gaps = completed.with_columns(previous.alias('__previous')).filter(
            pl.col('__ns') - pl.col('__previous') > 60_000_000_000)
        for r in gaps.select('__previous', '__ns').iter_rows():
            missing += (r[1] - r[0]) // 60_000_000_000 - 1
            windows.append(((r[0] + 60_000_000_000) // 1_000_000, (r[1] - 60_000_000_000) // 1_000_000))
        previous_completed = last

    for batch in parquet.iter_batches(columns=columns, batch_size=131072, use_threads=False):
        frame = _keyed(pl.from_arrow(batch))
        if frame['__ns'].null_count() or not frame['__ns'].is_sorted() or (
                frame.height > 1 and (frame['__ns'].diff().drop_nulls() <= 0).any()) or (
                previous_base is not None and frame['__ns'][0] <= previous_base):
            raise ValueError('noncanonical base timestamp order')
        high = frame['__ns'][-1]
        if not tail.is_empty():
            selected = tail.filter((pl.col('__ns') <= high) & (
                pl.lit(True) if previous_base is None else pl.col('__ns') > previous_base))
            if not selected.is_empty():
                combined = pl.concat([frame, selected], how='diagonal_relaxed')
                frame = combined.group_by('__ns').agg(pl.col('date').last(), *(
                    pl.col(c).drop_nulls().last() for c in columns[1:])).sort('__ns')
        consume(frame)
        previous_base = high
    if not tail.is_empty():
        consume(tail if previous_base is None else tail.filter(pl.col('__ns') > previous_base))
    result = {'contract': CONTRACT, 'path': str(path), 'rows': rows, 'file_binding': before,
        'validation': 'every_logical_OHLCV_value_and_key; linear_bounded_batch_scan',
        'infinite_values': infinities, 'invalid_observed_rows': invalid_rows,
        'confirmed_value_error': bool(invalid_rows or infinities), 'invalid_timestamps': 0,
        'off_grid_rows': off_grid, 'duplicate_excess_rows': 0, 'unfinished_or_future_rows': future,
        'observed_unique_completed_rows': observed,
        'theoretical_rows_in_observed_span': (last - first) // 60_000_000_000 + 1 if first is not None and not off_grid else None,
        'missing_minutes_in_observed_span': missing if not off_grid else None,
        'repair_windows_ms': merge_windows(windows) if not off_grid else [],
        'first_date': datetime.fromtimestamp(first / 1e9, UTC).isoformat() if first is not None else None,
        'last_date': datetime.fromtimestamp(last / 1e9, UTC).isoformat() if last is not None else None,
        'grid_basis': '24x7_1m_actual_completed_observed_span; not_listing_or_API_availability_proof',
        'status': 'invalid_values' if invalid_rows else 'invalid_timestamp_keys' if off_grid else
                  'gap_candidate' if missing else 'values_checked'}
    if file_binding(path, logical=True) != before:
        result.update(status='changed_during_check', confirmed_value_error=False, repair_windows_ms=[])
    return result


def market_profile(path: Path, args, *, crypto_1m: bool = False) -> dict:
    if crypto_1m:
        try:
            return _stream_crypto_profile(path)
        except (ValueError, TypeError, pl.exceptions.PolarsError):
            # Preserve canonical merge/duplicate behavior for unusual old files.
            pass
    return _full_market_profile(path, args, crypto_1m=crypto_1m)
