"""FinLab SDK boundary and shared Arrow/Polars storage primitives.

The upstream SDK owns its dataframe implementation. Convert once on entry;
our validation, aggregation and persistence never convert back to it. Source
metadata travels separately because Polars deliberately has no ``attrs``.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import re

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

PROCESSING_CONTRACT_VERSION = 1
PROCESSING_ENGINE = "polars_arrow_v1"
HISTORY_PARQUET_STORAGE_CONTRACT = "finlab_observed_wide_history_byte_planes_v1"


def history_period_slices(table: pa.Table, period: str) -> list[tuple[str, int, int]]:
    """Partition literal daily source labels without replacing or sorting them.

    The date prefix chooses storage only. The complete original index string,
    including subsecond/timezone text, remains in the returned table slices.
    Non-date, null or descending labels require their original storage path.
    """
    if period not in {"year", "month"}:
        raise ValueError("FinLab history period must be year or month")
    if "source_index" not in table.schema.names or table.schema.field("source_index").type != pa.string():
        raise ValueError("FinLab period storage requires literal string source_index labels")
    labels = table["source_index"]
    if labels.null_count:
        raise ValueError("FinLab period storage cannot fill null source labels")
    keys, previous = [], ""
    for label in labels.to_pylist():
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[ T].+)?", label):
            raise ValueError("FinLab source label is not an ISO calendar label")
        datetime.fromisoformat(label)  # Validate only; never store this conversion.
        day = label[:10]
        if day < previous:
            raise ValueError("FinLab period storage cannot sort source observations")
        previous = day
        keys.append(label[:4 if period == "year" else 7])
    runs = pc.run_end_encode(pa.array(keys, type=pa.string()))
    begin, result = 0, []
    for key, end in zip(runs.values.to_pylist(), runs.run_ends.to_pylist(), strict=True):
        result.append((key, begin, end - begin))
        begin = end
    return result


def history_parquet_options(key: str, table: pa.Table) -> dict:
    """Apply only the full-table append layout qualified on this source grain.

    The observed volume grain has a string index and Float64 source columns.
    Its 4096-row/plain/byte-plane encoding qualified through complete cold
    reconstruction and three readers. Other prices, types and smaller groups
    must not inherit that result. Existing immutable files remain unchanged.
    """
    if (key == 'price:成交股數' and table.num_rows >= 4096 and table.num_columns >= 256
            and table.schema.field(0).name == 'source_index'
            and table.schema.field(0).type == pa.string()
            and all(field.type == pa.float64() for field in list(table.schema)[1:])):
        return {'row_group_size': 4096, 'use_dictionary': False, 'use_byte_stream_split': True}
    return {}


def source_attrs(frame) -> dict:
    attrs = getattr(frame, "attrs", None)
    return dict(attrs) if isinstance(attrs, dict) else {}


def to_polars(frame) -> pl.DataFrame:
    if isinstance(frame, pl.DataFrame):
        return frame
    if isinstance(frame, (pa.Table, pa.RecordBatch)):
        return pl.from_arrow(frame, rechunk=False)
    # This is the only third-party dataframe conversion boundary. No local
    # SDK mutation, dataframe copy or dataframe processing is performed here.
    if not hasattr(frame, "columns") or not hasattr(frame, "index"):
        raise TypeError("FinLab response was not a DataFrame or Arrow table")
    return pl.from_arrow(sdk_to_arrow(frame), rechunk=False)


def sdk_to_arrow(frame) -> pa.Table:
    """Zero-copy numeric buffers where the SDK's dtypes permit it."""
    labels = [str(name) for name in frame.columns]
    if len(set(labels)) != len(labels):
        raise ValueError("duplicate FinLab source field labels")
    return pa.Table.from_pandas(frame, preserve_index=False, nthreads=1)


def sdk_index_metadata(frame) -> dict:
    """Carry source labels, not a temporary dataframe of copied index values."""
    index = frame.index
    columns = ["source_index" if index.nlevels == 1 else f"source_index_{n}"
               for n in range(index.nlevels)]
    return {"arrays": [pa.array([str(value) for value in index.get_level_values(n)], type=pa.string())
                       for n in range(index.nlevels)],
            "columns": columns, "labels": list(index), "dtype": str(index.dtype),
            "timestamp_index": getattr(index.dtype, "kind", None) == "M"}


def canonical_arrow(frame) -> pa.Table:
    table = (frame if isinstance(frame, pa.Table) else frame.to_arrow()
             if isinstance(frame, pl.DataFrame) else pa.Table.from_batches([frame])
             if isinstance(frame, pa.RecordBatch) else sdk_to_arrow(frame))
    # Keep the established source schema, rather than widening every string
    # merely because the in-memory Polars representation uses large offsets.
    fields = [pa.field(field.name, pa.string() if pa.types.is_large_string(field.type)
                       else field.type, nullable=field.nullable, metadata=field.metadata)
              for field in table.schema]
    schema = pa.schema(fields, metadata=table.schema.metadata)
    return table.cast(schema) if schema != table.schema else table


def write_parquet(frame, path: Path, *, attrs: dict | None = None,
                  row_group_size: int | None = None, use_dictionary: bool = True,
                  use_byte_stream_split: bool = False) -> None:
    if row_group_size is not None and (type(row_group_size) is not int
                                       or not 1 <= row_group_size <= 1024 * 1024):
        raise ValueError("FinLab Parquet row group size must be an integer in 1..1048576")
    if type(use_dictionary) is not bool or type(use_byte_stream_split) is not bool:
        raise ValueError("FinLab Parquet value encoding policies must be boolean")
    table = canonical_arrow(frame)
    metadata = dict(table.schema.metadata or {})
    metadata[b"finlab_processing_contract_version"] = str(PROCESSING_CONTRACT_VERSION).encode()
    if attrs:
        # Preserve the SDK's evidence just as the previous source objects did.
        metadata[b"PANDAS_ATTRS"] = json.dumps(attrs, ensure_ascii=False).encode()
    pq.write_table(table.replace_schema_metadata(metadata), path, compression="zstd",
                   row_group_size=row_group_size, use_dictionary=use_dictionary,
                   use_byte_stream_split=([f.name for f in table.schema if pa.types.is_floating(f.type)]
                                          if use_byte_stream_split else False))


def date_column(frame: pl.DataFrame, name: str = "trade_date") -> pl.Series:
    values = frame[name]
    if values.dtype == pl.Date:
        return values
    if isinstance(values.dtype, pl.Datetime):
        return values.dt.date()
    return values.cast(pl.String).str.to_date("%Y-%m-%d", strict=False)


def timestamp_bounds(frame: pl.DataFrame) -> tuple[str | None, str | None]:
    """Arrow scalars retain nanoseconds which Python datetime would truncate."""
    if frame.is_empty():
        return None, None
    bounds = pc.min_max(frame["timestamp"].to_arrow())
    return str(bounds["min"].as_py()), str(bounds["max"].as_py())
