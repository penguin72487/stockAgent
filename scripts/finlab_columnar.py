"""FinLab SDK boundary and shared Arrow/Polars storage primitives.

The upstream SDK owns its dataframe implementation. Convert once on entry;
our validation, aggregation and persistence never convert back to it. Source
metadata travels separately because Polars deliberately has no ``attrs``.
"""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

PROCESSING_CONTRACT_VERSION = 1
PROCESSING_ENGINE = "polars_arrow_v1"


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


def write_parquet(frame, path: Path, *, attrs: dict | None = None) -> None:
    table = canonical_arrow(frame)
    metadata = dict(table.schema.metadata or {})
    metadata[b"finlab_processing_contract_version"] = str(PROCESSING_CONTRACT_VERSION).encode()
    if attrs:
        # Preserve the SDK's evidence just as the previous source objects did.
        metadata[b"PANDAS_ATTRS"] = json.dumps(attrs, ensure_ascii=False).encode()
    pq.write_table(table.replace_schema_metadata(metadata), path, compression="zstd")


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
