"""Bounded Arrow acquisition for the three known oversized FinLab tables.

Use the installed SDK's authenticated metadata flow, not guessed blob URLs.
Stream bytes to a durable raw object before decoding. Wide metadata is stored
as sparse long records with original column labels; it is never silently
treated as a numeric training matrix. No pandas full-table conversion occurs.
"""
from __future__ import annotations

from collections import OrderedDict
from datetime import UTC, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import tempfile
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq


STREAMING_KEYS = frozenset({
    "broker_transactions", "after_market_fixed_price:市場別", "after_market_fixed_price:資料來源",
})
MAX_RAW_BYTES = 1536 * 1024**2
RAW_CONTRACT = 1


class FinlabResourceDeferred(RuntimeError):
    """Known transfer cannot fit the remaining account or disk budget."""


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".raw-", delete=False) as handle:
        temp = Path(handle.name)
        try:
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode())
            handle.flush()
            os.fsync(handle.fileno())
            os.replace(temp, path)
        finally:
            temp.unlink(missing_ok=True)


def _signature(path: Path) -> tuple[int, ...]:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _verified_raw(root: Path, receipt: dict, key: str) -> Path | None:
    if receipt.get("dataset") != key or receipt.get("contract_version") != RAW_CONTRACT:
        return None
    rel = Path(str(receipt.get("raw_path", "")))
    path = root / rel
    if (rel.is_absolute() or ".." in rel.parts or not path.resolve().is_relative_to(root.resolve())
            or not path.is_file() or path.stat().st_size != receipt.get("raw_bytes")):
        return None
    before = _signature(path)
    return path if _sha256(path) == receipt.get("raw_sha256") and _signature(path) == before else None


def stream_download(url: str, destination: Path, *, maximum_bytes: int, get=None) -> int:
    """No signed URL, response body, or SDK credential may escape in errors."""
    import requests

    parsed = urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname != "storage.googleapis.com"
            or parsed.username or parsed.password or parsed.port not in {None, 443} or parsed.fragment):
        raise ValueError("FinLab signed download origin refused")
    request = get or requests.get
    try:
        with request(url, stream=True, timeout=(20, 60), allow_redirects=False,
                     headers={"User-Agent": "stockAgent-FinLab-research/1.0", "Accept-Encoding": "identity"}) as response:
            if response.status_code != 200:
                raise ValueError("FinLab raw download refused")
            length = int(response.headers.get("Content-Length", "0"))
            if length < 0 or length > maximum_bytes:
                raise FinlabResourceDeferred("FinLab raw transfer exceeds byte budget")
            written = 0
            with destination.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024**2):
                    written += len(chunk)
                    if written > maximum_bytes:
                        raise FinlabResourceDeferred("FinLab raw transfer exceeds byte budget")
                    handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            if not written or (length and written != length):
                raise ValueError("FinLab raw transfer incomplete")
            return written
    except FinlabResourceDeferred:
        raise
    except Exception:
        raise ValueError("FinLab raw transfer failed; private details withheld") from None


def acquire_raw(key: str, root: Path, stem: str, *, refresh: bool,
                quota_reserve_mb: float = 50) -> tuple[Path, dict]:
    if key not in STREAMING_KEYS:
        raise ValueError("unsupported streaming dataset")
    raw_root = root / "raw"
    raw_root.mkdir(parents=True, exist_ok=True)
    receipt_path = root / "raw_receipts" / f"{stem}.json"
    previous = _json(receipt_path)
    path = _verified_raw(root, previous, key)
    # Interrupted normalization reuses a today's-quota-cycle upstream proof.
    # A copied old SDK cache has no such proof and cannot claim freshness.
    try:
        checked = datetime.fromisoformat(previous["source_checked_at_utc"])
        current_cycle = checked.tzinfo is not None and checked.astimezone(UTC).date() == datetime.now(UTC).date()
    except (KeyError, ValueError, TypeError):
        current_cycle = False
    if path and (not refresh or (current_cycle and previous.get("source_check_mode") == "upstream_forced")):
        return path, previous

    from finlab import utils
    from finlab.auth import get_data_status
    from finlab.data.auth import fetch_metadata_batch

    cache = Path(utils.get_tmp_dir()) / (key.replace(":", "#") + ".feather")
    if not refresh and cache.is_file():
        source_mode = "sdk_cache_allowed"
        origin = "existing_sdk_feather_cache"
        before = _signature(cache)
        if cache.stat().st_size > MAX_RAW_BYTES:
            raise FinlabResourceDeferred("FinLab cached object exceeds byte budget")
        if shutil.disk_usage(raw_root).free < cache.stat().st_size * 4 + 4 * 1024**3:
            raise FinlabResourceDeferred("FinLab raw disk reserve reached")
        with tempfile.NamedTemporaryFile(dir=raw_root, suffix=".feather.tmp", delete=False) as handle:
            temp = Path(handle.name)
        try:
            shutil.copyfile(cache, temp)
            if _signature(cache) != before:
                raise ValueError("FinLab cache changed during copy")
            result = _commit_raw(temp, root, stem, key, source_mode, origin)
        finally:
            temp.unlink(missing_ok=True)
        _atomic_json(receipt_path, result)
        return root / result["raw_path"], result

    status = get_data_status() or {}
    try:
        room_mb = float(status["limit_size"]) - float(status["quota"])
    except (KeyError, ValueError, TypeError):
        raise FinlabResourceDeferred("FinLab quota unknown") from None
    if not math.isfinite(room_mb) or not math.isfinite(quota_reserve_mb) or quota_reserve_mb < 0:
        raise FinlabResourceDeferred("FinLab quota unknown")
    # The SDK signs/account-charges whole tables. Date slicing is not remote
    # partitioning. Reserve before signing, not after wasting the allocation.
    floor = 1024 * 1024**2 if key == "broker_transactions" else 192 * 1024**2
    estimate = max(floor, int(previous.get("raw_bytes", 0) * 1.2))
    if room_mb * 1024**2 < estimate + quota_reserve_mb * 1024**2:
        raise FinlabResourceDeferred("FinLab remaining quota insufficient for bounded whole table")
    if shutil.disk_usage(raw_root).free < MAX_RAW_BYTES * 4 + 4 * 1024**3:
        raise FinlabResourceDeferred("FinLab raw disk reserve reached")
    result = fetch_metadata_batch(OrderedDict([(key, None)])).get(key)
    if result is None or result.error:
        # Parent categorizer redacts provider exceptions; never persist URLs.
        raise RuntimeError(result.error if result is not None else "FinLab dataset not found")
    if not result.url or result.data is not None:
        raise ValueError("unsupported FinLab raw response contract")
    with tempfile.NamedTemporaryFile(dir=raw_root, suffix=".feather.tmp", delete=False) as handle:
        temp = Path(handle.name)
    try:
        stream_download(result.url, temp, maximum_bytes=min(MAX_RAW_BYTES,
                        int((room_mb - quota_reserve_mb) * 1024**2)))
        receipt = _commit_raw(temp, root, stem, key, "upstream_forced", "sdk_authorized_signed_object")
        # Only safe provider publication metadata, never signed URLs or account fields.
        receipt["provider_expiry_at_utc"] = result.expiry.isoformat() if result.expiry else None
        _atomic_json(receipt_path, receipt)
        return root / receipt["raw_path"], receipt
    finally:
        temp.unlink(missing_ok=True)


def _commit_raw(temp: Path, root: Path, stem: str, key: str, mode: str, origin: str) -> dict:
    # Opening the footer checks format without decoding the whole object.
    with pa.memory_map(str(temp), "r") as handle:
        reader = pa.ipc.open_file(handle)
        if not reader.num_record_batches or "date" not in reader.schema.names:
            raise ValueError("FinLab raw schema missing date or record batches")
    digest = _sha256(temp)
    target = root / "raw" / f"{stem}-{digest}.feather"
    if target.exists():
        if _sha256(target) != digest:
            raise ValueError("FinLab raw object hash mismatch")
    else:
        os.replace(temp, target)
    return {"contract_version": RAW_CONTRACT, "dataset": key, "status": "raw_acquired_unvalidated",
            "raw_path": str(target.relative_to(root)), "raw_sha256": digest, "raw_bytes": target.stat().st_size,
            "source_checked_at_utc": datetime.now(UTC).isoformat(), "source_check_mode": mode,
            "origin": origin, "historical_point_in_time": False}


def convert_raw(key: str, raw_path: Path, destination: Path, *, column_batch_size: int = 512) -> dict:
    """Project wide IPC columns before decompression; stream long IPC batches."""
    if key not in STREAMING_KEYS or not 1 <= column_batch_size <= 512:
        raise ValueError("unsupported FinLab Arrow conversion")
    if key == "broker_transactions":
        return _convert_brokers(raw_path, destination)
    dataset = ds.dataset(raw_path, format="ipc")
    names = dataset.schema.names
    fields = [name for name in names if name != "date"]
    if "date" not in names or not fields or len(names) != len(set(names)):
        raise ValueError("invalid FinLab wide metadata schema")
    if any(not (pa.types.is_string(dataset.schema.field(name).type)
                or pa.types.is_large_string(dataset.schema.field(name).type)) for name in fields):
        raise ValueError("FinLab metadata type changed")
    dates = dataset.to_table(columns=["date"], use_threads=False)["date"].combine_chunks()
    valid_rows: set[int] = set()
    rows = 0
    schema = pa.schema([("source_index", pa.string()), ("date", dates.type),
                        ("symbol", pa.string()), ("source_column", pa.string()), ("value", pa.string())])
    with pq.ParquetWriter(destination, schema, compression="zstd") as writer:
        for offset in range(0, len(fields), column_batch_size):
            columns = fields[offset:offset + column_batch_size]
            table = dataset.to_table(columns=columns, use_threads=False)
            parts = []
            for name in columns:
                values = table[name].combine_chunks()
                indexes = pc.indices_nonzero(pc.is_valid(values))
                if not len(indexes):
                    continue
                valid_rows.update(indexes.to_pylist())
                selected_dates = pc.take(dates, indexes)
                parts.append(pa.Table.from_arrays([
                    pc.cast(selected_dates, pa.string()), selected_dates,
                    pa.repeat(name.split(" ", 1)[0], len(indexes)), pa.repeat(name, len(indexes)),
                    pc.cast(pc.take(values, indexes), pa.string()),
                ], schema=schema))
            if parts:
                block = pa.concat_tables(parts)
                writer.write_table(block, row_group_size=65536)
                rows += len(block)
            del table, parts
    observed = pc.take(dates, pa.array(sorted(valid_rows), type=pa.int64()))
    bounds = pc.min_max(observed).as_py()
    # Preserve *all* schema names, including columns with no observations.
    return {"rows": rows, "rows_with_values": rows, "source_rows": len(dates),
            "source_rows_with_values": len(valid_rows), "field_columns": len(fields),
            "source_index_columns": ["source_index"], "source_index_dtype": str(dates.type),
            "first_non_null_source_index": str(bounds["min"]) if bounds["min"] is not None else None,
            "last_non_null_source_index": str(bounds["max"]) if bounds["max"] is not None else None,
            "first_event_at": bounds["min"].isoformat() if bounds["min"] is not None else None,
            "last_event_at": bounds["max"].isoformat() if bounds["max"] is not None else None,
            "storage_layout": "sparse_long_raw_columns", "event_time_column": "date",
            "index_semantics": "source_timestamp_not_verified_publication_date",
            "null_semantics": "absent cell is source null, never zero; raw Arrow retains every column and null",
            "symbol_semantics": "prefix of original source_column; duplicates preserved, not averaged"}


def _convert_brokers(raw_path: Path, destination: Path) -> dict:
    rows = 0
    first = last = None
    with pa.memory_map(str(raw_path), "r") as handle:
        reader = pa.ipc.open_file(handle)
        names = reader.schema.names
        if ("date" not in names or not {"buy", "sell"} <= set(names)
                or not ({"stock_id", "symbol"} & set(names)) or len(names) != len(set(names))
                or "source_index" in names):
            raise ValueError("FinLab broker schema changed")
        schema = reader.schema.remove_metadata().append(pa.field("source_index", pa.string()))
        if "stock_id" not in names:
            schema = schema.append(pa.field("stock_id", reader.schema.field("symbol").type))
        if "symbol" not in names:
            schema = schema.append(pa.field("symbol", reader.schema.field("stock_id").type))
        with pq.ParquetWriter(destination, schema, compression="zstd") as writer:
            for i in range(reader.num_record_batches):
                batch = reader.get_batch(i)
                # Provider batches normally cap at 65,536 rows. Refuse drift
                # instead of allowing an accidental unbounded pandas fallback.
                if batch.nbytes > 256 * 1024**2:
                    raise FinlabResourceDeferred("FinLab broker Arrow batch exceeds memory budget")
                table = pa.Table.from_batches([batch]).replace_schema_metadata(None)
                bounds = pc.min_max(table["date"]).as_py()
                if bounds["min"] is not None:
                    first = bounds["min"] if first is None else min(first, bounds["min"])
                    last = bounds["max"] if last is None else max(last, bounds["max"])
                table = table.append_column("source_index", pc.cast(pa.array(range(rows, rows + len(batch))), pa.string()))
                for name, alias in [("stock_id", "symbol"), ("symbol", "stock_id")]:
                    if name not in table.column_names:
                        table = table.append_column(name, table[alias])
                writer.write_table(table, row_group_size=65536)
                rows += len(batch)
    return {"rows": rows, "rows_with_values": rows, "field_columns": len(schema) - 1,
            "source_index_columns": ["source_index"], "source_index_dtype": "int64",
            "first_non_null_source_index": "0" if rows else None,
            "last_non_null_source_index": str(rows - 1) if rows else None,
            "first_event_at": first.isoformat() if first is not None else None,
            "last_event_at": last.isoformat() if last is not None else None,
            "storage_layout": "provider_long_arrow", "event_time_column": "date",
            "index_semantics": "provider_index_unverified"}


def prepare_arrow(key: str, root: Path, stem: str, destination: Path, *, refresh: bool) -> dict:
    raw, evidence = acquire_raw(key, root, stem, refresh=refresh)
    stats = convert_raw(key, raw, destination)
    stats.update({k: evidence[k] for k in ["raw_path", "raw_sha256", "raw_bytes", "source_check_mode", "source_checked_at_utc"]})
    return stats


def capture_empty_evidence(key: str, root: Path, stem: str, *, cache_path: Path | None = None) -> dict:
    """Distinguish real source nulls from a lossy SDK normalization, offline.

    Only the two measured small all-null keys are inspected. Keep their exact
    Arrow objects as evidence, not as successful trainable dataset receipts.
    """
    if key not in {"dividend_otc:權息", "management_change_events:變更交易開始日"}:
        return {}
    if cache_path is None:
        from finlab import utils
        cache_path = Path(utils.get_tmp_dir()) / (key.replace(":", "#") + ".feather")
    if not cache_path.is_file() or cache_path.stat().st_size > 8 * 1024**2:
        return {}
    before = _signature(cache_path)
    rows = values = 0
    with pa.memory_map(str(cache_path), "r") as handle:
        reader = pa.ipc.open_file(handle)
        if "date" not in reader.schema.names:
            return {}
        fields = len(reader.schema) - 1
        for n in range(reader.num_record_batches):
            batch = reader.get_batch(n)
            rows += len(batch)
            for field, array in zip(batch.schema, batch.columns):
                if field.name == "date":
                    continue
                valid = pc.is_valid(array)
                if pa.types.is_floating(field.type):
                    valid = pc.and_kleene(valid, pc.invert(pc.is_nan(array)))
                values += pc.sum(pc.cast(valid, pa.int64())).as_py() or 0
    digest = _sha256(cache_path)
    if before != _signature(cache_path):
        raise ValueError("FinLab source changed during null audit")
    target = root / "empty_evidence" / f"{stem}-{digest}.feather"
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        with tempfile.NamedTemporaryFile(dir=target.parent, suffix=".tmp", delete=False) as handle:
            temp = Path(handle.name)
        try:
            shutil.copyfile(cache_path, temp)
            if before != _signature(cache_path) or _sha256(temp) != digest:
                raise ValueError("FinLab source changed during null evidence copy")
            os.replace(temp, target)
        finally:
            temp.unlink(missing_ok=True)
    elif _sha256(target) != digest:
        raise ValueError("FinLab null evidence hash mismatch")
    report = {"dataset": key, "observed_at_utc": datetime.now(UTC).isoformat(),
              "raw_rows": rows, "raw_field_columns": fields, "raw_non_null_values": values,
              "raw_sha256": digest, "raw_path": str(target.relative_to(root)),
              "status": "source_all_null" if not values else "sdk_normalization_requires_repair",
              "usable_observations": False}
    report_path = target.with_suffix(".json")
    _atomic_json(report_path, report)
    return {"raw_non_null_values": values, "empty_evidence_path": str(report_path.relative_to(root))}
