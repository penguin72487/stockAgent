"""Exact dated-source proofs for the canonical TW public feature producer.

Not an alternate collector or feature implementation. A changed dated archive
is scanned once to establish which original sessions changed. Unchanged source
receipts reuse their prior partition proofs. Unsupported/schema/calendar inputs
remain full-build dependencies; never infer an append from file size alone.
"""
from __future__ import annotations

from datetime import date, timedelta
import hashlib
import io
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

CONTRACT = "tw_public_exact_dated_source_partitions_v1"
SOURCE_SESSION_COLUMNS = {f"{name}.parquet": ("date", "iso", False) for name in (
    "twse_daily_ohlcv", "tpex_daily_ohlcv", "twse_daily_valuation", "tpex_daily_valuation",
    "twse_margin_balance", "tpex_margin_balance", "twse_institutional_trades",
    "tpex_institutional_trades", "twse_market_index", "twse_taiex_ohlc",
    "twse_day_trade_eligibility", "tpex_day_trade_eligibility",
)}
# These builders use the declared trading/settlement day, NOT the archive's
# `date`/`_as_of_date` capture clock. Rows with a null declared date are also
# dropped by the canonical builders. Non-null unsupported dates fail closed.
SOURCE_SESSION_COLUMNS.update({f"{name}.parquet": ("Date", "yyyymmdd", True) for name in (
    "taifex_daily_futures", "taifex_daily_options", "taifex_institutional_total",
    "taifex_large_trader_futures_oi",
)})
SOURCE_SESSION_COLUMNS["taifex_final_settlement_price.parquet"] = ("最後結算日", "yyyymmdd", True)
DATED_SOURCES = frozenset(SOURCE_SESSION_COLUMNS)
MAX_SESSION_ROWS = 250_000
MAX_CACHED_ROW_GROUPS = 16_384
PHYSICAL_PREFIX_CONTRACT = "tw_public_verified_parquet_groups_v1"


def _generation(path):
    info = path.stat()
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def implementation_sha256():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


class _HashSink(io.RawIOBase):
    def __init__(self):
        self.digest = hashlib.sha256()
        self.count = 0

    def writable(self):
        return True

    def tell(self):
        return self.count

    def write(self, payload):
        self.digest.update(payload)
        self.count += len(payload)
        return len(payload)


def _logical_proof_sha256(proof):
    return hashlib.sha256(json.dumps({name: proof[name] for name in
        ("schema_sha256", "sessions", "session_clock")}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _encoded_row_groups(path, parquet):
    """Bind original compressed pages AND decoding metadata, not statistics.

    Offset/encoding/layout changes are conservative misses. Never use min/max
    statistics to certify that an unparsed row belongs to a particular day.
    External column files and unbounded group inventories use the full scanner.
    """
    metadata = parquet.metadata
    if metadata.num_row_groups > MAX_CACHED_ROW_GROUPS:
        raise ValueError("row-group proof inventory exceeds bound")
    footer_start = path.stat().st_size - 8 - metadata.serialized_size
    result, read_bytes = [], 0
    with path.open("rb") as source:
        for index in range(metadata.num_row_groups):
            group = metadata.row_group(index)
            if group.num_rows <= 0 or group.num_columns <= 0:
                raise ValueError("empty physical row group")
            ranges = []
            for column_index in range(group.num_columns):
                column = group.column(column_index)
                if column.file_path:
                    raise ValueError("external column payload is not a local prefix")
                offsets = [value for value in (column.dictionary_page_offset, column.data_page_offset)
                           if value is not None and value > 0]
                if not offsets or column.total_compressed_size <= 0:
                    raise ValueError("unknown compressed column extent")
                start = min(offsets)
                ranges.append((start, start + column.total_compressed_size))
            start, stop = min(item[0] for item in ranges), max(item[1] for item in ranges)
            if not 4 <= start < stop <= footer_start:
                raise ValueError("compressed group escapes original payload")
            digest = hashlib.sha256(json.dumps({"group": group.to_dict(),
                "format": metadata.format_version, "writer": metadata.created_by},
                sort_keys=True, separators=(",", ":"), default=str).encode())
            source.seek(start)
            remaining = stop - start
            while remaining:
                payload = source.read(min(remaining, 1024 * 1024))
                if not payload:
                    raise OSError("truncated physical row-group proof")
                digest.update(payload)
                remaining -= len(payload)
                read_bytes += len(payload)
            result.append({"sha256": digest.hexdigest(), "rows": group.num_rows})
    return result, read_bytes


def _row_group_prefix(previous, schema_sha, clock, groups):
    """Return a certified group prefix, always rewinding its boundary day."""
    try:
        old = previous["physical_prefix"]
        before = old["groups"]
        if (old["contract"] != PHYSICAL_PREFIX_CONTRACT or previous["schema_sha256"] != schema_sha
                or previous["session_clock"] != clock or old["logical_sha256"] != _logical_proof_sha256(previous)
                or old["checkpoint_sha256"] != hashlib.sha256(json.dumps(
                    {key: value for key, value in old.items() if key != "checkpoint_sha256"},
                    sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                or not isinstance(before, list) or len(before) > MAX_CACHED_ROW_GROUPS):
            return 0
        matched = 0
        for actual, certified in zip(groups, before):
            if any(actual[key] != certified[key] for key in ("sha256", "rows")):
                break
            for name in ("first_day", "last_day"):
                if date.fromisoformat(certified[name]).isoformat() != certified[name]:
                    return 0
            if certified["first_day"] > certified["last_day"]:
                return 0
            matched += 1
        # Even when every extant group matches (e.g. truncation), re-read the
        # last matched group. Its final session might span into a changed group.
        first = max(0, matched - 1)
        while first and before[first - 1]["last_day"] >= before[first]["first_day"]:
            first -= 1
        return first
    except (KeyError, TypeError, ValueError, AttributeError):
        return 0


def _canonical_source_day(value, date_format):
    if isinstance(value, date):
        return value.isoformat()
    if date_format == "yyyymmdd" and isinstance(value, str) and len(value) == 8 and value.isascii() and value.isdigit():
        value = f"{value[:4]}-{value[4:6]}-{value[6:]}"
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("source date is null or not a canonical ISO session")
    return value


def session_fingerprints(path: Path, *, date_column="date", date_format="iso", ignore_null_dates=False,
                         previous=None, capture_physical=False) -> dict:
    """SHA every typed column/row, bounded to one sorted source session.

    Canonical IPC chunking is independent of Parquet row-group boundaries.
    Duplicates, original within-date order, NULL and floating-point bits stay
    significant. Non-ISO/null/unsorted dates are unsupported, not silently
    guessed. The normal full builder remains available for those archives.
    """
    before = _generation(path)
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow.remove_metadata()
    schema_sha = hashlib.sha256(schema.serialize()).hexdigest()
    clock = {"column": date_column, "format": date_format, "ignore_null": ignore_null_dates}
    physical = None
    proof_bytes = first_group = 0
    # Null-date filtering may remove complete physical groups. Keep that
    # source on the established full scanner until its sparse clock is proven.
    if capture_physical and not ignore_null_dates:
        try:
            physical, proof_bytes = _encoded_row_groups(path, parquet)
            first_group = _row_group_prefix(previous, schema_sha, clock, physical)
        except (OSError, ValueError, pa.ArrowException):
            physical = None
    selected = list(range(first_group, parquet.num_row_groups))
    result = _fingerprint_batches(schema, parquet.iter_batches(batch_size=65_536, row_groups=selected),
                                 date_column=date_column, date_format=date_format,
                                 ignore_null_dates=ignore_null_dates)
    if first_group:
        first_day = next(iter(result["sessions"]), None)
        if first_day is None:
            raise ValueError("physical suffix lost its certified boundary session")
        prefix = {day: proof for day, proof in previous["sessions"].items()
                  if day < first_day}
        result["sessions"] = {**prefix, **result["sessions"]}
    if physical is not None:
        for index, group in enumerate(physical):
            if index < first_group:
                bounds = previous["physical_prefix"]["groups"][index]
                group.update(first_day=bounds["first_day"], last_day=bounds["last_day"])
            else:
                # Actual decoded clock values, never potentially forged stats.
                days = parquet.read_row_group(index, columns=[date_column]).column(0)
                group.update(first_day=_canonical_source_day(days[0].as_py(), date_format),
                             last_day=_canonical_source_day(days[-1].as_py(), date_format))
        guard = {"contract": PHYSICAL_PREFIX_CONTRACT, "groups": physical,
                 "logical_sha256": _logical_proof_sha256(result)}
        guard["checkpoint_sha256"] = hashlib.sha256(json.dumps(guard,
            sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        result["physical_prefix"] = guard
    if capture_physical:
        result["row_group_work"] = {"total_groups": parquet.num_row_groups,
            "reused_groups": first_group, "decoded_groups": len(selected),
            "decoded_rows": sum(parquet.metadata.row_group(i).num_rows for i in selected),
            "compressed_proof_bytes": proof_bytes, "complete_source_sha_still_required": True}
    if before != _generation(path):
        raise OSError("dated source changed during partition proof")
    return result


def table_session_fingerprints(table: pa.Table) -> dict:
    """The same typed proof for a canonical, date-sorted derived event table."""
    return _fingerprint_batches(table.schema.remove_metadata(), table.to_batches(max_chunksize=65_536))


def table_content_sha256(table: pa.Table) -> str:
    """Exact typed ordered values, independent of slices and physical chunks."""
    table = table.replace_schema_metadata(None).combine_chunks()
    table = table.take(pa.array(np.arange(table.num_rows, dtype=np.int64)))
    sink = _HashSink()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table, max_chunksize=65_536)
    return sink.digest.hexdigest()


def _fingerprint_batches(schema, batches, *, date_column="date", date_format="iso", ignore_null_dates=False):
    if date_column not in schema.names or schema.field(date_column).type not in (pa.date32(), pa.string(), pa.large_string()):
        raise ValueError("dated source needs canonical date32 or ISO date strings")
    if date_format not in {"iso", "yyyymmdd"}:
        raise ValueError("unsupported source session format")
    output, fragments = {}, []
    current = None
    rows = 0

    def finish():
        if current is None:
            return
        table = pa.Table.from_batches(fragments, schema=schema).combine_chunks()
        # Arrow slices can retain adjacent rows in their underlying buffers;
        # IPC padding may otherwise depend on tomorrow's append. Gather just
        # this session into canonical offset-zero arrays before hashing.
        table = table.take(pa.array(np.arange(table.num_rows, dtype=np.int64)))
        sink = _HashSink()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_table(table, max_chunksize=65_536)
        output[current] = {"sha256": sink.digest.hexdigest(), "rows": rows}

    for batch in batches:
        batch = batch.replace_schema_metadata(None)
        if ignore_null_dates:
            batch = batch.filter(pc.is_valid(batch.column(schema.get_field_index(date_column))))
        runs = pc.run_end_encode(batch.column(schema.get_field_index(date_column)))
        raw_dates = runs.values.to_pylist()
        dates = []
        for value in raw_dates:
            dates.append(_canonical_source_day(value, date_format))
        if not dates:
            continue
        boundaries = [0, *runs.run_ends.to_pylist()]
        for day, start, stop in zip(dates, boundaries, boundaries[1:]):
            if current is not None and day < current:
                raise ValueError("source is not chronologically partitioned")
            if day != current:
                finish()
                current, fragments, rows = day, [], 0
            rows += stop - start
            if rows > MAX_SESSION_ROWS:
                raise ValueError("source session exceeds bounded partition proof")
            fragments.append(batch.slice(start, stop - start))
    finish()
    return {"schema_sha256": hashlib.sha256(schema.serialize()).hexdigest(), "sessions": output,
            "session_clock": {"column": date_column, "format": date_format, "ignore_null": ignore_null_dates}}


def capture_partitions(root: Path, receipts: list[dict], previous: dict | None) -> dict:
    previous = previous if isinstance(previous, dict) else {}
    compatible = (previous.get("contract") == CONTRACT
                  and previous.get("implementation_sha256") == implementation_sha256()
                  and previous.get("arrow_version") == pa.__version__)
    prior = previous.get("sources", {}) if compatible else {}
    prior = prior if isinstance(prior, dict) else {}
    sources = {}
    scanned = reused = 0
    for receipt in receipts:
        name = receipt["name"]
        if name not in DATED_SOURCES:
            continue
        old = prior.get(name, {})
        old = old if isinstance(old, dict) else {}
        if old.get("source_sha256") == receipt["sha256"] and "sessions" in old:
            sources[name] = old
            reused += 1
            continue
        try:
            column, fmt, ignore_null = SOURCE_SESSION_COLUMNS[name]
            proof = session_fingerprints(root / name, date_column=column,
                                         date_format=fmt, ignore_null_dates=ignore_null,
                                         previous=old, capture_physical=True)
        except (OSError, ValueError, pa.ArrowException) as exc:
            proof = {"unsupported": type(exc).__name__}
        sources[name] = {"source_sha256": receipt["sha256"], **proof}
        scanned += 1
    return {"contract": CONTRACT, "implementation_sha256": implementation_sha256(),
            "arrow_version": pa.__version__, "sources": sources,
            "work": {"scanned_sources": scanned, "reused_sources": reused}}


def affected_start(previous_summary: dict, receipts: list[dict], current: dict,
                   end_date: date) -> tuple[date | None, dict]:
    """Return the first provably affected date, or a full-build reason."""
    previous_summary = previous_summary if isinstance(previous_summary, dict) else {}
    old = previous_summary.get("input_partitions", {})
    old = old if isinstance(old, dict) else {}
    old_rows = previous_summary.get("source_receipts", [])
    if not isinstance(old_rows, list) or any(not isinstance(row, dict) or not isinstance(row.get("name"), str)
                                           or not isinstance(row.get("sha256"), str) for row in old_rows):
        old_rows = []
    old_receipts = {row["name"]: row["sha256"] for row in old_rows}
    new_receipts = {row["name"]: row["sha256"] for row in receipts}
    changed = sorted(name for name in new_receipts if old_receipts.get(name) != new_receipts[name])
    plan = {"contract": CONTRACT, "changed_sources": changed, "affected_source_sessions": {}}

    def reject(reason):
        return None, {**plan, "mode": "full", "reason": reason}

    if len(old_receipts) != len(old_rows):
        return reject("duplicate_source_receipts")
    if not old or any(old.get(key) != current.get(key) for key in (
        "contract", "implementation_sha256", "arrow_version")):
        return reject("partition_bootstrap_or_implementation_changed")
    if old_receipts.keys() != new_receipts.keys():
        return reject("source_membership_changed")
    if not set(changed) <= DATED_SOURCES:
        return reject("non_dated_or_global_dependency_changed")
    affected = []
    for name in changed:
        old_sources = old.get("sources", {})
        if not isinstance(old_sources, dict):
            return reject("source_partition_proof_missing_or_schema_changed")
        before, after = old_sources.get(name, {}), current["sources"].get(name, {})
        if not isinstance(before, dict) or not isinstance(after, dict):
            return reject("source_partition_proof_missing_or_schema_changed")
        if (before.get("source_sha256") != old_receipts[name]
                or after.get("source_sha256") != new_receipts[name]
                or not isinstance(before.get("sessions"), dict) or not isinstance(after.get("sessions"), dict)
                or before.get("session_clock") != after.get("session_clock")
                or before.get("schema_sha256") != after.get("schema_sha256")):
            return reject("source_partition_proof_missing_or_schema_changed")
        try:
            days = sorted(day for day in before["sessions"].keys() | after["sessions"].keys()
                          if before["sessions"].get(day) != after["sessions"].get(day))
            affected.extend(date.fromisoformat(day) for day in days)
        except (TypeError, ValueError):
            return reject("source_partition_dates_invalid")
        plan["affected_source_sessions"][name] = days
    try:
        old_end = date.fromisoformat(previous_summary["requested_end_date"])
    except (KeyError, TypeError, ValueError):
        return reject("previous_completed_session_missing")
    if end_date < old_end:
        return reject("requested_history_truncated")
    # Extending the decision calendar can expose a previously unavailable
    # next-session feature. Start at the first newly requested day, not just
    # the last release date, even when source rows themselves are unchanged.
    if end_date > old_end:
        affected.append(old_end + timedelta(days=1))
    start = min([end_date, *affected])
    return start, {**plan, "mode": "affected_tail", "start_date": start.isoformat()}
