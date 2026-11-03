"""Bounded-memory exact option-day JSON transport for the Complement owner.

This is a retry transport, not another downloader or a different query.  It
stages validated batches and publishes only after the entire provider envelope
has been parsed; provider row order and duplicate ticks are retained.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import time

import ijson
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import requests
from urllib3.exceptions import ReadTimeoutError

from downloader.artifact_io import table_from_records
from downloader.finmind_errors import SourceError
from downloader.finmind_updates import received_rows
from downloader import finmind_supplemental as supplemental


@dataclass
class StreamedOptionRows:
    path: Path
    rows: int
    sha256: str
    row_fingerprint: str
    field_non_null_counts: dict[str, int]
    observation: dict
    source_date: str

    def __len__(self):
        return self.rows


def _transport_code(exc: requests.RequestException) -> str:
    if isinstance(exc, requests.ConnectionError):
        pending, seen = [exc], set()
        while pending:
            value = pending.pop()
            if id(value) in seen:
                continue
            seen.add(id(value))
            if isinstance(value, ReadTimeoutError):
                return 'ReadTimeout'
            pending.extend(v for v in (*value.args, value.__cause__, value.__context__)
                           if isinstance(v, BaseException))
    return type(exc).__name__


def _skip_value(events, event: str) -> None:
    if event not in {'start_map', 'start_array'}:
        return
    depth = 1
    while depth:
        event, _ = next(events)
        depth += (event in {'start_map', 'start_array'}) - (event in {'end_map', 'end_array'})


def _read_object(events) -> dict:
    builder = ijson.common.ObjectBuilder()
    builder.event('start_map', None)
    depth = 1
    while depth:
        event, value = next(events)
        builder.event(event, value)
        depth += (event in {'start_map', 'start_array'}) - (event in {'end_map', 'end_array'})
    return builder.value


def _admit(root: Path, sampler, minimum_free: int) -> None:
    sample = sampler.sample() if sampler is not None else {'state': 'not_enrolled'}
    if sample.get('reason') in {'capacity_sample_pending', 'capacity_sample_expired'}:
        sample = sampler.initial_sample(timeout_seconds=29)
    if shutil.disk_usage(root).free < minimum_free or sample.get('state') not in {'admitted', 'not_enrolled'}:
        raise SourceError('object_disk_guard', retry_after=300)


def requires_stream(connection, task, previous_error: str | None, now: datetime) -> bool:
    """Retain an exact task's oversized-response decision across retries.

    Commit this small scheduling marker before contacting the provider: a
    later capacity or transport failure must not send the same oversized
    response through the bounded legacy JSON reader again.
    """
    if task.dataset != 'TaiwanOptionTick' or task.kind != 'id_day' or not task.data_id:
        return False
    table = 'finmind_option_stream_required'
    exists = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if previous_error == 'response_size_limit':
        if not exists:
            connection.execute('CREATE TABLE IF NOT EXISTS finmind_option_stream_required ('
                'dataset TEXT NOT NULL,data_id TEXT NOT NULL,partition TEXT NOT NULL,marked_at_utc TEXT NOT NULL,'
                'PRIMARY KEY(dataset,data_id,partition))')
        connection.execute('INSERT OR IGNORE INTO finmind_option_stream_required VALUES (?,?,?,?)',
                           (task.dataset, task.data_id, task.partition, now.isoformat()))
        connection.commit()
        return True
    return bool(exists and connection.execute(
        'SELECT 1 FROM finmind_option_stream_required WHERE dataset=? AND data_id=? AND partition=?',
        (task.dataset, task.data_id, task.partition)).fetchone())


def fetch(session, limiter, root: Path, task, token: str, *, today: date,
          capacity_sampler=None, batch_rows: int = 8192):
    """Fetch exactly the original per-product day once, then validate/store.

    The old 64 MiB Python-JSON limit is not expanded.  Only the separately
    selected retry uses this transport, and it keeps the ordinary quota,
    source identity, physical capacity and owner publication boundaries.
    """
    from downloader.download_finmind_complement import (
        BULK_MAX_RESPONSE_BYTES, MIN_FREE_BYTES, _client_error,
        _record_request_start, _response_rows, _sha256,
    )
    from downloader.download_finmind_free import ProviderError

    if (task.dataset != 'TaiwanOptionTick' or task.kind != 'id_day' or not task.data_id
            or not isinstance(batch_rows, int) or batch_rows <= 0):
        raise ValueError('option_stream_identity_mismatch')
    endpoint, params, metadata = supplemental.request_contract(task.dataset, task.data_id, task.partition, today)
    stage_root = root / '.option-json-stream'
    stage_root.mkdir(parents=True, exist_ok=True)
    _admit(stage_root, capacity_sampler, MIN_FREE_BYTES)
    limiter.wait()
    try:
        _record_request_start(root.parent, task.dataset)
    except ProviderError as error:
        raise SourceError(error.code, retry_after=int(error.retry_after)) from None
    started, monotonic_started = datetime.now(UTC), time.monotonic()
    response = None
    output_path = None
    try:
        response = session.get(endpoint, params=params, headers={'Authorization': f'Bearer {token}'},
                               timeout=(10, 90), stream=True)
        if response.status_code != 200:
            _response_rows(response, limiter, max_response_bytes=BULK_MAX_RESPONSE_BYTES)
            raise SourceError('provider_rejected', retry_after=3600)
        with tempfile.TemporaryDirectory(prefix='attempt-', dir=stage_root) as stage:
            folder = Path(stage)
            batch, pieces, schemas = [], [], []
            count, total = 0, 0

            def flush():
                nonlocal batch
                if not batch:
                    return
                _admit(stage_root, capacity_sampler, MIN_FREE_BYTES)
                supplemental.validate_response(task.dataset, task.data_id, task.partition, today, batch)
                table = table_from_records(batch)
                piece = folder / f'{len(pieces):08d}.arrow'
                with ipc.new_file(piece, table.schema, options=ipc.IpcWriteOptions(compression='zstd')) as writer:
                    writer.write_table(table)
                pieces.append(piece)
                schemas.append(table.schema)
                batch = []

            # A zero-length keepalive is not EOF; discard it before the
            # file-like adapter, which otherwise treats b'' as end of input.
            stream = ijson.from_iter(chunk for chunk in response.iter_content(chunk_size=64 * 1024) if chunk)
            events = iter(ijson.basic_parse(stream, use_float=True))
            if next(events)[0] != 'start_map':
                raise SourceError('provider_rejected', retry_after=3600)
            envelope, seen = {}, set()
            while True:
                event, key = next(events)
                if event == 'end_map':
                    break
                if event != 'map_key' or key in seen:
                    raise SourceError('invalid_json')
                seen.add(key)
                event, value = next(events)
                if key == 'data':
                    if event != 'start_array':
                        raise SourceError('invalid_rows')
                    while True:
                        event, value = next(events)
                        if event == 'end_array':
                            break
                        if event != 'start_map':
                            raise SourceError('invalid_rows')
                        row = _read_object(events)
                        raw = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()
                        total = (total + int.from_bytes(hashlib.sha256(raw).digest(), 'big')) % (1 << 256)
                        batch.append(row)
                        count += 1
                        if len(batch) >= batch_rows:
                            flush()
                elif key in {'status', 'msg'} and event not in {'start_map', 'start_array'}:
                    envelope[key] = value
                else:
                    _skip_value(events, event)
            try:
                next(events)
            except StopIteration:
                pass
            else:
                raise SourceError('invalid_json')
            if envelope.get('status') in {402, 429}:
                limiter.defer(3600)
                raise SourceError('rate_limited', retry_after=3600)
            if envelope.get('status') in {400, 401, 403, 404}:
                raise _client_error(envelope['status'], str(envelope.get('msg', '')), limiter)
            if envelope.get('status') != 200 or envelope.get('msg') != 'success':
                raise SourceError('provider_rejected', retry_after=3600)
            if 'data' not in seen:
                raise SourceError('invalid_rows')
            flush()
            observed = received_rows([], started, monotonic_started=monotonic_started)
            observed.observation['response_rows'] = count
            if not count:
                return observed, metadata
            schema = pa.unify_schemas(schemas, promote_options='permissive')
            fd, name = tempfile.mkstemp(prefix='validated-', suffix='.parquet', dir=stage_root)
            os.close(fd)
            output_path = Path(name)
            non_null = dict.fromkeys(schema.names, 0)
            with pq.ParquetWriter(output_path, schema, compression='zstd') as writer:
                for piece in pieces:
                    _admit(stage_root, capacity_sampler, MIN_FREE_BYTES)
                    with ipc.open_file(piece) as reader:
                        table = reader.read_all()
                    arrays = []
                    for field in schema:
                        column = (table[field.name].cast(field.type, safe=True) if field.name in table.column_names
                                  else pa.chunked_array([pa.nulls(table.num_rows, type=field.type)]))
                        arrays.append(column)
                        non_null[field.name] += table.num_rows - column.null_count
                    writer.write_table(pa.Table.from_arrays(arrays, schema=schema))
            if not any(non_null.values()) or pq.ParquetFile(output_path).metadata.num_rows != count:
                raise SourceError('invalid_rows')
            fingerprint = hashlib.sha256(f'{count}:{total:064x}'.encode()).hexdigest()
            metadata['response_processing'] = 'bounded_json_batches_preserve_order_duplicates_v1'
            value = StreamedOptionRows(output_path, count, _sha256(output_path), fingerprint,
                                       non_null, observed.observation, task.partition)
            output_path = None
            return value, metadata
    except (ijson.JSONError, UnicodeDecodeError, StopIteration, OverflowError):
        raise SourceError('invalid_json') from None
    except requests.RequestException as exc:
        raise SourceError(_transport_code(exc)) from None
    finally:
        if response is not None:
            response.close()
        if output_path is not None:
            output_path.unlink(missing_ok=True)
