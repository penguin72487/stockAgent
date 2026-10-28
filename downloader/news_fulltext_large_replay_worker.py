"""Isolated, read-only proofs for retained Cnyes metadata heads.

This worker has no database, HTTP, or publication access. The canonical owner
dispatches it asynchronously, then rechecks the current head and source proof
before using its ordinary acceptance path. A worker proof is not acceptance.
"""
from __future__ import annotations

import base64
from collections import Counter
import ctypes
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import sys
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.news_fulltext_metadata_replay import _needs_repair, _selected_batch_records
from downloader.news_fulltext_policy import CONTRACT_VERSION
from downloader.news_fulltext_queue import replay_source_path


WORKER_VERSION = 1
BATCH_WORKER_VERSION = 2
MAX_BATCH_HEADS = 2
MAX_REQUEST_BYTES = 16 << 10
MAX_RECEIPT_BYTES = 1 << 20
_HASH = re.compile(r'^[0-9a-f]{64}$')
_METADATA = ('article_id', 'body_sha256', 'published_at_raw', 'authors_json',
             'body_completeness', 'final_url', 'source_response_article_id')


class _ProofError(ValueError):
    pass


def _stamp(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _same_file(handle, path, before):
    # Both the descriptor and pathname must still point at the observed bytes.
    return _stamp(os.fstat(handle.fileno())) == before == _stamp(path.stat())


def _validate_request(request):
    common = {'version', 'root', 'source_path', 'receipt_path', 'receipt_sha256',
              'extra_address_bytes', 'max_output_bytes', 'parent_pid'}
    if not isinstance(request, dict):
        raise _ProofError('invalid_request')
    version = request.get('version')
    if version == WORKER_VERSION and set(request) == common | {'article_id', 'body_sha256'}:
        heads = [request]
    elif version == BATCH_WORKER_VERSION and set(request) == common | {'heads'}:
        heads = request['heads']
        if (not isinstance(heads, list) or not 2 <= len(heads) <= MAX_BATCH_HEADS
                or any(not isinstance(head, dict) or set(head) != {'article_id', 'body_sha256'} for head in heads)):
            raise _ProofError('invalid_request')
    else:
        raise _ProofError('invalid_request')
    if any(not isinstance(request[k], str) or not request[k] for k in ('root', 'source_path', 'receipt_path')):
        raise _ProofError('invalid_request')
    if not Path(request['root']).is_absolute() or any(Path(request[k]).is_absolute() for k in ('source_path', 'receipt_path')):
        raise _ProofError('invalid_request')
    if (not isinstance(request['receipt_sha256'], str) or not _HASH.fullmatch(request['receipt_sha256'])
            or any(not isinstance(head[k], str) or not _HASH.fullmatch(head[k])
                   for head in heads for k in ('article_id', 'body_sha256'))
            or len({head['article_id'] for head in heads}) != len(heads)):
        raise _ProofError('invalid_request')
    if (type(request['extra_address_bytes']) is not int or not 1 <= request['extra_address_bytes'] <= (1 << 40)
            or type(request['max_output_bytes']) is not int or not 1 <= request['max_output_bytes'] <= (128 << 20)
            or type(request['parent_pid']) is not int or request['parent_pid'] <= 1):
        raise _ProofError('invalid_request')


def _bind_parent(parent_pid):
    # Linux PR_SET_PDEATHSIG is registered by the child, so parent death cannot
    # leave a detached reader. Check on both sides of prctl to close the race.
    if os.getppid() != parent_pid:
        raise _ProofError('worker_parent_changed')
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'worker_parent_guard_unavailable')
    if os.getppid() != parent_pid:
        raise _ProofError('worker_parent_changed')


def _set_address_limit(extra):
    # This is a per-child virtual-address safety boundary, not an RSS ceiling.
    # Set it after imports so library mappings count in the observed baseline.
    # Native libraries can abort on allocation failure; do not produce a large
    # core file containing private retained source content.
    _, core_hard = resource.getrlimit(resource.RLIMIT_CORE)
    resource.setrlimit(resource.RLIMIT_CORE, (0, core_hard))
    pages = int(Path('/proc/self/statm').read_text().split()[0])
    current = pages * os.sysconf('SC_PAGE_SIZE')
    old_soft, old_hard = resource.getrlimit(resource.RLIMIT_AS)
    ceiling = current + extra
    if old_soft != resource.RLIM_INFINITY:
        ceiling = min(ceiling, old_soft)
    if old_hard != resource.RLIM_INFINITY:
        ceiling = min(ceiling, old_hard)
    resource.setrlimit(resource.RLIMIT_AS, (ceiling, old_hard))
    return old_soft, old_hard


def _primitive_record(record, payload):
    """Encode the flat retained schema without executing serialized input."""
    result = {}
    for key, value in record.items():
        if not isinstance(key, str):
            raise _ProofError('unsupported_record_schema')
        if key == 'raw_response_body':
            if value is None:
                result[key] = None
            elif isinstance(value, bytes) and value == payload:
                result[key] = {'same_as_envelope': True}
            elif isinstance(value, bytes):
                result[key] = {'base64': base64.b64encode(value).decode('ascii')}
            else:
                raise _ProofError('unsupported_record_schema')
        elif value is None or type(value) in (str, bool, int, float):
            result[key] = value
        elif isinstance(value, bytes):
            result[key] = {'bytes_base64': base64.b64encode(value).decode('ascii')}
        else:
            raise _ProofError('unsupported_record_schema')
    return result


def _stream_selected_records(batch, metadata, heads):
    """Read requested rows and last non-null shared envelopes in one pass.

    Avoid the full-table multi-head materialization: large Arrow dictionary
    expansion can greatly exceed the Parquet footer's declared size.
    """
    selected = {head['article_id'] for head in heads}
    positions = {}
    refs = set()
    for index, row in enumerate(metadata):
        article_id = row['article_id']
        positions.setdefault(article_id, []).append(index)
        if article_id in selected and isinstance(row.get('source_response_article_id'), str):
            refs.add(row['source_response_article_id'])
    selected_positions = {positions[key][-1]: key for key in selected}
    envelope_positions = {index: ref for ref in refs for index in positions.get(ref, ())}
    last_index = max([*selected_positions, *envelope_positions])
    indexed, envelopes, last_seen = {}, {}, -1
    for index, row_batch in enumerate(batch.iter_batches(batch_size=1, use_threads=False)):
        last_seen = index
        if index in selected_positions or index in envelope_positions:
            id_index = row_batch.schema.get_field_index('article_id')
            expected = selected_positions.get(index, envelope_positions.get(index))
            if id_index < 0 or row_batch.column(id_index)[0].as_py() != expected:
                raise _ProofError('retained_metadata_batch_head_not_unique')
            record = row_batch.to_pylist()[0] if index in selected_positions else None
            if record is not None:
                indexed[expected] = record
            if index in envelope_positions:
                raw_index = row_batch.schema.get_field_index('raw_response_body')
                value = (record.get('raw_response_body') if record is not None else
                         row_batch.column(raw_index)[0].as_py() if raw_index >= 0 else None)
                if value is not None:
                    envelopes[envelope_positions[index]] = value
        if index >= last_index:
            break
    if last_seen < last_index:
        raise _ProofError('retained_metadata_batch_grain_invalid')
    return indexed, envelopes


def _read(request):
    root = Path(request['root'])
    receipt_path = replay_source_path(root, request['receipt_path'])
    with receipt_path.open('rb') as receipt_file:
        receipt_stamp = _stamp(os.fstat(receipt_file.fileno()))
        receipt_bytes = receipt_file.read(MAX_RECEIPT_BYTES + 1)
        if len(receipt_bytes) > MAX_RECEIPT_BYTES:
            raise _ProofError('receipt_too_large')
        if (hashlib.sha256(receipt_bytes).hexdigest() != request['receipt_sha256']
                or not _same_file(receipt_file, receipt_path, receipt_stamp)):
            raise _ProofError('receipt_changed_or_digest_mismatch')
    receipt = json.loads(receipt_bytes)
    if (not isinstance(receipt, dict) or receipt.get('contract_version') != CONTRACT_VERSION
            or receipt.get('parquet_path') != request['source_path'] or receipt.get('publish') is not False
            or type(receipt.get('rows')) is not int or not 1 <= receipt['rows'] <= 512
            or not isinstance(receipt.get('sha256'), str) or not _HASH.fullmatch(receipt['sha256'])):
        raise _ProofError('invalid_receipt_contract')

    path = replay_source_path(root, request['source_path'])
    with path.open('rb') as source:
        source_stamp = _stamp(os.fstat(source.fileno()))
        digest = hashlib.sha256()
        while block := source.read(1 << 20):
            digest.update(block)
        if digest.hexdigest() != receipt['sha256'] or not _same_file(source, path, source_stamp):
            raise _ProofError('source_changed_or_digest_mismatch')
        source.seek(0)
        reader = pq.ParquetFile(source, pre_buffer=False)
        if reader.metadata.num_rows != receipt['rows']:
            raise _ProofError('batch_grain_mismatch')
        metadata = reader.read(columns=list(_METADATA), use_threads=False).to_pylist()
        counts = Counter(row['article_id'] for row in metadata)
        indexed_meta = {row['article_id']: row for row in metadata}
        heads = request['heads'] if request['version'] == BATCH_WORKER_VERSION else [request]
        items, selected = [], []
        for head in heads:
            article_id = head['article_id']
            meta = indexed_meta.get(article_id)
            if counts[article_id] != 1 or meta['body_sha256'] != head['body_sha256']:
                if request['version'] == WORKER_VERSION:
                    raise _ProofError('head_identity_mismatch')
                items.append({'article_id': article_id, 'result': {'version': WORKER_VERSION,
                    'state': 'proof_failure', 'error': 'head_identity_mismatch'}})
                continue
            result = {'version': WORKER_VERSION, 'state': 'ok', 'metadata': meta,
                      'receipt_sha256': request['receipt_sha256'], 'source_sha256': receipt['sha256']}
            if urlsplit(meta['final_url']).hostname != 'm.cnyes.com':
                result['outcome'] = 'not_mobile_html_candidate'
            elif not _needs_repair(meta):
                result['outcome'] = 'current_metadata_complete'
            else:
                selected.append(head)
            items.append({'article_id': article_id, 'result': result})
        if selected:
            if request['version'] == WORKER_VERSION:
                indexed, envelopes = _selected_batch_records(path, reader, metadata,
                    [{'id': bytes.fromhex(selected[0]['article_id'])}])
            else:
                indexed, envelopes = _stream_selected_records(reader, metadata, selected)
            for item in items:
                result, article_id = item['result'], item['article_id']
                if result['state'] != 'ok' or 'outcome' in result:
                    continue
                try:
                    record = indexed[article_id]
                    payload = envelopes[record['source_response_article_id']]
                    if (not isinstance(payload, bytes) or not isinstance(record['body'], str)
                            or hashlib.sha256(payload).hexdigest() != record['response_sha256']
                            or len(payload) != record['http_response_bytes']
                            or hashlib.sha256(record['body'].encode()).hexdigest() != record['body_sha256']
                            or len(record['body'].encode()) != record['body_bytes']):
                        raise _ProofError('content_digest_mismatch')
                    result.update(outcome='requires_parent_validation', record=_primitive_record(record, payload),
                                  envelope_base64=base64.b64encode(payload).decode('ascii'))
                except (_ProofError, ValueError, KeyError, TypeError) as error:
                    if request['version'] == WORKER_VERSION:
                        raise
                    item['result'] = {'version': WORKER_VERSION, 'state': 'proof_failure',
                        'error': str(error) if isinstance(error, _ProofError) else 'worker_source_or_proof_invalid'}
        if not _same_file(source, path, source_stamp):
            raise _ProofError('source_changed_during_decode')
    # Even a concurrently replaced receipt cannot authorize this result.
    if _stamp(receipt_path.stat()) != receipt_stamp:
        raise _ProofError('receipt_changed_during_decode')
    for item in items:
        if item['result']['state'] == 'ok':
            item['result']['source_stamp'] = list(source_stamp)
            item['result']['receipt_stamp'] = list(receipt_stamp)
    if request['version'] == WORKER_VERSION:
        return items[0]['result']
    return {'version': BATCH_WORKER_VERSION, 'state': 'ok', 'items': items}


def execute(request, *, serialized=False):
    """Return an isolated proof or stable failure; reuse the checked wire bytes."""
    if __name__ != '__main__':
        # RLIMIT_AS applies to the entire process. Never let an import turn the
        # canonical news owner into the supposedly isolated worker.
        return {'version': WORKER_VERSION, 'state': 'proof_failure', 'error': 'worker_process_required'}
    version = WORKER_VERSION
    original_limit = None
    try:
        _validate_request(request)
        version = request['version']
        _bind_parent(request['parent_pid'])
        original_limit = _set_address_limit(request['extra_address_bytes'])
        result = _read(request)
        encoded = json.dumps(result, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        if len(encoded) > request['max_output_bytes']:
            return {'version': version, 'state': 'resource_wait', 'error': 'output_bound_exceeded'}
        return encoded if serialized else result
    except (pa.ArrowMemoryError, MemoryError):
        return {'version': version, 'state': 'resource_wait', 'error': 'worker_memory_exhausted'}
    except OSError as error:
        return {'version': version, 'state': 'resource_wait' if error.errno == errno.ENOMEM else 'local_io_retry',
                'error': 'worker_memory_exhausted' if error.errno == errno.ENOMEM else 'worker_local_io_unavailable'}
    except (_ProofError, ValueError, KeyError, TypeError, pa.ArrowException, json.JSONDecodeError) as error:
        code = str(error) if isinstance(error, _ProofError) else 'worker_source_or_proof_invalid'
        return {'version': version, 'state': 'proof_failure', 'error': code}
    finally:
        # Releasing the soft limit lets the child serialize a small failure
        # response even when the native decoder exhausted its allocation.
        if original_limit is not None:
            resource.setrlimit(resource.RLIMIT_AS, original_limit)


def main():
    raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
    if len(raw) > MAX_REQUEST_BYTES:
        response = {'version': WORKER_VERSION, 'state': 'proof_failure', 'error': 'invalid_request'}
    else:
        try:
            response = execute(json.loads(raw), serialized=True)
        except (UnicodeDecodeError, json.JSONDecodeError):
            response = {'version': WORKER_VERSION, 'state': 'proof_failure', 'error': 'invalid_request'}
    wire = (response if isinstance(response, bytes) else
            json.dumps(response, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
    sys.stdout.buffer.write(wire)
    sys.stdout.buffer.flush()


if __name__ == '__main__':
    main()
