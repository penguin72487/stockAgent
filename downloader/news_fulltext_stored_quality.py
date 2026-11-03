"""Explicit quality review of sealed bodies through the existing repair inbox.

No HTTP, new body, lease, source clock or immutable payload is produced here.
The resident queue writer repeats the native validation before changing only
the head's outcome. The existing repair ledger records the separate review
clock and makes commit/archive recovery idempotent.
"""
from contextlib import closing
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.news_fulltext_policy import NewsError, extract_article, normalize_url

VERSION = 8
KIND = 'reviewed_stored_body_quality'
HEAD_FIELDS = ('id', 'url', 'provider', 'state', 'error_code', 'attempts', 'due',
               'lease_owner', 'lease_until', 'body_sha256', 'body_bytes', 'parquet_path',
               'captured_at_utc', 'last_checked_at_utc', 'authorization_fingerprint',
               'etag', 'last_modified', 'validator_url')
ERRORS = frozenset({'paywall_or_login', 'article_rights_or_scope_excluded',
                    'article_identity_not_verified', 'article_attribution_not_verified',
                    'article_title_not_verified', 'body_not_found'})
MAX_TARGETS = 8
MAX_BATCH_BYTES = 16 << 20
MAX_DECODED_BYTES = 32 << 20
ORIGINAL_COLUMNS = ('provider', 'requested_url', 'source_response_article_id', 'source_request_url',
                    'final_url', 'captured_at_utc', 'authorization_fingerprint', 'raw_response_body',
                    'response_sha256', 'http_response_bytes', 'body', 'body_bytes')


def _head(row):
    return {key: bytes(row[key]).hex() if key == 'id' else row[key] for key in HEAD_FIELDS}


def _eligible(row):
    if row is None or row['state'] != 'complete' or row['error_code'] is not None:
        raise ValueError('quality_review_requires_complete_head')
    if row['lease_owner'] is not None or row['lease_until'] is not None:
        raise ValueError('quality_review_leased_head')
    if (not isinstance(row['body_sha256'], str) or not re.fullmatch('[0-9a-f]{64}', row['body_sha256'])
            or type(row['body_bytes']) is not int or row['body_bytes'] <= 0
            or not isinstance(row['parquet_path'], str) or not row['parquet_path']
            or type(row['attempts']) is not int or row['attempts'] < 0
            or type(row['due']) not in (int, float) or not math.isfinite(row['due']) or row['due'] < 0):
        raise ValueError('quality_review_invalid_saved_head')
    from downloader.news_fulltext_repairs import _clock
    if any(_clock(row[key]) > time.time() for key in ('captured_at_utc', 'last_checked_at_utc')):
        raise ValueError('quality_review_future_observation')


def _original(root, db, row):
    """Bind one exact head to its accepted, bounded, unmodified raw record."""
    from downloader.news_fulltext_queue import source_path
    from downloader.news_fulltext_repairs import _unique
    accepted = db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?',
                          (row['parquet_path'],)).fetchone()
    if accepted is None:
        raise ValueError('quality_review_batch_not_accepted')
    if not (root / accepted[0]).exists() and not (root / accepted[0]).is_symlink():
        raise FileNotFoundError('quality_review_receipt_temporarily_unavailable')
    receipt_path = source_path(root, accepted[0])
    if receipt_path.stat().st_size > 65536:
        raise ValueError('quality_review_receipt_unbounded')
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes, object_pairs_hook=_unique)
    if not (root / row['parquet_path']).exists() and not (root / row['parquet_path']).is_symlink():
        raise FileNotFoundError('quality_review_batch_temporarily_unavailable')
    path = source_path(root, row['parquet_path'])
    if path.stat().st_size > MAX_BATCH_BYTES:
        raise ValueError('quality_review_batch_unbounded')
    with path.open('rb') as stream:
        payload = stream.read(MAX_BATCH_BYTES + 1)
    digest = hashlib.sha256(payload).hexdigest()
    if (len(payload) > MAX_BATCH_BYTES or receipt.get('parquet_path') != row['parquet_path']
            or receipt.get('contract_version') != 1 or receipt.get('sha256') != digest):
        raise ValueError('quality_review_batch_digest_mismatch')
    batch = pq.ParquetFile(pa.BufferReader(payload))
    if (not 1 <= batch.metadata.num_rows <= 128
            or receipt.get('rows') != batch.metadata.num_rows
            or sum(batch.metadata.row_group(i).total_byte_size for i in range(batch.num_row_groups)) > MAX_DECODED_BYTES):
        raise ValueError('quality_review_batch_grain_or_memory_bound')
    metadata = batch.read(columns=['article_id', 'body_sha256'], use_threads=False).to_pylist()
    positions = [i for i, record in enumerate(metadata)
                 if record['article_id'] == row['id'].hex() and record['body_sha256'] == row['body_sha256']]
    if len(positions) != 1:
        raise ValueError('quality_review_saved_head_not_unique')
    # Arrow projects the witness fields; only the selected row becomes Python
    # strings/bytes. Other bodies, authors and license JSON are not materialized.
    record = batch.read(columns=list(ORIGINAL_COLUMNS), use_threads=False).slice(positions[0], 1).to_pylist()[0]
    raw, body = record.get('raw_response_body'), record.get('body')
    if (record.get('provider') != row['provider'] or record.get('requested_url') != row['url']
            or record.get('source_response_article_id') != row['id'].hex()
            or record.get('source_request_url') != row['url'] or record.get('final_url') != row['url']
            or record.get('captured_at_utc') != row['captured_at_utc']
            or record.get('authorization_fingerprint') != row['authorization_fingerprint']
            or not isinstance(raw, bytes) or not 0 < len(raw) <= 2 << 20
            or hashlib.sha256(raw).hexdigest() != record.get('response_sha256')
            or len(raw) != record.get('http_response_bytes')
            or not isinstance(body, str) or hashlib.sha256(body.encode()).hexdigest() != row['body_sha256']
            or len(body.encode()) != row['body_bytes'] or record.get('body_bytes') != row['body_bytes']):
        raise ValueError('quality_review_original_head_binding_mismatch')
    return record, {'parquet_path': row['parquet_path'], 'parquet_sha256': digest,
        'receipt_path': accepted[0], 'receipt_sha256': hashlib.sha256(receipt_bytes).hexdigest(),
        'response_sha256': record['response_sha256'], 'source_observed_at_utc': record['captured_at_utc']}


def _native_error(source, row, record):
    # Historical batches lack status/MIME: do not invent a Response. This is
    # a native document-quality check, not a new transport observation.
    prefix = record['raw_response_body'].lstrip()[:256].lower()
    if not prefix.startswith((b'<!doctype html', b'<html')):
        raise ValueError('quality_review_requires_retained_html_document')
    try:
        extract_article(record['raw_response_body'], source, page_url=row['url'])
    except NewsError as error:
        if error.code in ERRORS:
            return error.code
        raise ValueError('quality_review_not_a_supported_semantic_failure') from error
    raise ValueError('quality_review_current_native_validation_succeeded')


def prepare_stored_quality_reviews(root, registry, urls, *, reason, expected_error):
    """Build the exact bounded owner request without writing an inbox or DB.

    Every head and original is validated as in submission. The owner must still
    repeat that validation at admission; this is not a queue mutation or an
    acceptance of the retained body as full text.
    """
    from itertools import islice
    from downloader.news_fulltext_queue import utc_now
    from downloader.news_fulltext_repairs import _bytes, _source, MAX_REQUEST_BYTES
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 240 or expected_error not in ERRORS:
        raise ValueError('quality_review_requires_reason_and_expected_semantic_error')
    selected = list(islice(urls, MAX_TARGETS + 1))
    if not 1 <= len(selected) <= MAX_TARGETS:
        raise ValueError('quality_review_requires_one_to_eight_urls')
    selected = list(dict.fromkeys(normalize_url(url) for url in selected))
    root = Path(root).resolve()
    targets, sources, originals = [], {}, {}
    with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        for url in selected:
            row = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
            _eligible(row)
            source, identity = _source(db, registry, url)
            record, witness = _original(root, db, row)
            if source.provider != row['provider'] or _native_error(source, row, record) != expected_error:
                raise ValueError('quality_review_expected_outcome_not_reproduced')
            targets.append(_head(row))
            originals[row['id'].hex()] = witness
            sources[source.provider] = identity
    request = {'contract_version': VERSION, 'kind': KIND, 'created_at_utc': utc_now(),
        'reason': reason.strip(), 'targets': targets, 'sources': sources,
        'originals': originals, 'expected_error': expected_error}
    validate_request(request)
    payload = _bytes(request)
    if len(payload) > MAX_REQUEST_BYTES:
        raise ValueError('quality_review_request_unbounded')
    return {'request': request, 'payload': payload, 'request_id': hashlib.sha256(payload).hexdigest(),
            'publisher_requests': 0, 'queue_mutations': False, 'submitted': False}


def submit_stored_quality_reviews(root, registry, urls, *, reason, expected_error):
    """Submit a bounded explicit review; never mutate SQLite from this caller."""
    from downloader.news_fulltext_repairs import _directory, _pending, _write_once, MAX_PENDING
    prepared = prepare_stored_quality_reviews(root, registry, urls, reason=reason, expected_error=expected_error)
    root = Path(root).resolve()
    directory = _directory(root, 'pending')
    if len(_pending(directory)) >= MAX_PENDING:
        raise ValueError('repair_inbox_full')
    digest, payload = prepared['request_id'], prepared['payload']
    path = directory / (digest + '.json')
    _write_once(path, payload)
    return {'contract_version': VERSION, 'submitted_targets': len(prepared['request']['targets']), 'request_id': digest,
        'request_path': str(path), 'publisher_requests': 0, 'queue_mutations': False,
        'accepted_as_bodies': False}


def validate_request(request):
    from downloader.news_fulltext_repairs import _clock, SOURCE_FIELDS
    fields = {'contract_version', 'kind', 'created_at_utc', 'reason', 'targets', 'sources', 'originals', 'expected_error'}
    if (set(request) != fields or type(request['contract_version']) is not int or request['contract_version'] != VERSION
            or request['kind'] != KIND or not isinstance(request['reason'], str) or not request['reason'].strip()
            or len(request['reason']) > 240 or request['expected_error'] not in ERRORS
            or _clock(request['created_at_utc']) > time.time()
            or not isinstance(request['targets'], list) or not 1 <= len(request['targets']) <= MAX_TARGETS
            or not isinstance(request['sources'], dict) or not isinstance(request['originals'], dict)):
        raise ValueError('invalid_stored_quality_request')
    seen = set()
    for target in request['targets']:
        if (not isinstance(target, dict) or set(target) != set(HEAD_FIELDS)
                or not isinstance(target['id'], str) or target['id'] in seen
                or target['id'] != hashlib.sha256(normalize_url(target['url']).encode()).hexdigest()
                or normalize_url(target['url']) != target['url'] or not isinstance(target['provider'], str)):
            raise ValueError('invalid_stored_quality_target')
        _eligible(target)
        if any(_clock(target[key]) > _clock(request['created_at_utc'])
               for key in ('captured_at_utc', 'last_checked_at_utc')):
            raise ValueError('invalid_stored_quality_target_clock')
        seen.add(target['id'])
        witness = request['originals'].get(target['id'])
        if (not isinstance(witness, dict) or set(witness) != {
                'parquet_path', 'parquet_sha256', 'receipt_path', 'receipt_sha256',
                'response_sha256', 'source_observed_at_utc'}
                or witness['parquet_path'] != target['parquet_path']
                or witness['source_observed_at_utc'] != target['captured_at_utc']
                or any(not isinstance(witness[key], str) or not re.fullmatch('[0-9a-f]{64}', witness[key])
                       for key in ('parquet_sha256', 'receipt_sha256', 'response_sha256'))
                or not isinstance(witness['receipt_path'], str)):
            raise ValueError('invalid_stored_quality_original')
    if set(request['originals']) != seen or set(request['sources']) != {t['provider'] for t in request['targets']}:
        raise ValueError('invalid_stored_quality_sources')
    for identity in request['sources'].values():
        if (not isinstance(identity, dict) or set(identity) != SOURCE_FIELDS
                or type(identity['contract_version']) is not int or identity['contract_version'] != 1
                or any(not isinstance(identity[key], str) or not re.fullmatch('[0-9a-f]{64}', identity[key])
                       for key in SOURCE_FIELDS - {'contract_version'})):
            raise ValueError('invalid_stored_quality_source_identity')
    return request


def admit(queue, request, target):
    from downloader.news_fulltext_repairs import _source
    row = queue.db.execute('SELECT * FROM articles WHERE id=?', (bytes.fromhex(target['id']),)).fetchone()
    if row is None or _head(row) != target:
        return 'quality_review_head_changed'
    try:
        _eligible(row)
        source, identity = _source(queue.db, queue.registry, row['url'])
        if source.provider != row['provider'] or identity != request['sources'][row['provider']]:
            return 'quality_review_source_changed'
        record, witness = _original(queue.root, queue.db, row)
        if witness != request['originals'][target['id']]:
            return 'quality_review_original_changed'
        error = _native_error(source, row, record)
        if error != request['expected_error']:
            return 'quality_review_outcome_changed'
    except (ValueError, NewsError, TypeError, KeyError, OverflowError, RecursionError, pa.ArrowException):
        return 'quality_review_evidence_invalid'
    # The existing inbox transaction commits this and its ledger item together.
    # Keep the body/ref, HTTP attempts, deadlines, source clocks and validators.
    queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE id=?', (error, error, row['id']))
    return 'stored_quality_reclassified'
