"""Recover interrupted CNYES list checkpoints from their own durable response.

This is local work performed by the existing queue owner, never a second HTTP
collector. A receipt, the original response, its policy and the exact pre-fetch
discovery head must all agree before any page metadata can be committed.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import re
import sqlite3
import time
import uuid
from types import SimpleNamespace

import pyarrow.parquet as pq

from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records, wordpress_base
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, cnyes_bulk_fingerprint


VERSION = 1
HEAD_FIELDS = ('url', 'kind', 'provider', 'due', 'etag', 'last_modified', 'attempts',
               'error_code', 'priority', 'response_sha256', 'parser_fingerprint',
               'last_checked_at_utc', 'superseded_by_windows_json')
HEADER_FIELDS = ('content-type', 'etag', 'last-modified')
MAX_LOCAL_IO_ATTEMPTS = 5
NO_BODY_INTENT_PREFIX = 'cnyes_page_intents/'


def initialize(queue):
    # Must exist before orphan body receipts are committed during startup.
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS cnyes_interrupted_page_checkpoints(
            parquet_path TEXT PRIMARY KEY, receipt_path TEXT NOT NULL,
            request_url TEXT NOT NULL, response_sha256 TEXT NOT NULL,
            witness_json TEXT NOT NULL, state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, not_before REAL NOT NULL DEFAULT 0,
            outcome TEXT
        );
        CREATE INDEX IF NOT EXISTS cnyes_interrupted_page_due
            ON cnyes_interrupted_page_checkpoints(state,not_before,parquet_path);
        CREATE INDEX IF NOT EXISTS cnyes_interrupted_page_url
            ON cnyes_interrupted_page_checkpoints(request_url,state);
        CREATE TABLE IF NOT EXISTS cnyes_page_local_io_incidents(
            parquet_path TEXT NOT NULL, phase TEXT NOT NULL, receipt_path TEXT NOT NULL,
            request_url TEXT NOT NULL, response_sha256 TEXT NOT NULL,
            first_observed_at_utc TEXT NOT NULL, last_observed_at_utc TEXT NOT NULL,
            observations INTEGER NOT NULL, last_error_type TEXT NOT NULL, last_errno INTEGER,
            PRIMARY KEY(parquet_path,phase)
        );
    ''')


def _clock(value):
    if not isinstance(value, str):
        raise ValueError('invalid_cnyes_checkpoint_clock')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError('invalid_cnyes_checkpoint_clock')
    return result.astimezone(timezone.utc)


def witness(task, response, source, captured_at_utc, parser_fingerprint):
    if any(key not in task for key in HEAD_FIELDS) or task['url'] != response.url or response.status != 200:
        return None  # Do not guess a missing head or pre-redirect request identity.
    value = {'version': VERSION, 'head': {key: task[key] for key in HEAD_FIELDS},
             'response_sha256': hashlib.sha256(response.body).hexdigest(),
             'captured_at_utc': captured_at_utc,
             'headers': {key: response.headers.get(key) for key in HEADER_FIELDS},
             'data_fingerprint': source.data_fingerprint,
             'bulk_fingerprint': cnyes_bulk_fingerprint(source),
             'authorization_fingerprint': source.authorization_fingerprint,
             'parser_fingerprint': parser_fingerprint}
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    try:
        parse_witness(encoded)
    except (ValueError, TypeError, KeyError, OverflowError):
        return None  # Keep the body; do not invent an ordering for this page.
    return encoded


def parse_witness(raw):
    if not isinstance(raw, str) or len(raw) > 32768:
        raise ValueError('invalid_cnyes_checkpoint_witness')
    value = json.loads(raw)
    if (not isinstance(value, dict) or set(value) != {'version', 'head', 'response_sha256',
            'captured_at_utc', 'headers', 'data_fingerprint', 'bulk_fingerprint',
            'authorization_fingerprint', 'parser_fingerprint'}
            or type(value['version']) is not int or value['version'] != VERSION
            or not isinstance(value['head'], dict) or set(value['head']) != set(HEAD_FIELDS)
            or not isinstance(value['headers'], dict) or set(value['headers']) != set(HEADER_FIELDS)):
        raise ValueError('invalid_cnyes_checkpoint_witness')
    head = value['head']
    text_keys = set(HEAD_FIELDS) - {'due', 'attempts', 'priority'}
    if (any(v is not None and (not isinstance(v, str) or len(v) > 8192)
            for k, v in head.items() if k in text_keys)
            or head['kind'] != 'cnyes_news' or head['provider'] != 'cnyes'
            or not isinstance(head['url'], str) or 'startAt=' not in head['url']
            or type(head['attempts']) is not int or head['attempts'] < 0
            or type(head['priority']) is not int
            or type(head['due']) not in (int, float) or not math.isfinite(head['due'])
            or any(v is not None and (not isinstance(v, str) or len(v) > 4096)
                   for v in value['headers'].values())
            or any(not isinstance(value[k], str) or not re.fullmatch(r'[0-9a-f]{64}', value[k])
                   for k in ('response_sha256', 'data_fingerprint', 'bulk_fingerprint',
                             'authorization_fingerprint', 'parser_fingerprint'))):
        raise ValueError('invalid_cnyes_checkpoint_witness')
    captured = _clock(value['captured_at_utc'])
    if (head['due'] > captured.timestamp()
            or head['last_checked_at_utc'] is not None and _clock(head['last_checked_at_utc']) >= captured):
        raise ValueError('unproven_cnyes_checkpoint_order')
    return value


def register(queue, records, relative, receipt_relative):
    """Register within the same transaction as the accepted body batch."""
    candidates = [r for r in records if r.get('cnyes_page_checkpoint_witness_json') is not None]
    if not candidates:
        return
    try:
        if len(candidates) != 1:
            raise ValueError('multiple_cnyes_checkpoint_witnesses')
        record = candidates[0]
        raw = record['cnyes_page_checkpoint_witness_json']
        value = parse_witness(raw)
        if (record.get('provider') != 'cnyes' or record.get('source_request_url') != value['head']['url']
                or record.get('response_sha256') != value['response_sha256']
                or record.get('captured_at_utc') != value['captured_at_utc']
                or record.get('authorization_fingerprint') != value['authorization_fingerprint']
                or record.get('data_fingerprint') != value['data_fingerprint']
                or not isinstance(record.get('raw_response_body'), bytes)
                or hashlib.sha256(record['raw_response_body']).hexdigest() != value['response_sha256']):
            raise ValueError('cnyes_checkpoint_record_mismatch')
    except (ValueError, TypeError, KeyError):
        queue.counter('cnyes_interrupted_page_invalid_witness', 1)
        return  # The source body remains independently accepted and auditable.
    queue.db.execute('INSERT OR IGNORE INTO cnyes_interrupted_page_checkpoints '
                     '(parquet_path,receipt_path,request_url,response_sha256,witness_json,state) '
                     "VALUES (?,?,?,?,?,'pending')",
                     (relative, receipt_relative, value['head']['url'], value['response_sha256'], raw))


def _is_no_body_intent(row):
    path = row['parquet_path']
    return (isinstance(path, str) and row['receipt_path'] == path
            and re.fullmatch(r'cnyes_page_intents/[0-9a-f]{32}\.json', path) is not None)


def register_no_body(queue, task, response, source, captured_at_utc, parser_fingerprint):
    """Durably fence a no-new-body source check before retaining its raw page."""
    raw = witness(task, response, source, captured_at_utc, parser_fingerprint)
    if raw is None:
        return None
    path = NO_BODY_INTENT_PREFIX + uuid.uuid4().hex + '.json'
    value = parse_witness(raw)
    with queue.db:
        current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()
        if current is None or any(current[key] != value['head'][key] for key in HEAD_FIELDS):
            return None
        queue.db.execute('INSERT INTO cnyes_interrupted_page_checkpoints '
                         '(parquet_path,receipt_path,request_url,response_sha256,witness_json,state) '
                         "VALUES (?,?,?,?,?,'pending')", (path, path, task['url'], value['response_sha256'], raw))
    # A crash before this write leaves a retryable marker, never a fabricated
    # source observation. A crash after it can validate the separately
    # retained exact-request raw envelope without writing that response twice.
    from downloader.artifact_io import atomic_write_text
    atomic_write_text(queue.root / path, raw, durable=True, mode=0o600)
    return path


def cleanup_no_body_intent(queue, relative):
    """Remove only our temporary witness after its page transaction commits."""
    if re.fullmatch(r'cnyes_page_intents/[0-9a-f]{32}\.json', relative or '') is None:
        return
    try:
        (queue.root / relative).unlink(missing_ok=True)
    except OSError:
        queue.metrics.count('cnyes', 'page_witness_cleanup_failures')


def finish(queue, relative):
    # Called only inside the successful live page metadata transaction.
    if not relative:
        return
    queue.db.execute("DELETE FROM cnyes_interrupted_page_checkpoints WHERE parquet_path=? AND state='pending'", (relative,))


def _record_local_io_incident(queue, row, error, phase):
    # This clock is the local failure observation, never the source clock.
    # Keep type and errno without persisting potentially sensitive error text.
    observed = datetime.now(timezone.utc).isoformat()
    errno = getattr(error, 'errno', None)
    errno = errno if type(errno) is int else None
    queue.db.execute('''INSERT INTO cnyes_page_local_io_incidents
        (parquet_path,phase,receipt_path,request_url,response_sha256,first_observed_at_utc,
         last_observed_at_utc,observations,last_error_type,last_errno)
        VALUES (?,?,?,?,?,?,?,1,?,?) ON CONFLICT(parquet_path,phase) DO UPDATE SET
        last_observed_at_utc=excluded.last_observed_at_utc,
        observations=observations+1,last_error_type=excluded.last_error_type,last_errno=excluded.last_errno''',
        (row['parquet_path'], phase, row['receipt_path'], row['request_url'], row['response_sha256'],
         observed, observed, type(error).__name__, errno))


def defer_live_io_failure(queue, task, response, error):
    """Keep the exact pre-fetch head when an accepted page can replay locally.

    This is only called for an I/O exception after a successful fetch. A
    committed receipt, a durable receipt awaiting adoption, or a no-body
    witness intent must match the response and head. An HTTP failure,
    another response, or any newer discovery head keeps normal failure
    handling. Full immutable-source verification still happens in ``recover``
    before an orphan is adopted or the page can be advanced.
    """
    if task.get('kind') == 'index':
        from downloader.news_fulltext_cnyes_index_checkpoint import defer_live_failure
        return defer_live_failure(queue, task, response, error)
    if (not isinstance(error, OSError) or task.get('kind') != 'cnyes_news'
            or task.get('provider') != 'cnyes' or not isinstance(response, Response)
            or response.status != 200 or response.url != task.get('url')
            or not isinstance(response.body, bytes)):
        return False
    source = queue.registry.sources.get('cnyes')
    if source is None or not source.allowed or wordpress_base(response.url) not in source.cnyes_apis:
        return False
    from downloader.news_fulltext import discovery_fingerprint
    digest = hashlib.sha256(response.body).hexdigest()
    rows = queue.db.execute(
        "SELECT c.* FROM cnyes_interrupted_page_checkpoints c LEFT JOIN accepted_batches b "
        "ON b.path=c.parquet_path AND b.receipt_path=c.receipt_path "
        "WHERE c.request_url=? AND c.response_sha256=? AND c.state='pending' "
        "AND (b.path IS NOT NULL OR c.outcome='acceptance_receipt_pending' "
        "OR c.parquet_path LIKE 'cnyes_page_intents/%')",
        (task['url'], digest))
    for row in rows:
        if row['parquet_path'].startswith(NO_BODY_INTENT_PREFIX) and not _is_no_body_intent(row):
            continue
        try:
            value = parse_witness(row['witness_json'])
            expected = {key: task[key] for key in HEAD_FIELDS}
        except (ValueError, TypeError, KeyError, OverflowError):
            continue
        if (value['head'] != expected or value['response_sha256'] != digest
                or value['data_fingerprint'] != source.data_fingerprint
                or value['bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
                or value['authorization_fingerprint'] != source.authorization_fingerprint
                or value['parser_fingerprint'] != discovery_fingerprint(source, kind='cnyes_news')):
            continue
        with queue.db:
            current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()
            if current is None or any(current[key] != expected[key] for key in HEAD_FIELDS):
                return False
            changed = queue.db.execute(
                "UPDATE cnyes_interrupted_page_checkpoints SET outcome=? "
                "WHERE parquet_path=? AND state='pending' AND witness_json=?",
                ('live_commit_' + type(error).__name__, row['parquet_path'], row['witness_json'])).rowcount
            if changed:
                _record_local_io_incident(queue, row, error, 'live_commit')
                queue.counter('cnyes_page_live_commit_io_failures', 1)
        if changed:
            queue.metrics.count('cnyes', 'page_live_commit_io_failures')
            return True
    return False


def _read_no_body_original(queue, row, value, source):
    """Verify a capture witness and reuse an exact, independently sealed raw page."""
    from downloader.news_fulltext_bulk import cnyes_scope, wordpress_page
    from downloader.news_fulltext_cnyes_history import (
        CONTRACT_VERSION, _checked_page_observation, _original_envelope_file_signature,
        _read_accepted_batch_original, _read_original_envelope_identity, _retained_envelopes,
    )
    from downloader.news_fulltext_queue import replay_source_path

    if not _is_no_body_intent(row):
        raise ValueError('invalid_no_body_checkpoint_intent')
    witness_path = replay_source_path(queue.root, row['parquet_path'])
    witness_before = _original_envelope_file_signature(witness_path)
    if witness_before[2] > 32768 or witness_path.read_text(encoding='utf-8') != row['witness_json']:
        raise ValueError('no_body_checkpoint_witness_mismatch')
    request, sha = row['request_url'], row['response_sha256']
    prior = queue.db.execute('SELECT * FROM cnyes_history_page_observations '
        'WHERE scope_url=? AND page=? AND response_sha256=? AND evidence_contract_version=? '
        'AND source_bulk_fingerprint=? AND authorization_fingerprint=?',
        (cnyes_scope(request), wordpress_page(request), sha, CONTRACT_VERSION,
         cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
    observation = _checked_page_observation(prior, source)
    candidates = []
    if observation and observation['request_url'] == request:
        artifact = observation['artifact']
        if artifact.get('kind') == 'accepted_body_batch':
            matched = _read_accepted_batch_original(queue, artifact, source, request, sha)
            if matched:
                if _original_envelope_file_signature(witness_path) != witness_before:
                    raise OSError('no_body_checkpoint_witness_changed')
                return matched['original']['raw_response_body'], artifact
        elif (artifact.get('kind') in ('canonical_quarantine', 'cnyes_history_source_envelope')
              and isinstance(artifact.get('parquet_path'), str)
              and isinstance(artifact.get('receipt_path'), str)):
            # A previously verified observation may refer to an older layout.
            # It still needs the same complete source and receipt verification.
            candidates.append(artifact)
    for path, kind in _retained_envelopes(queue.root, sha, request, source.authorization_fingerprint):
        relative = str(path.relative_to(queue.root))
        candidates.append({'kind': kind, 'parquet_path': relative, 'receipt_path': relative + '.receipt.json'})
    saw_invalid = False
    for artifact in candidates:
        relative = artifact['parquet_path']
        try:
            path = replay_source_path(queue.root, relative)
            receipt_path = replay_source_path(queue.root, artifact['receipt_path'])
            before = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)
            receipt_bytes = receipt_path.read_bytes()
            receipt = json.loads(receipt_bytes)
            verified = _read_original_envelope_identity(path, receipt_path, sha,
                expected_parquet_path=relative, expected_kind=artifact['kind'])
            if (receipt.get('parquet_path') != relative or verified is None
                    or verified[0] != (request, source.authorization_fingerprint)):
                saw_invalid = True
                continue
            records = pq.read_table(path, columns=['raw_response_body', 'captured_at_utc']).to_pylist()
            if len(records) != 1 or _clock(records[0]['captured_at_utc']) > datetime.now(timezone.utc):
                saw_invalid = True
                continue
            raw = records[0]['raw_response_body']
            raw = raw.encode() if isinstance(raw, str) else raw
            if (not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != sha
                    or before != (_original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path))
                    or receipt_path.read_bytes() != receipt_bytes
                    or _original_envelope_file_signature(witness_path) != witness_before):
                raise OSError('no_body_checkpoint_original_changed')
            return raw, artifact
        except FileNotFoundError:
            continue
        except (ValueError, KeyError, TypeError, OverflowError):
            saw_invalid = True
    if saw_invalid:
        raise ValueError('no_body_checkpoint_original_invalid')
    raise FileNotFoundError('no_body_checkpoint_original_unavailable')


def _read_original(queue, row, value, source, *, witness_field='cnyes_page_checkpoint_witness_json'):
    """Verify receipt, bytes and the witness recorded in the immutable batch."""
    from downloader.news_fulltext_cnyes_history import _original_envelope_file_signature
    from downloader.news_fulltext_queue import (acceptance_receipt_input_from_bytes, replay_source_path,
                                               verify_immutable_body_batch)

    committed = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?',
                                 (row['parquet_path'],)).fetchone()
    if committed and committed[0] != row['receipt_path']:
        raise ValueError('checkpoint_batch_not_committed')
    path = replay_source_path(queue.root, row['parquet_path'])
    receipt_path = replay_source_path(queue.root, row['receipt_path'])
    before = _original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path)
    if before[1][2] > 65536:
        raise ValueError('checkpoint_receipt_too_large')
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    batch = verify_immutable_body_batch(queue.root, receipt, expected_parquet_path=row['parquet_path'],
                                        include_references=False)
    matches = [record for record in batch['records'] if record.get(witness_field) is not None]
    if len(matches) != 1:
        raise ValueError('checkpoint_original_witness_count')
    original = matches[0]
    if (original[witness_field] != row['witness_json']
            or original.get('provider') != source.provider
            or original.get('source_request_url') != row['request_url']
            or original.get('response_sha256') != row['response_sha256']
            or original.get('captured_at_utc') != value['captured_at_utc']
            or original.get('authorization_fingerprint') != source.authorization_fingerprint
            or original.get('data_fingerprint') != source.data_fingerprint
            or not isinstance(original.get('raw_response_body'), bytes)
            or hashlib.sha256(original['raw_response_body']).hexdigest() != row['response_sha256']):
        raise ValueError('checkpoint_original_identity_mismatch')
    if (before != (_original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path))
            or receipt_path.read_bytes() != receipt_bytes):
        raise OSError('checkpoint_original_changed_during_read')
    if not committed:
        # Use this already checked, exact receipt rather than a directory scan.
        # The canonical receipt recovery checks its hash and article fences
        # again and registers the accepted batch in one transaction.
        queue._recover_acceptance_receipt(
            receipt_path, row['receipt_path'], set(),
            receipt_text=acceptance_receipt_input_from_bytes(receipt_bytes))
        committed = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?',
                                     (row['parquet_path'],)).fetchone()
        if not committed or committed[0] != row['receipt_path']:
            raise ValueError('checkpoint_batch_not_committed')
        if (before != (_original_envelope_file_signature(path), _original_envelope_file_signature(receipt_path))
                or receipt_path.read_bytes() != receipt_bytes):
            raise OSError('checkpoint_original_changed_during_read')
    return original


def _recover_one(queue, row):
    from downloader.news_fulltext import apply_cnyes_page_metadata, discovery_fingerprint

    from downloader.news_fulltext_cnyes_index_checkpoint import is_index_intent, recover_one
    if is_index_intent(row):
        return recover_one(queue, row)

    value = parse_witness(row['witness_json'])
    head, request = value['head'], value['head']['url']
    if request != row['request_url'] or value['response_sha256'] != row['response_sha256']:
        raise ValueError('checkpoint_identity_mismatch')
    source = queue.registry.sources.get('cnyes')
    if (source is None or not source.allowed or wordpress_base(request) not in source.cnyes_apis
            or source.data_fingerprint != value['data_fingerprint']
            or cnyes_bulk_fingerprint(source) != value['bulk_fingerprint']
            or source.authorization_fingerprint != value['authorization_fingerprint']
            or discovery_fingerprint(source, kind='cnyes_news') != value['parser_fingerprint']):
        return 'policy_changed'
    current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (request,)).fetchone()
    if current is None or any(current[key] != head[key] for key in HEAD_FIELDS):
        return 'head_superseded'
    if _is_no_body_intent(row):
        raw, artifact = _read_no_body_original(queue, row, value, source)
        captured_at_utc = value['captured_at_utc']
    else:
        artifact = {'kind': 'accepted_body_batch', 'parquet_path': row['parquet_path'],
                    'receipt_path': row['receipt_path']}
        original = _read_original(queue, row, value, source)
        raw, captured_at_utc = original['raw_response_body'], original['captured_at_utc']
    if captured_at_utc != value['captured_at_utc'] or _clock(captured_at_utc) > datetime.now(timezone.utc):
        raise ValueError('checkpoint_original_clock_mismatch')
    response = Response(request, 200, {k: v for k, v in value['headers'].items() if v is not None},
                        raw)
    info, posts = cnyes_pagination(response)
    records, errors = cnyes_records(response, SimpleNamespace(registry=queue.registry), source)
    if (len(posts) != len(records) + len(errors)
            or any(type(post.get('payment')) is not int or post['payment'] != 0 for post in posts)):
        raise ValueError('checkpoint_unusable_or_restricted_page')
    previous = queue.db.execute('SELECT page FROM bulk_page_evidence WHERE scope_url=? AND total=? '
                                'AND ids_sha256=? AND page!=?',
                                (info['scope'], info['total'], info['ids_sha256'], info['page'])).fetchone()
    if posts and previous:
        raise ValueError('checkpoint_repeated_page')
    observed = _clock(value['captured_at_utc'])
    coverage = queue.db.execute('SELECT observed_at_utc FROM bulk_api_coverage WHERE base_url=?',
                                (info['scope'],)).fetchone()
    if coverage and _clock(coverage[0]) > observed:
        return 'newer_scope_observation'
    # The single owner commits page evidence, next page and checkpoint outcome
    # together. A second crash rolls all of them back for the next local replay.
    with queue.db:
        current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (request,)).fetchone()
        if current is None or any(current[key] != head[key] for key in HEAD_FIELDS):
            return 'head_superseded'
        if (_is_no_body_intent(row) and not records and errors
                and all(e['error'] in {'bulk_body_not_provided', 'bulk_exact_duplicate_row'} for e in errors)):
            # The crash may precede normal link registration. Admit only
            # missing canonical HTML work; never overwrite a newer URL head.
            links = [e for e in errors if e['url'] and e['error'] == 'bulk_body_not_provided']
            if links:
                placeholders = ','.join('?' for _ in links)
                existing = {r[0] for r in queue.db.execute('SELECT url FROM articles WHERE url IN (' + placeholders + ')',
                                                           [e['url'] for e in links])}
                missing = [{'url': e['url'], 'title': e.get('title', ''), 'date': e.get('metadata_date_raw', '')}
                           for e in links if e['url'] not in existing]
                if missing:
                    queue.add_articles(missing)
        apply_cnyes_page_metadata(queue, head, response, source, info, posts, artifact,
                                  observed_at=value['captured_at_utc'])
        queue.db.execute('DELETE FROM bulk_item_errors WHERE request_url=?', (request,))
        queue.db.executemany('INSERT INTO bulk_item_errors VALUES (?,?,?,?,?,?)',
            ((request, e['item_number'], source.provider, e['url'], e['error'], value['captured_at_utc']) for e in errors))
        queue.db.execute('UPDATE discovery SET due=?,etag=coalesce(?,etag),last_modified=coalesce(?,last_modified),'
                         'response_sha256=?,parser_fingerprint=?,last_checked_at_utc=?,attempts=0,error_code=NULL WHERE url=?',
                         (observed.timestamp() + 365 * 86400, value['headers']['etag'], value['headers']['last-modified'],
                          value['response_sha256'], value['parser_fingerprint'], value['captured_at_utc'], request))
        if (_is_no_body_intent(row) and not records and errors
                and all(e['error'] in {'bulk_body_not_provided', 'bulk_exact_duplicate_row'} for e in errors)):
            from downloader.news_fulltext_replay import checkpoint_no_body_cnyes_replay
            checkpoint_no_body_cnyes_replay(queue, source, value['response_sha256'])
        queue.db.execute("DELETE FROM cnyes_interrupted_page_checkpoints WHERE parquet_path=? AND state='pending'",
                         (row['parquet_path'],))
    if _is_no_body_intent(row):
        cleanup_no_body_intent(queue, row['parquet_path'])
    return 'recovered'


def recover(queue, *, limit=4, max_seconds=.05, stop=None):
    """Bound local recovery; invalid evidence never becomes a success."""
    if type(limit) is not int or not 1 <= limit <= 128 or not 0 < max_seconds <= 10:
        raise ValueError('invalid_cnyes_checkpoint_recovery_budget')
    start, outcomes = time.monotonic(), []
    while len(outcomes) < limit and (not outcomes or time.monotonic() - start < max_seconds):
        if stop is not None and stop.is_set():
            break
        row = queue.db.execute("SELECT * FROM cnyes_interrupted_page_checkpoints "
                               "WHERE state='pending' AND not_before<=? ORDER BY not_before,parquet_path LIMIT 1",
                               (time.time(),)).fetchone()
        if row is None:
            break
        try:
            outcome = _recover_one(queue, row)
        except (OSError, sqlite3.Error) as error:
            # Retry transient local I/O independently of the publisher's
            # unchanged cooldown; after a bounded number of tries release the
            # exact page for its original source schedule. The immutable body
            # and failed recovery outcome are never removed.
            from downloader.news_fulltext_cnyes_index_checkpoint import is_index_intent
            attempts = row['attempts'] + 1
            with queue.db:
                _record_local_io_incident(queue, row, error, 'local_replay')
                queue.counter('cnyes_page_local_replay_io_failures', 1)
                if isinstance(error, FileNotFoundError) and is_index_intent(row):
                    outcome = 'original_missing'
                    queue.db.execute('UPDATE cnyes_interrupted_page_checkpoints SET attempts=? WHERE parquet_path=?',
                                     (attempts, row['parquet_path']))
                elif attempts < MAX_LOCAL_IO_ATTEMPTS:
                    queue.db.execute('UPDATE cnyes_interrupted_page_checkpoints SET attempts=?,not_before=?,outcome=? '
                                     'WHERE parquet_path=? AND state=\'pending\'',
                                     (attempts, time.time() + 2 ** attempts, 'local_io_retry', row['parquet_path']))
                    outcome = 'local_io_retry'
                else:
                    outcome = 'original_missing' if isinstance(error, FileNotFoundError) else 'local_io_exhausted'
                    queue.db.execute('UPDATE cnyes_interrupted_page_checkpoints SET attempts=? WHERE parquet_path=?',
                                     (attempts, row['parquet_path']))
        except (ValueError, TypeError, KeyError, OverflowError, NewsError):
            outcome = 'invalid_original'
        if outcome not in {'recovered', 'local_io_retry'}:
            with queue.db:
                queue.db.execute("UPDATE cnyes_interrupted_page_checkpoints SET state='skipped',outcome=? "
                                 "WHERE parquet_path=? AND state='pending'", (outcome, row['parquet_path']))
        outcomes.append(outcome)
    pending = queue.db.execute("SELECT min(not_before) FROM cnyes_interrupted_page_checkpoints WHERE state='pending'").fetchone()[0]
    return {'outcomes': outcomes, 'next_local_retry_epoch': pending,
            'ready': pending is not None and pending <= time.time()}
