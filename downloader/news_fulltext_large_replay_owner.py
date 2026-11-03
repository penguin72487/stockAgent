"""Schedule an isolated retained-source proof through the existing news owner.

The subprocess has no SQLite/HTTP access. Only this owner can checkpoint or
accept a result, after checking its receipt, file identity and current head.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import time
from urllib.parse import urlsplit

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_large_replay import (
    LargeReplayJob, UNKNOWN_PROGRESS_INITIAL_SECONDS, UNKNOWN_PROGRESS_MAX_SECONDS,
)
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import replay_source_path, utc_now


INITIAL_ADDRESS_BYTES = 256 << 20
INITIAL_OUTPUT_BYTES = 32 << 20
MAX_ADDRESS_BYTES = 1 << 40
MAX_OUTPUT_BYTES = 128 << 20
MAX_BATCH_HEADS = 2
RETRY_MAX_SECONDS = 300
MAX_RECEIPT_BYTES = 1 << 20


def _unknown_progress_timeout(last_error, attempts):
    """Give repeated unobservable readers more time without losing liveness."""
    if last_error != 'worker_progress_unknown_timeout':
        return UNKNOWN_PROGRESS_INITIAL_SECONDS
    exponent = min(max(attempts, 0), 5) if type(attempts) is int else 0
    return min(UNKNOWN_PROGRESS_MAX_SECONDS, UNKNOWN_PROGRESS_INITIAL_SECONDS * (1 << exponent))


def initialize(db):
    # Retry is keyed by head, not batch: one unusually large response must not
    # prevent the remaining heads sharing that source file from progressing.
    db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_large_retries(
        source_fingerprint TEXT NOT NULL, article_id BLOB NOT NULL, original_path TEXT NOT NULL,
        attempts INTEGER NOT NULL, not_before_epoch REAL NOT NULL, extra_address_bytes INTEGER NOT NULL,
        max_output_bytes INTEGER NOT NULL, last_error TEXT NOT NULL, updated_at_utc TEXT NOT NULL,
        PRIMARY KEY(source_fingerprint,article_id)) WITHOUT ROWID''')
    db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_large_retry_history(
        history_id INTEGER PRIMARY KEY, source_fingerprint TEXT NOT NULL, article_id BLOB NOT NULL,
        original_path TEXT NOT NULL, attempts INTEGER NOT NULL, extra_address_bytes INTEGER NOT NULL,
        max_output_bytes INTEGER NOT NULL, error TEXT NOT NULL, observed_at_utc TEXT NOT NULL)''')


def _candidates(queue, fingerprint, maximum_bytes, *, targets=None, ready=True,
                path=None, exclude=None, fresh=False):
    where_targets = '' if targets is None else ' AND r.article_id IN (' + ','.join('?' for _ in targets) + ')'
    deadline = ' AND (t.not_before_epoch IS NULL OR t.not_before_epoch<=?)' if ready else ''
    same_path = ' AND r.original_path=?' if path is not None else ''
    other_head = ' AND r.article_id!=?' if exclude is not None else ''
    fresh_only = ' AND t.attempts IS NULL' if fresh else ''
    return queue.db.execute('''SELECT r.article_id,r.original_path,r.original_body_sha256,
            w.required_bytes,t.not_before_epoch,t.extra_address_bytes,t.max_output_bytes,
            t.attempts AS retry_attempts,t.last_error AS retry_last_error
        FROM retained_metadata_resource_waits w
        CROSS JOIN retained_metadata_replays r INDEXED BY retained_metadata_ready
        CROSS JOIN articles a
        LEFT JOIN retained_metadata_large_retries t ON t.source_fingerprint=r.source_fingerprint
            AND t.article_id=r.article_id AND t.original_path=r.original_path
        WHERE w.source_fingerprint=? AND w.required_bytes>?
            AND w.phase IN ('compressed_file','arrow_uncompressed')
            AND r.source_fingerprint=w.source_fingerprint AND r.original_path=w.original_path
            AND (r.state='resource_wait' OR (r.state='failed_validation'
                AND r.error_code='retained_metadata_batch_exceeds_resource_bound'))
            AND a.id=r.article_id AND a.parquet_path=r.original_path AND a.body_sha256=r.original_body_sha256
            AND a.state='complete' AND a.lease_owner IS NULL AND a.lease_until IS NULL'''
        + deadline + same_path + other_head + fresh_only + where_targets
        + ' ORDER BY r.original_path,r.article_id LIMIT 1',
        (fingerprint, maximum_bytes, *((time.time(),) if ready else ()),
         *((path,) if path is not None else ()), *((exclude,) if exclude is not None else ()),
         *(targets or ()))).fetchone()


def next_wake_delay(queue, fingerprint, maximum, bound):
    if getattr(queue, '_large_metadata_job', None):
        return min(maximum, 1.0)
    if _candidates(queue, fingerprint, bound):
        return min(maximum, 10.0)
    # Inspect all retry deadlines, rather than just the first path: its retry
    # must not make another ready path sleep longer.
    row = queue.db.execute('''SELECT min(t.not_before_epoch) FROM retained_metadata_large_retries t
        JOIN retained_metadata_resource_waits w ON w.source_fingerprint=t.source_fingerprint
            AND w.original_path=t.original_path
        JOIN retained_metadata_replays r ON r.source_fingerprint=t.source_fingerprint
            AND r.article_id=t.article_id AND r.original_path=t.original_path
        JOIN articles a ON a.id=r.article_id AND a.parquet_path=r.original_path
            AND a.body_sha256=r.original_body_sha256
        WHERE t.source_fingerprint=? AND w.required_bytes>? AND w.phase IN ('compressed_file','arrow_uncompressed')
            AND (r.state='resource_wait' OR (r.state='failed_validation'
                AND r.error_code='retained_metadata_batch_exceeds_resource_bound'))
            AND a.state='complete' AND a.lease_owner IS NULL AND a.lease_until IS NULL''',
        (fingerprint, bound)).fetchone()
    return min(maximum, max(1.0, row[0] - time.time())) if row and row[0] is not None else maximum


def _defer(queue, task, error, *, increase_address=False, increase_output=False):
    fp, article_id, path = task['fingerprint'], task['article_id'], task['path']
    prior = queue.db.execute('SELECT attempts FROM retained_metadata_large_retries '
                             'WHERE source_fingerprint=? AND article_id=? AND original_path=?',
                             (fp, article_id, path)).fetchone()
    attempts = (prior[0] if prior else 0) + 1
    extra = min(MAX_ADDRESS_BYTES, task['extra'] * (2 if increase_address else 1))
    output = min(MAX_OUTPUT_BYTES, task['output'] * (2 if increase_output else 1))
    delay = min(RETRY_MAX_SECONDS, 2 ** min(attempts - 1, 9))
    # If the bounded transport itself cannot carry this head, do not hot-loop.
    if increase_output and task['output'] >= MAX_OUTPUT_BYTES:
        delay = RETRY_MAX_SECONDS
    with queue.db:
        queue.db.execute('''INSERT INTO retained_metadata_large_retry_history
            (source_fingerprint,article_id,original_path,attempts,extra_address_bytes,max_output_bytes,error,observed_at_utc)
            VALUES (?,?,?,?,?,?,?,?)''',
            (fp, article_id, path, attempts, task['extra'], task['output'], error, utc_now()))
        queue.db.execute('''INSERT INTO retained_metadata_large_retries VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_fingerprint,article_id) DO UPDATE SET
              original_path=excluded.original_path, attempts=excluded.attempts,
              not_before_epoch=excluded.not_before_epoch, extra_address_bytes=excluded.extra_address_bytes,
              max_output_bytes=excluded.max_output_bytes, last_error=excluded.last_error,
              updated_at_utc=excluded.updated_at_utc''',
            (fp, article_id, path, attempts, time.time() + delay, extra, output, error, utc_now()))
    queue.metrics.count('cnyes', 'retained_metadata_large_deferrals')


def _clear(queue, task, *, clear_batch=False):
    with queue.db:
        queue.db.execute('DELETE FROM retained_metadata_large_retries WHERE source_fingerprint=? AND article_id=? '
                         'AND original_path=?', (task['fingerprint'], task['article_id'], task['path']))
        if clear_batch:
            remaining = queue.db.execute('''SELECT 1 FROM retained_metadata_replays
                WHERE source_fingerprint=? AND original_path=? AND (state='resource_wait'
                    OR (state='failed_validation' AND error_code='retained_metadata_batch_exceeds_resource_bound'))
                LIMIT 1''',
                (task['fingerprint'], task['path'])).fetchone()
            if not remaining:
                queue.db.execute('DELETE FROM retained_metadata_resource_waits '
                                 'WHERE source_fingerprint=? AND original_path=?',
                                 (task['fingerprint'], task['path']))


def _stamp(path):
    s = path.stat()
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns]


def _finish(queue, source, task, result, *, stop=None):
    from downloader.news_fulltext_metadata_replay import HEAD_FIELDS, _checkpoint_batch, _needs_repair, _repair_record

    fp, article_id, path = task['fingerprint'], task['article_id'], task['path']
    row = queue.db.execute('SELECT * FROM retained_metadata_replays WHERE source_fingerprint=? AND article_id=?',
                           (fp, article_id)).fetchone()
    head = queue.db.execute('SELECT * FROM articles WHERE id=?', (article_id,)).fetchone()
    if (source.data_fingerprint != fp or not row or row['original_path'] != path
            or row['original_body_sha256'] != task['head']['body_sha256']):
        _clear(queue, task)
        return 0, 0
    if not (row['state'] == 'resource_wait' or (row['state'] == 'failed_validation'
            and row['error_code'] == 'retained_metadata_batch_exceeds_resource_bound')):
        _clear(queue, task)
        return 0, 0
    if not head or (head['parquet_path'], head['body_sha256']) != (path, row['original_body_sha256']):
        _checkpoint_batch(queue, fp, [(article_id, 'superseded', None)])
        _clear(queue, task, clear_batch=True)
        return 0, 0
    if head['state'] != 'complete' or head['lease_owner'] is not None or head['lease_until'] is not None:
        # Recheck eligibility itself as well as snapshot equality. A change
        # between candidate selection and snapshot must not become authority.
        return 0, 0
    if any(head[key] != task['head'][key] for key in HEAD_FIELDS):
        # A refusal/lease/HTTP observation is authoritative; never overwrite it.
        return 0, 0
    state, error = result.get('state'), result.get('error')
    if state in ('resource_wait', 'local_io_retry'):
        _defer(queue, task, error or 'worker_unavailable',
               increase_address=error == 'worker_memory_exhausted',
               increase_output=error in ('worker_output_too_large', 'output_bound_exceeded'))
        return 0, 0
    if state != 'ok':
        # Child-level parsing or infrastructure errors are retry evidence, not
        # a conclusion that the publisher produced incorrect data.
        _defer(queue, task, error or 'worker_proof_unavailable')
        return 0, 0
    try:
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone()
        source_path = replay_source_path(queue.root, path)
        receipt_path = replay_source_path(queue.root, task['receipt_path'])
        if (not receipt or receipt[0] != task['receipt_path'] or result.get('receipt_sha256') != task['receipt_sha256']
                or _stamp(source_path) != result.get('source_stamp') or _stamp(receipt_path) != result.get('receipt_stamp')
                or receipt_path.stat().st_size > MAX_RECEIPT_BYTES):
            raise ValueError('worker_proof_changed_before_acceptance')
        receipt_bytes = receipt_path.read_bytes()
        receipt_data = json.loads(receipt_bytes)
        if (not isinstance(receipt_data, dict) or result.get('source_sha256') != receipt_data.get('sha256')
                or hashlib.sha256(receipt_bytes).hexdigest() != task['receipt_sha256']
                or _stamp(receipt_path) != result.get('receipt_stamp')):
            raise ValueError('worker_proof_changed_before_acceptance')
        meta = result['metadata']
        if not isinstance(meta, dict):
            raise ValueError('worker_metadata_invalid')
        if meta['article_id'] != article_id.hex() or meta['body_sha256'] != head['body_sha256']:
            raise ValueError('worker_head_identity_mismatch')
        outcome = result['outcome']
        host = urlsplit(meta['final_url']).hostname
        if stop and stop.is_set():
            return 0, 0
        if outcome == 'not_mobile_html_candidate' and host != 'm.cnyes.com':
            _checkpoint_batch(queue, fp, [(article_id, outcome, None)])
            _clear(queue, task, clear_batch=True)
            return 1, 0
        if outcome == 'current_metadata_complete' and host == 'm.cnyes.com' and not _needs_repair(meta):
            _checkpoint_batch(queue, fp, [(article_id, outcome, None)])
            _clear(queue, task, clear_batch=True)
            return 1, 0
        if outcome != 'requires_parent_validation' or host != 'm.cnyes.com' or not _needs_repair(meta):
            raise ValueError('worker_outcome_mismatch')
        old = result['record']
        if not isinstance(old, dict):
            raise ValueError('worker_record_invalid')
        payload = base64.b64decode(result['envelope_base64'], validate=True)
        for key, value in old.items():
            if key != 'raw_response_body' and isinstance(value, dict):
                if set(value) != {'bytes_base64'}:
                    raise ValueError('worker_record_encoding_invalid')
                old[key] = base64.b64decode(value['bytes_base64'], validate=True)
        encoded_raw = old.get('raw_response_body')
        if encoded_raw == {'same_as_envelope': True}:
            old['raw_response_body'] = payload
        elif isinstance(encoded_raw, dict) and set(encoded_raw) == {'base64'}:
            old['raw_response_body'] = base64.b64decode(encoded_raw['base64'], validate=True)
        elif encoded_raw is not None:
            raise ValueError('worker_record_encoding_invalid')
        repaired = _repair_record(old, payload, dict(head), source, task['receipt_path'], task['receipt_sha256'])
        if _stamp(source_path) != result.get('source_stamp') or _stamp(receipt_path) != result.get('receipt_stamp'):
            raise ValueError('worker_proof_changed_before_acceptance')
    except OSError:
        _defer(queue, task, 'worker_local_io_unavailable')
        return 0, 0
    except (ValueError, KeyError, TypeError, binascii.Error, NewsError) as exc:
        # A checked identity/parser conflict is retained for review, never a
        # silent rewrite. Worker protocol failures remain retryable.
        code = exc.code if isinstance(exc, NewsError) else str(exc)[:160]
        if code.startswith('worker_'):
            _defer(queue, task, code)
            return 0, 0
        _checkpoint_batch(queue, fp, [(article_id, 'failed_validation', code)])
        _clear(queue, task, clear_batch=True)
        return 1, 0
    # Immutable acceptance is still the canonical owner operation; errors and
    # crashes before/after it leave the original or changed head recoverable.
    if stop and stop.is_set():
        return 0, 0
    count, adopted = queue.accept([repaired], return_adopted=True)
    if article_id not in adopted:
        _defer(queue, task, 'metadata_acceptance_not_adopted')
        return 1, 0
    _checkpoint_batch(queue, fp, [(article_id, 'verified_repair', None)])
    _clear(queue, task, clear_batch=True)
    return 1, count


def poll_or_start(queue, source, bound, *, stop=None, targets=None, start=True):
    """Poll one child without waiting; accept at most one proof per call."""
    checked = repaired = 0
    task = getattr(queue, '_large_metadata_job', None)
    if task and stop and stop.is_set():
        task['job'].close()
        queue._large_metadata_job = None
        return checked, repaired
    if task:
        pending = task.get('pending_results')
        if pending is None:
            result = task['job'].poll()
            if result is None:
                return checked, repaired
            tasks = [task, *task.get('peers', [])]
            if len(tasks) == 1:
                pending = [(task, result)]
            else:
                items = result.get('items') if result.get('state') == 'ok' and result.get('version') == 2 else None
                expected = [item['article_id'].hex() for item in tasks]
                if (not isinstance(items, list) or len(items) != len(tasks)
                        or any(not isinstance(item, dict) or set(item) != {'article_id', 'result'}
                               or not isinstance(item['result'], dict) or item['result'].get('version') != 1
                               for item in items)
                        or [item['article_id'] for item in items] != expected):
                    # Any batch-level failure falls back to independent heads;
                    # a single huge/bad head cannot indefinitely poison peers.
                    error = result.get('error') if result.get('state') != 'ok' else 'worker_output_invalid'
                    failure = {'version': 1, 'state': 'local_io_retry',
                               'error': 'batch_' + (error or 'worker_unavailable')[:120]}
                    pending = [(item, failure) for item in tasks]
                else:
                    pending = [(item, proof['result']) for item, proof in zip(tasks, items)]
            task['pending_results'] = pending
        head_task, proof = pending.pop(0)
        if not pending:
            queue._large_metadata_job = None
        checked, repaired = _finish(queue, source, head_task, proof, stop=stop)
        if pending:
            return checked, repaired
    if not start or stop and stop.is_set():
        return checked, repaired
    row = _candidates(queue, source.data_fingerprint, bound, targets=targets)
    if not row:
        return checked, repaired
    head = queue.db.execute('SELECT * FROM articles WHERE id=?', (row['article_id'],)).fetchone()
    if (not head or (head['parquet_path'], head['body_sha256']) != (row['original_path'], row['original_body_sha256'])
            or head['state'] != 'complete' or head['lease_owner'] is not None or head['lease_until'] is not None):
        return checked, repaired
    task = {'fingerprint': source.data_fingerprint, 'article_id': row['article_id'],
            'path': row['original_path'], 'head': dict(head),
            'extra': row['extra_address_bytes'] or min(MAX_ADDRESS_BYTES, max(INITIAL_ADDRESS_BYTES, row['required_bytes'] * 4)),
            'output': row['max_output_bytes'] or INITIAL_OUTPUT_BYTES}
    # Group only fresh, due heads from the same immutable accepted batch.
    # Retried heads always run independently, so failed grouping cannot loop.
    if MAX_BATCH_HEADS > 1 and row['retry_attempts'] is None:
        peer = _candidates(queue, source.data_fingerprint, bound, targets=targets,
                           path=task['path'], exclude=task['article_id'], fresh=True)
        if peer:
            peer_head = queue.db.execute('SELECT * FROM articles WHERE id=?', (peer['article_id'],)).fetchone()
            if (peer_head and (peer_head['parquet_path'], peer_head['body_sha256']) == (peer['original_path'], peer['original_body_sha256'])
                    and peer_head['state'] == 'complete' and peer_head['lease_owner'] is None and peer_head['lease_until'] is None):
                task['peers'] = [{'fingerprint': task['fingerprint'], 'article_id': peer['article_id'],
                    'path': task['path'], 'head': dict(peer_head),
                    'extra': peer['extra_address_bytes'] or min(MAX_ADDRESS_BYTES, max(INITIAL_ADDRESS_BYTES, peer['required_bytes'] * 4)),
                    'output': peer['max_output_bytes'] or INITIAL_OUTPUT_BYTES}]
    # A persistently inaccessible /proc must not trap other oversized heads,
    # but repeated unknown timeouts may describe a slow, productive read.
    # Give this head progressively more time using its durable retry history.
    unknown_timeout = _unknown_progress_timeout(row['retry_last_error'], row['retry_attempts'])
    try:
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (task['path'],)).fetchone()
        if not receipt:
            raise ValueError('retained_metadata_missing_acceptance_receipt')
        task['receipt_path'] = receipt[0]
        receipt_path = replay_source_path(queue.root, task['receipt_path'])
        if receipt_path.stat().st_size > MAX_RECEIPT_BYTES:
            raise ValueError('worker_receipt_too_large')
        task['receipt_sha256'] = sha256_file(receipt_path)
        request = {'version': 1, 'root': str(queue.root.resolve()), 'source_path': task['path'],
                   'receipt_path': task['receipt_path'], 'receipt_sha256': task['receipt_sha256'],
                   'article_id': task['article_id'].hex(), 'body_sha256': task['head']['body_sha256'],
                   'extra_address_bytes': task['extra'], 'max_output_bytes': task['output']}
        peers = task.get('peers', [])
        if peers:
            for peer in peers:
                peer['receipt_path'] = task['receipt_path']
                peer['receipt_sha256'] = task['receipt_sha256']
            request['version'] = 2
            request['heads'] = [{'article_id': head['article_id'].hex(), 'body_sha256': head['head']['body_sha256']}
                                for head in (task, *peers)]
            del request['article_id'], request['body_sha256']
            # Keep the same aggregate transport ceiling; an oversized result
            # falls back to the established bounded single-head protocol.
            request['extra_address_bytes'] = min(MAX_ADDRESS_BYTES, task['extra'] + min(task['extra'], 128 << 20))
        task['job'] = LargeReplayJob(request, unknown_stall_seconds=unknown_timeout)
    except (OSError, ValueError) as exc:
        _defer(queue, task, 'worker_local_io_unavailable' if isinstance(exc, OSError) else str(exc)[:160])
        return checked, repaired
    queue._large_metadata_job = task
    queue.metrics.count('cnyes', 'retained_metadata_large_jobs_started')
    return checked, repaired
