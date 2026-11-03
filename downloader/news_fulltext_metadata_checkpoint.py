"""Bounded local recovery of interrupted metadata-only feed observations.

The existing queue owner is the sole writer. An intent records the dispatched
head and response identity before the immutable RSS original is written; it is
not itself proof that an original or a publisher observation exists.
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

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_cnyes_checkpoint import HEAD_FIELDS
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import replay_source_path


VERSION = 1
HEADER_FIELDS = ('content-type', 'etag', 'last-modified')
MAX_BODY_BYTES = 32 << 20
MAX_ARTIFACT_BYTES = 64 << 20
MAX_UNCOMPRESSED_BYTES = 160 << 20
MAX_LOCAL_IO_ATTEMPTS = 5
_HEX = re.compile(r'[0-9a-f]{64}\Z')
_COLUMNS = frozenset({'feed_url', 'provider', 'raw_response_body', 'items_json', 'response_sha256',
    'metadata_fingerprint', 'authorization_json', 'observed_at_utc', 'content_kind',
    'article_bodies_accepted', 'publish', 'remote_training_export'})
_MISSING_RECEIPT = object()


def initialize(queue):
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS metadata_feed_interrupted_checkpoints(
            intent_id TEXT PRIMARY KEY, request_url TEXT NOT NULL, response_sha256 TEXT NOT NULL,
            witness_json TEXT NOT NULL, state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, not_before REAL NOT NULL DEFAULT 0, outcome TEXT
        );
        CREATE INDEX IF NOT EXISTS metadata_feed_checkpoint_due
            ON metadata_feed_interrupted_checkpoints(state,not_before,intent_id);
        CREATE INDEX IF NOT EXISTS metadata_feed_checkpoint_url
            ON metadata_feed_interrupted_checkpoints(request_url,state);
    ''')
    from downloader.news_fulltext_metadata_legacy import initialize as initialize_legacy
    initialize_legacy(queue)


def _clock(value):
    if not isinstance(value, str):
        raise ValueError('invalid_metadata_checkpoint_clock')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('invalid_metadata_checkpoint_clock')
    return parsed.astimezone(timezone.utc)


def _parse_witness(raw):
    if not isinstance(raw, str) or len(raw.encode()) > 32768:
        raise ValueError('invalid_metadata_checkpoint_witness')
    value = json.loads(raw)
    if (not isinstance(value, dict) or set(value) != {'version', 'head', 'request_started_at_utc',
            'checked_at_utc', 'headers', 'metadata_fingerprint', 'response_sha256', 'response_bytes'}
            or type(value['version']) is not int or value['version'] != VERSION or not isinstance(value['head'], dict)
            or set(value['head']) != set(HEAD_FIELDS) or value['head']['kind'] != 'metadata_feed'
            or not isinstance(value['head']['url'], str) or not isinstance(value['head']['provider'], str)
            or not isinstance(value['metadata_fingerprint'], str) or not _HEX.fullmatch(value['metadata_fingerprint'])
            or not isinstance(value['response_sha256'], str) or not _HEX.fullmatch(value['response_sha256'])
            or type(value['response_bytes']) is not int or not 0 <= value['response_bytes'] <= MAX_BODY_BYTES
            or not isinstance(value['headers'], dict) or set(value['headers']) != set(HEADER_FIELDS)
            or any(v is not None and (not isinstance(v, str) or len(v) > 8192)
                   for v in value['headers'].values())):
        raise ValueError('invalid_metadata_checkpoint_witness')
    checked = _clock(value['checked_at_utc'])
    if value['request_started_at_utc'] is not None and _clock(value['request_started_at_utc']) > checked:
        raise ValueError('invalid_metadata_checkpoint_order')
    for field in ('due', 'priority', 'attempts'):
        v = value['head'][field]
        if type(v) not in (int, float) or not math.isfinite(v):
            raise ValueError('invalid_metadata_checkpoint_head')
    return value


def _request_started_or_none(task, checked_at_utc):
    started = task.get('_discovery_request_started_at_utc')
    try:
        if started is not None and _clock(started) > _clock(checked_at_utc):
            return None
        return started
    except (ValueError, TypeError, OverflowError):
        return None


def register(queue, task, response, source, checked_at_utc):
    """Persist an exact dispatch witness before writing the source original."""
    if (any(key not in task for key in HEAD_FIELDS) or task['kind'] != 'metadata_feed'
            or task['url'] != response.url or task['provider'] != source.provider
            or response.status != 200 or not isinstance(response.body, bytes)
            or len(response.body) > MAX_BODY_BYTES):
        return None
    started = _request_started_or_none(task, checked_at_utc)
    value = {'version': VERSION, 'head': {key: task[key] for key in HEAD_FIELDS},
        'request_started_at_utc': started, 'checked_at_utc': checked_at_utc,
        'headers': {key: response.headers.get(key) for key in HEADER_FIELDS},
        'metadata_fingerprint': source.metadata_feed_fingerprint,
        'response_sha256': hashlib.sha256(response.body).hexdigest(), 'response_bytes': len(response.body)}
    try:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
        _parse_witness(raw)
    except (ValueError, TypeError, KeyError, OverflowError):
        return None  # Never invent a dispatch head or request clock.
    intent_id = uuid.uuid4().hex
    with queue.db:
        queue.db.execute('INSERT INTO metadata_feed_interrupted_checkpoints '
                         '(intent_id,request_url,response_sha256,witness_json,state) VALUES (?,?,?,?,?)',
                         (intent_id, task['url'], value['response_sha256'], raw, 'pending'))
    return intent_id


def finish(queue, intent_id):
    if intent_id:
        queue.db.execute("DELETE FROM metadata_feed_interrupted_checkpoints WHERE intent_id=? AND state='pending'",
                         (intent_id,))


def defer_live_failure(queue, task, response, error):
    """Keep the dispatch head on a local commit failure if its intent exists."""
    if (not isinstance(error, (OSError, sqlite3.Error)) or task.get('kind') != 'metadata_feed'
            or not isinstance(response, Response) or response.status != 200 or response.url != task.get('url')
            or not isinstance(response.body, bytes)):
        return False
    digest = hashlib.sha256(response.body).hexdigest()
    source = queue.registry.sources.get(task.get('provider'))
    if source is None:
        return False
    for row in queue.db.execute("SELECT * FROM metadata_feed_interrupted_checkpoints "
                                "WHERE request_url=? AND response_sha256=? AND state='pending'",
                                (task['url'], digest)):
        try:
            value = _parse_witness(row['witness_json'])
            if (value['head'] != {key: task[key] for key in HEAD_FIELDS}
                    or value['metadata_fingerprint'] != source.metadata_feed_fingerprint
                    or value['response_bytes'] != len(response.body)
                    or value['headers'] != {key: response.headers.get(key) for key in HEADER_FIELDS}
                    or value['request_started_at_utc'] != _request_started_or_none(task, value['checked_at_utc'])):
                continue
        except (ValueError, TypeError, KeyError, OverflowError):
            continue
        current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()
        if current is None or any(current[key] != value['head'][key] for key in HEAD_FIELDS):
            return False
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_interrupted_checkpoints SET outcome=? "
                             "WHERE intent_id=? AND state='pending'", ('live_commit_' + type(error).__name__, row['intent_id']))
        queue.metrics.count(task['provider'], 'metadata_feed_local_commit_failures')
        return True
    return False


def _file_stamp(value):
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _receipt(queue, relative, *, required, with_identity=False):
    try:
        path = replay_source_path(queue.root, relative + '.receipt.json')
    except FileNotFoundError:
        if required:
            raise
        return _MISSING_RECEIPT
    before = path.stat()
    if before.st_size > 65536:
        raise ValueError('metadata_receipt_too_large')
    value = json.loads(path.read_bytes())
    after = path.stat()
    if _file_stamp(before) != _file_stamp(after):
        raise OSError('metadata_receipt_changed_during_read')
    return (value, path, _file_stamp(after)) if with_identity else value


def _original(queue, value, source, *, legacy=False, parsed=None, details=False,
              _legacy_receipt_sized=False):
    """Validate a durable original and any receipt before it can advance a head.

    ``parsed`` is only for the owner that just parsed these exact response bytes;
    it avoids a second XML parse while still checking the stored bytes and rows.
    """
    from downloader.news_fulltext_metadata_feeds import VERSION as DATA_VERSION, _parse
    url, digest, fingerprint = value['head']['url'], value['response_sha256'], value['metadata_fingerprint']
    relative = f"metadata_feeds/{fingerprint}/{hashlib.sha256(url.encode()).hexdigest()}/{digest}.parquet"
    path = replay_source_path(queue.root, relative)
    receipt_identity = None
    if _legacy_receipt_sized:
        # An old local RSS original has no dispatch witness carrying its size.
        # Read its required receipt once, then check that same file is still
        # present and unchanged after all original-byte validation.
        if not legacy or 'response_bytes' in value:
            raise ValueError('invalid_metadata_legacy_size_request')
        receipt, receipt_path, receipt_stamp = _receipt(queue, relative, required=True, with_identity=True)
        response_bytes = receipt.get('response_bytes') if isinstance(receipt, dict) else None
        if type(response_bytes) is not int or not 0 <= response_bytes <= MAX_BODY_BYTES:
            raise ValueError('invalid_metadata_legacy_response_size')
        receipt_identity = (receipt_path, receipt_stamp)
    else:
        response_bytes = value['response_bytes']
    before = path.stat()
    if before.st_size > MAX_ARTIFACT_BYTES:
        raise ValueError('metadata_original_too_large')
    parquet = pq.ParquetFile(path)
    if (parquet.metadata.num_rows != 1 or set(parquet.schema_arrow.names) != _COLUMNS
            or sum(parquet.metadata.row_group(i).total_byte_size for i in range(parquet.metadata.num_row_groups))
               > MAX_UNCOMPRESSED_BYTES):
        raise ValueError('invalid_metadata_original_schema')
    original = parquet.read().to_pylist()[0]
    body = original['raw_response_body']
    if (not isinstance(body, bytes) or len(body) != response_bytes
            or hashlib.sha256(body).hexdigest() != digest
            or original['feed_url'] != url or original['provider'] != source.provider
            or original['response_sha256'] != digest or original['metadata_fingerprint'] != fingerprint
            or json.loads(original['authorization_json']) != source.metadata_feed_authorization
            or original['content_kind'] != 'rss_information_not_fulltext'
            or type(original['article_bodies_accepted']) is not int or original['article_bodies_accepted'] != 0
            or original['publish'] is not False
            or original['remote_training_export'] is not False):
        raise ValueError('invalid_metadata_original_identity')
    _clock(original['observed_at_utc'])
    response = Response(url, 200, {key: val for key, val in value['headers'].items() if val is not None}, body)
    if parsed is None:
        rows, preservation = _parse(response, source, url)
    else:
        parsed_body, rows, preservation = parsed
        if body != parsed_body:
            raise ValueError('invalid_metadata_original_response_body')
    if json.loads(original['items_json']) != rows:
        raise ValueError('invalid_metadata_original_items')
    if receipt_identity is None:
        receipt = _receipt(queue, relative, required=legacy)
    if receipt is not _MISSING_RECEIPT:
        version = DATA_VERSION if fingerprint == source.metadata_feed_fingerprint else 1
        if not legacy and version != DATA_VERSION:
            raise ValueError('invalid_metadata_checkpoint_version')
        if version == 1 and preservation is not None:
            raise ValueError('legacy_metadata_cannot_have_repaired_xml')
        expected = {'contract_version': version, 'parquet_path': relative, 'sha256': sha256_file(path),
            'response_sha256': digest, 'feed_url': url, 'provider': source.provider,
            'metadata_fingerprint': fingerprint, 'rows': len(rows), 'response_bytes': len(body),
            'article_bodies_accepted': 0, 'publish': False, 'remote_training_export': False}
        if version == DATA_VERSION:
            expected['xml_value_preservation'] = preservation
        if receipt != expected:
            raise ValueError('invalid_metadata_receipt')
    after = path.stat()
    if _file_stamp(before) != _file_stamp(after):
        raise OSError('metadata_original_changed_during_read')
    if receipt_identity is not None:
        try:
            receipt_path = replay_source_path(queue.root, relative + '.receipt.json')
            unchanged = (receipt_path == receipt_identity[0]
                         and _file_stamp(receipt_path.stat()) == receipt_identity[1])
        except (FileNotFoundError, ValueError):
            unchanged = False
        if not unchanged:
            raise OSError('metadata_receipt_changed_during_validation')
    if details:
        return response, original, rows, receipt
    return (response, original, rows) if legacy else response


def _recover_one(queue, row):
    from downloader.news_fulltext_metadata_feeds import accept, require_request
    value = _parse_witness(row['witness_json'])
    head = value['head']
    if head['url'] != row['request_url'] or value['response_sha256'] != row['response_sha256']:
        raise ValueError('metadata_checkpoint_identity_mismatch')
    try:
        source = require_request(queue.registry, head['url'], head['url'])
    except NewsError:
        return 'policy_changed'
    if source.provider != head['provider'] or source.metadata_feed_fingerprint != value['metadata_fingerprint']:
        return 'policy_changed'
    response = _original(queue, value, source)
    task = dict(head, _discovery_request_started_at_utc=value['request_started_at_utc'])
    accept(queue, task, response, [], [], [], _checkpoint_id=row['intent_id'], _checked=value['checked_at_utc'])
    return 'recovered'


def recover(queue, *, limit=4, max_seconds=.05, stop=None):
    """Replay indexed local originals, with finite retries and a bounded slice."""
    if type(limit) is not int or not 1 <= limit <= 128 or not 0 < max_seconds <= 10:
        raise ValueError('invalid_metadata_checkpoint_recovery_budget')
    start, outcomes = time.monotonic(), []
    while len(outcomes) < limit and (not outcomes or time.monotonic() - start < max_seconds):
        if stop is not None and stop.is_set():
            break
        row = queue.db.execute("SELECT * FROM metadata_feed_interrupted_checkpoints "
                               "WHERE state='pending' AND not_before<=? ORDER BY not_before,intent_id LIMIT 1",
                               (time.time(),)).fetchone()
        if row is None:
            break
        try:
            outcome = _recover_one(queue, row)
        except FileNotFoundError:
            outcome = 'original_missing'  # The original request remains eligible on its old schedule.
        except (OSError, sqlite3.Error):
            attempts = row['attempts'] + 1
            outcome = 'local_io_retry' if attempts < MAX_LOCAL_IO_ATTEMPTS else 'local_io_exhausted'
            with queue.db:
                queue.db.execute('UPDATE metadata_feed_interrupted_checkpoints SET attempts=?,not_before=? '
                                 'WHERE intent_id=? AND state=\'pending\'',
                                 (attempts, time.time() + 2 ** attempts, row['intent_id']))
        except (ValueError, TypeError, KeyError, OverflowError, NewsError, pa.ArrowException):
            outcome = 'invalid_original'
        if outcome not in {'recovered', 'local_io_retry'}:
            with queue.db:
                queue.db.execute("UPDATE metadata_feed_interrupted_checkpoints SET state='skipped',outcome=? "
                                 "WHERE intent_id=? AND state='pending'", (outcome, row['intent_id']))
        outcomes.append(outcome)
    pending = queue.db.execute("SELECT min(not_before) FROM metadata_feed_interrupted_checkpoints WHERE state='pending'").fetchone()[0]
    return {'outcomes': outcomes, 'next_local_retry_epoch': pending,
            'ready': pending is not None and pending <= time.time()}
