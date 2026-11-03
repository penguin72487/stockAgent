"""Recover interrupted RSS/WordPress pages from their own source bytes.

The existing queue owner is the only writer.  A checkpoint is registered with
either a durable body receipt or a temporary transactional no-new-body journal;
neither grants permission, invents a publisher clock, or turns a different
response into page evidence.
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
import zlib
from types import SimpleNamespace

import pyarrow as pa

from downloader.news_fulltext_bulk import BULK_RESTRICTED_ITEM_ERRORS, wordpress_base, wordpress_page, wordpress_records, wordpress_url
from downloader.news_fulltext_cnyes_checkpoint import HEAD_FIELDS, _read_original
from downloader.news_fulltext_feed import XML_BODY_METADATA_FALLBACK, feed_page, feed_records, feed_url
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError

VERSION = 1
WITNESS_FIELD = 'bulk_page_checkpoint_witness_json'
HEADER_FIELDS = ('content-type', 'etag', 'last-modified', 'x-wp-total', 'x-wp-totalpages')
MAX_LOCAL_IO_ATTEMPTS = 5
MAX_JOURNAL_BODY_BYTES = 32 << 20
JOURNAL_PREFIX = 'bulk_page_journal/'
_HEX = re.compile(r'[0-9a-f]{64}\Z')


def initialize(queue):
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS bulk_interrupted_page_checkpoints(
            parquet_path TEXT PRIMARY KEY, receipt_path TEXT NOT NULL,
            provider TEXT NOT NULL, request_url TEXT NOT NULL, response_sha256 TEXT NOT NULL,
            witness_json TEXT NOT NULL, state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, not_before REAL NOT NULL DEFAULT 0,
            outcome TEXT
        );
        CREATE INDEX IF NOT EXISTS bulk_interrupted_page_due
            ON bulk_interrupted_page_checkpoints(state,provider,not_before,parquet_path);
        CREATE INDEX IF NOT EXISTS bulk_interrupted_page_url
            ON bulk_interrupted_page_checkpoints(request_url,state);
        CREATE TABLE IF NOT EXISTS bulk_interrupted_page_payloads(
            parquet_path TEXT PRIMARY KEY, codec TEXT NOT NULL,
            raw_bytes INTEGER NOT NULL, payload BLOB NOT NULL
        );
    ''')


def _clock(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('invalid_bulk_checkpoint_clock')
    return parsed.astimezone(timezone.utc)


def _parse(raw):
    if not isinstance(raw, str) or len(raw.encode()) > 32768:
        raise ValueError('invalid_bulk_checkpoint_witness')
    value = json.loads(raw)
    if (not isinstance(value, dict) or set(value) != {'version', 'head', 'response_sha256',
            'captured_at_utc', 'request_started_at_utc', 'headers', 'data_fingerprint',
            'authorization_fingerprint', 'parser_fingerprint'}
            or type(value['version']) is not int or value['version'] != VERSION
            or not isinstance(value['head'], dict) or set(value['head']) != set(HEAD_FIELDS)
            or not isinstance(value['headers'], dict) or set(value['headers']) != set(HEADER_FIELDS)):
        raise ValueError('invalid_bulk_checkpoint_witness')
    head = value['head']
    if (head['kind'] not in {'rss_fulltext', 'wordpress_posts'}
            or not isinstance(head['url'], str) or not isinstance(head['provider'], str)
            or any(v is not None and (not isinstance(v, str) or len(v) > 8192)
                   for k, v in head.items() if k not in {'due', 'attempts', 'priority'})
            or type(head['due']) not in (int, float) or not math.isfinite(head['due'])
            or type(head['attempts']) is not int or head['attempts'] < 0
            or type(head['priority']) is not int
            or any(v is not None and (not isinstance(v, str) or len(v) > 4096)
                   for v in value['headers'].values())
            or any(not isinstance(value[k], str) or not _HEX.fullmatch(value[k])
                   for k in ('response_sha256', 'data_fingerprint', 'authorization_fingerprint', 'parser_fingerprint'))):
        raise ValueError('invalid_bulk_checkpoint_witness')
    captured = _clock(value['captured_at_utc'])
    started = value['request_started_at_utc']
    if (head['due'] > captured.timestamp()
            or head['last_checked_at_utc'] is not None and _clock(head['last_checked_at_utc']) >= captured
            or started is not None and _clock(started) > captured):
        raise ValueError('invalid_bulk_checkpoint_order')
    return value


def witness(task, response, source, captured_at_utc, parser_fingerprint):
    if (any(key not in task for key in HEAD_FIELDS) or task.get('kind') not in {'rss_fulltext', 'wordpress_posts'}
            or task['provider'] != source.provider or task['url'] != response.url
            or response.status != 200 or not isinstance(response.body, bytes)):
        return None
    value = {'version': VERSION, 'head': {key: task[key] for key in HEAD_FIELDS},
             'response_sha256': hashlib.sha256(response.body).hexdigest(), 'captured_at_utc': captured_at_utc,
             'request_started_at_utc': task.get('_discovery_request_started_at_utc'),
             'headers': {key: response.headers.get(key) for key in HEADER_FIELDS},
             'data_fingerprint': source.data_fingerprint, 'authorization_fingerprint': source.authorization_fingerprint,
             'parser_fingerprint': parser_fingerprint}
    try:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
        _parse(raw)
        return raw
    except (ValueError, TypeError, KeyError, OverflowError, AttributeError):
        return None


def register(queue, records, relative, receipt_relative):
    """Called inside the acceptance transaction, including orphan adoption."""
    candidates = [record for record in records if record.get(WITNESS_FIELD) is not None]
    if not candidates:
        return
    try:
        if len(candidates) != 1:
            raise ValueError('multiple_bulk_checkpoint_witnesses')
        record = candidates[0]
        value = _parse(record[WITNESS_FIELD])
        head = value['head']
        if (record.get('provider') != head['provider'] or record.get('source_request_url') != head['url']
                or record.get('response_sha256') != value['response_sha256']
                or record.get('captured_at_utc') != value['captured_at_utc']
                or record.get('authorization_fingerprint') != value['authorization_fingerprint']
                or record.get('data_fingerprint') != value['data_fingerprint']
                or not isinstance(record.get('raw_response_body'), bytes)
                or hashlib.sha256(record['raw_response_body']).hexdigest() != value['response_sha256']):
            raise ValueError('bulk_checkpoint_record_mismatch')
    except (ValueError, TypeError, KeyError, OverflowError, AttributeError):
        queue.counter('bulk_interrupted_page_invalid_witness', 1)
        return
    queue.db.execute('INSERT OR IGNORE INTO bulk_interrupted_page_checkpoints '
                     '(parquet_path,receipt_path,provider,request_url,response_sha256,witness_json,state) '
                     "VALUES (?,?,?,?,?,?,'pending')", (relative, receipt_relative, head['provider'],
                     head['url'], value['response_sha256'], record[WITNESS_FIELD]))


def _is_journal(row):
    path = row['parquet_path']
    return (isinstance(path, str) and row['receipt_path'] == path
            and re.fullmatch(r'bulk_page_journal/[0-9a-f]{32}', path) is not None)


def register_no_body(queue, task, response, source, captured_at_utc, parser_fingerprint, *,
                     in_registration_transaction=False):
    """Journal one eligible page and its witness atomically for crash recovery.

    This is temporary recovery state, not an additional source archive.  The
    successful page transaction removes it; failures retain the exact bytes.
    Pages above the bounded journal size retain the pre-existing retry path.
    """
    bases = (source.fulltext_feeds if task.get('kind') == 'rss_fulltext'
             else source.wordpress_apis if task.get('kind') == 'wordpress_posts' else ())
    if (not source.allowed or not source.request_enabled or wordpress_base(task.get('url', '')) not in bases
            or not isinstance(response.body, bytes) or len(response.body) > MAX_JOURNAL_BODY_BYTES):
        return None
    raw = witness(task, response, source, captured_at_utc, parser_fingerprint)
    if raw is None:
        return None
    value = _parse(raw)
    compressed = zlib.compress(response.body, 1)
    codec, payload = ('zlib', compressed) if len(compressed) < len(response.body) else ('raw', response.body)
    path = JOURNAL_PREFIX + uuid.uuid4().hex
    def insert():
        current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()
        if current is None or any(current[key] != value['head'][key] for key in HEAD_FIELDS):
            return None
        queue.db.execute('INSERT INTO bulk_interrupted_page_checkpoints '
                         '(parquet_path,receipt_path,provider,request_url,response_sha256,witness_json,state) '
                         "VALUES (?,?,?,?,?,?,'pending')", (path, path, source.provider, task['url'],
                         value['response_sha256'], raw))
        queue.db.execute('INSERT INTO bulk_interrupted_page_payloads VALUES (?,?,?,?)',
                         (path, codec, len(response.body), sqlite3.Binary(payload)))
        return path

    if in_registration_transaction:
        if not queue.db.in_transaction:
            raise ValueError('bulk_journal_requires_registration_transaction')
        return insert()
    with queue.db:
        queue.db.execute('BEGIN IMMEDIATE')
        return insert()


def _read_journal(queue, row, value):
    if not _is_journal(row):
        raise ValueError('invalid_bulk_page_journal')
    payload = queue.db.execute('SELECT codec,raw_bytes,payload FROM bulk_interrupted_page_payloads '
                               'WHERE parquet_path=?', (row['parquet_path'],)).fetchone()
    if (payload is None or payload['codec'] not in {'raw', 'zlib'}
            or type(payload['raw_bytes']) is not int or not 0 <= payload['raw_bytes'] <= MAX_JOURNAL_BODY_BYTES
            or not isinstance(payload['payload'], bytes) or len(payload['payload']) > MAX_JOURNAL_BODY_BYTES):
        raise ValueError('invalid_bulk_page_journal_payload')
    if payload['codec'] == 'zlib':
        try:
            decoder = zlib.decompressobj()
            body = decoder.decompress(payload['payload'], payload['raw_bytes'] + 1)
            if not decoder.eof or decoder.unconsumed_tail or decoder.unused_data:
                raise ValueError('invalid_bulk_page_journal_compression')
        except zlib.error as error:
            raise ValueError('invalid_bulk_page_journal_compression') from error
    else:
        body = payload['payload']
    if len(body) != payload['raw_bytes'] or hashlib.sha256(body).hexdigest() != value['response_sha256']:
        raise ValueError('bulk_page_journal_digest_mismatch')
    return body


def finish(queue, relative):
    if relative:
        if isinstance(relative, str) and re.fullmatch(r'bulk_page_journal/[0-9a-f]{32}', relative):
            queue.db.execute('DELETE FROM bulk_interrupted_page_payloads WHERE parquet_path=?', (relative,))
        queue.db.execute("DELETE FROM bulk_interrupted_page_checkpoints WHERE parquet_path=? AND state='pending'", (relative,))


def defer_live_failure(queue, task, response, error):
    """Keep the original head only when its exact durable intent exists."""
    if (not isinstance(error, (OSError, sqlite3.Error)) or task.get('kind') not in {'rss_fulltext', 'wordpress_posts'}
            or not isinstance(response, Response) or response.status != 200 or response.url != task.get('url')
            or not isinstance(response.body, bytes)):
        return False
    from downloader.news_fulltext import discovery_fingerprint
    source = queue.registry.sources.get(task.get('provider'))
    if source is None or not source.allowed:
        return False
    digest = hashlib.sha256(response.body).hexdigest()
    current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone()
    if current is None or any(current[key] != task.get(key) for key in HEAD_FIELDS):
        return False
    for row in queue.db.execute("SELECT * FROM bulk_interrupted_page_checkpoints WHERE request_url=? "
                                "AND response_sha256=? AND state='pending'", (task['url'], digest)):
        try:
            value = _parse(row['witness_json'])
            if (value['head'] == {key: task[key] for key in HEAD_FIELDS}
                    and value['request_started_at_utc'] == task.get('_discovery_request_started_at_utc')
                    and value['headers'] == {key: response.headers.get(key) for key in HEADER_FIELDS}
                    and value['data_fingerprint'] == source.data_fingerprint
                    and value['authorization_fingerprint'] == source.authorization_fingerprint
                    and value['parser_fingerprint'] == discovery_fingerprint(source, kind=task['kind'])):
                return True
        except (ValueError, TypeError, KeyError, OverflowError, AttributeError):
            continue
    return False


def _recover_one(queue, row):
    from downloader.news_fulltext import discovery_fingerprint

    value = _parse(row['witness_json'])
    head, request = value['head'], value['head']['url']
    if (request != row['request_url'] or head['provider'] != row['provider']
            or value['response_sha256'] != row['response_sha256']):
        raise ValueError('bulk_checkpoint_identity_mismatch')
    source = queue.registry.sources.get(head['provider'])
    bases = (source.fulltext_feeds if source and head['kind'] == 'rss_fulltext'
             else source.wordpress_apis if source else ())
    if (source is None or not source.allowed or not source.request_enabled or wordpress_base(request) not in bases
            or source.data_fingerprint != value['data_fingerprint']
            or source.authorization_fingerprint != value['authorization_fingerprint']
            or discovery_fingerprint(source, kind=head['kind']) != value['parser_fingerprint']):
        return 'policy_changed'
    current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (request,)).fetchone()
    if current is None or any(current[key] != head[key] for key in HEAD_FIELDS):
        return 'head_superseded'
    journal = _is_journal(row)
    raw_body = (_read_journal(queue, row, value) if journal else
                _read_original(queue, row, value, source, witness_field=WITNESS_FIELD)['raw_response_body'])
    captured = _clock(value['captured_at_utc'])
    if captured > datetime.now(timezone.utc):
        raise ValueError('bulk_checkpoint_future_clock')
    response = Response(request, 200, {k: v for k, v in value['headers'].items() if v is not None},
                        raw_body)
    registry = SimpleNamespace(registry=queue.registry)
    records, errors = (feed_records(response, registry, source) if head['kind'] == 'rss_fulltext'
                       else wordpress_records(response, registry, source))
    if ((not records and (not journal or errors)) or len({r['article_id'] for r in records}) != len(records)
            or any(e['error'] in BULK_RESTRICTED_ITEM_ERRORS or e['error'] == XML_BODY_METADATA_FALLBACK
                   or e.get('xml_body_metadata_only') is True for e in errors)):
        raise ValueError('bulk_checkpoint_unusable_or_restricted_page')
    base = wordpress_base(request)
    if head['kind'] == 'rss_fulltext':
        page = feed_page(request)
        identities = sorted(r['requested_url'] for r in records) + sorted(e['url'] or '' for e in errors)
        digest = hashlib.sha256(json.dumps(identities).encode()).hexdigest()
        previous = queue.db.execute('SELECT page FROM bulk_page_evidence WHERE scope_url=? AND ids_sha256=? AND page!=?',
                                    (base, digest, page)).fetchone()
        if identities and previous:
            raise ValueError('bulk_checkpoint_repeated_page')
        prior = queue.db.execute('SELECT observed_at_utc FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                 (base, page)).fetchone()
        if prior and _clock(prior[0]) > captured:
            return 'newer_scope_observation'
    else:
        page = wordpress_page(request)
        total, pages = int(response.headers['x-wp-total']), int(response.headers['x-wp-totalpages'])
        expected = min(100, max(0, total - (page - 1) * 100))
        if (total < 0 or not 0 <= pages <= 100000 or (total + 99) // 100 != pages
                or page > max(1, pages) or len(records) + len(errors) != expected):
            raise ValueError('bulk_checkpoint_invalid_page_totals')
        prior = queue.db.execute('SELECT observed_at_utc FROM bulk_api_coverage WHERE base_url=?', (base,)).fetchone()
        if prior and _clock(prior[0]) > captured:
            return 'newer_scope_observation'
    with queue.db:
        # Do not let a newer independent check or failure be overwritten.
        queue.db.execute('BEGIN IMMEDIATE')
        current = queue.db.execute('SELECT * FROM discovery WHERE url=?', (request,)).fetchone()
        if current is None or any(current[key] != head[key] for key in HEAD_FIELDS):
            return 'head_superseded'
        if head['kind'] == 'rss_fulltext':
            queue.db.execute('INSERT OR REPLACE INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                             (base, page, -1, digest, value['captured_at_utc']))
            if identities:
                queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider,priority) VALUES (?,'rss_fulltext',?,?)",
                                 (feed_url(base, page + 1), source.provider, page + 1))
        else:
            queue.observe_bulk_coverage(base, source.provider, total, pages, value['captured_at_utc'])
            queue.db.executemany("INSERT OR IGNORE INTO discovery(url,kind,provider,priority) VALUES (?,'wordpress_posts',?,?)",
                                 ((wordpress_url(base, number), source.provider, number) for number in range(2, pages + 1)))
        queue.db.execute('DELETE FROM bulk_item_errors WHERE request_url=?', (request,))
        queue.db.executemany('INSERT INTO bulk_item_errors VALUES (?,?,?,?,?,?)',
            ((request, e['item_number'], source.provider, e['url'], e['error'], value['captured_at_utc']) for e in errors))
        # This receipt may predate an independent page failure.  The provider
        # cooldown has no observation clock, so replay cannot prove it is old
        # enough to clear; preserve it for the live owner to handle.
        delay = source.discovery_interval_seconds if page == 1 else 365 * 86400
        if head['kind'] == 'rss_fulltext' and base in source.historical_fulltext_feeds:
            delay = 365 * 86400
        queue.db.execute('UPDATE discovery SET due=?,etag=coalesce(?,etag),last_modified=coalesce(?,last_modified),'
                         'response_sha256=?,parser_fingerprint=?,last_checked_at_utc=?,attempts=0,error_code=NULL WHERE url=?',
                         (captured.timestamp() + delay, value['headers']['etag'], value['headers']['last-modified'],
                          value['response_sha256'], value['parser_fingerprint'], value['captured_at_utc'], request))
        finish(queue, row['parquet_path'])
        queue.counter('bulk_interrupted_page_recovered', 1)
    queue.metrics.count(source.provider, 'bulk_interrupted_page_recovered')
    return 'recovered'


def recover(queue, *, providers, limit=4, max_seconds=.05, stop=None):
    """Attempt local evidence first; failures release only their own URL."""
    if type(limit) is not int or not 1 <= limit <= 128 or not 0 < max_seconds <= 10:
        raise ValueError('invalid_bulk_checkpoint_recovery_budget')
    providers = sorted(set(providers))
    if not providers:
        return {'outcomes': [], 'ready': False, 'next_local_retry_epoch': None}
    where = 'provider IN (' + ','.join('?' for _ in providers) + ") AND state='pending'"
    start, outcomes = time.monotonic(), []
    while len(outcomes) < limit and (not outcomes or time.monotonic() - start < max_seconds):
        if stop is not None and stop.is_set():
            break
        row = queue.db.execute('SELECT * FROM bulk_interrupted_page_checkpoints WHERE ' + where
                               + ' AND not_before<=? ORDER BY not_before,parquet_path LIMIT 1',
                               (*providers, time.time())).fetchone()
        if row is None:
            break
        try:
            outcome = _recover_one(queue, row)
        except (OSError, sqlite3.Error) as error:
            attempts = row['attempts'] + 1
            outcome = 'local_io_retry' if attempts < MAX_LOCAL_IO_ATTEMPTS else 'local_io_exhausted'
            with queue.db:
                queue.db.execute('UPDATE bulk_interrupted_page_checkpoints SET attempts=?,not_before=?,outcome=? '
                                 "WHERE parquet_path=? AND state='pending'", (attempts,
                                 time.time() + 2 ** attempts, outcome, row['parquet_path']))
                queue.counter('bulk_interrupted_page_local_io_failures', 1)
        except (ValueError, TypeError, KeyError, OverflowError, AttributeError, NewsError, pa.ArrowException):
            outcome = 'invalid_original'
        if outcome not in {'recovered', 'local_io_retry'}:
            with queue.db:
                queue.db.execute("UPDATE bulk_interrupted_page_checkpoints SET state='skipped',outcome=? "
                                 "WHERE parquet_path=? AND state='pending'", (outcome, row['parquet_path']))
        outcomes.append(outcome)
    pending = queue.db.execute('SELECT min(not_before) FROM bulk_interrupted_page_checkpoints WHERE ' + where,
                               providers).fetchone()[0]
    return {'outcomes': outcomes, 'ready': pending is not None and pending <= time.time(),
            'next_local_retry_epoch': pending}
