"""Explicit URL metadata for the resident queue owner, without another collector.

The CLI only seals a bounded private request. The normal owner registers new
identities and preserves existing heads exactly; its usual policy, finite scope,
disk, quota and HTTP gates still decide whether an article may be fetched.
"""
from __future__ import annotations

from collections import Counter
from contextlib import closing
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from urllib.parse import urlsplit

from downloader.news_fulltext_policy import NewsError, normalize_url
from downloader.news_fulltext_queue import utc_now
from downloader.news_fulltext_repairs import (
    MAX_PENDING, MAX_REQUEST_BYTES, MAX_TARGETS, _bytes, _clock, _directory,
    _pending, _quarantine, _read_private_request, _write_once,
)

CONTRACT_VERSION = 1
KIND = 'explicit_article_url_metadata'
DECLARED_CONTRACT_VERSION = 2
OBSERVED_DECLARED_CONTRACT_VERSION = 3
DECLARED_KIND = 'explicit_declared_original_acquisition'
MAX_DECLARED_TARGETS = 16


def has_live_owner(root: Path) -> bool:
    """Use the actual queue lock, never a stale PID/lifecycle file."""
    try:
        descriptor = os.open(Path(root)/'owner.lock', os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0))
    except FileNotFoundError:
        return False
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError('invalid_news_owner_lock')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


def _source_identity(registry, url):
    source = registry.match(url)
    return ({'provider': source.provider, 'allowed': source.allowed,
             'data_fingerprint': source.data_fingerprint,
             'authorization_fingerprint': source.authorization_fingerprint} if source else
            {'provider': 'unregistered:' + urlsplit(url).hostname, 'allowed': False,
             'data_fingerprint': None, 'authorization_fingerprint': None})


def submit_urls(root: Path, registry, urls) -> dict:
    """No queue writer, lifecycle, HTTP, repair, refresh or body admission."""
    unique = {}
    for value in urls:
        url = normalize_url(value)
        if url not in unique:
            if len(unique) >= MAX_TARGETS:
                raise ValueError('resident_url_metadata_limit_512; split explicit URL input into bounded submissions')
            unique[url] = {'url': url, 'source': _source_identity(registry, url)}
    if not unique:
        raise ValueError('explicit_url_metadata_requires_urls')
    request = {'contract_version': CONTRACT_VERSION, 'kind': KIND,
               'created_at_utc': utc_now(), 'targets': list(unique.values())}
    return _submit(root, request)


def submit_declared_urls(root: Path, registry, entries, *, contract_version=DECLARED_CONTRACT_VERSION) -> dict:
    """Seal reviewed metadata acquisition only; the resident owner does the GET.

    A read-only preflight reuses the already closed declaration; it neither
    constructs a queue owner nor manufactures a successful raw/body receipt.
    """
    from downloader.news_fulltext_cohorts import validate_declared_original
    if type(contract_version) is not int or contract_version not in {DECLARED_CONTRACT_VERSION, OBSERVED_DECLARED_CONTRACT_VERSION}:
        raise ValueError('invalid_declared_original_contract_version')
    unique = {}
    with closing(sqlite3.connect((Path(root).resolve()/'queue.sqlite3').as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {'url', 'declared_original'}:
                raise ValueError('invalid_declared_original_entry')
            if (contract_version == DECLARED_CONTRACT_VERSION and isinstance(entry['declared_original'], dict)
                    and 'kind' in entry['declared_original']):
                raise ValueError('observed_original_requires_contract3')
            url = normalize_url(entry['url'])
            if url != entry['url']:
                raise ValueError('declared_original_requires_literal_url')
            if url in unique:
                if unique[url]['declared_original'] != entry['declared_original']:
                    raise ValueError('conflicting_declared_original_witness')
                continue
            if len(unique) >= MAX_DECLARED_TARGETS:
                raise ValueError('declared_original_limit_16')
            validate_declared_original(db, registry, url, entry['declared_original'])
            unique[url] = {**entry, 'source': _source_identity(registry, url)}
    if not unique:
        raise ValueError('explicit_declared_original_requires_urls')
    return _submit(root, {'contract_version': contract_version, 'kind': DECLARED_KIND,
        'created_at_utc': utc_now(), 'targets': list(unique.values())})


def _submit(root, request):
    payload = _bytes(request)
    if len(payload) > MAX_REQUEST_BYTES:
        raise ValueError('url_metadata_request_too_large')
    directory = _directory(Path(root), 'url_metadata', 'pending')
    if len(_pending(directory)) >= MAX_PENDING:
        raise ValueError('url_metadata_inbox_full')
    request_id = hashlib.sha256(payload).hexdigest()
    path = directory/(request_id + '.json')
    _write_once(path, payload)
    return {'contract_version': request['contract_version'], 'kind': request['kind'], 'request_id': request_id,
            'submitted_urls': len(request['targets']), 'pending_path': str(path),
            'admission_pending': True, 'http_requests': 0, 'queue_writes': 0,
            'owner_restarted': False, 'body_acceptance_proven': False}


def _load(path):
    request = _read_private_request(path)
    declared = (isinstance(request, dict) and type(request.get('contract_version')) is int
                and request['contract_version'] in {DECLARED_CONTRACT_VERSION, OBSERVED_DECLARED_CONTRACT_VERSION}
                and request.get('kind') == DECLARED_KIND)
    if (not isinstance(request, dict) or set(request) != {'contract_version', 'kind', 'created_at_utc', 'targets'}
            or type(request['contract_version']) is not int
            or not (declared or request['contract_version'] == CONTRACT_VERSION and request['kind'] == KIND)
            or not isinstance(request['targets'], list)
            or not 1 <= len(request['targets']) <= (MAX_DECLARED_TARGETS if declared else MAX_TARGETS)
            or _clock(request['created_at_utc']) > time.time() + 60):
        raise ValueError('invalid_url_metadata_request')
    seen = set()
    fields = {'provider', 'allowed', 'data_fingerprint', 'authorization_fingerprint'}
    for target in request['targets']:
        if (not isinstance(target, dict) or set(target) != ({'url', 'source', 'declared_original'} if declared else {'url', 'source'})
                or not isinstance(target['url'], str) or normalize_url(target['url']) != target['url']
                or target['url'] in seen or not isinstance(target['source'], dict)
                or set(target['source']) != fields or type(target['source']['allowed']) is not bool):
            raise ValueError('invalid_url_metadata_target')
        if declared:
            from downloader.news_fulltext_original_declarations import witness_shape
            witness = target['declared_original']
            legacy = (isinstance(witness, dict) and set(witness) == {'request_id', 'job_sha256'}
                and all(isinstance(v, str) and re.fullmatch(r'[0-9a-f]{64}', v) for v in witness.values()))
            observed = request['contract_version'] == OBSERVED_DECLARED_CONTRACT_VERSION and witness_shape(witness)
            if not (legacy or observed):
                raise ValueError('invalid_declared_original_target')
        seen.add(target['url'])
    return request


def _schema(queue):
    if getattr(queue, '_url_metadata_schema_ready', False):
        return
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS url_metadata_jobs(
            request_id TEXT PRIMARY KEY,targets INTEGER NOT NULL,next_index INTEGER NOT NULL,
            created_at_utc TEXT NOT NULL,completed_at_utc TEXT
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS url_metadata_items(
            request_id TEXT NOT NULL,item_number INTEGER NOT NULL,article_id BLOB NOT NULL,
            provider TEXT NOT NULL,outcome TEXT NOT NULL,observed_at_utc TEXT NOT NULL,owner_id TEXT NOT NULL,
            PRIMARY KEY(request_id,item_number)
        ) WITHOUT ROWID;
    ''')
    queue._url_metadata_schema_ready = True


def has_pending(queue):
    base = queue.root/'reviewed_repairs/url_metadata'
    if not base.exists() and not base.is_symlink():
        return False
    try:
        paths = _pending(_directory(queue.root, 'url_metadata', 'pending'))
        if not paths:
            return False
        _schema(queue)
        for path in paths:
            job = queue.db.execute('SELECT completed_at_utc FROM url_metadata_jobs WHERE request_id=?',
                                   (path.stem,)).fetchone()
            if job is None or job['completed_at_utc'] is None:
                return True
        return False
    except (OSError, ValueError):
        return False  # The normal admission pump retains invalid-file handling.


def inbox_progress(queue):
    """Report sealed URL submissions without admitting them or exposing URLs.

    A submission is work, not a new article: other submissions or existing
    heads may name the same URL. Cache validated target counts by file identity
    so a blocked owner's heartbeats do not repeatedly parse the private inbox.
    Actual admission always validates the request again.
    """
    result = {'contract_version': 1, 'available': True, 'request_files': 0,
              'pending_requests': 0, 'targets_unexamined': 0,
              'completed_unarchived': 0, 'invalid_or_unreadable': 0}
    directory = queue.root
    try:
        for part in ('reviewed_repairs', 'url_metadata', 'pending'):
            directory = directory / part
            try:
                info = directory.lstat()
            except FileNotFoundError:
                return result
            if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:
                raise ValueError('url_metadata_inbox_not_private_directory')
        paths = _pending(directory)
        if len(paths) > MAX_PENDING:
            raise ValueError('url_metadata_inbox_exceeds_limit')
        result['request_files'] = len(paths)
        if not paths:
            queue._url_metadata_progress_cache = {}
            return result
        has_jobs = bool(queue.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='url_metadata_jobs'").fetchone())
        previous = getattr(queue, '_url_metadata_progress_cache', {})
        cache = {}
        for path in paths:
            try:
                def signature():
                    s = path.lstat()
                    return (s.st_dev, s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
                before = signature()
                cached = previous.get(path.name)
                if cached is not None and cached[0] == before:
                    target_count = cached[1]
                else:
                    target_count = len(_load(path)['targets'])
                    if signature() != before:
                        raise ValueError('url_metadata_request_changed_during_read')
                cache[path.name] = (before, target_count)
                job = (queue.db.execute('SELECT targets,next_index,completed_at_utc FROM url_metadata_jobs WHERE request_id=?',
                                        (path.stem,)).fetchone() if has_jobs else None)
                if job is None:
                    cursor = 0
                else:
                    if (job['targets'] != target_count or type(job['next_index']) is not int
                            or not 0 <= job['next_index'] <= target_count
                            or (job['completed_at_utc'] is None) == (job['next_index'] == target_count)):
                        raise ValueError('url_metadata_job_cursor_inconsistent')
                    cursor = job['next_index']
                if cursor == target_count:
                    result['completed_unarchived'] += 1
                else:
                    result['pending_requests'] += 1
                    result['targets_unexamined'] += target_count - cursor
            except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError, NewsError):
                result['invalid_or_unreadable'] += 1
        queue._url_metadata_progress_cache = cache
    except (OSError, ValueError, sqlite3.Error):
        return {'contract_version': 1, 'available': False}
    return result


def _archive(queue, path, request):
    job = queue.db.execute('SELECT * FROM url_metadata_jobs WHERE request_id=?', (path.stem,)).fetchone()
    counts = dict(queue.db.execute('SELECT outcome,count(*) FROM url_metadata_items WHERE request_id=? GROUP BY outcome',
                                  (path.stem,)))
    directory = _directory(queue.root, 'url_metadata', 'completed')
    _write_once(directory/(path.stem + '.receipt.json'), _bytes({
        'contract_version': request['contract_version'], 'kind': request['kind'], 'request_id': path.stem,
        'targets': job['targets'], 'outcomes': counts, 'completed_at_utc': job['completed_at_utc'],
        'body_acceptance_proven': False, 'http_requests': 0}))
    archived = directory/(path.stem + '.request.json')
    if archived.is_symlink():
        raise ValueError('url_metadata_archive_symlink')
    if archived.exists():
        with os.fdopen(os.open(archived, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
                              | getattr(os, 'O_NONBLOCK', 0)), 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_REQUEST_BYTES or info.st_mode & 0o077:
                raise ValueError('url_metadata_archive_not_bounded_private_file')
            raw = stream.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES or hashlib.sha256(raw).hexdigest() != path.stem:
            raise ValueError('url_metadata_archive_conflict')
    os.replace(path, archived)
    for parent in (directory, path.parent):
        descriptor = os.open(parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def consume_urls(queue, *, limit=32, max_seconds=0.05, stop=None):
    """The existing single writer commits each URL and cursor atomically."""
    if (type(limit) is not int or not 1 <= limit <= MAX_TARGETS or not math.isfinite(max_seconds)
            or not 0 < max_seconds <= 0.1):
        raise ValueError('invalid_url_metadata_slice')
    started = time.monotonic()
    from downloader.news_fulltext_original_declarations import consume as consume_declarations
    declarations = consume_declarations(queue, limit=min(limit, 16), max_seconds=min(max_seconds, .02), stop=stop)
    counts, processed = Counter(declarations['outcomes']), declarations['processed']
    base = queue.root/'reviewed_repairs/url_metadata'
    if not base.exists() and not base.is_symlink():
        return {'processed': processed, 'outcomes': dict(counts)}
    try:
        paths = _pending(_directory(queue.root, 'url_metadata', 'pending'))
    except (OSError, ValueError):
        queue.metrics.count('url_metadata', 'inbox_unavailable')
        return {'processed': 0, 'outcomes': {}, 'inbox_unavailable': True}
    for path in paths:
        if processed >= limit or time.monotonic()-started >= max_seconds or stop is not None and stop.is_set():
            break
        try:
            request = _load(path)
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError, NewsError) as error:
            try:
                _quarantine(queue, path, error)
            except (OSError, ValueError):
                queue.metrics.count('url_metadata', 'quarantine_deferred')
            processed += 1
            counts['invalid_request'] += 1
            continue
        _schema(queue)
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO url_metadata_jobs VALUES (?,?,0,?,NULL)',
                             (path.stem,len(request['targets']),request['created_at_utc']))
            job = queue.db.execute('SELECT * FROM url_metadata_jobs WHERE request_id=?',(path.stem,)).fetchone()
            cursor = job['next_index']
            while cursor < len(request['targets']) and processed < limit:
                if time.monotonic()-started >= max_seconds or stop is not None and stop.is_set():
                    break
                target = request['targets'][cursor]
                url, expected = target['url'], target['source']
                identity = _source_identity(queue.registry,url)
                key = hashlib.sha256(url.encode()).digest()
                if identity != expected:
                    outcome = 'source_contract_changed'
                elif request['kind'] == DECLARED_KIND:
                    from downloader.news_fulltext_cohorts import approve_declared_original
                    head = queue.db.execute('SELECT * FROM articles WHERE id=?', (key,)).fetchone()
                    if head is not None and (head['state'] != 'waiting_cohort' or head['attempts']
                            or head['body_sha256'] or head['lease_owner'] or head['lease_until'] or head['due'] > time.time()):
                        outcome = 'existing_head_preserved'
                    else:
                        try:
                            approve_declared_original(queue, url, target['declared_original'])
                            outcome = 'declared_original_acquisition_registered'
                        except (ValueError, NewsError):
                            outcome = 'declared_original_witness_rejected'
                elif queue.db.execute('SELECT 1 FROM articles WHERE id=?',(key,)).fetchone():
                    outcome = 'existing_head_preserved'
                else:
                    queue.add_articles([{'url':url}])
                    outcome = 'metadata_registered'
                queue.db.execute('INSERT INTO url_metadata_items VALUES (?,?,?,?,?,?,?)',
                    (path.stem,cursor,key,identity['provider'],outcome,utc_now(),queue.owner))
                counts[outcome] += 1
                queue.counter('url_metadata_' + outcome,1)
                processed += 1
                cursor += 1
            complete = cursor == len(request['targets'])
            queue.db.execute('UPDATE url_metadata_jobs SET next_index=?,completed_at_utc=coalesce(completed_at_utc,?) WHERE request_id=?',
                             (cursor,utc_now() if complete else None,path.stem))
        if complete:
            try:
                _archive(queue,path,request)
            except (OSError, ValueError):
                queue.metrics.count('url_metadata','archive_deferred')
    return {'processed': processed,'outcomes':dict(counts)}
