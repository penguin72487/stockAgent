"""Explicit URL metadata for the resident queue owner, without another collector.

The CLI only seals a bounded private request. The normal owner registers new
identities and preserves existing heads exactly; its usual policy, finite scope,
disk, quota and HTTP gates still decide whether an article may be fetched.
"""
from __future__ import annotations

from bisect import bisect_right
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
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
    _pending, _quarantine, _read_private_request, _unsafe_request_path, _write_once,
)

CONTRACT_VERSION = 1
KIND = 'explicit_article_url_metadata'
LEGACY_FINLAB_CNYES_HINT_VERSION = 2
FINLAB_CNYES_HINT_VERSION = 3
FINLAB_CNYES_HINT_KIND = 'finlab_cnyes_source_date_hint'
MAX_SOURCE_DATE_CHARS = 128
MAX_FINLAB_CNYES_RAW_HINTS = 4096
LOCAL_RETRY_SECONDS = .25
FINLAB_ROW_VERIFY_TIMEOUT_SECONDS = 2.0
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


def _finlab_file_identity(path):
    """Pin the local generation previously checked against the receipt hash."""
    info = Path(path).stat()
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


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
    source_date_hint = (isinstance(request, dict) and type(request.get('contract_version')) is int
                        and request['contract_version'] in {LEGACY_FINLAB_CNYES_HINT_VERSION, FINLAB_CNYES_HINT_VERSION}
                        and request.get('kind') == FINLAB_CNYES_HINT_KIND)
    bound_hint = source_date_hint and request['contract_version'] == FINLAB_CNYES_HINT_VERSION
    expected_request_fields = {'contract_version', 'kind', 'created_at_utc', 'targets'}
    if bound_hint:
        expected_request_fields.update({'source_origin', 'priority_evidence'})
    if (not isinstance(request, dict) or set(request) != expected_request_fields
            or type(request['contract_version']) is not int
            or not (declared or source_date_hint
                    or request['contract_version'] == CONTRACT_VERSION and request['kind'] == KIND)
            or not isinstance(request['targets'], list)
            or not 1 <= len(request['targets']) <= (MAX_DECLARED_TARGETS if declared else MAX_TARGETS)
            or _clock(request['created_at_utc']) > time.time() + 60):
        raise ValueError('invalid_url_metadata_request')
    if bound_hint:
        origin = request['source_origin']
        if (not isinstance(origin, dict) or set(origin) != {'origin_id', 'sha256', 'expected_rows', 'file_identity'}
                or type(origin['origin_id']) is not int or not 0 < origin['origin_id'] < 2**63
                or not isinstance(origin['sha256'], str) or not re.fullmatch(r'[0-9a-f]{64}', origin['sha256'])
                or type(origin['expected_rows']) is not int or not 0 < origin['expected_rows'] < 2**63
                or not isinstance(origin['file_identity'], list) or len(origin['file_identity']) != 5
                or any(type(value) is not int or value < 0 for value in origin['file_identity'])):
            raise ValueError('invalid_finlab_cnyes_source_origin')
        evidence = request['priority_evidence']
        if (not isinstance(evidence, dict) or set(evidence) != {
                'baseline_origin_id', 'baseline_sha256', 'raw_unique_delta', 'raw_hints_examined'}
                or type(evidence['baseline_origin_id']) is not int
                or not 0 < evidence['baseline_origin_id'] < 2**63
                or evidence['baseline_origin_id'] == origin['origin_id']
                or not isinstance(evidence['baseline_sha256'], str)
                or not re.fullmatch(r'[0-9a-f]{64}', evidence['baseline_sha256'])
                or type(evidence['raw_unique_delta']) is not int
                or not 0 < evidence['raw_unique_delta'] < 2**53
                or type(evidence['raw_hints_examined']) is not int
                or evidence['raw_hints_examined'] != min(evidence['raw_unique_delta'], MAX_FINLAB_CNYES_RAW_HINTS)
                or len(request['targets']) > evidence['raw_hints_examined']):
            raise ValueError('invalid_finlab_cnyes_priority_evidence')
    seen, source_rows = set(), set()
    fields = {'provider', 'allowed', 'data_fingerprint', 'authorization_fingerprint'}
    for target in request['targets']:
        expected_fields = ({'url', 'source', 'declared_original'} if declared else
                           {'url', 'source', 'source_date', 'source_row_index'} if bound_hint else
                           {'url', 'source', 'source_date'} if source_date_hint else {'url', 'source'})
        if (not isinstance(target, dict) or set(target) != expected_fields
                or not isinstance(target['url'], str) or normalize_url(target['url']) != target['url']
                or target['url'] in seen or not isinstance(target['source'], dict)
                or set(target['source']) != fields or type(target['source']['allowed']) is not bool):
            raise ValueError('invalid_url_metadata_target')
        if source_date_hint and (target['source']['provider'] != 'cnyes' or not target['source']['allowed']
                or not isinstance(target['source_date'], str) or len(target['source_date']) > MAX_SOURCE_DATE_CHARS
                or '\x00' in target['source_date']):
            raise ValueError('invalid_finlab_cnyes_source_date_hint')
        if bound_hint:
            index = target['source_row_index']
            if type(index) is not int or not 0 <= index < origin['expected_rows'] or index in source_rows:
                raise ValueError('invalid_finlab_cnyes_source_row')
            source_rows.add(index)
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


def _finlab_hint_source_record(queue, request):
    """Match a bound hint to its registered FinLab receipt generation."""
    origin = request['source_origin']
    row = queue.db.execute(
        'SELECT o.provider,o.sha256,o.rows,o.parquet_path,p.expected_rows,p.link_column,o.receipt_path '
        'FROM origins o LEFT JOIN metadata_ingest_progress p ON p.origin_id=o.id WHERE o.id=?',
        (origin['origin_id'],)).fetchone()
    if (not row or row[0] != 'finlab' or row[1] != origin['sha256']
            or (row[4] is None and row[2] != origin['expected_rows'])
            or (row[4] is not None and (row[4] != origin['expected_rows'] or row[5] != 'url'))):
        return None
    return row


def _finlab_hint_source_path(row, request):
    """Check that a bound receipt's local file generation is unchanged.

    This pins the file generation; it is not a publisher-clock verification
    or the ordered import's source-link receipt. The archived request retains
    the original row index for an independent later readback.
    """
    if row is None:
        return None
    try:
        return row[3] if list(_finlab_file_identity(row[3])) == request['source_origin']['file_identity'] else None
    except (OSError, TypeError, ValueError):
        return None


def _read_finlab_hint_source_rows(path, identity, expected_rows, indices):
    """Read each needed source row group once, off the SQLite owner thread.

    File identity is checked both here and around the later owner transaction.
    Results contain source-row URLs and dates, not a publisher clock or a body.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    try:
        if list(_finlab_file_identity(path)) != identity:
            return None
        file = pq.ParquetFile(path)
        try:
            if file.metadata.num_rows != expected_rows or 'url' not in file.schema_arrow.names:
                return None
            has_date = 'date' in file.schema_arrow.names
            starts = [0]
            for group in range(file.num_row_groups):
                starts.append(starts[-1] + file.metadata.row_group(group).num_rows)
            groups = {}
            for slot, index in enumerate(indices):
                group = bisect_right(starts, index) - 1
                if index < 0 or group >= file.num_row_groups:
                    return None
                groups.setdefault(group, []).append((slot, index - starts[group]))
            rows = [None] * len(indices)
            for group, positions in groups.items():
                table = file.read_row_group(group, columns=['url', 'date'] if has_date else ['url'])
                urls = table.column('url')
                dates = table.column('date') if has_date else None
                for slot, offset in positions:
                    try:
                        url = normalize_url(urls[offset].as_py())
                        value = dates[offset].as_py() if dates is not None else None
                        date = str(value or '')
                        if len(date) > MAX_SOURCE_DATE_CHARS or '\x00' in date:
                            date = ''
                        rows[slot] = (url, date)
                    except (NewsError, TypeError, ValueError, OverflowError):
                        continue
        finally:
            file.close()
        return tuple(rows) if list(_finlab_file_identity(path)) == identity else None
    except (OSError, ValueError, TypeError, OverflowError, pa.ArrowException):
        return None


def _verify_finlab_hint_rows(path, identity, expected_rows, targets):
    rows = _read_finlab_hint_source_rows(path, identity, expected_rows,
                                         [target['source_row_index'] for target in targets])
    if rows is None:
        return None
    return tuple(row == (target['url'], target['source_date'])
                 for row, target in zip(rows, targets, strict=True))


class _FinlabRowVerifier:
    """At most one local source reader; never block the resident queue owner."""

    def __init__(self):
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='news-cnyes-source-row')
        self.active = None
        self.completed = {}
        self.pending_ids = set()

    def retain(self, request_ids):
        self.pending_ids = request_ids
        self.completed = {key: value for key, value in self.completed.items() if key in request_ids}

    def prime(self, request_id, matches):
        # The existing producer can verify source rows in its own background
        # comparison. A sealed request ID binds those results to its targets.
        self.completed[request_id] = matches

    def check(self, request_id, source_path, request):
        if request_id in self.completed:
            return True, self.completed[request_id]
        if self.active is not None:
            active_id, future, started = self.active
            if future.done():
                try:
                    result = future.result()
                except Exception:
                    result = None
                if active_id in self.pending_ids:
                    self.completed[active_id] = result
                self.active = None
            elif time.monotonic() - started >= FINLAB_ROW_VERIFY_TIMEOUT_SECONDS:
                # A stuck local read must not stop the mailbox; source dates
                # remain unverified until the ordinary importer reads them.
                return True, None
        if request_id in self.completed:
            return True, self.completed[request_id]
        if self.active is not None:
            return False, None
        origin = request['source_origin']
        future = self.pool.submit(_verify_finlab_hint_rows, source_path, origin['file_identity'],
                                  origin['expected_rows'], request['targets'])
        self.active = (request_id, future, time.monotonic())
        return False, None

    def forget(self, request_id):
        self.completed.pop(request_id, None)

    def close(self):
        self.pool.shutdown(wait=False, cancel_futures=True)


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


def has_pending(queue, *, strict=False):
    """Inspect pending URL requests; strict callers distinguish I/O from empty."""
    base = queue.root/'reviewed_repairs/url_metadata'
    try:
        try:
            base.lstat()
        except FileNotFoundError:
            return False
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
        if strict:
            raise
        return False  # The normal admission pump retains invalid-file handling.


def local_retry_delay(queue, fallback):
    """Bound local I/O retries without changing any publisher's schedule."""
    retry_at = getattr(queue, '_url_metadata_retry_at', 0.0)
    return min(fallback, max(0.0, retry_at - time.monotonic())) if retry_at else fallback


def local_retry_ready(queue):
    return time.monotonic() >= getattr(queue, '_url_metadata_retry_at', 0.0)


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


def _consume_url_inbox(queue, *, limit, max_seconds, stop):
    """Process bounded private requests on the existing single writer."""
    started, counts, processed, considered, advanced, deferred = time.monotonic(), Counter(), 0, 0, 0, False
    archive_deferred = False
    base = queue.root/'reviewed_repairs/url_metadata'
    if not base.exists() and not base.is_symlink():
        return {'processed': processed, 'outcomes': dict(counts), '_advanced': 0, '_deferred': False,
                '_archive_deferred': False}
    try:
        paths = _pending(_directory(queue.root, 'url_metadata', 'pending'))
    except (OSError, ValueError):
        queue.metrics.count('url_metadata', 'inbox_unavailable')
        return {'processed': 0, 'outcomes': {}, 'inbox_unavailable': True, '_advanced': 0, '_deferred': True,
                '_archive_deferred': False}
    verifier = getattr(queue, '_finlab_hint_row_verifier', None)
    if verifier is not None:
        verifier.retain({path.stem for path in paths})
    # Rotate only within the bounded local mailbox. The per-request durable
    # cursor remains authoritative; this hint is neither identity nor proof.
    saved = getattr(queue, '_url_metadata_last_request', None)
    if saved:
        split = next((i for i, path in enumerate(paths) if path.name > saved), len(paths))
        paths = paths[split:] + paths[:split]
    for path in paths:
        if (processed >= limit or stop is not None and stop.is_set()
                or (considered > 0 and time.monotonic()-started >= max_seconds)):
            break
        try:
            request = _load(path)
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError, NewsError) as error:
            if stop is not None and stop.is_set():
                break
            if isinstance(error, OSError) and not _unsafe_request_path(path, error):
                counts['request_io_deferred'] += 1
                queue.metrics.count('url_metadata', 'request_io_deferred')
                deferred = True
                processed += 1
                considered += 1
                queue._url_metadata_last_request = path.name
                continue
            try:
                _quarantine(queue, path, error)
                advanced += 1
            except (OSError, ValueError):
                queue.metrics.count('url_metadata', 'quarantine_deferred')
                deferred = True
            processed += 1
            considered += 1
            queue._url_metadata_last_request = path.name
            counts['invalid_request'] += 1
            continue
        if stop is not None and stop.is_set():
            break
        request_counts, request_processed = Counter(), 0
        try:
            _schema(queue)
            bound_hint = (request['kind'] == FINLAB_CNYES_HINT_KIND
                          and request['contract_version'] == FINLAB_CNYES_HINT_VERSION)
            checked_path, row_matches = None, None
            if bound_hint:
                previous_job = queue.db.execute(
                    'SELECT next_index,completed_at_utc FROM url_metadata_jobs WHERE request_id=?',
                    (path.stem,)).fetchone()
                already_complete = (previous_job is not None and previous_job['completed_at_utc'] is not None
                                    and previous_job['next_index'] == len(request['targets']))
                if not already_complete:
                    pre_record = _finlab_hint_source_record(queue, request)
                    checked_path = _finlab_hint_source_path(pre_record, request)
                    if checked_path is not None:
                        try:
                            verifier = getattr(queue, '_finlab_hint_row_verifier', None)
                            if verifier is None:
                                verifier = queue._finlab_hint_row_verifier = _FinlabRowVerifier()
                                verifier.retain({item.stem for item in paths})
                            ready, row_matches = verifier.check(path.stem, checked_path, request)
                        except (OSError, RuntimeError):
                            # Local worker creation failure cannot stop other
                            # sources or make an unverified date trustworthy.
                            queue.metrics.count('url_metadata', 'finlab_row_verifier_unavailable')
                            ready, row_matches = True, None
                        if not ready:
                            # Other mailbox work and providers keep running.
                            # A bounded local retry observes the background read.
                            counts['finlab_row_verification_pending'] += 1
                            deferred = True
                            considered += 1
                            queue._url_metadata_last_request = path.name
                            continue
            with queue.db:
                source_record = _finlab_hint_source_record(queue, request) if bound_hint else None
                source_path = _finlab_hint_source_path(source_record, request) if bound_hint else None
                if source_record is not None:
                    from downloader.news_fulltext_finlab_priority import recover_from_bound_request
                    recover_from_bound_request(queue, request, path.stem, source_record)
                queue.db.execute('INSERT OR IGNORE INTO url_metadata_jobs VALUES (?,?,0,?,NULL)',
                                 (path.stem,len(request['targets']),request['created_at_utc']))
                job = queue.db.execute('SELECT * FROM url_metadata_jobs WHERE request_id=?',(path.stem,)).fetchone()
                cursor = job['next_index']
                while cursor < len(request['targets']) and processed + request_processed < limit:
                    if (stop is not None and stop.is_set()
                            or ((processed > 0 or request_processed > 0 or considered > 0)
                                and time.monotonic()-started >= max_seconds)):
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
                        # A FinLab date is a sorting hint from the source row,
                        # never an accepted publication clock. Existing heads
                        # remain untouched; ordered ingestion owns links.
                        row_verified = (bound_hint and source_path is not None and source_path == checked_path
                                        and isinstance(row_matches, tuple) and len(row_matches) == len(request['targets'])
                                        and row_matches[cursor] is True)
                        queue.add_articles([{'url': url, 'date': target['source_date']}
                            if row_verified else {'url': url}])
                        outcome = ('metadata_registered_unverified_origin'
                                   if request['kind'] == FINLAB_CNYES_HINT_KIND
                                   and not row_verified else 'metadata_registered')
                    queue.db.execute('INSERT INTO url_metadata_items VALUES (?,?,?,?,?,?,?)',
                        (path.stem,cursor,key,identity['provider'],outcome,utc_now(),queue.owner))
                    request_counts[outcome] += 1
                    queue.counter('url_metadata_' + outcome,1)
                    request_processed += 1
                    cursor += 1
                if source_path is not None:
                    try:
                        source_unchanged = (list(_finlab_file_identity(source_path))
                                            == request['source_origin']['file_identity'])
                    except (OSError, ValueError):
                        source_unchanged = False
                    if not source_unchanged:
                        # Roll back this slice; a later pass can register the
                        # URLs without using a date from an unpinned source.
                        raise OSError('finlab_hint_source_changed_during_admission')
                complete = cursor == len(request['targets'])
                queue.db.execute('UPDATE url_metadata_jobs SET next_index=?,completed_at_utc=coalesce(completed_at_utc,?) WHERE request_id=?',
                                 (cursor,utc_now() if complete else None,path.stem))
        except (OSError, sqlite3.OperationalError) as error:
            # The transaction rolls back every target in this request slice;
            # report no admissions and keep the input for a later attempt.
            outcome = ('original_declaration_storage_deferred' if request['kind'] == DECLARED_KIND
                       else 'url_metadata_storage_deferred')
            counts[outcome] += 1
            queue.metrics.count('url_metadata', outcome)
            deferred = True
            from downloader.news_fulltext_cohorts import DeclaredOriginalProofConflict
            if isinstance(error, DeclaredOriginalProofConflict):
                queue.metrics.count('url_metadata', 'declared_original_local_proof_conflicts')
            processed += 1
            considered += 1
            if stop is None or not stop.is_set():
                queue._url_metadata_last_request = path.name
            continue
        counts.update(request_counts)
        processed += request_processed
        advanced += request_processed
        considered += 1
        if stop is None or not stop.is_set():
            queue._url_metadata_last_request = path.name
        if complete:
            if verifier is not None:
                verifier.forget(path.stem)
            try:
                _archive(queue,path,request)
                if not request_processed:
                    advanced += 1
            except (OSError, ValueError):
                queue.metrics.count('url_metadata','archive_deferred')
                deferred = True
                archive_deferred = True
    return {'processed': processed, 'outcomes': dict(counts), '_advanced': advanced, '_deferred': deferred,
            '_archive_deferred': archive_deferred}


def consume_urls(queue, *, limit=32, max_seconds=0.05, stop=None):
    """Share bounded turns between declarations and explicit URL metadata.

    Slow local I/O can exceed the soft time slice. If no item has been
    processed, each group gets a bounded chance to make progress. Group
    rotation is an in-memory scheduling hint, while admitted URLs and each
    request's progress are still committed by the original queue owner.
    """
    if (type(limit) is not int or not 1 <= limit <= MAX_TARGETS or not math.isfinite(max_seconds)
            or not 0 < max_seconds <= 0.1):
        raise ValueError('invalid_url_metadata_slice')
    queue._url_metadata_continuation_pending = False
    if stop is not None and stop.is_set():
        return {'processed': 0, 'outcomes': {}}
    from downloader.news_fulltext_original_declarations import consume as consume_declarations, has_pending as declarations_pending

    started, counts, processed, inbox_unavailable, inbox_advanced = time.monotonic(), Counter(), 0, False, 0
    declarations_advanced = 0
    inbox_attempted = inbox_deferred = archive_deferred = declarations_attempted = declarations_deferred = False
    inbox_first = getattr(queue, '_url_metadata_inbox_first', False)
    order = ('inbox', 'declarations') if inbox_first else ('declarations', 'inbox')
    for kind in order:
        if processed >= limit or stop is not None and stop.is_set():
            break
        remaining = max_seconds - (time.monotonic() - started)
        if processed and remaining <= 0:
            break
        # If discovery of an empty first group was slow, still give the other
        # group one bounded chance rather than repeating zero-work slices.
        allowance = remaining if remaining > 0 else max_seconds
        if kind == 'inbox':
            result = _consume_url_inbox(queue, limit=limit-processed, max_seconds=allowance, stop=stop)
            inbox_attempted = True
            inbox_advanced += result.pop('_advanced', 0)
            inbox_deferred |= result.pop('_deferred', False)
            archive_deferred |= result.pop('_archive_deferred', False)
        else:
            declarations_attempted = True
            result = consume_declarations(queue, limit=min(limit-processed, 16),
                                          max_seconds=min(allowance, .02), stop=stop)
            declarations_advanced += result['processed']
            declarations_deferred |= bool(result['outcomes'].get('original_declaration_storage_deferred'))
        processed += result['processed']
        counts.update(result['outcomes'])
        inbox_unavailable |= result.get('inbox_unavailable', False)
    if stop is None or not stop.is_set():
        queue._url_metadata_inbox_first = not inbox_first
        if inbox_attempted or declarations_attempted:
            queue._url_metadata_retry_at = (time.monotonic() + LOCAL_RETRY_SECONDS
                                            if archive_deferred or (inbox_deferred or declarations_deferred)
                                            and not (inbox_advanced or declarations_advanced) else 0.0)
        if inbox_advanced or declarations_advanced:
            try:
                queue._url_metadata_continuation_pending = has_pending(queue, strict=True) or declarations_pending(queue)
            except (OSError, sqlite3.Error, ValueError):
                # A committed slice stays committed; retry local discovery
                # without spinning or changing source deadlines.
                queue.metrics.count('url_metadata', 'continuation_check_deferred')
                queue._url_metadata_retry_at = time.monotonic() + LOCAL_RETRY_SECONDS
    result = {'processed': processed, 'outcomes': dict(counts)}
    if inbox_unavailable:
        result['inbox_unavailable'] = True
    return result
