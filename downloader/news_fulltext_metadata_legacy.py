"""Bounded, restartable indexing of retained RSS information originals.

The canonical queue owner discovers local files and verifies their native
receipts before indexing observations. This module never fetches a publisher,
changes a feed head, advances a source deadline or accepts an article body.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time

import pyarrow as pa

from downloader.news_fulltext_metadata_checkpoint import _original
from downloader.news_fulltext_policy import NewsError


VERSION = 1
_DIGEST_FILE = re.compile(r'([0-9a-f]{64})\.parquet\Z')
# Normal RSS arrivals are already indexed by the writer. This local check is
# for archives restored or added after the initial scan; unchanged directories
# do not require another enumeration or receipt/Parquet decoding.
DIRECTORY_RECHECK_SECONDS = 3600
_DIRECTORY_ABSENT = 'absent'
_INVALID_RECHECK = 'recheck_in_progress'


def initialize(queue):
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS metadata_feed_legacy_directories(
            provider TEXT NOT NULL, feed_url TEXT NOT NULL, fingerprint TEXT NOT NULL,
            state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            not_before REAL NOT NULL DEFAULT 0, outcome TEXT, directory_stamp TEXT,
            PRIMARY KEY(provider,feed_url,fingerprint)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS metadata_feed_legacy_directory_due
            ON metadata_feed_legacy_directories(provider,state,not_before);
        CREATE TABLE IF NOT EXISTS metadata_feed_legacy_originals(
            provider TEXT NOT NULL, feed_url TEXT NOT NULL, fingerprint TEXT NOT NULL,
            response_sha256 TEXT NOT NULL, state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, not_before REAL NOT NULL DEFAULT 0, outcome TEXT,
            original_observed_at_utc TEXT, invalid_file_stamp TEXT,
            PRIMARY KEY(provider,feed_url,fingerprint,response_sha256)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS metadata_feed_legacy_original_due
            ON metadata_feed_legacy_originals(provider,state,not_before);
        CREATE TABLE IF NOT EXISTS metadata_feed_legacy_invalid_history(
            event_id INTEGER PRIMARY KEY, provider TEXT NOT NULL, feed_url TEXT NOT NULL,
            fingerprint TEXT NOT NULL, response_sha256 TEXT NOT NULL,
            observed_at_utc TEXT, reason TEXT, event_kind TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS metadata_feed_legacy_invalid_history_scope
            ON metadata_feed_legacy_invalid_history(provider,feed_url,fingerprint,response_sha256);
    ''')
    original_columns = {row[1] for row in queue.db.execute('PRAGMA table_info(metadata_feed_legacy_originals)')}
    if 'original_observed_at_utc' not in original_columns:
        queue.db.execute('ALTER TABLE metadata_feed_legacy_originals ADD COLUMN original_observed_at_utc TEXT')
    if 'invalid_file_stamp' not in original_columns:
        queue.db.execute('ALTER TABLE metadata_feed_legacy_originals ADD COLUMN invalid_file_stamp TEXT')
    if 'directory_stamp' not in {row[1] for row in queue.db.execute('PRAGMA table_info(metadata_feed_legacy_directories)')}:
        queue.db.execute('ALTER TABLE metadata_feed_legacy_directories ADD COLUMN directory_stamp TEXT')


def _scope(source):
    from downloader.news_fulltext_metadata_feeds import compatible_fingerprints
    return source.provider, tuple(source.metadata_feeds), compatible_fingerprints(source)


def _guard(source):
    provider, feeds, fingerprints = _scope(source)
    return ('provider=? AND feed_url IN (' + ','.join('?' for _ in feeds) + ') '
            'AND fingerprint IN (' + ','.join('?' for _ in fingerprints) + ')',
            (provider, *feeds, *fingerprints))


def _ensure_scope(queue, source):
    scope = _scope(source)
    if getattr(queue, '_metadata_legacy_scope', None) == scope:
        return
    close_scanner(queue)
    with queue.db:
        queue.db.executemany('INSERT OR IGNORE INTO metadata_feed_legacy_directories '
            '(provider,feed_url,fingerprint,state) VALUES (?,?,?,\'pending\')',
            ((source.provider, url, fingerprint) for fingerprint in scope[2] for url in scope[1]))
    queue._metadata_legacy_scope = scope


def close_scanner(queue):
    scan = getattr(queue, '_metadata_legacy_scan', None)
    if scan is not None:
        scan['entries'].close()
        queue._metadata_legacy_scan = None


def _directory(root, fingerprint, url):
    return root / 'metadata_feeds' / fingerprint / hashlib.sha256(url.encode()).hexdigest()


def _directory_stamp(path):
    value = path.stat(follow_symlinks=False)
    if path.is_symlink() or not path.is_dir():
        raise ValueError('unsafe_metadata_legacy_directory')
    return value.st_dev, value.st_ino, value.st_mtime_ns, value.st_ctime_ns


def _directory_marker(stamp):
    return ':'.join(str(part) for part in stamp)


def _within_root(path, root):
    try:
        return path.resolve().is_relative_to(root.resolve())
    except RuntimeError as error:
        # pathlib reports a symlink loop as RuntimeError on some Python
        # versions; keep it local to the affected directory's retry state.
        raise ValueError('unsafe_metadata_legacy_path_resolution') from error


def _candidate_file_marker(queue, row):
    """Metadata only, never content; never follow a candidate file symlink."""
    digest = row['response_sha256']
    if not isinstance(digest, str) or _DIGEST_FILE.fullmatch(digest + '.parquet') is None:
        raise ValueError('invalid_metadata_legacy_digest')
    directory = _directory(queue.root, row['fingerprint'], row['feed_url'])

    def marker(path):
        try:
            value = path.stat(follow_symlinks=False)
        except FileNotFoundError:
            return None
        return [value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns, value.st_ctime_ns]

    directory_marker = marker(directory)
    if directory.is_symlink() or not _within_root(directory, queue.root):
        # Record the unsafe parent itself without examining its targets.
        return json.dumps([directory_marker, 'unsafe_directory'], separators=(',', ':'))
    # Other normal feed writes modify directory mtime/ctime but do not repair
    # this pair. Retain only the parent identity, not its unrelated activity.
    directory_marker = directory_marker[:3] if directory_marker is not None else None
    path = directory / (digest + '.parquet')
    return json.dumps([directory_marker, marker(path), marker(path.with_suffix('.parquet.receipt.json'))],
                      separators=(',', ':'))


def _scan_failure(queue, row, error):
    attempts = row['attempts'] + 1
    delay = min(300, 2 ** min(attempts, 8))
    with queue.db:
        queue.db.execute("UPDATE metadata_feed_legacy_directories SET state='retry',attempts=?,not_before=?,outcome=? "
            'WHERE provider=? AND feed_url=? AND fingerprint=?',
            (attempts, time.time() + delay, type(error).__name__, row['provider'], row['feed_url'], row['fingerprint']))


def _recheck_complete_directory(queue, source):
    """Examine one due directory, retaining the last scan until change is seen."""
    guard, parameters = _guard(source)
    row = queue.db.execute('SELECT * FROM metadata_feed_legacy_directories WHERE ' + guard +
        " AND state='complete' AND not_before<=? ORDER BY not_before,feed_url,fingerprint LIMIT 1",
        (*parameters, time.time())).fetchone()
    if row is None:
        return
    row = dict(row)
    path = _directory(queue.root, row['fingerprint'], row['feed_url'])
    try:
        if not _within_root(path, queue.root):
            raise ValueError('unsafe_metadata_legacy_directory')
        try:
            marker = _directory_marker(_directory_stamp(path))
        except FileNotFoundError:
            marker = _DIRECTORY_ABSENT
        with queue.db:
            if marker == row['directory_stamp']:
                queue.db.execute('UPDATE metadata_feed_legacy_directories SET not_before=? '
                    'WHERE provider=? AND feed_url=? AND fingerprint=?',
                    (time.time() + DIRECTORY_RECHECK_SECONDS, row['provider'], row['feed_url'], row['fingerprint']))
            else:
                # Older completed rows have no stamp: rescan once instead of
                # assuming their old observation proves the present directory.
                queue.db.execute("UPDATE metadata_feed_legacy_directories SET state='pending',not_before=0,"
                    "outcome='directory_changed_after_scan' WHERE provider=? AND feed_url=? AND fingerprint=?",
                    (row['provider'], row['feed_url'], row['fingerprint']))
    except (OSError, ValueError) as error:
        _scan_failure(queue, row, error)


def _scan(queue, source, *, limit, deadline, stop):
    scan = getattr(queue, '_metadata_legacy_scan', None)
    if scan is None:
        guard, parameters = _guard(source)
        row = queue.db.execute('SELECT * FROM metadata_feed_legacy_directories WHERE ' + guard +
            " AND state IN ('pending','retry') AND not_before<=? ORDER BY feed_url,fingerprint LIMIT 1",
            (*parameters, time.time())).fetchone()
        if row is None:
            _recheck_complete_directory(queue, source)
            row = queue.db.execute('SELECT * FROM metadata_feed_legacy_directories WHERE ' + guard +
                " AND state IN ('pending','retry') AND not_before<=? ORDER BY feed_url,fingerprint LIMIT 1",
                (*parameters, time.time())).fetchone()
            if row is None:
                return 0
        row = dict(row)
        path = _directory(queue.root, row['fingerprint'], row['feed_url'])
        try:
            if not _within_root(path, queue.root):
                raise ValueError('unsafe_metadata_legacy_directory')
            stamp = _directory_stamp(path)
            scan = {'row': row, 'path': path, 'stamp': stamp, 'entries': os.scandir(path)}
            queue._metadata_legacy_scan = scan
        except FileNotFoundError:
            with queue.db:
                queue.db.execute("UPDATE metadata_feed_legacy_directories SET state='complete',attempts=0,"
                    "not_before=?,outcome='directory_absent',directory_stamp=? "
                    'WHERE provider=? AND feed_url=? AND fingerprint=?',
                    (time.time() + DIRECTORY_RECHECK_SECONDS, _DIRECTORY_ABSENT,
                     row['provider'], row['feed_url'], row['fingerprint']))
            return 0
        except (OSError, ValueError) as error:
            _scan_failure(queue, row, error)
            return 0
    row, pending, inspected, finished = scan['row'], [], 0, False
    try:
        while inspected < limit and (inspected == 0 or time.monotonic() < deadline):
            if stop is not None and stop.is_set():
                break
            try:
                entry = next(scan['entries'])
            except StopIteration:
                finished = True
                break
            inspected += 1
            match = _DIGEST_FILE.fullmatch(entry.name)
            if match is not None and entry.is_file(follow_symlinks=False):
                digest = match.group(1)
                pending.append((row['provider'], row['feed_url'], row['fingerprint'], digest,
                                row['provider'], row['feed_url'], row['fingerprint'], digest))
        current_stamp = _directory_stamp(scan['path']) if finished else None
        with queue.db:
            queue.db.executemany('INSERT OR IGNORE INTO metadata_feed_legacy_originals '
                '(provider,feed_url,fingerprint,response_sha256,state) '
                "SELECT ?,?,?,?,'pending' WHERE NOT EXISTS (SELECT 1 FROM metadata_feed_observations "
                'WHERE provider=? AND feed_url=? AND parser_fingerprint=? AND response_sha256=?)', pending)
            if finished:
                unchanged = current_stamp == scan['stamp']
                queue.db.execute('UPDATE metadata_feed_legacy_directories SET state=?,attempts=0,not_before=?,outcome=?,directory_stamp=? '
                    'WHERE provider=? AND feed_url=? AND fingerprint=?',
                    ('complete' if unchanged else 'pending', time.time() + DIRECTORY_RECHECK_SECONDS if unchanged else 0,
                     'scanned' if unchanged else 'changed_during_scan', _directory_marker(current_stamp) if unchanged else None,
                     row['provider'], row['feed_url'], row['fingerprint']))
        if finished:
            close_scanner(queue)
        return inspected
    except (OSError, ValueError) as error:
        close_scanner(queue)
        _scan_failure(queue, row, error)
        return inspected


def _recover_candidate(queue, row, source):
    from downloader.news_fulltext import _missing_article_links
    from downloader.news_fulltext_metadata_feeds import compatible_fingerprints, require_request
    from downloader.news_fulltext_queue import utc_now

    url, fingerprint, digest = row['feed_url'], row['fingerprint'], row['response_sha256']
    current = require_request(queue.registry, url, url)
    if (current.provider != row['provider'] or current.provider != source.provider
            or fingerprint not in compatible_fingerprints(current)):
        raise NewsError('metadata_legacy_policy_changed')
    # The historical raw envelope does not contain HTTP headers. A synthetic
    # media type is used only to validate its retained RSS representation.
    value = {'head': {'url': url}, 'response_sha256': digest, 'metadata_fingerprint': fingerprint,
             'headers': {'content-type': 'application/rss+xml'}}
    _, original, items = _original(queue, value, current, legacy=True, _legacy_receipt_sized=True)
    relative = f'metadata_feeds/{fingerprint}/{hashlib.sha256(url.encode()).hexdigest()}/{digest}.parquet.receipt.json'
    with queue.db:
        existing = queue.db.execute('SELECT receipt_path,rows FROM metadata_feed_observations '
            'WHERE provider=? AND feed_url=? AND parser_fingerprint=? AND response_sha256=?',
            (row['provider'], url, fingerprint, digest)).fetchone()
        if existing is not None and (existing['receipt_path'] != relative or existing['rows'] != len(items)):
            raise ValueError('metadata_legacy_observation_conflict')
        queue.db.execute('INSERT OR IGNORE INTO metadata_feed_observations VALUES (?,?,?,?,?,?,?)',
            (row['provider'], url, fingerprint, digest, relative, len(items), utc_now()))
        links = _missing_article_links(queue, [(item['url'], item['title'], item['publication_raw']) for item in items])
        queue.add_articles([{'url': link, 'title': title, 'date': date} for link, title, date in links])
        queue.db.execute("UPDATE metadata_feed_legacy_originals SET state='complete',outcome='verified',"
            "original_observed_at_utc=?,invalid_file_stamp=NULL "
            'WHERE provider=? AND feed_url=? AND fingerprint=? AND response_sha256=?',
            (original['observed_at_utc'], row['provider'], url, fingerprint, digest))
    queue.metrics.count(row['provider'], 'legacy_metadata_originals_verified')
    return len(links)


def _process(queue, row, source):
    from downloader.news_fulltext_queue import utc_now

    rechecking = row['invalid_file_stamp'] == _INVALID_RECHECK
    before = None
    if rechecking:
        try:
            before = _candidate_file_marker(queue, row)
        except (OSError, ValueError) as error:
            _delay_invalid_check(queue, row, error)
            return 0
    try:
        return _recover_candidate(queue, row, source)
    except OSError as error:
        attempts = row['attempts'] + 1
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET state='retry',attempts=?,not_before=?,outcome=? "
                'WHERE provider=? AND feed_url=? AND fingerprint=? AND response_sha256=?',
                (attempts, time.time() + min(300, 2 ** min(attempts, 8)), type(error).__name__,
                 row['provider'], row['feed_url'], row['fingerprint'], row['response_sha256']))
    except (ValueError, TypeError, KeyError, OverflowError, NewsError, pa.ArrowException) as error:
        reason = error.code if isinstance(error, NewsError) else type(error).__name__
        try:
            after = _candidate_file_marker(queue, row) if rechecking else None
        except (OSError, ValueError):
            after = None
        unstable = rechecking and (after is None or after != before)
        # The first failure has no before-marker. Recheck it once after a short
        # delay; then only reparse again when the original/receipt pair changes.
        state = 'retry' if unstable else 'invalid'
        marker = _INVALID_RECHECK if unstable else after
        delay = 2 if unstable or not rechecking else DIRECTORY_RECHECK_SECONDS
        with queue.db:
            queue.db.execute('INSERT INTO metadata_feed_legacy_invalid_history '
                '(provider,feed_url,fingerprint,response_sha256,observed_at_utc,reason,event_kind) '
                "VALUES (?,?,?,?,?,?,'rejected')",
                (row['provider'], row['feed_url'], row['fingerprint'], row['response_sha256'], utc_now(), reason))
            queue.db.execute('UPDATE metadata_feed_legacy_originals SET state=?,outcome=?,not_before=?,invalid_file_stamp=? '
                'WHERE provider=? AND feed_url=? AND fingerprint=? AND response_sha256=?',
                (state, reason, time.time() + delay, marker,
                 row['provider'], row['feed_url'], row['fingerprint'], row['response_sha256']))
    return 0


def _delay_invalid_check(queue, row, error):
    attempts = row['attempts'] + 1
    delay = min(300, 2 ** min(attempts, 8)) if isinstance(error, OSError) else DIRECTORY_RECHECK_SECONDS
    with queue.db:
        queue.db.execute("UPDATE metadata_feed_legacy_originals SET state='invalid',attempts=?,not_before=? "
            'WHERE provider=? AND feed_url=? AND fingerprint=? AND response_sha256=?',
            (attempts, time.time() + delay, row['provider'], row['feed_url'], row['fingerprint'], row['response_sha256']))


def _recheck_invalid(queue, source):
    """Reopen only a changed invalid pair; retain old failure observations."""
    guard, parameters = _guard(source)
    row = queue.db.execute('SELECT * FROM metadata_feed_legacy_originals WHERE ' + guard +
        " AND state='invalid' AND not_before<=? ORDER BY not_before,feed_url,fingerprint,response_sha256 LIMIT 1",
        (*parameters, time.time())).fetchone()
    if row is None:
        return
    row = dict(row)
    scope = (row['provider'], row['feed_url'], row['fingerprint'], row['response_sha256'])
    try:
        marker = _candidate_file_marker(queue, row)
    except (OSError, ValueError) as error:
        _delay_invalid_check(queue, row, error)
        return
    with queue.db:
        if row['invalid_file_stamp'] is not None and marker == row['invalid_file_stamp']:
            queue.db.execute('UPDATE metadata_feed_legacy_originals SET not_before=? '
                'WHERE provider=? AND feed_url=? AND fingerprint=? AND response_sha256=?',
                (time.time() + DIRECTORY_RECHECK_SECONDS, *scope))
        else:
            # Older invalid states have no reliable observation time. Preserve
            # their recorded reason without fabricating one before retrying.
            previous = queue.db.execute('SELECT 1 FROM metadata_feed_legacy_invalid_history '
                'WHERE provider=? AND feed_url=? AND fingerprint=? AND response_sha256=? LIMIT 1', scope).fetchone()
            if previous is None:
                queue.db.execute('INSERT INTO metadata_feed_legacy_invalid_history '
                    '(provider,feed_url,fingerprint,response_sha256,observed_at_utc,reason,event_kind) '
                    "VALUES (?,?,?,?,NULL,?,'legacy_state')", (*scope, row['outcome']))
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET state='pending',not_before=0,invalid_file_stamp=? "
                'WHERE provider=? AND feed_url=? AND fingerprint=? AND response_sha256=?',
                (_INVALID_RECHECK, *scope))


def progress(db, source):
    """Count only discovered, not previously indexed, candidate originals."""
    if not source.metadata_feeds:
        return None
    guard, parameters = _guard(source)
    directories = dict(db.execute('SELECT state,count(*) FROM metadata_feed_legacy_directories WHERE ' +
                                  guard + ' GROUP BY state', parameters))
    if not directories:
        return None
    originals = dict(db.execute('SELECT state,count(*) FROM metadata_feed_legacy_originals WHERE ' +
                                guard + ' GROUP BY state', parameters))
    expected = len(source.metadata_feeds) * len(_scope(source)[2])
    registered = sum(directories.values())
    completed = directories.get('complete', 0)
    discovered = sum(originals.values())
    return {'contract_version': VERSION, 'expected_directories': expected,
        'registered_directories': registered, 'examined_directories': completed,
        'candidate_originals': discovered, 'verified_originals': originals.get('complete', 0),
        'pending_originals': originals.get('pending', 0), 'retry_originals': originals.get('retry', 0),
        'invalid_originals': originals.get('invalid', 0),
        'other_originals': sum(count for state, count in originals.items()
                               if state not in {'complete', 'pending', 'retry', 'invalid'}),
        'discovery_complete': registered == expected and completed == expected}


def backfill(queue, source, *, stop=None, scan_limit=256, item_limit=8, max_seconds=.05):
    """Bound each owner slice; committed work and directory state survive restart."""
    if (type(scan_limit) is not int or not 1 <= scan_limit <= 4096
            or type(item_limit) is not int or not 1 <= item_limit <= 128
            or not 0 < max_seconds <= 10):
        raise ValueError('invalid_metadata_legacy_slice')
    if not source or not source.metadata_feeds:
        return {'ready': False, 'next_retry_epoch': None, 'scanned': 0, 'processed': 0, 'new_urls': 0}
    _ensure_scope(queue, source)
    deadline = time.monotonic() + max_seconds
    scanned = _scan(queue, source, limit=scan_limit, deadline=deadline, stop=stop)
    if stop is None or not stop.is_set():
        _recheck_invalid(queue, source)
    processed = new_urls = 0
    guard, parameters = _guard(source)
    while processed < item_limit and (processed == 0 or time.monotonic() < deadline):
        if stop is not None and stop.is_set():
            break
        now = time.time()
        rows = [queue.db.execute('SELECT * FROM metadata_feed_legacy_originals WHERE ' + guard +
            ' AND state=? AND not_before<=? ORDER BY not_before,feed_url,fingerprint,response_sha256 LIMIT 1',
            (*parameters, state, now)).fetchone() for state in ('pending', 'retry')]
        rows = [row for row in rows if row is not None]
        if not rows:
            break
        row = min(rows, key=lambda item: (item['not_before'], item['feed_url'], item['fingerprint'], item['response_sha256']))
        new_urls += _process(queue, dict(row), source)
        processed += 1
    due = []
    for table, states in (('metadata_feed_legacy_directories', "('pending','retry','complete')"),
                          ('metadata_feed_legacy_originals', "('pending','retry','invalid')")):
        value = queue.db.execute('SELECT min(not_before) FROM ' + table + ' WHERE ' + guard +
                                 ' AND state IN ' + states, parameters).fetchone()[0]
        if value is not None:
            due.append(value)
    next_retry = min(due) if due else None
    return {'ready': next_retry is not None and next_retry <= time.time(), 'next_retry_epoch': next_retry,
            'scanned': scanned, 'processed': processed, 'new_urls': new_urls}
