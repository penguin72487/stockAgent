"""Bounded recovery of retained public Cnyes envelopes; never another crawler.

The canonical queue owner reparses original SHA-verified responses, preserving
their observation clocks. Existing heads, leases and retry/refusal deadlines
are not replaced. Receipt/SQLite acceptance recovery remains authoritative.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records, wordpress_base
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import source_path, utc_now


REPLAY_CHECKPOINT_CONTRACT_VERSION = 2


def ensure_replay_schema(queue):
    queue.db.execute('''CREATE TABLE IF NOT EXISTS cached_bulk_replays(
        response_sha256 TEXT NOT NULL, source_fingerprint TEXT NOT NULL,
        priority REAL NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
        error_code TEXT, accepted_heads INTEGER NOT NULL DEFAULT 0,
        checked_at_utc TEXT, checkpoint_contract_version INTEGER NOT NULL DEFAULT 2,
        PRIMARY KEY(response_sha256,source_fingerprint)) WITHOUT ROWID''')
    columns = {row[1] for row in queue.db.execute('PRAGMA table_info(cached_bulk_replays)')}
    if 'checkpoint_contract_version' not in columns:
        # Legacy "checked" did not prove that temporarily blocked heads were
        # revisited. Re-examine those exact source checkpoints incrementally,
        # without resetting articles, receipts, failures or publisher deadlines.
        queue.db.execute('ALTER TABLE cached_bulk_replays ADD COLUMN '
                         'checkpoint_contract_version INTEGER NOT NULL DEFAULT 1')
    queue.db.execute('CREATE INDEX IF NOT EXISTS cached_bulk_replay_ready '
                     'ON cached_bulk_replays(source_fingerprint,state,priority,response_sha256)')
    queue.db.execute('CREATE INDEX IF NOT EXISTS cached_bulk_replay_contract_ready '
                     'ON cached_bulk_replays(source_fingerprint,state,checkpoint_contract_version,priority,response_sha256)')
    queue.db.execute('''CREATE TABLE IF NOT EXISTS cached_bulk_replay_waits(
        source_fingerprint TEXT NOT NULL, response_sha256 TEXT NOT NULL,
        article_id BLOB NOT NULL,
        PRIMARY KEY(source_fingerprint,response_sha256,article_id)) WITHOUT ROWID''')


def _ready_replays(queue, fingerprint, maximum):
    # Wake a deferred page only when a blocked article changes or its original
    # retry expires. Inspect indexed head metadata, not the raw envelope on
    # every ten-second maintenance tick. In-flight work is never overwritten.
    waiting = queue.db.execute('''SELECT DISTINCT c.response_sha256,c.priority
        FROM cached_bulk_replay_waits w
        JOIN cached_bulk_replays c ON c.source_fingerprint=w.source_fingerprint
            AND c.response_sha256=w.response_sha256
        LEFT JOIN articles a ON a.id=w.article_id
        WHERE w.source_fingerprint=? AND c.state='deferred' AND
            (a.id IS NULL OR a.body_sha256 IS NOT NULL OR
             (a.state!='inflight' AND (a.state NOT IN ('retry','retry_cooldown') OR a.due<=?)))
        ORDER BY c.priority,c.response_sha256 LIMIT ?''', (fingerprint, time.time(), maximum)).fetchall()
    candidates = list(waiting)
    if len(candidates) < maximum:
        remaining = maximum - len(candidates)
        fresh = queue.db.execute('''SELECT response_sha256,priority FROM cached_bulk_replays
            WHERE source_fingerprint=? AND state='pending'
            ORDER BY priority,response_sha256 LIMIT ?''', (fingerprint, remaining)).fetchall()
        legacy = queue.db.execute('''SELECT response_sha256,priority FROM cached_bulk_replays
            WHERE source_fingerprint=? AND state='checked' AND checkpoint_contract_version=1
            ORDER BY priority,response_sha256 LIMIT ?''', (fingerprint, remaining)).fetchall()
        # Indexed, independently bounded branches, not an OR that sorts the
        # entire retained lake every tick. Waiting bodies get first service;
        # other work merges at most 2 * maximum compact metadata rows.
        candidates.extend(sorted(list(fresh) + list(legacy), key=lambda row: (row[1], row[0]))[:remaining])
    return candidates


def checkpoint_no_body_cnyes_replay(queue, source, digest):
    """Within the caller's transaction, record that this API parse had no body.

    This is NOT article acceptance or a storage-integrity claim. The original
    envelope and item errors remain retained, and HTML fallback stays pending.
    API/auth/config changes create a different fingerprint and permit replay.
    """
    ensure_replay_schema(queue)
    changed = queue.db.execute('''INSERT INTO cached_bulk_replays
        (response_sha256,source_fingerprint,priority,state,error_code,checked_at_utc)
        VALUES (?,?,?,'no_replayable_body','current_live_parser_found_no_source_bodies',?)
        ON CONFLICT(response_sha256,source_fingerprint) DO UPDATE SET
            state=excluded.state,error_code=excluded.error_code,checked_at_utc=excluded.checked_at_utc,
            checkpoint_contract_version=excluded.checkpoint_contract_version
        WHERE cached_bulk_replays.state='pending' ''',
        (digest, cnyes_bulk_fingerprint(source), -time.time(), utc_now())).rowcount
    if changed:
        queue.metrics.count('cnyes', 'cached_bulk_no_body_replays_avoided')


def _inventory(queue, source, *, maximum_entries=4096, maximum_wall_seconds=0.05, stop=None):
    """Cooperative hourly filename discovery, not source/body validation.

    Keep one bounded directory iterator per owner. Existing exact-fingerprint
    checkpoints need no file stat or redundant INSERT. Every new replay still
    undergoes the unchanged receipt/SHA/authorization/clock validation below.
    Completion, not one slice, starts the hourly rescan clock.
    """
    if (type(maximum_entries) is not int or not 1 <= maximum_entries <= 8192
            or type(maximum_wall_seconds) not in (int, float)
            or not math.isfinite(maximum_wall_seconds) or not 0 < maximum_wall_seconds <= 0.1):
        raise ValueError('unbounded cached inventory slice')
    fingerprint = cnyes_bulk_fingerprint(source)
    now = time.monotonic()
    if (getattr(queue, '_cached_cnyes_inventory_fingerprint', None) == fingerprint
            and getattr(queue, '_cached_cnyes_inventory_at', -float('inf')) + 3600 > now):
        return
    if stop and stop.is_set():
        return
    ensure_replay_schema(queue)
    with queue.db:
        # v21 used the HTML+API fingerprint for API replay. Reuse only examined
        # checkpoints of that exact configuration; do not reset failed proofs,
        # pending work or existing destination outcomes. This registry-derived
        # pair differs by the short-HTML ABI alone, never by rights/API rules.
        if source.cnyes_bulk_data_fingerprint and source.data_fingerprint != fingerprint:
            marker = 'cnyes_cached_api_contract_v1:' + source.data_fingerprint + ':' + fingerprint
            if not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
                migrated = queue.db.execute('''INSERT INTO cached_bulk_replays
                    (response_sha256,source_fingerprint,priority,state,error_code,accepted_heads,
                     checked_at_utc,checkpoint_contract_version)
                    SELECT response_sha256,?,priority,state,error_code,accepted_heads,checked_at_utc,checkpoint_contract_version
                    FROM cached_bulk_replays WHERE source_fingerprint=?
                        AND state IN ('checked','failed_validation','other_source','no_replayable_body')
                    ON CONFLICT(response_sha256,source_fingerprint) DO UPDATE SET
                        state=excluded.state,error_code=excluded.error_code,
                        accepted_heads=excluded.accepted_heads,checked_at_utc=excluded.checked_at_utc,
                        checkpoint_contract_version=excluded.checkpoint_contract_version
                    WHERE cached_bulk_replays.state='pending' ''',
                    (fingerprint, source.data_fingerprint)).rowcount
                queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
                                 (marker, json.dumps({'examined_checkpoints_reused': migrated})))
                queue.metrics.count('cnyes', 'cached_bulk_html_only_checkpoints_reused', migrated)
    entries = getattr(queue, '_cached_cnyes_inventory_entries', None)
    if entries is not None and getattr(queue, '_cached_cnyes_inventory_scan_fingerprint', None) != fingerprint:
        entries.close()
        entries = None
    if entries is None:
        try:
            entries = os.scandir(queue.root / 'quarantine')
        except (FileNotFoundError, NotADirectoryError):
            queue._cached_cnyes_inventory_at = time.monotonic()
            queue._cached_cnyes_inventory_fingerprint = fingerprint
            queue._cached_cnyes_inventory_entries = None
            return
        queue._cached_cnyes_inventory_entries = entries
        queue._cached_cnyes_inventory_scan_fingerprint = fingerprint
    examined, indexed, skipped, complete = 0, 0, 0, False
    with queue.metrics.measure('cached_bulk_inventory', 'cnyes'):
        while examined < maximum_entries and time.monotonic() - now < maximum_wall_seconds:
            if stop and stop.is_set():
                break
            # At most256 indexed digest lookups/stat/insert operations before
            # the next wall/stop check. Do not materialize the whole directory
            # or the entire replay index in memory.
            candidates = []
            for _ in range(min(256, maximum_entries - examined)):
                try:
                    entry = next(entries)
                except StopIteration:
                    complete = True
                    break
                examined += 1
                if re.fullmatch(r'[0-9a-f]{64}[.]parquet', entry.name) and not entry.is_symlink():
                    candidates.append(entry)
            if candidates:
                digests = [entry.name[:-8] for entry in candidates]
                known = {row[0] for row in queue.db.execute('SELECT response_sha256 FROM cached_bulk_replays '
                    'WHERE source_fingerprint=? AND response_sha256 IN (' + ','.join('?' for _ in digests) + ')',
                    (fingerprint, *digests))}
                rows = [(entry.name[:-8], fingerprint, -entry.stat(follow_symlinks=False).st_mtime)
                        for entry in candidates if entry.name[:-8] not in known]
                with queue.db:
                    queue.db.executemany('INSERT OR IGNORE INTO cached_bulk_replays '
                        '(response_sha256,source_fingerprint,priority) VALUES (?,?,?)', rows)
                indexed += len(rows)
                skipped += len(known)
            if complete:
                entries.close()
                queue._cached_cnyes_inventory_entries = None
                queue._cached_cnyes_inventory_at = time.monotonic()
                queue._cached_cnyes_inventory_fingerprint = fingerprint
                break
    queue.metrics.count('cnyes', 'cached_bulk_inventory_entries_examined', examined)
    queue.metrics.count('cnyes', 'cached_bulk_inventory_new_envelopes_indexed', indexed)
    queue.metrics.count('cnyes', 'cached_bulk_inventory_existing_envelopes_skipped', skipped)
    queue.metrics.count('cnyes', 'cached_bulk_inventory_slices')
    if complete:
        queue.metrics.count('cnyes', 'cached_bulk_inventory_scans_completed')


def replay_cached_cnyes(queue, *, maximum_pages=64, maximum_wall_seconds=0.75,
                        maximum_raw_bytes=8 << 20, stop=None):
    """Revalidate cached bodies with current source contract in bounded quanta.

    Only literal-public rows are eligible. A cache is not a current HTTP
    response or permission to promote metadata, summaries, paid text or OCR.
    Fullbody acceptance and replay checkpoints are separate crash-safe commits:
    an interrupted checkpoint simply sees the already committed head on retry.
    """
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        return {'pages_checked': 0, 'accepted_heads': 0, 'http_requests': 0}
    if not 1 <= maximum_pages <= 64 or not 0 < maximum_wall_seconds <= 2 or not 1 <= maximum_raw_bytes <= 16 << 20:
        raise ValueError('unbounded cached news replay')
    started = time.monotonic()
    with queue.metrics.measure('cached_bulk_replay', 'cnyes'):
        fingerprint = cnyes_bulk_fingerprint(source)
        _inventory(queue, source, maximum_wall_seconds=min(0.05, maximum_wall_seconds), stop=stop)
        jobs = _ready_replays(queue, fingerprint, maximum_pages)
        totals = Counter(pages_checked=0, accepted_heads=0, raw_bytes_checked=0, still_missing_body_rows=0)
        for job in jobs:
            if time.monotonic() - started >= maximum_wall_seconds or totals['raw_bytes_checked'] >= maximum_raw_bytes or stop and stop.is_set():
                break
            digest = job[0]
            accepted, error_code, state = 0, None, 'checked'
            eligible, waiting_ids = [], []
            try:
                relative = 'quarantine/' + digest + '.parquet'
                path = source_path(queue.root, relative)
                if path.stat().st_size > maximum_raw_bytes:
                    raise ValueError('cached_bulk_envelope_oversized')
                receipt = json.loads(source_path(queue.root, relative + '.receipt.json').read_text())
                if (receipt['parquet_path'] != relative or receipt['publish'] is not False
                        or receipt['response_sha256'] != digest or sha256_file(path) != receipt['sha256']):
                    raise ValueError('cached_bulk_receipt_invalid')
                table = pq.read_table(path, use_threads=False)
                if table.num_rows != 1:
                    raise ValueError('cached_bulk_grain_invalid')
                raw = table.to_pylist()[0]
                if len(raw['raw_response_body']) > maximum_raw_bytes:
                    raise ValueError('cached_bulk_raw_response_oversized')
                totals['raw_bytes_checked'] += len(raw['raw_response_body'])
                if (raw['publish'] is not False or raw['response_sha256'] != digest
                        or hashlib.sha256(raw['raw_response_body']).hexdigest() != digest):
                    raise ValueError('cached_bulk_source_digest_invalid')
                current = queue.registry.require(raw['request_url'])
                if current.provider != 'cnyes' or wordpress_base(raw['request_url']) not in source.cnyes_apis:
                    state = 'other_source'
                else:
                    if (raw['authorization_fingerprint'] != source.authorization_fingerprint
                            or json.loads(raw['authorization_json']) != source.authorization):
                        raise ValueError('cached_bulk_authorization_changed')
                    captured = datetime.fromisoformat(raw['captured_at_utc'])
                    if captured.tzinfo is None or captured > datetime.now(timezone.utc):
                        raise ValueError('cached_bulk_observation_clock_invalid')
                    # JSON MIME/status were checked by the original canonical
                    # adapter before quarantine; statusCode/pagination are
                    # rechecked here. This is explicitly NOT new HTTP traffic.
                    response = Response(raw['request_url'], 200, {'content-type': 'application/json'}, raw['raw_response_body'])
                    _, posts = cnyes_pagination(response)
                    if any(type(p.get('payment')) is not int or p['payment'] != 0 for p in posts):
                        raise NewsError('bulk_post_not_public')
                    if receipt['article_count'] != len(posts):
                        raise ValueError('cached_bulk_row_count_invalid')
                    records, errors = cnyes_records(response, SimpleNamespace(registry=queue.registry), source)
                    for record in records:
                        row = queue.db.execute('SELECT state,due,body_sha256 FROM articles WHERE id=?',
                                               (bytes.fromhex(record['article_id']),)).fetchone()
                        if row and row['body_sha256']:
                            continue
                        if row and (row['state'] == 'inflight' or
                                    row['state'] in {'retry', 'retry_cooldown'} and row['due'] > time.time()):
                            waiting_ids.append(bytes.fromhex(record['article_id']))
                            continue
                        if row and row['state'] not in {'pending', 'retry', 'retry_cooldown', 'body_not_found'}:
                            continue
                        record.update(captured_at_utc=raw['captured_at_utc'],
                                      source_observed_at_utc=raw['captured_at_utc'], reparsed_at_utc=utc_now(),
                                      acquisition_transport='retained_raw_reparse', replay_source_parquet=relative)
                        eligible.append(record)
                    totals['still_missing_body_rows'] += sum(e['error'] not in {'bulk_exact_duplicate_row'} for e in errors)
            except (ValueError, KeyError, OSError, TypeError, NewsError, pa.ArrowException) as error:
                error_code = error.code if isinstance(error, NewsError) else str(error)[:160]
                state = 'failed_validation'
                queue.metrics.count('cnyes', 'cached_bulk_replay_validation_errors')
            if not error_code and eligible:
                # Storage/commit errors must propagate: do NOT checkpoint a
                # failed acceptance as a permanently examined source page.
                # The same canonical receipt recovery safely resumes it.
                with queue.db:
                    queue.add_articles([{'url': r['requested_url'], 'title': r['title'], 'date': r['metadata_date_raw']} for r in eligible])
                eligible[0]['raw_response_body'] = response.body
                for record in eligible:
                    record['source_response_article_id'] = eligible[0]['article_id']
                queue.accept(eligible)
                accepted = len(eligible)
            with queue.db:
                queue.db.execute('DELETE FROM cached_bulk_replay_waits WHERE source_fingerprint=? AND response_sha256=?',
                                 (fingerprint, digest))
                if not error_code and waiting_ids:
                    state = 'deferred'
                    queue.db.executemany('INSERT INTO cached_bulk_replay_waits VALUES (?,?,?)',
                                         ((fingerprint, digest, key) for key in waiting_ids))
                    queue.metrics.count('cnyes', 'cached_bulk_temporarily_blocked_heads', len(waiting_ids))
                queue.db.execute('UPDATE cached_bulk_replays SET state=?,error_code=?,accepted_heads=accepted_heads+?,checked_at_utc=?,checkpoint_contract_version=? '
                    'WHERE response_sha256=? AND source_fingerprint=?',
                    (state, error_code, accepted, utc_now(), REPLAY_CHECKPOINT_CONTRACT_VERSION, digest, fingerprint))
                queue.counter('cached_cnyes_pages_checked', 1)
                queue.counter('cached_cnyes_heads_recovered', accepted)
                if error_code:
                    queue.counter('cached_cnyes_validation_failures', 1)
            totals['pages_checked'] += 1
            totals['accepted_heads'] += accepted
            queue.metrics.count('cnyes', 'cached_bulk_replay_pages_checked')
            queue.metrics.count('cnyes', 'cached_bulk_replay_new_heads', accepted)
    return {**totals, 'http_requests': 0, 'all_history_complete': False}
