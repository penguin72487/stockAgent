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
RETAINED_FAILURE_REPLAY_VERSION = 1
RETAINED_NONTEXT_OUTCOME_KIND = 'reviewed_retained_nontext_outcome'
_ORIGINAL_REFERENCE_FIELDS = frozenset({'kind','path','sha256','receipt_path','receipt_sha256','response_sha256'})


def validate_nontext_original_reference(reference):
    """Only canonical immutable originals; no arbitrary local-file reader."""
    if not isinstance(reference,dict) or set(reference)!=_ORIGINAL_REFERENCE_FIELDS:
        raise ValueError('invalid_retained_outcome_reference')
    if any(not isinstance(reference[key],str) or re.fullmatch(r'[0-9a-f]{64}',reference[key]) is None
           for key in ('sha256','receipt_sha256','response_sha256')):
        raise ValueError('invalid_retained_outcome_reference')
    if reference['kind']=='native_candidate_original':
        pattern=r'reviewed_repairs/candidate_diagnostics/originals/[0-9a-f]{64}\.parquet'
    elif reference['kind']=='failure_diagnostic_original':
        pattern=r'diagnostics/[0-9]{8}/globalvoices/[0-9]{8}T[0-9]{12}Z-[0-9a-f]{8}\.parquet'
    else:
        raise ValueError('invalid_retained_outcome_reference')
    if (not isinstance(reference['path'],str) or re.fullmatch(pattern,reference['path']) is None
            or reference['receipt_path']!=reference['path'][:-8]+'.receipt.json'):
        raise ValueError('invalid_retained_outcome_reference')


def classify_retained_nontext_outcome(queue, source, target, reference):
    """Existing single writer projects one verified original, without HTTP.

    This is a missing-only source outcome, not body acceptance, a new check or
    an invented publication clock. The existing repair transaction/receipt
    owns crash recovery; invalid/stale evidence never turns into a retry.
    """
    from dataclasses import replace
    from downloader.news_fulltext_candidate_originals import private_globalvoices_original_permit
    from downloader.news_fulltext_policy import GLOBALVOICES_PHOTO_CREDIT_OUTCOME_CONTRACT_VERSION
    from downloader.news_fulltext_publishers import globalvoices_verified_nontext_body
    from downloader.news_fulltext_repairs import _clock,_head,_read_private_request

    validate_nontext_original_reference(reference)
    if (source.provider!='globalvoices' or GLOBALVOICES_PHOTO_CREDIT_OUTCOME_CONTRACT_VERSION!=1
            or target['state']!='body_not_found' or target['provider']!='globalvoices'
            or not re.fullmatch(r'[0-9a-f]{64}',source.parser_repair_prior_capture_fingerprint)
            or source.data_fingerprint==source.parser_repair_prior_capture_fingerprint):
        raise ValueError('unsupported_retained_outcome_contract')
    paths=[source_path(queue.root,reference[key]) for key in ('path','receipt_path')]
    original,receipt_path=paths
    if (any((queue.root/reference[key]).is_symlink() for key in ('path','receipt_path'))
            or original.stat().st_size>4<<20 or receipt_path.stat().st_size>65536
            or sha256_file(original)!=reference['sha256'] or sha256_file(receipt_path)!=reference['receipt_sha256']):
        raise ValueError('invalid_retained_outcome_evidence')
    receipt=json.loads(receipt_path.read_bytes());parquet=pq.ParquetFile(original)
    if (parquet.metadata.num_rows!=1 or parquet.metadata.num_row_groups!=1
            or parquet.metadata.row_group(0).total_byte_size>4<<20):
        raise ValueError('unbounded_retained_outcome_evidence')
    record,=parquet.read().to_pylist();raw=record['raw_response_body']
    old_capture=source.parser_repair_prior_capture_fingerprint
    if (not isinstance(raw,bytes) or not 0<len(raw)<=2<<20
            or hashlib.sha256(raw).hexdigest()!=reference['response_sha256']
            or record['response_sha256']!=reference['response_sha256']
            or record['request_url']!=target['url'] or record['final_url']!=target['url']
            or record['provider']!='globalvoices' or record['data_fingerprint']!=old_capture
            or record.get('diagnostic_only') is not True or record.get('accepted_as_article') is not False
            or record.get('publish') is not False):
        raise ValueError('invalid_retained_outcome_evidence')
    if reference['kind']=='native_candidate_original':
        request_id=original.stem
        processed=queue.root/'reviewed_repairs/candidate_diagnostics/processed'
        request=_read_private_request(processed/(request_id+'.json'))
        outcome_path=processed/(request_id+'.outcome.json')
        if outcome_path.is_symlink() or outcome_path.stat().st_size>65536:
            raise ValueError('invalid_retained_outcome_actor')
        outcome=json.loads(outcome_path.read_bytes())
        job=queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?',(request_id,)).fetchone()
        expected={'path':reference['path'],'sha256':reference['sha256'],'receipt_path':reference['receipt_path'],
            'response_sha256':reference['response_sha256'],'response_bytes':len(raw)}
        if (request.get('evidence_contract_version')!=13 or request.get('target')!=target
                or request.get('source')!={'contract_version':1,'data_fingerprint':old_capture,
                    'parser_fingerprint':source.parser_repair_fingerprint,'authorization_fingerprint':source.authorization_fingerprint}
                or job is None or job['state']!='observed' or job['owner_id']!=outcome.get('owner_id')
                or json.loads(job['request_json'])!=request or outcome.get('state')!='observed'
                or outcome.get('raw_retention_state')!='retained_verified'
                or outcome.get('article_heads_mutated') is not False
                or json.loads(job['result_json'])!=outcome.get('result')
                or outcome['result'].get('private_original')!=expected or receipt.get('original')!=expected
                or record.get('request_id')!=request_id or record.get('source_document_url')!=target['url']
                or json.loads(record['transport_redirects_json'])!=[]
                or record.get('authorization_fingerprint')!=source.authorization_fingerprint
                or not _clock(target['last_checked_at_utc'])<=_clock(record['observed_at_utc'])<=time.time()):
            raise ValueError('stale_or_invalid_retained_outcome_actor')
    else:
        # These old failure sidecars predate a stored target witness. Only
        # the original first failure, committed within one second of response
        # observation, is eligible; a later check/attempt is never overwritten.
        observed=_clock(receipt['captured_at_utc']);checked=_clock(target['last_checked_at_utc'])
        if (target['attempts']!=1 or not 0<=checked-observed<=1 or observed>time.time()
                or receipt.get('provider')!='globalvoices' or receipt.get('error_code')!='body_not_found'
                or receipt.get('raw_response_retained') is not True or receipt.get('diagnostic_only') is not True
                or receipt.get('accepted_as_article') is not False or receipt.get('publish') is not False
                or receipt.get('data_fingerprint')!=old_capture or receipt.get('path')!=reference['path']
                or receipt.get('sha256')!=reference['sha256'] or receipt.get('response_sha256')!=reference['response_sha256']
                or receipt.get('request_url')!=target['url'] or receipt.get('final_url')!=target['url']
                or receipt.get('status')!=200 or record.get('status')!=200
                or receipt.get('response_bytes')!=len(raw) or record.get('captured_at_utc')!=receipt['captured_at_utc']
                or json.loads(record['authorization_json'])!=source.authorization
                or json.loads(receipt['authorization_json'])!=source.authorization):
            raise ValueError('stale_or_invalid_retained_failure_outcome')
    # One DOM parse supplies the old permit and the new nontext decision.
    # No lower body threshold, response re-download, original rewrite or body
    # writer is involved. Every native CC/author/clock/access gate still runs.
    headers=json.loads(record['headers_json'])
    if not isinstance(headers,dict) or any(not isinstance(k,str) or not isinstance(v,str) for k,v in headers.items()):
        raise ValueError('invalid_retained_outcome_headers')
    response=Response(target['url'],200,headers,raw)
    old_source=replace(source,data_fingerprint=old_capture)
    permit,document=private_globalvoices_original_permit(response,old_source,target['url'],include_document=True)
    if reference['kind']=='native_candidate_original' and receipt.get('permit')!=permit:
        raise ValueError('invalid_retained_outcome_permit')
    if globalvoices_verified_nontext_body(document,source,page_url=target['url'])!='source_body_image_only':
        return 'retained_nontext_outcome_unresolved'
    current=queue.db.execute('SELECT * FROM articles WHERE id=?',(bytes.fromhex(target['id']),)).fetchone()
    from downloader.news_fulltext_repairs import _head_problem
    if _head_problem(current,time.time(),registry=queue.registry) or _head(current)!=target:
        return 'head_changed_during_retained_validation'
    queue.db.execute("UPDATE articles SET state='source_body_image_only',error_code='source_body_image_only' WHERE id=?",
        (current['id'],))
    return 'classified_source_body_image_only'


def replay_retained_cnyes_failures(queue, *, maximum_responses=4, maximum_wall_seconds=0.5,
                                 maximum_raw_bytes=8 << 20, stop=None):
    """Repair the explicit diagnostic cohort through the existing body writer.

    No discovery, publisher request, deadline reset or new evidence budget.
    Indexed finite ledger entries are checked once per parser contract. A
    newer HTTP outcome, active lease, future retry or existing body is never
    replaced. Storage failures propagate before checkpointing, so the shared
    durable receipt recovery remains the only acceptance authority.
    """
    from downloader.news_fulltext import article_response_record, extract_article_response
    from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
    focus = getattr(queue, 'failure_diagnostic_focus', None)
    source = queue.registry.sources.get('cnyes')
    if (focus is None or not source or not source.allowed or source.acquisition_mode == 'finite'):
        return {'responses_checked': 0, 'accepted_heads': 0, 'http_requests': 0}
    if (type(maximum_responses) is not int or not 1 <= maximum_responses <= 16
            or type(maximum_wall_seconds) not in (int, float) or not math.isfinite(maximum_wall_seconds)
            or not 0 < maximum_wall_seconds <= 2 or type(maximum_raw_bytes) is not int
            or not 1 <= maximum_raw_bytes <= 16 << 20):
        raise ValueError('unbounded retained failure replay')
    with queue.db:
        queue.db.execute('''CREATE TABLE IF NOT EXISTS retained_failure_replays(
            receipt_path TEXT NOT NULL,parser_fingerprint TEXT NOT NULL,state TEXT NOT NULL,
            error_code TEXT,checked_at_utc TEXT NOT NULL,
            PRIMARY KEY(receipt_path,parser_fingerprint)) WITHOUT ROWID''')
    jobs = queue.db.execute('''SELECT f.url_sha256,f.attempt_number,f.reserved_bytes,
            f.response_sha256,f.receipt_path FROM failure_evidence_focus_attempts f
        JOIN articles a ON a.id=f.url_sha256 AND a.provider=f.provider
        WHERE f.cohort_id=? AND f.outcome='raw_saved' AND NOT EXISTS(
            SELECT 1 FROM retained_failure_replays r WHERE r.receipt_path=f.receipt_path
                AND r.parser_fingerprint=?)
            AND a.body_sha256 IS NULL AND a.state IN ('pending','retry','retry_cooldown','body_not_found')
            AND a.lease_owner IS NULL AND a.lease_until IS NULL AND a.due<=?
        ORDER BY f.attempt_number DESC LIMIT 128''',
        (focus.id, source.parser_repair_fingerprint, time.time())).fetchall()
    started = time.monotonic()
    totals = Counter(responses_checked=0, accepted_heads=0, raw_bytes_checked=0)
    for job in jobs:
        if (totals['responses_checked'] >= maximum_responses or time.monotonic() - started >= maximum_wall_seconds
                or stop and stop.is_set()):
            break
        head = queue.db.execute('SELECT * FROM articles WHERE id=? AND provider=?', (job[0], 'cnyes')).fetchone()
        if (head is None or head['body_sha256'] is not None or head['state'] not in {'pending', 'retry', 'retry_cooldown', 'body_not_found'}
                or head['lease_owner'] is not None or head['lease_until'] is not None):
            continue
        if head['due'] > time.time():
            continue  # The same persisted retry deadline, not another HTTP lane.
        if job[2] > maximum_raw_bytes - totals['raw_bytes_checked']:
            continue
        state, code, record = 'parse_unresolved', None, None
        try:
            receipt, raw, outcome, receipt_sha = focus.read_retained_response(job)
            totals['raw_bytes_checked'] += job[2]
            observed = datetime.fromisoformat(raw['captured_at_utc'])
            checked = datetime.fromisoformat(head['last_checked_at_utc'])
            if (outcome != 'raw_saved' or raw['request_url'] != head['url']
                    or raw['final_url'] != head['url'] or head['url'] not in focus.targets
                    or json.loads(raw['authorization_json']) != source.authorization
                    or observed.utcoffset() is None or checked.utcoffset() is None
                    or observed > datetime.now(timezone.utc) or not 0 <= (checked - observed).total_seconds() <= 60):
                raise ValueError('retained_failure_superseded_or_scope_changed')
            headers = json.loads(raw['headers_json'])
            if not isinstance(headers, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in headers.items()):
                raise ValueError('retained_failure_headers_invalid')
            response = Response(raw['final_url'], raw['status'], headers, raw['raw_response_body'])
            with queue.metrics.measure('retained_failure_extraction', 'cnyes'):
                article = extract_article_response(response, source)
            record = article_response_record(source, dict(head), response, article,
                captured_at_utc=raw['captured_at_utc'], http_duration_seconds=None,
                parse_duration_seconds=None, duration_seconds=None,
                resolution_basis='verified_finite_diagnostic_original; not a new HTTP response')
            record.update(acquisition_transport='retained_raw_reparse',
                source_observed_at_utc=raw['captured_at_utc'], reparsed_at_utc=utc_now(),
                replay_source_parquet=receipt['path'], replay_source_receipt=job[4],
                replay_source_receipt_sha256=receipt_sha,
                retained_failure_replay_contract_version=RETAINED_FAILURE_REPLAY_VERSION,
                retained_failure_scope_id=focus.id, retained_failure_attempt_number=job[1],
                historical_point_in_time=False)
            state = 'validated'
        except (OSError, ValueError, TypeError, KeyError, NewsError, pa.ArrowException) as error:
            code = error.code if isinstance(error, NewsError) else 'retained_failure_evidence_invalid'
            state = 'parse_unresolved' if isinstance(error, NewsError) else 'failed_validation'
            queue.metrics.count('cnyes', 'retained_failure_' + state)
        totals['responses_checked'] += 1
        if record is not None and state == 'validated':
            # Claim only AFTER validation. No failed local parse consumes an
            # HTTP attempt or changes the original source failure/deadline.
            row = queue.claim_url(head['url'], 600)
            if row is None:
                continue
            witness = queue.db.execute('SELECT * FROM articles WHERE id=?', (job[0],)).fetchone()
            record['retained_failure_head_json'] = json.dumps(
                {key: witness[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS}, sort_keys=True)
            queue.accept([record])  # Disk/receipt/commit errors intentionally propagate.
            totals['accepted_heads'] += 1
            state = 'accepted'
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO retained_failure_replays VALUES (?,?,?,?,?)',
                (job[4], source.parser_repair_fingerprint, state, code, utc_now()))
    for key in ('responses_checked', 'accepted_heads'):
        queue.metrics.count('cnyes', 'retained_failure_' + key, totals[key])
    return {**totals, 'contract_version': RETAINED_FAILURE_REPLAY_VERSION, 'http_requests': 0,
            'all_history_complete': False, 'original_observation_clocks_preserved': True}


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
