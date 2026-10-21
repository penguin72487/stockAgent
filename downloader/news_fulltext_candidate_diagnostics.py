"""Finite same-response diagnosis through the existing single HTTP/queue owner.

Submitters write only the reviewed private mailbox. The owner reserves a
once-per-candidate observation before dispatch, never changes article heads,
and never retries an uncertain network outcome after a crash. Normal source
work gets worker/provider slots first. Diagnostics cannot accept article data.
"""
from __future__ import annotations

from contextlib import closing
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

from downloader.news_fulltext_candidates import native_candidate_source, candidate_response_metadata, candidate_evidence_allowed
from downloader.news_fulltext_candidate_originals import (
    PendingOriginal, QUESTION, private_original_permit, retain_original, retention_flags,
)
from downloader.news_fulltext_policy import NewsError, normalize_url
from downloader.news_fulltext_queue import utc_now
from downloader.news_fulltext_repairs import (
    HEAD_FIELDS, REPAIRABLE, SOURCE_FIELDS, _bytes, _clock, _directory, _head, _head_problem, _pending,
    _quarantine, _read_private_request, _source, _write_once,
)

CONTRACT_VERSION = 1
MAX_PENDING = 16
PUMP_INTERVAL_SECONDS = 5
_KIND = 'reviewed_native_candidate_metadata'
_SAFE_HASH = re.compile(r'[0-9a-f]{64}')
_TERMINAL = frozenset({'observed', 'observed_error', 'skipped', 'uncertain_outcome'})
_CANDIDATE_FAILURES = REPAIRABLE - {'body_not_found'}


def _inbox(root):
    return _directory(Path(root), 'candidate_diagnostics', 'pending')


def _key(target, candidate, *, evidence_contract_version=1):
    # User reasons/timestamps and changing failure counters cannot buy a second
    # observation of the same URL and candidate contract.
    if (type(evidence_contract_version) is not int or evidence_contract_version not in {1, 2, 3, 4}
            or evidence_contract_version in {3, 4} and target['provider'] != 'pocket'):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    basis = [CONTRACT_VERSION, target['provider'], target['id'], candidate]
    if evidence_contract_version == 2:
        # A fixed, reviewed NEW question, not an arbitrary reason or hash that
        # could rearm v1. Completed v1 rows/keys/receipts remain unchanged.
        basis = [2, target['provider'], target['id'], candidate, 'videoland_native_credit_clock_v1']
    elif evidence_contract_version == 3:
        basis = [3, target['provider'], target['id'], candidate, 'pocket_native_identity_boundary_v1']
    elif evidence_contract_version == 4:
        # The same failed URL/question cannot be fetched again by promoting a
        # parser, changing its fingerprint or altering a user-supplied reason.
        basis = [4, target['provider'], target['id'], QUESTION]
    return hashlib.sha256(_bytes(basis)).hexdigest()


def submit_candidate(root, config, registry, url, layout, *, reason, evidence_contract_version=1):
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 240:
        raise ValueError('candidate_requires_bounded_reason')
    if not isinstance(url, str) or normalize_url(url) != url:
        raise ValueError('literal_candidate_url_required')
    root = Path(root).resolve()
    candidate = native_candidate_source(config, registry, registry.base, layout)
    if not candidate_evidence_allowed(registry.sources[candidate.provider], candidate, evidence_contract_version):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    if evidence_contract_version in {3, 4}:
        from downloader.news_fulltext_pocket import pocket_article_id
        if pocket_article_id(url) is None:
            raise ValueError('literal_current_pocket_canonical_route_required')
    with closing(sqlite3.connect((root/'queue.sqlite3').as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        row = db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
        problem = _head_problem(row, time.time(), registry=registry)
        if problem:
            raise ValueError(problem)
        if row['state'] not in _CANDIDATE_FAILURES:
            raise ValueError('reviewed_validation_failure_required')
        if evidence_contract_version in {3, 4} and row['state'] != 'article_identity_not_verified':
            raise ValueError('reviewed_pocket_identity_failure_required')
        source, identity = _source(db, registry, url)
        if source.provider != row['provider'] or source.provider != candidate.provider:
            raise ValueError('candidate_provider_mismatch')
        target = _head(row)
        key = _key(target, candidate.data_fingerprint, evidence_contract_version=evidence_contract_version)
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone():
            if evidence_contract_version == 2:
                prior = db.execute('SELECT state FROM native_candidate_jobs WHERE observation_key=?',
                    (_key(target, candidate.data_fingerprint),)).fetchone()
                if prior and prior['state'] in {'ready', 'reserved', 'uncertain_outcome'}:
                    raise ValueError('prior_observation_not_terminal')
            existing = db.execute('SELECT request_id,state FROM native_candidate_jobs WHERE observation_key=?', (key,)).fetchone()
            if existing:
                return {'contract_version': CONTRACT_VERSION, 'submitted': False,
                    'request_id': existing['request_id'], 'state': existing['state'],
                    'publisher_requests': 0, 'article_heads_mutated': False, 'accepted_as_article': False}
    request = {'contract_version': CONTRACT_VERSION, 'kind': _KIND,
        'created_at_utc': utc_now(), 'reason': reason.strip(), 'target': target, 'source': identity,
        'candidate_layout': layout, 'candidate_fingerprint': candidate.data_fingerprint, 'observation_key': key}
    if evidence_contract_version >= 2:
        request.update(contract_version=evidence_contract_version, evidence_contract_version=evidence_contract_version)
    directory = _inbox(root)
    paths = _pending(directory)
    for path in paths:
        try:
            previous = _load(path)
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError):
            continue  # Only the owner may quarantine bad control files.
        if previous['observation_key'] == key:
            return {'contract_version': CONTRACT_VERSION, 'submitted': False, 'request_id': path.stem,
                'state': 'pending', 'publisher_requests': 0, 'article_heads_mutated': False, 'accepted_as_article': False}
    if len(paths) >= MAX_PENDING:
        raise ValueError('candidate_inbox_full')
    payload = _bytes(request)
    digest = hashlib.sha256(payload).hexdigest()
    _write_once(directory/(digest+'.json'), payload)
    return {'contract_version': CONTRACT_VERSION, 'submitted': True, 'request_id': digest,
        'state': 'pending', 'publisher_requests': 0, 'article_heads_mutated': False, 'accepted_as_article': False}


def _load(path):
    request = _read_private_request(path)
    fields = {'contract_version', 'kind', 'created_at_utc', 'reason', 'target', 'source',
              'candidate_layout', 'candidate_fingerprint', 'observation_key'}
    version = request.get('contract_version') if isinstance(request, dict) else None
    evidence = request.get('evidence_contract_version', 1) if isinstance(request, dict) else None
    if version in {2, 3, 4}:
        fields.add('evidence_contract_version')
    if (not isinstance(request, dict) or set(request) != fields
            or type(version) is not int or version not in {1, 2, 3, 4}
            or type(evidence) is not int or evidence != version
            or request['kind'] != _KIND or _clock(request['created_at_utc']) > time.time()
            or not isinstance(request['reason'], str) or not request['reason'].strip() or len(request['reason']) > 240
            or not isinstance(request['target'], dict) or set(request['target']) != set(HEAD_FIELDS)
            or not isinstance(request['source'], dict) or set(request['source']) != SOURCE_FIELDS
            or type(request['source']['contract_version']) is not int
            or request['source']['contract_version'] < 1
            or any(not isinstance(request['source'][key], str) or not _SAFE_HASH.fullmatch(request['source'][key])
                   for key in SOURCE_FIELDS - {'contract_version'})
            or not isinstance(request['candidate_layout'], str) or not re.fullmatch(
                r'(?:pocket_visible_article_v[0-9]{1,2}|videoland_visible_article_v4|ettoday_visible_article_v39|max_visible_post_v[0-9]{1,2})',
                request['candidate_layout'])
            or not isinstance(request['candidate_fingerprint'], str) or not _SAFE_HASH.fullmatch(request['candidate_fingerprint'])
            or not isinstance(request['observation_key'], str)):
        raise ValueError('invalid_native_candidate_request')
    target = request['target']
    if (not isinstance(target['url'], str) or normalize_url(target['url']) != target['url']
            or target['id'] != hashlib.sha256(target['url'].encode()).hexdigest()
            or not isinstance(target['provider'], str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,79}', target['provider'])
            or target['state'] not in _CANDIDATE_FAILURES or target['error_code'] != target['state']
            or type(target['attempts']) is not int or target['attempts'] < 0
            or type(target['due']) not in (int, float) or not math.isfinite(target['due'])
            or not 0 <= target['due'] <= time.time() or _clock(target['last_checked_at_utc']) > time.time()
            or evidence == 2 and (target['provider'] != 'videoland'
                or request['candidate_layout'] != 'videoland_visible_article_v4')
            or (request['candidate_layout'] == 'pocket_visible_article_v3') != (evidence == 3)
            or evidence in {3, 4} and (target['provider'] != 'pocket' or target['state'] != 'article_identity_not_verified')
            or evidence == 4 and (not re.fullmatch(r'pocket_visible_article_v[0-9]{1,2}', request['candidate_layout'])
                or int(request['candidate_layout'].rsplit('v', 1)[1]) < 5)
            or request['candidate_layout'].startswith('pocket_') and evidence not in {3, 4}
            or request['observation_key'] != _key(target, request['candidate_fingerprint'],
                evidence_contract_version=evidence)):
        raise ValueError('invalid_native_candidate_target')
    if evidence in {3, 4}:
        from downloader.news_fulltext_pocket import pocket_article_id
        if pocket_article_id(target['url']) is None:
            raise ValueError('literal_current_pocket_canonical_route_required')
    return request


def _schema(queue):
    if getattr(queue, 'native_candidate_schema_ready', False):
        return
    with queue.db:
        queue.db.execute('CREATE TABLE IF NOT EXISTS native_candidate_jobs('
            'request_id TEXT PRIMARY KEY,observation_key TEXT UNIQUE NOT NULL,provider TEXT NOT NULL,'
            'state TEXT NOT NULL,request_json TEXT NOT NULL,result_json TEXT,'
            'created_at_utc TEXT NOT NULL,reserved_at_utc TEXT,completed_at_utc TEXT,owner_id TEXT NOT NULL)')
        queue.db.execute('CREATE INDEX IF NOT EXISTS native_candidate_ready ON native_candidate_jobs(state,provider,created_at_utc)')
        # A dispatch reservation is the unknown-outcome barrier, not permission
        # to send again. Committed results are archived below without any GET.
        queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome',completed_at_utc=? "
            "WHERE state='reserved' AND owner_id<>?", (utc_now(), queue.owner))
    queue.native_candidate_schema_ready = True


def _archive(queue, path, job, *, duplicate_of=None):
    result = json.loads(job['result_json']) if job['result_json'] else None
    outcome = {'contract_version': CONTRACT_VERSION, 'request_id': path.stem,
        'observation_request_id': job['request_id'], 'observation_key': job['observation_key'],
        'provider': job['provider'], 'state': job['state'], 'owner_id': job['owner_id'],
        'created_at_utc': job['created_at_utc'], 'reserved_at_utc': job['reserved_at_utc'],
        'completed_at_utc': job['completed_at_utc'], 'result': result,
        'duplicate_of': duplicate_of, 'accepted_as_article': False, 'raw_or_names_or_body_retained': False,
        'article_heads_mutated': False, 'publish': False, 'remote_training_export': False}
    outcome.update(retention_flags(json.loads(job['request_json']), result, job['state']))
    directory = _directory(queue.root, 'candidate_diagnostics', 'processed')
    _write_once(directory/(path.stem+'.outcome.json'), _bytes(outcome))
    archived = directory/path.name
    if archived.is_symlink() or archived.exists() and hashlib.sha256(archived.read_bytes()).hexdigest() != path.stem:
        raise ValueError('candidate_archive_conflict')
    os.replace(path, archived)
    for parent in (directory, path.parent):
        descriptor = os.open(parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def pump_candidates(queue, *, stop=None, max_seconds=0.05):
    """At most16 private requests per tick; no article tree or body scan."""
    if type(max_seconds) not in (int, float) or not math.isfinite(max_seconds) or not 0 < max_seconds <= 0.1:
        raise ValueError('invalid_candidate_slice_budget')
    started = time.monotonic()
    base = queue.root/'reviewed_repairs'/'candidate_diagnostics'
    if not base.exists() and not base.is_symlink():
        return
    try:
        paths = _pending(_inbox(queue.root))[:MAX_PENDING]
        _schema(queue)
    except (OSError, ValueError):
        queue.metrics.count('local_repairs', 'candidate_inbox_unavailable')
        return
    for path in paths:
        if time.monotonic()-started >= max_seconds or stop is not None and stop.is_set():
            break
        try:
            request = _load(path)
            provider = request['target']['provider']
            if (provider not in queue.registry.sources or not queue.registry.sources[provider].allowed
                    or not queue.registry.sources[provider].request_enabled):
                raise ValueError('candidate_provider_no_longer_allowed')
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError) as error:
            try:
                _quarantine(queue, path, error)
            except (OSError, ValueError):
                queue.metrics.count('local_repairs', 'candidate_quarantine_deferred')
            continue
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO native_candidate_jobs '
                '(request_id,observation_key,provider,state,request_json,created_at_utc,owner_id) VALUES (?,?,?,?,?,?,?)',
                (path.stem, request['observation_key'], request['target']['provider'], 'ready',
                 _bytes(request).decode(), request['created_at_utc'], queue.owner))
        job = queue.db.execute('SELECT * FROM native_candidate_jobs WHERE observation_key=?',
            (request['observation_key'],)).fetchone()
        if job['state'] in _TERMINAL:
            try:
                _archive(queue, path, job, duplicate_of=job['request_id'] if job['request_id'] != path.stem else None)
            except (OSError, ValueError):
                queue.metrics.count('local_repairs', 'candidate_archive_deferred')
    queue.native_candidate_ready_providers = {row[0] for row in queue.db.execute(
        "SELECT DISTINCT provider FROM native_candidate_jobs WHERE state='ready'")}


def has_unadmitted_candidates(queue):
    """Wake an idle owner for NEW input, not for an already-paused ready job."""
    directory = queue.root/'reviewed_repairs'/'candidate_diagnostics'/'pending'
    try:
        for parent in (directory.parent.parent, directory.parent, directory):
            mode = parent.lstat().st_mode
            if not stat.S_ISDIR(mode) or mode & 0o077:
                return False
        names = [path.stem for path in _pending(directory)[:MAX_PENDING]]
    except OSError:
        return False
    if not names:
        return False
    if not getattr(queue, 'native_candidate_schema_ready', False):
        return True
    registered = {row[0] for row in queue.db.execute(
        'SELECT request_id FROM native_candidate_jobs WHERE request_id IN ('+','.join('?' for _ in names)+')', names)}
    return any(name not in registered for name in names)


def claim_candidate(queue, http, config, provider):
    if (provider not in getattr(queue, 'native_candidate_ready_providers', ())
            or http.provider_not_before.get(provider, 0) > time.time()):
        return None
    for job in queue.db.execute("SELECT * FROM native_candidate_jobs WHERE state='ready' AND provider=? "
            'ORDER BY created_at_utc LIMIT ?', (provider, MAX_PENDING)).fetchall():
        request = json.loads(job['request_json'])
        target = request['target']
        evidence_version = request.get('evidence_contract_version', 1)
        parts = urlsplit(target['url'])
        if (parts.netloc in http.blocked_gateway_hosts(provider)
                or parts.scheme+'://'+parts.netloc in http.blocked_robots_origins(provider)):
            continue
        try:
            if evidence_version == 2:
                prior = queue.db.execute('SELECT state FROM native_candidate_jobs WHERE observation_key=?',
                    (_key(target, request['candidate_fingerprint']),)).fetchone()
                if prior and prior['state'] in {'ready', 'reserved'}:
                    continue
                if prior and prior['state'] == 'uncertain_outcome':
                    raise ValueError('prior_observation_uncertain')
            row = queue.db.execute('SELECT * FROM articles WHERE id=?', (bytes.fromhex(target['id']),)).fetchone()
            problem = _head_problem(row, time.time(), registry=queue.registry)
            if problem or _head(row) != target:
                raise ValueError('head_changed_or_protected')
            source, identity = _source(queue.db, queue.registry, target['url'])
            if source.provider != provider or identity != request['source']:
                raise ValueError('source_identity_changed')
            candidate = native_candidate_source(config, queue.registry, queue.registry.base, request['candidate_layout'])
            if candidate.provider != provider or candidate.data_fingerprint != request['candidate_fingerprint']:
                raise ValueError('candidate_identity_changed')
            if not candidate_evidence_allowed(source, candidate, evidence_version):
                raise ValueError('reviewed_candidate_evidence_contract_required')
        except (ValueError, NewsError) as error:
            with queue.db:
                queue.db.execute("UPDATE native_candidate_jobs SET state='skipped',completed_at_utc=?,owner_id=?,result_json=? "
                    'WHERE request_id=?', (utc_now(), queue.owner,
                        _bytes({'reason': error.code if isinstance(error, NewsError) else 'head_or_source_no_longer_eligible'}).decode(), job['request_id']))
            continue
        # The same main-thread writer owns reservation and future completion.
        # Reserve durably before handing any HTTP operation to the executor.
        with queue.db:
            queue.db.execute("UPDATE native_candidate_jobs SET state='reserved',reserved_at_utc=?,owner_id=? "
                "WHERE request_id=? AND state='ready'", (utc_now(), queue.owner, job['request_id']))
        return {'request_id': job['request_id'], 'provider': provider, 'url': target['url'],
            'source': source, 'candidate': candidate, 'evidence_contract_version': evidence_version}
    return None


def fetch_candidate(http, job):
    evidence_version = job.get('evidence_contract_version', 1)
    if (job['provider'] != job['source'].provider
            or not candidate_evidence_allowed(job['source'], job['candidate'], evidence_version)):
        raise ValueError('reviewed_candidate_evidence_contract_required')
    started = time.perf_counter()
    response = http.get(job['url'])
    received = time.perf_counter()
    if response.url != job['url'] or response.redirects:
        raise NewsError('candidate_literal_response_not_verified')
    facts = candidate_response_metadata(response, job['source'], job['candidate'],
                                       evidence_contract_version=evidence_version)
    parsed = time.perf_counter()
    permit = None
    if evidence_version == 4 and facts['current_error'] == 'article_identity_not_verified':
        try:
            permit = private_original_permit(response, job['source'])
        except NewsError as error:
            from downloader.news_fulltext_candidates import extraction_failure_location
            facts['private_original_scope_error'] = 'private_original_scope_not_verified'
            facts['private_original_scope_failure_location'] = extraction_failure_location(error)
    finished = time.perf_counter()
    facts['timing'] = {'http_seconds': received-started, 'parse_seconds': parsed-received,
        'scope_seconds': finished-parsed, 'total_seconds': finished-started}
    if permit is not None:
        return PendingOriginal(response, facts, permit)
    return facts


def finish_candidate(queue, job, *, result=None, error=None):
    original_result = isinstance(result, PendingOriginal)
    retention_uncertain = False
    if original_result and error is None:
        # Check ownership BEFORE a sidecar can be created. A failed local write
        # consumes this observation and does not kill/restart the HTTP owner.
        reserved = queue.db.execute("SELECT 1 FROM native_candidate_jobs WHERE request_id=? "
            "AND state='reserved' AND owner_id=?", (job['request_id'], queue.owner)).fetchone()
        if not reserved or job.get('evidence_contract_version') != 4:
            raise ValueError('candidate_completion_owner_mismatch')
        try:
            if (not isinstance(result.facts, dict) or result.facts.get('accepted_as_article') is not False
                    or result.facts.get('raw_or_names_or_body_retained') is not False
                    or len(_bytes(result.facts)) > 60000):
                raise ValueError('finite_candidate_metadata_required')
            write_started = time.perf_counter()
            result = retain_original(queue, job, result)
            result['original_write_readback_seconds'] = time.perf_counter()-write_started
        except (OSError, ValueError, TypeError, OverflowError, RecursionError) as failure:
            error, retention_uncertain = failure, True
    if error is not None:
        code = error.code if isinstance(error, NewsError) else 'worker_'+type(error).__name__
        if not isinstance(code, str) or not re.fullmatch(r'[A-Za-z0-9_]{1,96}', code):
            code = 'unclassified_worker_error'
        result, state = {'error': code, 'accepted_as_article': False, 'raw_or_names_or_body_retained': False}, 'observed_error'
        if retention_uncertain:
            result.update(raw_or_names_or_body_retained=None, raw_retention_state='unknown')
    else:
        state = 'observed'
    try:
        if (not isinstance(result, dict) or result.get('accepted_as_article') is not False
                or result.get('raw_or_names_or_body_retained') is not False
                and not (original_result and job.get('evidence_contract_version') == 4
                    and (result.get('raw_or_names_or_body_retained') is True or retention_uncertain))):
            raise ValueError('finite_candidate_metadata_required')
        payload = _bytes(result)
    except (TypeError, ValueError, OverflowError, RecursionError):
        payload, state = _bytes({'error': 'invalid_candidate_result', 'accepted_as_article': False,
                                'raw_or_names_or_body_retained': False}), 'observed_error'
    if len(payload) > 65536:
        payload, state = _bytes({'error': 'candidate_result_exceeds_bound', 'accepted_as_article': False,
                                'raw_or_names_or_body_retained': False}), 'observed_error'
    with queue.db:
        cursor = queue.db.execute('UPDATE native_candidate_jobs SET state=?,result_json=?,completed_at_utc=? '
            "WHERE request_id=? AND state='reserved' AND owner_id=?",
            (state, payload.decode(), utc_now(), job['request_id'], queue.owner))
        if cursor.rowcount != 1:
            raise ValueError('candidate_completion_owner_mismatch')
    queue.metrics.count(job['provider'], 'native_candidate_'+state)


def candidate_status(root, request_id):
    """Private read-only status; never acquire a news/HTTP owner lock."""
    if not isinstance(request_id, str) or not _SAFE_HASH.fullmatch(request_id):
        raise ValueError('invalid_candidate_request_id')
    root = Path(root).resolve()
    flags = {'contract_version': CONTRACT_VERSION, 'accepted_as_article': False,
             'raw_or_names_or_body_retained': False, 'article_heads_mutated': False,
             'publish': False, 'remote_training_export': False}
    with closing(sqlite3.connect((root/'queue.sqlite3').as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs' AND type='table'").fetchone():
            row = db.execute('SELECT request_id,provider,state,created_at_utc,reserved_at_utc,completed_at_utc,result_json,request_json '
                'FROM native_candidate_jobs WHERE request_id=?', (request_id,)).fetchone()
            if row:
                result = json.loads(row['result_json']) if row['result_json'] else None
                return dict(flags, **retention_flags(json.loads(row['request_json']), result, row['state']),
                    **{key: result if key == 'result_json' else value
                       for key, value in dict(row).items() if key != 'request_json'})
    path = root/'reviewed_repairs'/'candidate_diagnostics'/'pending'/(request_id+'.json')
    if path.exists():
        _load(path)
        return dict(flags, request_id=request_id, state='pending', publisher_requests=0)
    return dict(flags, request_id=request_id, state='unknown')
