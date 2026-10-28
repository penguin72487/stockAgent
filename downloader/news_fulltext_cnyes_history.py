"""Durable Anue list evidence, distinct from article-body acceptance.

Retain exact request/response clocks and source IDs before closing a partition
gap. Equal effective dates alone never prove equal results: an old scope may
reuse a current list only when *every* retained page ID hash and total match.
No source raw envelope, article, byline, failure or retry clock is relabelled.
"""
from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
import time
from types import SimpleNamespace
from urllib.parse import parse_qs

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_bulk import (
    BULK_RESTRICTED_ITEM_ERRORS, CNYES_QUERY_DAY_OFFSET_SECONDS, cnyes_pagination,
    cnyes_scope, cnyes_url, wordpress_base, wordpress_page,
)
from downloader.news_fulltext_policy import CONTRACT_VERSION as NEWS_CONTRACT_VERSION, NewsError, cnyes_bulk_fingerprint


CONTRACT_VERSION = 1
CONSISTENCY_RECHECK_CONTRACT_VERSION = 2
CONSISTENCY_RECHECK_MAX_FACTOR = 16
COMPLETE_STATES = ('list_complete', 'list_complete_by_page_ID_equivalence', 'partition_list_complete')
REPAIR_STATES = ('waiting_page_evidence', 'inconsistent_source_snapshot')
REPAIR_CURSOR = 'cnyes_history_incomplete_scope_cursor_v1'
# Merge the existing index's per-state ordered seeks. An IN query with one
# global ORDER BY requires SQLite to materialize a temporary sort on each
# cursor slice; each scope has exactly one state, so UNION ALL is equivalent.
_REPAIR_SCOPE_BRANCH = ('SELECT scope_url FROM cnyes_history_scope_heads WHERE provider=? '
    'AND evidence_contract_version=? AND source_bulk_fingerprint=? AND authorization_fingerprint=? '
    'AND state=? AND scope_url>?')
REPAIR_SCOPE_SEEK = ' UNION ALL '.join([_REPAIR_SCOPE_BRANCH] * len(REPAIR_STATES)) + ' ORDER BY scope_url LIMIT ?'
OVERFLOW_RETAINED_SCAN_VERSION = 1
OVERFLOW_RETAINED_SCAN_PREFIX = 'cnyes_history_overflow_retained_scan_v1:'
LEGACY_MISMATCH_SCAN_KEY = 'cnyes_history_retained_mismatch_scan_v1'
LEGACY_UNAVAILABLE_SCAN_KEY = 'cnyes_history_retained_unavailable_scan_v1'
LEGACY_EMPTY_SCAN_PREFIX = 'cnyes_history_unobserved_empty_scan_v1:'
LEGACY_NONZERO_SCAN_PREFIX = 'cnyes_history_unobserved_nonzero_scan_v1:'
ACCEPTED_BATCH_SCAN_PREFIX = 'cnyes_history_accepted_batch_scan_v1:'
SCOPE_KEY_CACHE_SIZE = 4096
SCOPE_KEY_CACHE_MAX_URL_CHARS = 2048


def now():
    return datetime.now(timezone.utc).isoformat()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def _retained_envelopes(root, sha):
    """Known content-addressed locations; callers still verify exact request."""
    if type(sha) is not str or len(sha) != 64 or any(ch not in '0123456789abcdef' for ch in sha):
        return ()
    return ((root / 'quarantine' / (sha + '.parquet'), 'canonical_quarantine'),
            (root / 'discovery_responses' / (sha + '.parquet'), 'cnyes_history_source_envelope'))


def scope_key(url):
    """Cache only immutable URL arithmetic, never source rows or evidence.

    Heartbeats and index seeding revisit more scopes than urlsplit's own
    small cache holds. The offset is part of the key; a changed date rule
    cannot borrow an old result. Unusual/oversized inputs retain the original
    parser and its exceptions, without occupying persistent cache space.
    """
    if (type(url) is str and len(url) <= SCOPE_KEY_CACHE_MAX_URL_CHARS
            and type(CNYES_QUERY_DAY_OFFSET_SECONDS) is int):
        return _cached_scope_key(url, CNYES_QUERY_DAY_OFFSET_SECONDS)
    return _parse_scope_key(url, CNYES_QUERY_DAY_OFFSET_SECONDS)


@lru_cache(maxsize=SCOPE_KEY_CACHE_SIZE)
def _cached_scope_key(url, query_day_offset):
    return _parse_scope_key(url, query_day_offset)


def _parse_scope_key(url, query_day_offset):
    query = parse_qs(url.split('?', 1)[1]) if '?' in url else {}
    if not {'startAt', 'endAt'} <= query.keys():
        return None
    low, high = int(query['startAt'][0]), int(query['endAt'][0])
    if high < low or query.get('limit') != ['30']:
        return None
    first, last = ((value + query_day_offset) // 86400 for value in (low, high))
    return wordpress_base(url), first, last


def index_scope(queue, scope, provider):
    key = scope_key(scope)
    if key:
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_scope_keys VALUES (?,?,?,?,?)',
                         (scope, provider, *key))


def index_children(queue, scope, provider, children):
    index_scope(queue, scope, provider)
    for url in children:
        child = cnyes_scope(url)
        index_scope(queue, child, provider)
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_edges VALUES (?,?,?)', (scope, child, url))


def initialize_accepted_batch_candidates(queue):
    """Prepare the reference and outcome audit before orphan receipt replay."""
    queue.db.execute('''
        CREATE TABLE IF NOT EXISTS cnyes_history_accepted_batch_candidates(
            request_url TEXT NOT NULL,response_sha256 TEXT NOT NULL,parquet_path TEXT NOT NULL,
            receipt_path TEXT NOT NULL,PRIMARY KEY(request_url,response_sha256,parquet_path)
        ) WITHOUT ROWID
    ''')
    queue.db.execute('''
        CREATE TABLE IF NOT EXISTS cnyes_history_retained_outcome_resolutions(
            resolution_sha256 TEXT PRIMARY KEY,request_url TEXT NOT NULL,original_state TEXT NOT NULL,
            original_outcome_json TEXT,resolution_state TEXT NOT NULL,observation_sha256 TEXT,
            evidence_kind TEXT,resolved_at_utc TEXT NOT NULL
        ) WITHOUT ROWID
    ''')
    # A new database may not have the history work table until after orphan
    # receipt replay. In that case there cannot yet be old work to wake.
    queue._cnyes_retained_work_initialized = bool(queue.db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cnyes_history_retained_work'").fetchone())


def initialize(queue):
    initialize_accepted_batch_candidates(queue)
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS cnyes_history_page_observations(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,response_sha256 TEXT NOT NULL,
            evidence_contract_version INTEGER NOT NULL,source_bulk_fingerprint TEXT NOT NULL,
            authorization_fingerprint TEXT NOT NULL,
            observation_sha256 TEXT NOT NULL UNIQUE,observation_json TEXT NOT NULL,
            PRIMARY KEY(scope_url,page,response_sha256,evidence_contract_version,source_bulk_fingerprint,authorization_fingerprint)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_page_heads(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,observation_sha256 TEXT NOT NULL,
            last_confirmed_at_utc TEXT NOT NULL,
            PRIMARY KEY(scope_url,page)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_page_ID_references(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,observation_sha256 TEXT NOT NULL,
            original_ID_evidence_json TEXT NOT NULL,reference_scope TEXT NOT NULL,
            PRIMARY KEY(scope_url,page,observation_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_unproven_pages(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,evidence_json TEXT NOT NULL,
            PRIMARY KEY(scope_url,page)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_scope_keys(
            scope_url TEXT PRIMARY KEY,provider TEXT NOT NULL,endpoint TEXT NOT NULL,
            first_day INTEGER NOT NULL,last_day INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_effective_dates
            ON cnyes_history_scope_keys(endpoint,first_day,last_day,provider);
        CREATE TABLE IF NOT EXISTS cnyes_history_edges(
            parent_scope TEXT NOT NULL,child_scope TEXT NOT NULL,child_request_url TEXT NOT NULL,
            PRIMARY KEY(parent_scope,child_scope)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_parent_lookup ON cnyes_history_edges(child_scope,parent_scope);
        CREATE TABLE IF NOT EXISTS cnyes_history_scope_proofs(
            proof_sha256 TEXT PRIMARY KEY,scope_url TEXT NOT NULL,proof_json TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_scope_heads(
            scope_url TEXT PRIMARY KEY,provider TEXT NOT NULL,state TEXT NOT NULL,
            proof_sha256 TEXT NOT NULL,member_ids_json TEXT NOT NULL,
            source_bulk_fingerprint TEXT NOT NULL,authorization_fingerprint TEXT NOT NULL,
            evidence_contract_version INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_state_counts ON cnyes_history_scope_heads(state);
        CREATE INDEX IF NOT EXISTS cnyes_history_current_contract ON cnyes_history_scope_heads(
            provider,evidence_contract_version,source_bulk_fingerprint,authorization_fingerprint,state);
        CREATE TABLE IF NOT EXISTS cnyes_history_gap_retirements(
            scope_url TEXT NOT NULL,proof_sha256 TEXT NOT NULL,gap_json TEXT NOT NULL,retired_at_utc TEXT NOT NULL,
            PRIMARY KEY(scope_url,proof_sha256)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_retained_work(
            request_url TEXT PRIMARY KEY,scope_url TEXT NOT NULL,page INTEGER NOT NULL,response_sha256 TEXT NOT NULL,
            expected_total INTEGER NOT NULL,expected_ids_sha256 TEXT NOT NULL,source_checked_at_utc TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending',outcome_json TEXT
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_frontier ON cnyes_history_retained_work(state,request_url);
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_scope_page
            ON cnyes_history_retained_work(scope_url,page,state);
        CREATE TABLE IF NOT EXISTS cnyes_history_local_inventory_scopes(
            scope_url TEXT PRIMARY KEY,source_bulk_fingerprint TEXT NOT NULL,
            authorization_fingerprint TEXT NOT NULL,
            scan_state TEXT NOT NULL CHECK(scan_state IN ('scanning','ready')),
            created_at_utc TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_local_inventory_state
            ON cnyes_history_local_inventory_scopes(scan_state);
        CREATE TABLE IF NOT EXISTS cnyes_history_accepted_batch_outcomes(
            parquet_path TEXT NOT NULL,receipt_path TEXT NOT NULL,state TEXT NOT NULL,
            reason TEXT NOT NULL,observed_at_utc TEXT NOT NULL,
            PRIMARY KEY(parquet_path,observed_at_utc,state)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_history_accepted_batch_local_retries(
            scan_key TEXT NOT NULL,parquet_path TEXT NOT NULL,receipt_path TEXT NOT NULL,
            not_before_epoch REAL NOT NULL,local_attempts INTEGER NOT NULL,
            PRIMARY KEY(scan_key,parquet_path)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_accepted_batch_retry_due
            ON cnyes_history_accepted_batch_local_retries(scan_key,not_before_epoch,parquet_path);
        CREATE TABLE IF NOT EXISTS cnyes_history_retained_local_retries(
            request_url TEXT PRIMARY KEY,
            original_state TEXT NOT NULL CHECK(original_state IN ('pending','overflow_pending')),
            not_before_epoch REAL NOT NULL,local_attempts INTEGER NOT NULL,
            error_code TEXT NOT NULL,last_attempt_at_utc TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_local_retry_due
            ON cnyes_history_retained_local_retries(not_before_epoch,request_url);
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_local_retry_origin_due
            ON cnyes_history_retained_local_retries(original_state,not_before_epoch,request_url);
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_waiting_batch_scan
            ON cnyes_history_retained_local_retries(request_url)
            WHERE error_code='accepted_batch_scan_pending';
        CREATE INDEX IF NOT EXISTS cnyes_history_retained_resolutions_request
            ON cnyes_history_retained_outcome_resolutions(request_url);
    ''')
    queue._cnyes_retained_work_initialized = True
    marker = 'cnyes_history_evidence_initialization_v' + str(CONTRACT_VERSION)
    if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
        return
    sources = {p: s for p, s in queue.registry.sources.items() if s.allowed and s.cnyes_apis}
    with queue.db:
        for row in queue.db.execute('SELECT * FROM bulk_window_splits').fetchall():
            source = sources.get(row['provider'])
            if source and wordpress_base(row['scope_url']) in source.cnyes_apis:
                index_children(queue, row['scope_url'], row['provider'], json.loads(row['child_urls_json']))
        # Only replacement leaves of an actual historical gap need migration.
        # Do not re-enumerate every old complete article or every history root.
        leaves = queue.db.execute('''
            SELECT DISTINCT p.* FROM cnyes_history_edges e
            JOIN bulk_window_gaps g ON g.scope_url=e.parent_scope
            JOIN bulk_api_coverage c ON c.base_url=e.child_scope
            JOIN bulk_page_evidence p ON p.scope_url=c.base_url
            LEFT JOIN bulk_window_splits s ON s.scope_url=c.base_url
            WHERE c.total_pages<=30 AND s.scope_url IS NULL AND p.page<=c.total_pages
        ''').fetchall()
        # The old adapter stored no page hash for exact zero-row windows.
        # Expect an empty set, but require a real retained/new empty envelope;
        # do not manufacture a historical observation from this expectation.
        empty = queue.db.execute('''
            SELECT DISTINCT c.base_url AS scope_url,1 AS page,0 AS total,c.observed_at_utc
            FROM cnyes_history_edges e JOIN bulk_window_gaps g ON g.scope_url=e.parent_scope
            JOIN bulk_api_coverage c ON c.base_url=e.child_scope
            LEFT JOIN bulk_window_splits s ON s.scope_url=c.base_url
            WHERE c.total_articles=0 AND c.total_pages=1 AND s.scope_url IS NULL
        ''').fetchall()
        leaves.extend({**dict(row), 'ids_sha256': hashlib.sha256(json.dumps([]).encode()).hexdigest()} for row in empty)
        for row in leaves:
            key = scope_key(row['scope_url'])
            if not key:
                continue
            query = parse_qs(row['scope_url'].split('?', 1)[1])
            url = cnyes_url(key[0], row['page'], start=int(query['startAt'][0]), end=int(query['endAt'][0]))
            job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
            if job and job['response_sha256'] and job['last_checked_at_utc']:
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_work '
                    '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
                    'VALUES (?,?,?,?,?,?,?)', (url, row['scope_url'], row['page'], job['response_sha256'], row['total'],
                                               row['ids_sha256'], row['observed_at_utc']))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, encoded({
            'observed_at_utc': now(), 'retained_page_candidates': len(leaves),
            'article_or_source_deadlines_reset': False, 'scope_enumeration_is_not_body_acceptance': True})))


def retain_response(queue, response, source, *, accepted, restricted):
    """Reference a committed envelope; save one only if none was accepted.

    A restricted shared envelope is never persisted. Its public ID/date
    projection and real response digest remain metadata-only evidence.
    """
    if restricted:
        return {'kind': 'restricted_public_metadata_only', 'raw_envelope_persisted': False}
    if accepted:
        row = queue.db.execute('SELECT a.parquet_path,b.receipt_path FROM articles a JOIN accepted_batches b '
            'ON a.parquet_path=b.path WHERE a.id=?', (bytes.fromhex(accepted[0]['article_id']),)).fetchone()
        if not row:
            raise ValueError('committed_bulk_source_batch_required')
        return {'kind': 'accepted_body_batch', 'parquet_path': row[0], 'receipt_path': row[1]}
    sha = hashlib.sha256(response.body).hexdigest()
    prior = queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
        'WHERE scope_url=? AND page=? AND response_sha256=? AND evidence_contract_version=? '
        'AND source_bulk_fingerprint=? AND authorization_fingerprint=?',
        (cnyes_scope(response.url), int(parse_qs(response.url.split('?', 1)[1]).get('page', ['1'])[0]), sha,
         CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
    if prior:
        observation = json.loads(prior[0])
        artifact = observation['artifact']
        if (observation['source_bulk_fingerprint'] == cnyes_bulk_fingerprint(source)
                and observation['authorization_fingerprint'] == source.authorization_fingerprint
                and artifact.get('parquet_path') and artifact.get('receipt_path')
                and (queue.root / artifact['parquet_path']).is_file()
                and (queue.root / artifact['receipt_path']).is_file()):
            return artifact  # Same exact bytes, not another body/source copy or a new capture clock.
    relative = 'quarantine/' + sha + '.parquet'
    target = queue.root / relative
    if target.is_file() and target.with_suffix('.parquet.receipt.json').is_file():
        return {'kind': 'canonical_quarantine', 'parquet_path': relative,
                'receipt_path': relative + '.receipt.json'}
    relative = 'discovery_responses/' + sha + '.parquet'
    target = queue.root / relative
    if not target.is_file():
        atomic_write_parquet(target, table_from_records([{
            'request_url': response.url, 'response_sha256': sha, 'raw_response_body': response.body,
            'captured_at_utc': now(), 'authorization_fingerprint': source.authorization_fingerprint,
            'authorization_json': encoded(source.authorization), 'publish': False,
        }]), compression='zstd', compression_level=9, durable=True)
    if not target.with_suffix('.parquet.receipt.json').is_file():
        atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
            'contract': 'cnyes_history_source_envelope_v1', 'parquet_path': relative,
            'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False,
            'raw_response_bytes': len(response.body), 'article_bodies_accepted': False,
        }, durable=True)
    return {'kind': 'cnyes_history_source_envelope', 'parquet_path': relative,
            'receipt_path': relative + '.receipt.json'}


def record_page(queue, response, info, posts, source, artifact, *, observed_at=None, origin='normal_source_response'):
    """Caller commits only after body acceptance; repeated bytes keep clocks."""
    if not scope_key(info['scope']):
        return
    if not source.allowed or wordpress_base(response.url) not in source.cnyes_apis:
        raise NewsError('cnyes_history_unregistered_endpoint')
    stamp = observed_at or now()
    identities = [{'newsId': row['newsId'], 'publishAt': row['publishAt'],
                   'source_post_sha256': digest(row)} for row in posts]
    observation = {'contract_version': CONTRACT_VERSION, 'provider': source.provider,
        'request_url': response.url, 'scope_url': info['scope'], 'page': info['page'],
        'response_sha256': hashlib.sha256(response.body).hexdigest(), 'pagination': info,
        'identities': identities, 'source_day_offset_seconds': CNYES_QUERY_DAY_OFFSET_SECONDS,
        'source_observed_at_utc': stamp, 'evidence_recorded_at_utc': now(), 'origin': origin,
        'source_bulk_fingerprint': cnyes_bulk_fingerprint(source),
        'authorization_fingerprint': source.authorization_fingerprint, 'artifact': artifact,
        'body_completeness_verified': False}
    old = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_observations '
        'WHERE scope_url=? AND page=? AND response_sha256=? AND evidence_contract_version=? '
        'AND source_bulk_fingerprint=? AND authorization_fingerprint=?',
        (info['scope'], info['page'], observation['response_sha256'], CONTRACT_VERSION,
         cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
    sha = old[0] if old else digest(observation)
    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)',
        (info['scope'], info['page'], observation['response_sha256'], CONTRACT_VERSION,
         cnyes_bulk_fingerprint(source), source.authorization_fingerprint, sha, encoded(observation)))
    current = queue.db.execute('SELECT last_confirmed_at_utc FROM cnyes_history_page_heads WHERE scope_url=? AND page=?',
                               (info['scope'], info['page'])).fetchone()
    if current and datetime.fromisoformat(current[0]) > datetime.fromisoformat(stamp):
        return  # Old retained bytes never overwrite forward-confirmed metadata.
    queue.db.execute('INSERT OR REPLACE INTO cnyes_history_page_heads VALUES (?,?,?,?)', (info['scope'], info['page'], sha, stamp))
    queue.db.execute('DELETE FROM cnyes_history_unproven_pages WHERE scope_url=? AND page=?', (info['scope'], info['page']))
    index_scope(queue, info['scope'], source.provider)
    reconcile_scope(queue, info['scope'], source)
    # A local inventory is still collecting independent original pages. Do
    # not wake or alias neighbouring source jobs halfway through that scan.
    fence = queue.db.execute('SELECT 1 FROM cnyes_history_local_inventory_scopes WHERE scope_url=?',
                             (info['scope'],)).fetchone()
    if not fence:
        share_matching_page_IDs(queue, info['scope'], sha, info, source, stamp)
    resolve_forward_retained_outcomes(queue, info['scope'], info['page'], source)
    if fence:
        _release_complete_local_inventory_fence(queue, info['scope'], source)


def schedule_original_page_check(queue, url, *, reason, evidence_sha):
    """One purpose-specific recheck; never touch failed/retired source jobs."""
    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
    if (not job or job['error_code'] is not None or job['attempts'] != 0
            or job['superseded_by_windows_json'] is not None):
        return False
    marker = 'cnyes_history_direct_page_recheck_v1:' + digest((url, evidence_sha))
    if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
        return False
    queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, encoded({
        'prior_job': dict(job), 'reason': reason, 'changed_at_utc': now(),
        'source_failures_or_deadlines_reset': False})))
    queue.db.execute('UPDATE discovery SET due=min(due,?),etag=NULL,last_modified=NULL WHERE url=?', (time.time(), url))
    return True


def schedule_consistency_recheck(queue, url, source, *, scope, evidence_sha, confirmed_at):
    """Retry an unresolved list contradiction, never a completed diagnostic.

    Keep the original one-shot witness immutable. A successful but still
    contradictory response is not list completion: the next check uses the
    source's discovery cadence with capped exponential spacing. The actual
    successful response clock fences each attempt across owner restarts; a
    loop, new proof SHA or process restart cannot create a fresh allowance.
    Refused/failed/split jobs retain their own stronger controls.
    """
    if (not source.allowed or source.acquisition_mode == 'finite' or source.known_url_only
            or wordpress_base(url) not in source.cnyes_apis or cnyes_scope(url) != scope):
        return False
    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
    if (not job or job['kind'] != 'cnyes_news' or job['provider'] != source.provider
            or job['error_code'] is not None or job['attempts'] != 0
            or job['superseded_by_windows_json'] is not None or job['due'] <= time.time()):
        return False
    current = queue.db.execute('SELECT state,source_bulk_fingerprint,authorization_fingerprint,evidence_contract_version '
        'FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()
    if (not current or current['state'] not in REPAIR_STATES
            or tuple(current)[1:] != (cnyes_bulk_fingerprint(source), source.authorization_fingerprint, CONTRACT_VERSION)):
        return False
    marker = 'cnyes_history_direct_page_recheck_v1:' + digest((url, evidence_sha))
    # A different representation/proof must not erase this URL's retry history.
    stable = 'cnyes_history_consistency_recheck_v2:' + digest((
        url, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (stable,)).fetchone()
    try:
        previous = json.loads(saved[0]) if saved else None
        if saved:
            if (type(previous) is not dict
                    or previous['contract_version'] != CONSISTENCY_RECHECK_CONTRACT_VERSION
                    or previous['url'] != url or previous['scope_url'] != scope):
                return False
            marker = previous['prior_v1_marker']
        page = int(parse_qs(url.split('?', 1)[1]).get('page', ['1'])[0])
        observed = queue.db.execute('SELECT h.last_confirmed_at_utc,o.response_sha256,o.observation_json '
            'FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
            'ON o.observation_sha256=h.observation_sha256 WHERE h.scope_url=? AND h.page=? '
            'AND o.evidence_contract_version=? AND o.source_bulk_fingerprint=? AND o.authorization_fingerprint=?',
            (scope, page, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
        if (not observed or observed['last_confirmed_at_utc'] != confirmed_at
                or observed['response_sha256'] != job['response_sha256']):
            return False
        coverage = queue.db.execute('SELECT total_articles,total_pages FROM bulk_api_coverage WHERE base_url=?', (scope,)).fetchone()
        pagination = json.loads(observed['observation_json'])['pagination']
        if not coverage or (pagination['total'], pagination['pages']) == tuple(coverage):
            return False  # Waiting for other pages is not permission to revisit a healthy page.
        prior = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if not prior:
            if saved:
                return False
            return schedule_original_page_check(queue, url,
                reason='contradictory_list_page_evidence', evidence_sha=evidence_sha)
        original = json.loads(prior[0])
        original_job = original['prior_job']
        if (type(original_job) is not dict or original_job['url'] != url
                or original_job['kind'] != 'cnyes_news' or original_job['provider'] != source.provider
                or original_job['attempts'] != 0 or original_job['error_code'] is not None
                or original_job['superseded_by_windows_json'] is not None):
            return False
        checked = datetime.fromisoformat(job['last_checked_at_utc'])
        confirmed = datetime.fromisoformat(confirmed_at)
        initial = datetime.fromisoformat(original['changed_at_utc'])
        original_checked = datetime.fromisoformat(original_job['last_checked_at_utc'])
        if (any(clock.tzinfo is None for clock in (checked, confirmed, initial, original_checked))
                or not original_checked <= initial < confirmed <= checked
                or original['reason'] != 'contradictory_list_page_evidence'):
            return False
        count = previous['attempts'] if saved else 0
        if type(count) is not int or count < (1 if saved else 0):
            return False
        if saved:
            if (previous['prior_v1_marker_sha256'] != hashlib.sha256(prior[0].encode()).hexdigest()
                    or checked <= datetime.fromisoformat(previous['successful_response_checked_at_utc'])
                    or confirmed <= datetime.fromisoformat(previous['page_confirmed_at_utc'])):
                return False
        delay = source.discovery_interval_seconds * min(2 ** min(count, 4), CONSISTENCY_RECHECK_MAX_FACTOR)
        due = max(time.time(), checked.timestamp() + delay)
    except (IndexError, KeyError, ValueError, TypeError, OverflowError):
        # A malformed/missing old witness is not permission to reset a budget.
        return False
    if due >= job['due']:
        return False
    proof = {'contract_version': CONSISTENCY_RECHECK_CONTRACT_VERSION,
        'url': url, 'scope_url': scope, 'attempts': count + 1,
        'successful_response_checked_at_utc': job['last_checked_at_utc'],
        'page_confirmed_at_utc': confirmed_at, 'scheduled_at_utc': now(),
        'current_evidence_sha256': evidence_sha,
        'next_due_epoch': due, 'spacing_seconds': delay,
        'prior_v1_marker': marker, 'prior_v1_marker_sha256': hashlib.sha256(prior[0].encode()).hexdigest(),
        'prior_discovery_job': dict(job), 'source_failures_or_deadlines_reset': False,
        'article_heads_or_completed_questions_requeued': False}
    queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (stable, encoded(proof)))
    queue.db.execute('UPDATE discovery SET due=?,etag=NULL,last_modified=NULL WHERE url=?', (due, url))
    queue.metrics.count(source.provider, 'history_consistency_rechecks_scheduled')
    return True


def _local_inventory_defers_source_recheck(queue, scope):
    """Finish local evidence first; never reset publisher failures or clocks."""
    marker = queue.db.execute('SELECT scan_state FROM cnyes_history_local_inventory_scopes '
                              'WHERE scope_url=?', (scope,)).fetchone()
    if not marker:
        return False
    if marker['scan_state'] == 'scanning' or queue.db.execute(
            "SELECT 1 FROM cnyes_history_retained_work WHERE scope_url=? "
            "AND state IN ('pending','overflow_pending','local_retry') LIMIT 1", (scope,)).fetchone():
        return True
    # Once local work settles, use the existing dispatch fairness policy.
    # Waiting for every article to drain could starve a growing history queue.
    queue.db.execute('DELETE FROM cnyes_history_local_inventory_scopes WHERE scope_url=?', (scope,))
    return False


def _release_complete_local_inventory_fence(queue, scope, source):
    """Retire a local-only fence after all pages have current proof."""
    queue.db.execute('DELETE FROM cnyes_history_local_inventory_scopes WHERE scope_url=? AND scan_state=? '
        'AND EXISTS (SELECT 1 FROM cnyes_history_scope_heads h WHERE h.scope_url=? AND h.state IN (?,?,?) '
        'AND h.evidence_contract_version=? AND h.source_bulk_fingerprint=? AND h.authorization_fingerprint=?) '
        "AND NOT EXISTS (SELECT 1 FROM cnyes_history_retained_work w WHERE w.scope_url=? "
        "AND w.state IN ('pending','overflow_pending','local_retry'))",
        (scope, 'ready', scope, *COMPLETE_STATES, CONTRACT_VERSION, cnyes_bulk_fingerprint(source),
         source.authorization_fingerprint, scope))


def repair_scope_pages(queue, scope, source):
    """Repair missing/currently contradictory evidence, not completed history.

    An old successful job's annual revisit is not a list-completeness proof.
    Prefer the existing retained-envelope worker; otherwise wake only that
    original successful page. Refusals, active/due jobs and split parents keep
    their own schedules. Conflicting successful observations use durable,
    cadence-spaced recovery rather than a permanent one-shot dead end.
    """
    result = {'retained_pages_queued': 0, 'page_checks_scheduled': 0}
    key = scope_key(scope)
    if (not key or not source.allowed or key[0] not in source.cnyes_apis
            or source.acquisition_mode == 'finite' or source.known_url_only):
        return result
    if _local_inventory_defers_source_recheck(queue, scope):
        return result
    coverage = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?', (scope,)).fetchone()
    if (not coverage or not 1 <= coverage['total_pages'] <= 30
            or queue.db.execute('SELECT 1 FROM bulk_window_splits WHERE scope_url=?', (scope,)).fetchone()):
        return result
    observations = {row['page']: (row['observation_sha256'], json.loads(row['observation_json']), row['last_confirmed_at_utc'])
        for row in queue.db.execute('SELECT h.page,h.last_confirmed_at_utc,o.observation_sha256,o.observation_json '
            'FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
            'ON o.observation_sha256=h.observation_sha256 WHERE h.scope_url=? AND h.page<=?',
            (scope, coverage['total_pages']))}
    query = parse_qs(scope.split('?', 1)[1])
    batch_scan_complete = None
    for page in range(1, coverage['total_pages'] + 1):
        prior = observations.get(page)
        if prior and (prior[1]['pagination']['total'], prior[1]['pagination']['pages'],
                prior[1]['contract_version'], prior[1]['source_bulk_fingerprint'],
                prior[1]['authorization_fingerprint'], prior[1]['source_day_offset_seconds']) == (
                coverage['total_articles'], coverage['total_pages'], CONTRACT_VERSION,
                cnyes_bulk_fingerprint(source), source.authorization_fingerprint, CNYES_QUERY_DAY_OFFSET_SECONDS):
            continue
        url = cnyes_url(key[0], page, start=int(query['startAt'][0]), end=int(query['endAt'][0]))
        job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
        if (not job or job['kind'] != 'cnyes_news' or job['provider'] != source.provider
                or job['error_code'] is not None or job['attempts'] != 0
                or job['superseded_by_windows_json'] is not None or job['due'] <= time.time()):
            continue
        work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
        if work and work['state'] in ('pending', 'overflow_pending', 'local_retry'):
            continue  # A local candidate or its retry must settle before a new source check.
        if not prior:
            legacy = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                      (scope, page)).fetchone()
            if not legacy and page == 1 and coverage['total_articles'] == 0 and coverage['total_pages'] == 1:
                # A valid zero-result response has no legacy page-ID row. Its
                # empty set is an expectation, never proof without raw bytes.
                legacy = {'total': 0, 'ids_sha256': hashlib.sha256(json.dumps([]).encode()).hexdigest(),
                          'observed_at_utc': coverage['observed_at_utc']}
            sha = job['response_sha256']
            files_present = False
            if not work and legacy and sha and job['last_checked_at_utc']:
                try:
                    files_present = any(path.is_file() and path.with_suffix('.parquet.receipt.json').is_file()
                                        for path, _ in _retained_envelopes(queue.root, sha))
                except (OSError, MemoryError):
                    # Persist the candidate for the local worker; an unreadable
                    # disk is not evidence that a publisher request is needed.
                    files_present = True
                if not files_present:
                    files_present = bool(queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates '
                        'WHERE request_url=? AND response_sha256=? LIMIT 1', (url, sha)).fetchone())
            if files_present:
                queue.db.execute('INSERT INTO cnyes_history_retained_work '
                    '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
                    'VALUES (?,?,?,?,?,?,?)', (url, scope, page, sha, legacy['total'], legacy['ids_sha256'],
                                               legacy['observed_at_utc']))
                result['retained_pages_queued'] += 1
                continue
            if legacy and legacy['total'] > 0 and _retained_envelopes(queue.root, sha):
                # An unindexed accepted body batch may still contain this
                # exact original. Do not create a redundant publisher job
                # while that bounded local inventory is incomplete.
                if batch_scan_complete is None:
                    batch_scan_complete = accepted_batch_inventory_complete(queue, source)
                if not batch_scan_complete:
                    continue
        evidence = digest((scope, page, coverage['total_articles'], coverage['total_pages'],
            CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
            prior[0] if prior else ('missing_page', job['last_checked_at_utc'], job['response_sha256'])))
        prior_current = prior and prior[1]['scope_url'] == scope and (
            prior[1]['contract_version'], prior[1]['source_bulk_fingerprint'],
            prior[1]['authorization_fingerprint'], prior[1]['source_day_offset_seconds']) == (
                CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
                CNYES_QUERY_DAY_OFFSET_SECONDS)
        if prior_current:
            result['page_checks_scheduled'] += schedule_consistency_recheck(queue, url, source,
                scope=scope, evidence_sha=evidence, confirmed_at=prior[2])
        else:
            result['page_checks_scheduled'] += schedule_original_page_check(queue, url,
                reason='missing_current_list_page_evidence' if not prior else 'contradictory_list_page_evidence',
                evidence_sha=evidence)
    return result


def repair_incomplete_scopes(queue, *, stop=None, limit=8, max_seconds=.2):
    """Bounded, indexed restart recovery; rotate past unresolved early scopes."""
    started, done = time.monotonic(), 0
    result = {'scopes_checked': 0, 'retained_pages_queued': 0, 'page_checks_scheduled': 0}
    source = queue.registry.sources.get('cnyes')
    if (not source or not source.allowed or limit <= 0 or max_seconds <= 0
            or stop is not None and stop.is_set()):
        return result
    cursor = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone()
    prefix = (source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)
    after = cursor[0] if cursor else ''
    params = tuple(value for state in REPAIR_STATES for value in (*prefix, state, after)) + (limit,)
    scopes = queue.db.execute(REPAIR_SCOPE_SEEK, params).fetchall()
    with queue.db:
        for row in scopes:
            if time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
                break
            for name, count in repair_scope_pages(queue, row[0], source).items():
                result[name] += count
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (REPAIR_CURSOR, row[0]))
            done += 1
        if done == len(scopes) and len(scopes) < limit:
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (REPAIR_CURSOR,))
    result['scopes_checked'] = done
    if done < len(scopes):
        # A timeout or stop before the first row does not write a cursor.
        # The owner must still know that the fetched slice is unfinished.
        result['unfinished_slice'] = True
    return result


def share_matching_page_IDs(queue, scope, sha, info, source, stamp):
    """Reuse actual IDs/dates across equal-day queries one proven page at a time.

    Keep the observation's ORIGINAL query, raw artifact and capture clock.
    The other page's ORIGINAL ID hash/clock is an immutable reference receipt,
    not a fabricated raw response or an alias article. Missing/disagreeing
    pages fall back to their original canonical jobs, with refusal gates intact.
    """
    key = scope_key(scope)
    query_source = queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations WHERE observation_sha256=?',
                                    (sha,)).fetchone()
    if not query_source:
        return
    observed = json.loads(query_source[0])
    if any(not key[1] <= (entry['publishAt'] + CNYES_QUERY_DAY_OFFSET_SECONDS) // 86400 <= key[2]
           for entry in observed['identities']):
        return
    for (other,) in queue.db.execute('SELECT scope_url FROM cnyes_history_scope_keys WHERE '
        'endpoint=? AND first_day=? AND last_day=? AND provider=? AND scope_url!=?', (*key, source.provider, scope)).fetchall():
        if queue.db.execute('SELECT 1 FROM cnyes_history_local_inventory_scopes WHERE scope_url=?', (other,)).fetchone():
            continue  # The original scope is still collecting its own pages.
        coverage = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?', (other,)).fetchone()
        if not coverage or info['page'] > min(coverage['total_pages'], 30):
            continue
        existing = queue.db.execute('SELECT o.observation_json,h.last_confirmed_at_utc FROM cnyes_history_page_heads h '
            'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
            'WHERE h.scope_url=? AND h.page=?', (other, info['page'])).fetchone()
        if existing:
            own = json.loads(existing[0])
            compatible = (own['contract_version'] == CONTRACT_VERSION
                and own['source_bulk_fingerprint'] == cnyes_bulk_fingerprint(source)
                and own['authorization_fingerprint'] == source.authorization_fingerprint)
            if compatible and (own['scope_url'] == other or datetime.fromisoformat(existing[1]) >= datetime.fromisoformat(stamp)):
                continue  # Independent/current source evidence is stronger than a shared reference.
        original = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                    (other, info['page'])).fetchone()
        if (original is None or coverage['total_articles'] != info['total'] or coverage['total_pages'] != info['pages']
                or (original['total'], original['ids_sha256']) != (info['total'], info['ids_sha256'])):
            q = parse_qs(other.split('?', 1)[1])
            url = cnyes_url(key[0], info['page'], start=int(q['startAt'][0]), end=int(q['endAt'][0]))
            queue.db.execute('INSERT OR REPLACE INTO cnyes_history_unproven_pages VALUES (?,?,?)', (other, info['page'], encoded({
                'actual_observation_sha256': sha, 'actual_reference_scope': scope,
                'actual_confirmed_at_utc': stamp, 'original_page_evidence': None if original is None else dict(original),
                'original_request_url': url, 'reason': 'unproven_or_different_original_page_ID_set'})))
            schedule_original_page_check(queue, url, reason='unproven_or_different_original_page_ID_set', evidence_sha=sha)
            reconcile_scope(queue, other, source)
            continue
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_page_ID_references VALUES (?,?,?,?,?)',
                         (other, info['page'], sha, encoded(dict(original)), scope))
        queue.db.execute('INSERT OR REPLACE INTO cnyes_history_page_heads VALUES (?,?,?,?)',
                         (other, info['page'], sha, stamp))
        queue.db.execute('DELETE FROM cnyes_history_unproven_pages WHERE scope_url=? AND page=?', (other, info['page']))
        reconcile_scope(queue, other, source)
        resolve_forward_retained_outcomes(queue, other, info['page'], source)


def save_proof(queue, scope, source, state, facts, members):
    previous = queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()
    proof = {'contract_version': CONTRACT_VERSION, 'scope_url': scope, 'provider': source.provider,
        'state': state, 'facts': facts, 'member_ids_sha256': digest(sorted(members)),
        'unique_article_IDs': len(members), 'body_completeness_verified': False,
        'historical_point_in_time': False, 'source_bulk_fingerprint': cnyes_bulk_fingerprint(source)}
    proof['authorization_fingerprint'] = source.authorization_fingerprint
    sha = digest(proof)
    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_scope_proofs VALUES (?,?,?)', (sha, scope, encoded(proof)))
    queue.db.execute('INSERT OR REPLACE INTO cnyes_history_scope_heads VALUES (?,?,?,?,?,?,?,?)',
                     (scope, source.provider, state, sha, encoded(sorted(members)), cnyes_bulk_fingerprint(source),
                      source.authorization_fingerprint, CONTRACT_VERSION))
    if state not in COMPLETE_STATES and previous and previous[0] in COMPLETE_STATES:
        queue.db.execute('INSERT OR IGNORE INTO bulk_window_gaps VALUES (?,?,?,?)',
                         (scope, source.provider, 'cnyes_source_list_evidence_changed', now()))
    if state in COMPLETE_STATES:
        gap = queue.db.execute('SELECT * FROM bulk_window_gaps WHERE scope_url=?', (scope,)).fetchone()
        if gap:
            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_gap_retirements VALUES (?,?,?,?)',
                             (scope, sha, encoded(dict(gap)), now()))
            queue.db.execute('DELETE FROM bulk_window_gaps WHERE scope_url=?', (scope,))
    return sha


def reconcile_scope(queue, scope, source):
    coverage = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?', (scope,)).fetchone()
    if not coverage:
        return
    if coverage['total_pages'] > 30:
        if queue.db.execute('SELECT 1 FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone():
            save_proof(queue, scope, source, 'source_page_cap_exceeded', {
                'total_articles_raw': coverage['total_articles'], 'total_pages_raw': coverage['total_pages']}, [])
            reconcile_parents(queue, scope, source)
        return
    rows = queue.db.execute('SELECT o.observation_sha256,o.observation_json FROM cnyes_history_page_heads h '
        'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
        'WHERE h.scope_url=? AND h.page<=? ORDER BY h.page', (scope, coverage['total_pages'])).fetchall()
    observations = [json.loads(row[1]) for row in rows]
    pages, total = coverage['total_pages'], coverage['total_articles']
    facts = {'total_articles_raw': total, 'total_pages_raw': pages,
             'page_observation_sha256': [row[0] for row in rows], 'source_day_bounds': scope_key(scope)[1:],
             'shared_page_ID_references': []}
    members, identities, state = set(), {}, 'waiting_page_evidence'
    if {row['page'] for row in observations} == set(range(1, pages + 1)):
        state = 'list_complete'
        for observation in observations:
            info = observation['pagination']
            if (info['total'] != total or info['pages'] != pages
                    or observation['contract_version'] != CONTRACT_VERSION
                    or observation['source_day_offset_seconds'] != CNYES_QUERY_DAY_OFFSET_SECONDS
                    or observation['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
                    or observation['authorization_fingerprint'] != source.authorization_fingerprint):
                state = 'inconsistent_source_snapshot'
                break
            if observation['scope_url'] != scope:
                reference = queue.db.execute('SELECT * FROM cnyes_history_page_ID_references '
                    'WHERE scope_url=? AND page=? AND observation_sha256=?',
                    (scope, observation['page'], digest(observation))).fetchone()
                original = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                            (scope, observation['page'])).fetchone()
                if (not reference or not original or scope_key(observation['scope_url']) != scope_key(scope)
                        or (original['total'], original['ids_sha256']) != (info['total'], info['ids_sha256'])):
                    state = 'inconsistent_original_page_ID_reference'
                    break
                facts['shared_page_ID_references'].append(dict(reference))
            first, last = facts['source_day_bounds']
            for entry in observation['identities']:
                day = (entry['publishAt'] + CNYES_QUERY_DAY_OFFSET_SECONDS) // 86400
                prior = identities.setdefault(entry['newsId'], (entry['publishAt'], entry['source_post_sha256']))
                if not first <= day <= last or prior != (entry['publishAt'], entry['source_post_sha256']):
                    state = 'inconsistent_source_identity_or_date'
                    break
                members.add(entry['newsId'])
            if state != 'list_complete':
                break
    facts['declared_totals_are_not_unique_article_denominators'] = True
    facts['duplicate_rows_observed'] = sum(row['pagination']['duplicate_rows'] for row in observations)
    unproven = [json.loads(row[0]) for row in queue.db.execute(
        'SELECT evidence_json FROM cnyes_history_unproven_pages WHERE scope_url=? AND page<=? ORDER BY page', (scope, pages))]
    if unproven and state in ('list_complete', 'waiting_page_evidence'):
        facts['original_page_rechecks_required'] = unproven
        state = 'waiting_original_page_recheck'
    if state == 'list_complete' and facts['shared_page_ID_references']:
        state = 'list_complete_by_page_ID_equivalence'
    save_proof(queue, scope, source, state, facts, members)
    if state in REPAIR_STATES:
        repair_scope_pages(queue, scope, source)
    reconcile_parents(queue, scope, source)


def reconcile_parents(queue, child, source):
    for (parent,) in queue.db.execute('SELECT parent_scope FROM cnyes_history_edges WHERE child_scope=?', (child,)).fetchall():
        gap = queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (parent,)).fetchone()
        previous = queue.db.execute('SELECT 1 FROM cnyes_history_scope_heads WHERE scope_url=?', (parent,)).fetchone()
        if not gap and not previous:
            continue  # Full-history roots are not silently certified by this gap-repair contract.
        children = queue.db.execute('SELECT e.child_scope,h.state,h.proof_sha256,h.member_ids_json,c.total_articles, '
            'h.source_bulk_fingerprint,h.authorization_fingerprint,h.evidence_contract_version '
            'FROM cnyes_history_edges e LEFT JOIN cnyes_history_scope_heads h ON h.scope_url=e.child_scope '
            'LEFT JOIN bulk_api_coverage c ON c.base_url=e.child_scope WHERE e.parent_scope=? ORDER BY e.child_scope',
            (parent,)).fetchall()
        old = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?', (parent,)).fetchone()
        if not old or not children:
            continue
        if any(row['state'] not in COMPLETE_STATES or row['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
               or row['authorization_fingerprint'] != source.authorization_fingerprint
               or row['evidence_contract_version'] != CONTRACT_VERSION for row in children):
            if previous:
                save_proof(queue, parent, source, 'waiting_child_evidence', {
                    'child_scopes': [row['child_scope'] for row in children],
                    'child_states': [row['state'] for row in children]}, [])
            continue
        ranges = sorted(scope_key(row['child_scope'])[1:] for row in children)
        expected = scope_key(parent)[1:]
        all_ids, disjoint = set(), True
        for row in children:
            ids = set(json.loads(row['member_ids_json']))
            disjoint = disjoint and all_ids.isdisjoint(ids)
            all_ids.update(ids)
        if (ranges[0][0] != expected[0] or ranges[-1][1] != expected[1]
                or any(left[1] + 1 != right[0] for left, right in zip(ranges, ranges[1:]))
                or not disjoint or sum(row['total_articles'] for row in children) != old['total_articles']):
            if previous:
                save_proof(queue, parent, source, 'inconsistent_partition_evidence', {
                    'child_proofs': [row['proof_sha256'] for row in children],
                    'original_parent_total_raw': old['total_articles'],
                    'current_child_total_sum': sum(row['total_articles'] for row in children)}, [])
            continue  # Preserve the genuine cap/count conflict, never manufacture a complete parent.
        save_proof(queue, parent, source, 'partition_list_complete', {
            'total_articles_raw': old['total_articles'], 'source_day_bounds': expected,
            'children': [{'scope_url': row['child_scope'], 'proof_sha256': row['proof_sha256']} for row in children],
            'all_child_ID_sets_disjoint': True, 'child_total_sum_matches_retained_parent': True,
            'original_parent_page_evidence_preserved': True,
        }, all_ids)


def progress(db, source=None):
    states = {row[0]: row[1] for row in db.execute('SELECT state,count(*) FROM cnyes_history_scope_heads GROUP BY state')}
    work = {row[0]: row[1] for row in db.execute('SELECT state,count(*) FROM cnyes_history_retained_work GROUP BY state')}
    current = None if source is None else {row[0]: row[1] for row in db.execute(
        'SELECT state,count(*) FROM cnyes_history_scope_heads WHERE provider=? AND evidence_contract_version=? AND source_bulk_fingerprint=? '
        'AND authorization_fingerprint=? GROUP BY state',
        (source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))}
    scan_state = 'unknown'
    if source is not None:
        marker = db.execute('SELECT value FROM queue_settings WHERE key=?', (overflow_scan_key(source),)).fetchone()
        if marker is None:
            scan_state = 'not_started'
        else:
            try:
                checkpoint = json.loads(marker[0])
                if (type(checkpoint) is dict and type(checkpoint.get('version')) is int
                        and checkpoint['version'] == OVERFLOW_RETAINED_SCAN_VERSION
                        and checkpoint.get('state') in ('scanning', 'complete')
                        and type(checkpoint.get('scope_url')) is str and type(checkpoint.get('page')) is int
                        and 0 <= checkpoint['page'] <= 30):
                    scan_state = checkpoint['state']
            except (TypeError, ValueError):
                pass
    retry_overflow = db.execute("SELECT count(*) FROM cnyes_history_retained_local_retries "
        "WHERE original_state='overflow_pending'").fetchone()[0]
    local_recovery = {'contract_version': 1, 'scan_state': scan_state,
        'pending_retained_pages': work.get('overflow_pending', 0) + retry_overflow,
        'local_retry_pages': work.get('local_retry', 0), 'local_raw_only': True,
        'category_gap_completion_claimed': False, 'publisher_requests': 0}
    nonzero = {'contract_version': 1, 'scan_state': 'unknown', 'checked_steps': None,
        'queued_candidate_pages': None, 'local_raw_only': True,
        'all_history_complete': False, 'publisher_requests': 0}
    if source is not None:
        marker = db.execute('SELECT value FROM queue_settings WHERE key=?', (nonzero_scan_key(source),)).fetchone()
        if marker is None:
            nonzero.update(scan_state='not_started', checked_steps=0, queued_candidate_pages=0)
        else:
            try:
                checkpoint = json.loads(marker[0])
                if (type(checkpoint) is dict and type(checkpoint.get('version')) is int and checkpoint['version'] == 1
                        and checkpoint.get('state') in ('scanning', 'complete')
                        and type(checkpoint.get('scope_url')) is str and type(checkpoint.get('page')) is int
                        and 0 <= checkpoint['page'] <= 30
                        and type(checkpoint.get('checked')) is int and checkpoint['checked'] >= 0
                        and type(checkpoint.get('queued')) is int and checkpoint['queued'] >= 0):
                    nonzero.update(scan_state=checkpoint['state'], checked_steps=checkpoint['checked'],
                                   queued_candidate_pages=checkpoint['queued'])
            except (TypeError, ValueError):
                pass
    accepted = {'contract_version': 1, 'scan_state': 'unknown', 'scanned_batches': None,
        'indexed_candidates': None, 'local_raw_only': True, 'all_history_complete': False, 'publisher_requests': 0}
    if source is not None:
        marker = db.execute('SELECT value FROM queue_settings WHERE key=?', (accepted_batch_scan_key(source),)).fetchone()
        if marker is None:
            accepted.update(scan_state='not_started', scanned_batches=0, indexed_candidates=0)
        else:
            try:
                checkpoint = json.loads(marker[0])
                if (type(checkpoint) is dict and type(checkpoint.get('version')) is int and checkpoint['version'] == 1
                        and checkpoint.get('state') in ('scanning', 'complete')
                        and type(checkpoint.get('path')) is str and type(checkpoint.get('end_path')) is str
                        and all(type(checkpoint.get(k)) is int and checkpoint[k] >= 0 for k in ('checked', 'indexed'))
                        and type(checkpoint.get('retry_after')) in (int, float)
                        and math.isfinite(checkpoint['retry_after']) and checkpoint['retry_after'] >= 0):
                    scan_state = ('local_retry_wait' if checkpoint['state'] == 'scanning'
                                  and checkpoint['retry_after'] > time.time() else checkpoint['state'])
                    accepted.update(scan_state=scan_state, scanned_batches=checkpoint['checked'],
                                    indexed_candidates=checkpoint['indexed'])
            except (TypeError, ValueError):
                pass
    return {'contract_version': CONTRACT_VERSION, 'scope_states': states, 'retained_metadata_work': work,
            'current_source_contract_scope_states': current,
            'retired_gap_receipts': db.execute('SELECT count(*) FROM cnyes_history_gap_retirements').fetchone()[0],
            'overflow_retained_recovery': local_recovery,
            'unobserved_nonzero_retained_scan': nonzero,
            'accepted_body_batch_scan': accepted,
            'list_coverage_is_not_body_coverage': True, 'all_history_complete': False, 'finish_eta_utc': None}


def overflow_scan_key(source):
    return OVERFLOW_RETAINED_SCAN_PREFIX + digest((
        source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))


def nonzero_scan_key(source):
    return LEGACY_NONZERO_SCAN_PREFIX + digest((
        source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))


def accepted_batch_scan_key(source):
    return ACCEPTED_BATCH_SCAN_PREFIX + digest((
        source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))


def accepted_batch_inventory_complete(queue, source):
    """An absent/corrupt checkpoint never proves that stored originals are absent."""
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                             (accepted_batch_scan_key(source),)).fetchone()
    if not saved:
        return not queue.db.execute('SELECT 1 FROM accepted_batches LIMIT 1').fetchone()
    try:
        state = json.loads(saved[0])
        return (type(state) is dict and type(state.get('version')) is int and state['version'] == 1
                and state.get('state') == 'complete' and type(state.get('path')) is str
                and type(state.get('end_path')) is str
                and all(type(state.get(k)) is int and state[k] >= 0 for k in ('checked', 'indexed', 'attempts'))
                and type(state.get('retry_after')) in (int, float)
                and math.isfinite(state['retry_after']) and state['retry_after'] >= 0
                and not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries '
                                         'WHERE scan_key=? LIMIT 1', (accepted_batch_scan_key(source),)).fetchone())
    except (TypeError, ValueError):
        return False


def _register_accepted_batch_candidate(queue, url, sha, relative, receipt_relative):
    inserted = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_candidates VALUES (?,?,?,?)',
                                (url, sha, relative, receipt_relative)).rowcount
    if inserted and getattr(queue, '_cnyes_retained_work_initialized', False):
        prior = queue.db.execute('SELECT * FROM cnyes_history_retained_work '
            "WHERE request_url=? AND response_sha256=? AND state IN "
            "('retained_unavailable','retained_invalid','retained_request_mismatch')",
            (url, sha)).fetchone()
        if prior:
            # Preserve the old failure as an outcome; a candidate is only
            # permission to validate it, never evidence of a source page.
            _save_retained_resolution(queue, prior, 'pending', None, 'new_accepted_body_batch_candidate')
    return inserted


def register_accepted_body_batch(queue, records, relative, receipt_relative):
    """Index newly accepted raw references in their existing commit."""
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        return
    for row in records:
        url, sha = row.get('source_request_url'), row.get('response_sha256')
        if (row.get('provider') != source.provider or not isinstance(url, str) or 'startAt=' not in url
                or row.get('raw_response_body') is None or not _retained_envelopes(queue.root, sha)):
            continue
        try:
            key = scope_key(url)
            page = int(parse_qs(url.split('?', 1)[1]).get('page', ['1'])[0])
        except (TypeError, ValueError, IndexError, OverflowError):
            continue
        if key and key[0] in source.cnyes_apis and 1 <= page <= 30:
            _register_accepted_batch_candidate(queue, url, sha, relative, receipt_relative)


def _accepted_batch_metadata_pairs(path, source):
    """Find candidate request/hash pairs from footers; never read body text."""
    batch = pq.ParquetFile(path)
    required = {'source_request_url', 'response_sha256', 'raw_response_body'}
    if not required <= set(batch.schema_arrow.names):
        return
    bases = tuple(source.cnyes_apis)
    for number in range(batch.metadata.num_row_groups):
        group = batch.metadata.row_group(number)
        columns = {group.column(i).path_in_schema: group.column(i) for i in range(group.num_columns)}
        raw = columns['raw_response_body'].statistics
        if raw and raw.has_null_count and raw.null_count == group.num_rows:
            continue
        request = columns['source_request_url'].statistics
        response = columns['response_sha256'].statistics
        if request and request.has_min_max and isinstance(request.min, str) and isinstance(request.max, str):
            low, high = request.min, request.max
            if not any(low <= base + '\uffff' and high >= base for base in bases):
                continue
            if (low == high and 'startAt=' in low and response and response.has_min_max
                    and isinstance(response.min, str) and response.min == response.max):
                yield low, response.min
                continue
        # Mixed or missing statistics cannot prove absence. Read only the two
        # metadata columns; the validating worker will inspect the raw row.
        for chunk in batch.iter_batches(batch_size=256, row_groups=[number],
                columns=['source_request_url', 'response_sha256'], use_threads=False):
            for row in chunk.to_pylist():
                if isinstance(row.get('source_request_url'), str) and isinstance(row.get('response_sha256'), str):
                    yield row['source_request_url'], row['response_sha256']


def seed_accepted_body_batch_candidates(queue, *, stop=None, limit=128, max_seconds=.01):
    """Index retained accepted batches once; candidates are not source proof."""
    from downloader.news_fulltext_queue import replay_source_path
    result = {'complete': False, 'checked': 0, 'indexed': 0, 'blocked': None, 'next_retry_epoch': None}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result.update(complete=True, blocked='source_disabled')
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    marker = accepted_batch_scan_key(source)
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    try:
        last = queue.db.execute('SELECT path FROM accepted_batches ORDER BY path DESC LIMIT 1').fetchone() if not saved else None
        state = json.loads(saved[0]) if saved else {'version': 1, 'state': 'scanning', 'path': '',
            'end_path': last[0] if last else '', 'checked': 0, 'indexed': 0, 'retry_after': 0, 'attempts': 0}
        if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != 1
                or state.get('state') not in ('scanning', 'complete') or type(state.get('path')) is not str
                or type(state.get('end_path')) is not str or any(type(state.get(k)) is not int or state[k] < 0
                    for k in ('checked', 'indexed', 'attempts')) or type(state.get('retry_after')) not in (int, float)
                or not math.isfinite(state['retry_after']) or state['retry_after'] < 0):
            raise ValueError('invalid checkpoint')
    except (TypeError, ValueError, KeyError):
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)',
                (ACCEPTED_BATCH_SCAN_PREFIX + 'invalid:' + digest((marker, saved[0])), saved[0]))
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (marker,))
        last = queue.db.execute('SELECT path FROM accepted_batches ORDER BY path DESC LIMIT 1').fetchone()
        state = {'version': 1, 'state': 'scanning', 'path': '', 'end_path': last[0] if last else '',
                 'checked': 0, 'indexed': 0, 'retry_after': 0, 'attempts': 0}
    if state['state'] == 'complete':
        result['complete'] = True
        return result
    if state['retry_after'] > time.time():
        result.update(blocked='local_retry_wait', next_retry_epoch=state['retry_after'])
        return result
    # Retry at most one failed file per slice; a failing file must not hold
    # the cursor in front of every other independent retained batch.
    retry = queue.db.execute('SELECT parquet_path AS path,receipt_path,local_attempts '
        'FROM cnyes_history_accepted_batch_local_retries WHERE scan_key=? AND not_before_epoch<=? '
        'ORDER BY not_before_epoch,parquet_path LIMIT 1', (marker, time.time())).fetchone()
    rows = queue.db.execute('SELECT path,receipt_path FROM accepted_batches WHERE path>? AND path<=? '
                            'ORDER BY path LIMIT ?', (state['path'], state['end_path'], limit - bool(retry))).fetchall()
    work = ([(retry, True)] if retry else []) + [(row, False) for row in rows]
    started = time.monotonic()
    with queue.db:
        for index, (row, is_retry) in enumerate(work):
            if stop is not None and stop.is_set() or index and time.monotonic() - started >= max_seconds:
                break
            try:
                path = replay_source_path(queue.root, row['path'])
                pairs = _accepted_batch_metadata_pairs(path, source)
                for url, sha in pairs:
                    if not _retained_envelopes(queue.root, sha):
                        continue
                    try:
                        key = scope_key(url)
                        page = int(parse_qs(url.split('?', 1)[1]).get('page', ['1'])[0])
                    except (TypeError, ValueError, IndexError, OverflowError):
                        continue
                    if not key or key[0] not in source.cnyes_apis or not 1 <= page <= 30:
                        continue
                    job = queue.db.execute('SELECT response_sha256,provider,kind FROM discovery WHERE url=?', (url,)).fetchone()
                    legacy = queue.db.execute('SELECT 1 FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                              (cnyes_scope(url), page)).fetchone()
                    if job and job['provider'] == source.provider and job['kind'] == 'cnyes_news' and job['response_sha256'] == sha and legacy:
                        result['indexed'] += _register_accepted_batch_candidate(queue, url, sha, row['path'],
                                                                               row['receipt_path'])
                if is_retry or state['attempts']:
                    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                        (row['path'], row['receipt_path'], 'recovered', 'local_batch_read_recovered', now()))
                state.update(attempts=0, retry_after=0)
                if is_retry:
                    queue.db.execute('DELETE FROM cnyes_history_accepted_batch_local_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
            except FileNotFoundError:
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                    (row['path'], row['receipt_path'], 'unavailable', 'local_batch_file_missing', now()))
                if is_retry:
                    queue.db.execute('DELETE FROM cnyes_history_accepted_batch_local_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
                state.update(attempts=0, retry_after=0)
            except OSError as error:
                attempts = (row['local_attempts'] if is_retry else state['attempts']) + 1
                deadline = time.time() + min(300, 2 ** min(attempts - 1, 9))
                queue.db.execute('INSERT OR REPLACE INTO cnyes_history_accepted_batch_local_retries VALUES (?,?,?,?,?)',
                    (marker, row['path'], row['receipt_path'], deadline, attempts))
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                    (row['path'], row['receipt_path'], 'local_retry', type(error).__name__, now()))
                state.update(attempts=0, retry_after=0)
            except MemoryError as error:
                attempts = state['attempts'] + 1
                state.update(attempts=attempts, retry_after=time.time() + min(300, 2 ** min(attempts - 1, 9)))
                result.update(blocked='local_retry_wait', next_retry_epoch=state['retry_after'])
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                    (row['path'], row['receipt_path'], 'local_retry', type(error).__name__, now()))
                break  # Resource pressure is not an independent per-file failure.
            except (pa.ArrowException, ValueError, TypeError, KeyError) as error:
                queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                    (row['path'], row['receipt_path'], 'invalid', type(error).__name__, now()))
                if is_retry:
                    queue.db.execute('DELETE FROM cnyes_history_accepted_batch_local_retries '
                                     'WHERE scan_key=? AND parquet_path=?', (marker, row['path']))
                state.update(attempts=0, retry_after=0)
            if not is_retry:
                state['path'] = row['path']
                result['checked'] += 1
        remaining = queue.db.execute('SELECT 1 FROM accepted_batches WHERE path>? AND path<=? LIMIT 1',
                                     (state['path'], state['end_path'])).fetchone()
        retry_after = queue.db.execute('SELECT min(not_before_epoch) FROM cnyes_history_accepted_batch_local_retries '
                                       'WHERE scan_key=?', (marker,)).fetchone()[0]
        if not remaining and retry_after is None and result['blocked'] is None:
            state['state'] = 'complete'
            result['complete'] = True
        elif not remaining and retry_after is not None and result['blocked'] is None:
            result.update(blocked='local_retry_wait', next_retry_epoch=retry_after)
        state['checked'] += result['checked']
        state['indexed'] += result['indexed']
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, encoded(state)))
    return result


def seed_overflow_retained(queue, *, stop=None, limit=128, max_seconds=.05):
    """Find legacy raw pages in single-day overflows, once per source contract.

    This is a bounded local migration, not permission to fetch a page again.
    Index all eligible scopes before processing the new work so existing
    equal-day page-ID references can be shared even across restart/slices.
    Missing files, unequal requests and unobserved pages stay unresolved.
    """
    result = {'complete': False, 'checked': 0, 'queued': 0, 'blocked': None}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result.update(complete=True, blocked='source_disabled')
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    marker = overflow_scan_key(source)
    raw = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    try:
        state = json.loads(raw[0]) if raw else {'version': OVERFLOW_RETAINED_SCAN_VERSION,
            'state': 'scanning', 'scope_url': '', 'page': 30}
        if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != OVERFLOW_RETAINED_SCAN_VERSION
                or state.get('state') not in ('scanning', 'complete')
                or type(state.get('scope_url')) is not str or type(state.get('page')) is not int
                or not 0 <= state['page'] <= 30):
            raise ValueError('invalid checkpoint')
    except (TypeError, ValueError, KeyError):
        result['blocked'] = 'invalid_overflow_scan_checkpoint'
        return result
    if state['state'] == 'complete':
        result['complete'] = True
        return result

    started = time.monotonic()
    # The scope primary key is also the seek and ordering key.  CROSS JOIN
    # keeps SQLite from scanning the much larger coverage table first.
    comparator = '>' if state['page'] == 30 else '>='
    scopes = queue.db.execute('SELECT g.scope_url,c.total_articles FROM bulk_window_gaps g '
        'CROSS JOIN bulk_api_coverage c ON c.base_url=g.scope_url '
        'WHERE g.scope_url' + comparator + '? AND g.provider=? AND g.error_code=? '
        'AND c.total_pages>30 ORDER BY g.scope_url LIMIT ?',
        (state['scope_url'], source.provider, 'cnyes_single_day_page_cap', limit)).fetchall()
    exhausted_scopes = True
    with queue.db:
        for row in scopes:
            if result['checked'] >= limit or time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
                exhausted_scopes = False
                break
            scope, total = row['scope_url'], row['total_articles']
            try:
                key = scope_key(scope)
            except (TypeError, ValueError, OverflowError):
                key = None
            if (not key or key[0] not in source.cnyes_apis or key[1] != key[2]
                    or queue.db.execute('SELECT 1 FROM bulk_window_splits WHERE scope_url=?', (scope,)).fetchone()):
                state.update(scope_url=scope, page=30)
                result['checked'] += 1
                continue
            index_scope(queue, scope, source.provider)
            after = state['page'] if state['scope_url'] == scope else 0
            pages = queue.db.execute('SELECT p.page,p.total,p.ids_sha256,p.observed_at_utc '
                'FROM bulk_page_evidence p LEFT JOIN cnyes_history_page_heads h '
                'ON h.scope_url=p.scope_url AND h.page=p.page '
                "WHERE p.scope_url=? AND typeof(p.page)='integer' AND p.page>=1 "
                'AND p.page>? AND p.page<=30 AND h.scope_url IS NULL ORDER BY p.page',
                (scope, after)).fetchall()
            query = parse_qs(scope.split('?', 1)[1])
            for page in pages:
                if result['checked'] >= limit or time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
                    exhausted_scopes = False
                    break
                url = cnyes_url(key[0], page['page'], start=int(query['startAt'][0]), end=int(query['endAt'][0]))
                job = queue.db.execute('SELECT kind,provider,response_sha256,last_checked_at_utc '
                                       'FROM discovery WHERE url=?', (url,)).fetchone()
                sha = job['response_sha256'] if job else None
                if (job and job['kind'] == 'cnyes_news' and job['provider'] == source.provider
                        and job['last_checked_at_utc'] and page['total'] == total
                        and type(sha) is str and len(sha) == 64
                        and all(ch in '0123456789abcdef' for ch in sha)):
                    path = queue.root / 'quarantine' / (sha + '.parquet')
                    try:
                        files_present = path.is_file() and path.with_suffix('.parquet.receipt.json').is_file()
                        requests = []
                        if files_present:
                            requests = pq.read_table(path, columns=['request_url']).column('request_url').to_pylist()
                    except (OSError, MemoryError):
                        # Persist a candidate before advancing the scan. It is
                        # not an identity proof; the local worker validates the
                        # actual envelope after storage becomes available.
                        requests = [url]
                    except (ValueError, KeyError, TypeError, OverflowError):
                        requests = [url]  # Worker preserves invalid-evidence outcome.
                    if requests == [url]:
                        inserted = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_work '
                            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
                            'source_checked_at_utc,state) VALUES (?,?,?,?,?,?,?,?)',
                            (url, scope, page['page'], sha, page['total'], page['ids_sha256'],
                             page['observed_at_utc'], 'overflow_pending'))
                        result['queued'] += inserted.rowcount
                state.update(scope_url=scope, page=page['page'])
                result['checked'] += 1
            else:
                state.update(scope_url=scope, page=30)
                if not pages:
                    result['checked'] += 1
                continue
            break
        if exhausted_scopes and len(scopes) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, encoded(state)))
    return result


def prepare_restricted_history_dispatch(queue, task, source, *, compatible_fingerprint):
    """Recover a missing historical page witness on its existing due request.

    An older mixed public/restricted envelope cannot be retained, so a 304
    cannot reconstruct its IDs. Do not change the job, its validator, retry
    deadline, source circuit or protected content: bind only this dispatch to
    an unconditional response when no equivalent witness is already available.
    The per-item error survives transport failures and identifies old cases
    whose page-level error was later replaced by a 429 or a 304.
    """
    if (task.get('kind') != 'cnyes_news' or task.get('provider') != source.provider
            or not source.allowed or not source.request_enabled
            or source.acquisition_mode == 'finite' or source.known_url_only
            or not (task.get('etag') or task.get('last_modified'))
            or not task.get('response_sha256')
            or task.get('parser_fingerprint') != compatible_fingerprint):
        return
    try:
        url = task['url']
        if not scope_key(url) or wordpress_base(url) not in source.cnyes_apis:
            return
        page = wordpress_page(url)
        if not 1 <= page <= 30:
            return
    except (KeyError, ValueError, TypeError, AttributeError, OverflowError):
        return
    if task.get('error_code') != 'bulk_response_contains_restricted_text':
        marks = ','.join('?' for _ in BULK_RESTRICTED_ITEM_ERRORS)
        restricted = queue.db.execute(
            'SELECT 1 FROM bulk_item_errors WHERE request_url=? AND provider=? '
            f'AND error_code IN ({marks}) LIMIT 1',
            (url, source.provider, *sorted(BULK_RESTRICTED_ITEM_ERRORS))).fetchone()
        if not restricted:
            return
    scope = cnyes_scope(url)
    prior = queue.db.execute('SELECT total,ids_sha256 FROM bulk_page_evidence '
                             'WHERE scope_url=? AND page=?', (scope, page)).fetchone()
    if prior:
        work = {'request_url': url, 'scope_url': scope, 'page': page,
                'expected_total': prior['total'], 'expected_ids_sha256': prior['ids_sha256']}
        sha, witness_kind = _matching_forward_evidence(queue, work, source, allow_shared=True)
        if sha:
            row = queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                                   'WHERE observation_sha256=?', (sha,)).fetchone()
            try:
                observation = json.loads(row['observation_json'])
                info = observation['pagination']
                current_direct = (witness_kind != 'direct'
                                  or observation['response_sha256'] == task['response_sha256'])
                if current_direct and (info['total'], info['ids_sha256']) == (prior['total'], prior['ids_sha256']):
                    return
            except (KeyError, ValueError, TypeError, IndexError):
                pass
    task['_cnyes_missing_restricted_history_evidence'] = True


def _matching_forward_evidence(queue, work, source, *, allow_shared):
    """Return a current direct or independently verified page reference."""
    if not source or not source.allowed:
        return None, None
    direct = queue.db.execute('SELECT o.observation_sha256,o.observation_json,o.scope_url,o.page '
        'FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
        'ON o.observation_sha256=h.observation_sha256 '
        'WHERE h.scope_url=? AND h.page=? AND o.page=? AND o.evidence_contract_version=? '
        'AND o.source_bulk_fingerprint=? AND o.authorization_fingerprint=?',
        (work['scope_url'], work['page'], work['page'], CONTRACT_VERSION,
         cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
    if not direct:
        return None, None
    try:
        observation = json.loads(direct['observation_json'])
        info = observation['pagination']
        if (not isinstance(observation, dict) or digest(observation) != direct['observation_sha256']
                or observation['contract_version'] != CONTRACT_VERSION or observation['provider'] != source.provider
                or observation['source_bulk_fingerprint'] != cnyes_bulk_fingerprint(source)
                or observation['authorization_fingerprint'] != source.authorization_fingerprint
                or observation['scope_url'] != direct['scope_url'] or observation['page'] != work['page']
                or info['scope'] != direct['scope_url'] or info['page'] != work['page']
                or wordpress_base(observation['request_url']) not in source.cnyes_apis):
            return None, None
        if direct['scope_url'] == work['scope_url'] and observation['request_url'] == work['request_url']:
            return direct['observation_sha256'], 'direct'
        if not allow_shared:
            return None, None
        key = scope_key(work['scope_url'])
        ref = queue.db.execute('SELECT * FROM cnyes_history_page_ID_references '
            'WHERE scope_url=? AND page=? AND observation_sha256=?',
            (work['scope_url'], work['page'], direct['observation_sha256'])).fetchone()
        original = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                    (work['scope_url'], work['page'])).fetchone()
        if (not key or key != scope_key(direct['scope_url']) or not ref or not original
                or ref['reference_scope'] != direct['scope_url']
                or json.loads(ref['original_ID_evidence_json']) != dict(original)
                or (original['total'], original['ids_sha256']) != (info['total'], info['ids_sha256'])
                or (work['expected_total'], work['expected_ids_sha256']) != (info['total'], info['ids_sha256'])
                or observation['source_day_offset_seconds'] != CNYES_QUERY_DAY_OFFSET_SECONDS):
            return None, None
        for item in observation['identities']:
            stamp = item['publishAt']
            if type(stamp) is not int or not key[1] <= (stamp + CNYES_QUERY_DAY_OFFSET_SECONDS) // 86400 <= key[2]:
                return None, None
        return direct['observation_sha256'], 'shared_page_id_reference'
    except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
        return None, None


def _save_retained_resolution(queue, work, resolution, observation_sha, evidence_kind):
    values = (work['request_url'], work['state'], work['outcome_json'], resolution, observation_sha, evidence_kind)
    queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_outcome_resolutions VALUES (?,?,?,?,?,?,?,?)',
                     (digest(values), *values, now()))
    queue.db.execute('UPDATE cnyes_history_retained_work SET state=? WHERE request_url=?',
                     (resolution, work['request_url']))


def resolve_forward_retained_outcomes(queue, scope, page, source):
    """When a new page head appears, update matching old work in the same transaction."""
    rows = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE scope_url=? AND page=? '
        "AND state IN ('retained_unavailable','retained_request_mismatch','retained_invalid','pending','local_retry')",
        (scope, page)).fetchall()
    for work in rows:
        original = work
        if work['state'] in ('pending', 'local_retry'):
            # A newly found batch may have woken an old failure before the
            # same source page is directly accepted. Preserve the original
            # failure in both transition receipts; ordinary pending work does
            # not acquire this treatment without a candidate-wake receipt.
            event = queue.db.execute('SELECT original_state,original_outcome_json '
                'FROM cnyes_history_retained_outcome_resolutions WHERE request_url=? '
                "AND resolution_state='pending' AND evidence_kind='new_accepted_body_batch_candidate' "
                'ORDER BY resolved_at_utc DESC LIMIT 1', (work['request_url'],)).fetchone()
            if not event:
                continue
            original = {**dict(work), 'state': event['original_state'], 'outcome_json': event['original_outcome_json']}
        if work['state'] == 'retained_invalid':
            try:
                if json.loads(work['outcome_json']).get('error') != 'retained_original_request_differs_not_relabelled':
                    continue
            except (ValueError, TypeError, AttributeError):
                continue
        sha, kind = _matching_forward_evidence(queue, work, source, allow_shared=True)
        if sha:
            _save_retained_resolution(queue, original, 'forward_evidence_available', sha, kind)
            queue.db.execute('DELETE FROM cnyes_history_retained_local_retries WHERE request_url=?',
                             (work['request_url'],))


def reconcile_legacy_request_mismatches(queue, *, stop=None, limit=128, max_seconds=.05):
    """Reclassify old wrong-request outcomes without losing the original result.

    Direct observations must belong to this exact request and current source
    contract. This pass never creates observations or changes source jobs.
    """
    result = {'complete': False, 'checked': 0, 'reclassified': 0, 'blocked': None}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result['blocked'] = 'source_disabled'
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (LEGACY_MISMATCH_SCAN_KEY,)).fetchone()
    try:
        state = json.loads(saved[0]) if saved else {'version': 1, 'state': 'scanning', 'request_url': ''}
        if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != 1
                or state.get('state') not in ('scanning', 'complete') or type(state.get('request_url')) is not str):
            raise ValueError('invalid checkpoint')
    except (TypeError, ValueError, KeyError):
        result['blocked'] = 'invalid_legacy_mismatch_checkpoint'
        return result
    if state['state'] == 'complete':
        result['complete'] = True
        return result
    started = time.monotonic()
    rows = queue.db.execute("SELECT * FROM cnyes_history_retained_work WHERE state='retained_invalid' "
                            'AND request_url>? ORDER BY request_url LIMIT ?', (state['request_url'], limit)).fetchall()
    finished = True
    with queue.db:
        for work in rows:
            if time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
                finished = False
                break
            try:
                previous = json.loads(work['outcome_json'])
            except (ValueError, TypeError):
                previous = None
            if isinstance(previous, dict) and previous.get('error') == 'retained_original_request_differs_not_relabelled':
                observation_sha, kind = _matching_forward_evidence(queue, work, source, allow_shared=False)
                resolution = 'forward_evidence_available' if observation_sha else 'retained_request_mismatch'
                _save_retained_resolution(queue, work, resolution, observation_sha, kind)
                result['reclassified'] += 1
            state['request_url'] = work['request_url']
            result['checked'] += 1
        if finished and len(rows) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (LEGACY_MISMATCH_SCAN_KEY, encoded(state)))
    return result


def reconcile_legacy_unavailable(queue, *, stop=None, limit=128, max_seconds=.05):
    """Find later page evidence for previously unavailable local originals."""
    result = {'complete': False, 'checked': 0, 'resolved': 0, 'blocked': None}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result['blocked'] = 'source_disabled'
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (LEGACY_UNAVAILABLE_SCAN_KEY,)).fetchone()
    try:
        state = json.loads(saved[0]) if saved else {'version': 1, 'state': 'scanning', 'request_url': ''}
        if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != 1
                or state.get('state') not in ('scanning', 'complete') or type(state.get('request_url')) is not str):
            raise ValueError('invalid checkpoint')
    except (TypeError, ValueError, KeyError):
        result['blocked'] = 'invalid_legacy_unavailable_checkpoint'
        return result
    if state['state'] == 'complete':
        result['complete'] = True
        return result
    started = time.monotonic()
    rows = queue.db.execute("SELECT * FROM cnyes_history_retained_work WHERE state='retained_unavailable' "
                            'AND request_url>? ORDER BY request_url LIMIT ?', (state['request_url'], limit)).fetchall()
    finished = True
    with queue.db:
        for work in rows:
            if time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
                finished = False
                break
            sha, kind = _matching_forward_evidence(queue, work, source, allow_shared=True)
            if sha:
                _save_retained_resolution(queue, work, 'forward_evidence_available', sha, kind)
                result['resolved'] += 1
            state['request_url'] = work['request_url']
            result['checked'] += 1
        if finished and len(rows) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (LEGACY_UNAVAILABLE_SCAN_KEY, encoded(state)))
    return result


def seed_unobserved_empty_scopes(queue, *, stop=None, limit=128, max_seconds=.01):
    """Find old zero-result history without rescanning or certifying it.

    Walk the coverage primary key once in bounded slices. New successful
    responses already write a history head, so this pass only admits old,
    successful, future-due jobs lacking one. Existing source failures and
    already-due jobs remain under the normal HTTP owner's control.
    """
    result = {'complete': False, 'scanned': 0, 'reconciled': 0, 'blocked': None}
    source = queue.registry.sources.get('cnyes')
    if not source or not source.allowed or not source.cnyes_apis:
        result['blocked'] = 'source_disabled'
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    marker = LEGACY_EMPTY_SCAN_PREFIX + digest((
        source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    try:
        state = json.loads(saved[0]) if saved else {'version': 1, 'state': 'scanning', 'base_url': ''}
        if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != 1
                or state.get('state') not in ('scanning', 'complete') or type(state.get('base_url')) is not str):
            raise ValueError('invalid checkpoint')
    except (TypeError, ValueError, KeyError):
        result['blocked'] = 'invalid_legacy_empty_scope_checkpoint'
        return result
    if state['state'] == 'complete':
        result['complete'] = True
        return result
    started = time.monotonic()
    rows = queue.db.execute('SELECT base_url,provider,total_articles,total_pages FROM bulk_api_coverage '
        'WHERE base_url>? ORDER BY base_url LIMIT ?', (state['base_url'], limit)).fetchall()
    finished = True
    with queue.db:
        for row in rows:
            if time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
                finished = False
                break
            scope = row['base_url']
            if row['provider'] == source.provider and row['total_articles'] == 0 and row['total_pages'] == 1:
                try:
                    key = scope_key(scope)
                    query = parse_qs(scope.split('?', 1)[1]) if key else {}
                    url = cnyes_url(key[0], 1, start=int(query['startAt'][0]), end=int(query['endAt'][0])) if key else None
                except (ValueError, TypeError, IndexError, KeyError):
                    key, url = None, None
                if (key and key[0] in source.cnyes_apis
                        and not queue.db.execute('SELECT 1 FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()
                        and not queue.db.execute('SELECT 1 FROM bulk_window_splits WHERE scope_url=?', (scope,)).fetchone()):
                    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
                    if (job and job['kind'] == 'cnyes_news' and job['provider'] == source.provider
                            and job['error_code'] is None and job['attempts'] == 0
                            and job['superseded_by_windows_json'] is None and job['due'] > time.time()):
                        reconcile_scope(queue, scope, source)
                        result['reconciled'] += 1
            state['base_url'] = scope
            result['scanned'] += 1
        if finished and len(rows) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, encoded(state)))
    return result


def seed_unobserved_nonzero_pages(queue, *, stop=None, limit=128, max_seconds=.01):
    """Inventory legacy local page envelopes once, without a publisher request.

    Coverage and page IDs only locate candidates. The retained worker must
    verify their original request, receipt, bytes, authorization and contents.
    A per-scope fence survives bounded slices and crashes so incomplete local
    inventory cannot wake all of that scope's publisher pages prematurely.
    """
    result = {'complete': False, 'checked': 0, 'queued': 0, 'blocked': None, 'stale_fences_reconciled': 0,
              'restarted_invalid_checkpoint': False}
    source = queue.registry.sources.get('cnyes')
    if (not source or not source.allowed or not source.cnyes_apis
            or source.acquisition_mode == 'finite' or source.known_url_only):
        result.update(complete=True, blocked='source_disabled')
        return result
    if limit <= 0 or max_seconds <= 0 or stop is not None and stop.is_set():
        return result
    source_fp, auth_fp = cnyes_bulk_fingerprint(source), source.authorization_fingerprint
    # Contract changes can interrupt an old scan. Release only its local
    # fence, retain the old identity, and give its pending work a scope head
    # so the usual retry path still runs if its old raw fails validation.
    stale = queue.db.execute("SELECT scope_url FROM cnyes_history_local_inventory_scopes "
        "WHERE scan_state='scanning' AND (source_bulk_fingerprint!=? OR authorization_fingerprint!=?) LIMIT ?",
        (source_fp, auth_fp, limit)).fetchall()
    with queue.db:
        for row in stale:
            scope = row['scope_url']
            queue.db.execute("UPDATE cnyes_history_local_inventory_scopes SET scan_state='ready' WHERE scope_url=?",
                             (scope,))
            try:
                key = scope_key(scope)
            except (TypeError, ValueError, OverflowError):
                key = None
            if key and key[0] in source.cnyes_apis:
                reconcile_scope(queue, scope, source)
            result['stale_fences_reconciled'] += 1
    marker = nonzero_scan_key(source)
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
    try:
        state = json.loads(saved[0]) if saved else {'version': 1, 'state': 'scanning', 'scope_url': '', 'page': 30,
                                                   'checked': 0, 'queued': 0}
        if (type(state) is not dict or type(state.get('version')) is not int or state['version'] != 1
                or state.get('state') not in ('scanning', 'complete')
                or type(state.get('scope_url')) is not str or type(state.get('page')) is not int
                or not 0 <= state['page'] <= 30
                or type(state.get('checked')) is not int or state['checked'] < 0
                or type(state.get('queued')) is not int or state['queued'] < 0):
            raise ValueError('invalid checkpoint')
    except (TypeError, ValueError, KeyError):
        # Preserve the bad bytes and re-seek from the start. Existing work is
        # idempotent; a fence in progress remains in place until re-scanned.
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)',
                (LEGACY_NONZERO_SCAN_PREFIX + 'invalid:' + digest((marker, saved[0])), saved[0]))
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (marker,))
        state = {'version': 1, 'state': 'scanning', 'scope_url': '', 'page': 30, 'checked': 0, 'queued': 0}
        result['restarted_invalid_checkpoint'] = True
    if state['state'] == 'complete':
        result['complete'] = len(stale) < limit
        return result

    started = time.monotonic()
    comparator = '>' if state['page'] == 30 else '>='
    scopes = queue.db.execute('SELECT base_url,provider,total_articles,total_pages FROM bulk_api_coverage '
        'WHERE base_url' + comparator + '? ORDER BY base_url LIMIT ?', (state['scope_url'], limit)).fetchall()
    exhausted_scopes = True
    with queue.db:
        for row in scopes:
            if result['checked'] >= limit or time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
                exhausted_scopes = False
                break
            scope, total, pages_total = row['base_url'], row['total_articles'], row['total_pages']
            try:
                key = scope_key(scope)
                query = parse_qs(scope.split('?', 1)[1]) if key else {}
                low, high = int(query['startAt'][0]), int(query['endAt'][0])
            except (ValueError, TypeError, OverflowError, IndexError, KeyError):
                key = None
            own = queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes WHERE scope_url=?',
                                   (scope,)).fetchone()
            continuing = bool(own and own['scan_state'] == 'scanning'
                              and own['source_bulk_fingerprint'] == source_fp
                              and own['authorization_fingerprint'] == auth_fp)
            eligible = bool(row['provider'] == source.provider and total > 0 and 1 <= pages_total <= 30
                            and key and key[0] in source.cnyes_apis)
            if eligible and not continuing:
                eligible = not any(queue.db.execute(sql, (scope,)).fetchone() for sql in (
                    'SELECT 1 FROM cnyes_history_scope_heads WHERE scope_url=?',
                    'SELECT 1 FROM cnyes_history_retained_work WHERE scope_url=? LIMIT 1',
                    'SELECT 1 FROM bulk_window_splits WHERE scope_url=?',
                    'SELECT 1 FROM bulk_window_gaps WHERE scope_url=?',
                    'SELECT 1 FROM cnyes_history_edges e JOIN bulk_window_gaps g ON g.scope_url=e.parent_scope '
                        'WHERE e.child_scope=? LIMIT 1'))
            if not eligible:
                state.update(scope_url=scope, page=30)
                result['checked'] += 1
                continue
            after = state['page'] if state['scope_url'] == scope else 0
            pages = queue.db.execute('SELECT p.page,p.total,p.ids_sha256,p.observed_at_utc '
                'FROM bulk_page_evidence p LEFT JOIN cnyes_history_page_heads h '
                'ON h.scope_url=p.scope_url AND h.page=p.page '
                "WHERE p.scope_url=? AND typeof(p.page)='integer' AND p.page>=1 "
                'AND p.page>? AND p.page<=? AND h.scope_url IS NULL ORDER BY p.page',
                (scope, after, pages_total)).fetchall()
            for page in pages:
                if result['checked'] >= limit or time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
                    exhausted_scopes = False
                    break
                url = cnyes_url(key[0], page['page'], start=low, end=high)
                job = queue.db.execute('SELECT kind,provider,response_sha256,last_checked_at_utc '
                                       'FROM discovery WHERE url=?', (url,)).fetchone()
                sha = job['response_sha256'] if job else None
                if (job and job['kind'] == 'cnyes_news' and job['provider'] == source.provider
                        and job['last_checked_at_utc'] and page['total'] == total):
                    try:
                        present = any(path.is_file() and path.with_suffix('.parquet.receipt.json').is_file()
                                      for path, _ in _retained_envelopes(queue.root, sha))
                    except (OSError, MemoryError):
                        present = bool(_retained_envelopes(queue.root, sha))
                    if not present:
                        present = bool(queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates '
                            'WHERE request_url=? AND response_sha256=? LIMIT 1', (url, sha)).fetchone())
                    if present:
                        inserted = queue.db.execute('INSERT OR IGNORE INTO cnyes_history_retained_work '
                            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
                            'source_checked_at_utc) VALUES (?,?,?,?,?,?,?)',
                            (url, scope, page['page'], sha, total, page['ids_sha256'], page['observed_at_utc']))
                        if inserted.rowcount:
                            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_local_inventory_scopes '
                                'VALUES (?,?,?,?,?)', (scope, source_fp, auth_fp, 'scanning', now()))
                            result['queued'] += 1
                state.update(scope_url=scope, page=page['page'])
                result['checked'] += 1
            else:
                state.update(scope_url=scope, page=30)
                if not pages:
                    result['checked'] += 1
                queue.db.execute("UPDATE cnyes_history_local_inventory_scopes SET scan_state='ready' "
                    "WHERE scope_url=? AND scan_state='scanning' AND source_bulk_fingerprint=? "
                    "AND authorization_fingerprint=?", (scope, source_fp, auth_fp))
                # Even when no retained original exists, this eligible
                # history scope is still unproven. Give the normal repair
                # sweep a durable head after all its pages have been searched;
                # it will preserve source errors and existing due deadlines.
                reconcile_scope(queue, scope, source)
                continue
            break
        if exhausted_scopes and len(scopes) < limit:
            state['state'] = 'complete'
            result['complete'] = True
        state['checked'] += result['checked']
        state['queued'] += result['queued']
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, encoded(state)))
    return result


def _defer_retained_work(queue, work, original_state, error_code, *, candidate_errors=None):
    """Persist a local retry without changing source request state or clocks."""
    with queue.db:
        prior = queue.db.execute('SELECT local_attempts FROM cnyes_history_retained_local_retries '
                                 'WHERE request_url=?', (work['request_url'],)).fetchone()
        attempts = (prior[0] if prior else 0) + 1
        delay = 300 if error_code != 'local_io_unavailable' else min(300, 2 ** min(attempts - 1, 9))
        queue.db.execute('INSERT OR REPLACE INTO cnyes_history_retained_local_retries '
            '(request_url,original_state,not_before_epoch,local_attempts,error_code,last_attempt_at_utc) '
            'VALUES (?,?,?,?,?,?)', (work['request_url'], original_state, time.time() + delay,
                                     attempts, error_code, now()))
        outcome = {'source_bodies_reaccepted': False, 'publisher_requests': 0,
                   'local_retry_reason': error_code}
        if candidate_errors:
            outcome['candidate_errors'] = candidate_errors
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='local_retry',outcome_json=? WHERE request_url=?",
                         (encoded(outcome), work['request_url']))


def maintenance_delay(result, *, now_epoch=None):
    """Wake for durable local retry deadlines, bounded like normal maintenance."""
    if not result['exhausted']:
        scan = result.get('unobserved_nonzero_page_scan') or {}
        accepted_scan = result.get('accepted_body_batch_scan') or {}
        repair = result.get('evidence_repair') or {}
        if (result.get('processed', 0) > 0 or (not scan.get('complete') and scan.get('checked', 0) > 0)
                or (not accepted_scan.get('complete') and accepted_scan.get('checked', 0) > 0)
                or (result.get('repair_sweep_pending') and repair.get('scopes_checked', 0) > 0)):
            # Useful bounded work can continue on the next owner turn. HTTP
            # dispatch and response handling still occur between slices.
            return 0
        return 2
    deadline = result.get('next_local_retry_epoch')
    if type(deadline) in (int, float) and math.isfinite(deadline) and deadline >= 0:
        return min(300, max(1, deadline - (time.time() if now_epoch is None else now_epoch)))
    return 300


def backfill(queue, *, stop=None, limit=8, max_seconds=.2):
    """Bounded normal-owner metadata work; no second HTTP owner or body write."""
    started, done = time.monotonic(), 0
    legacy = reconcile_legacy_request_mismatches(queue, stop=stop, limit=limit, max_seconds=max_seconds / 8)
    unavailable = reconcile_legacy_unavailable(queue, stop=stop, limit=limit, max_seconds=max_seconds / 8)
    empty = seed_unobserved_empty_scopes(queue, stop=stop, limit=min(limit * 8, 1024), max_seconds=max_seconds / 8)
    accepted_scan = seed_accepted_body_batch_candidates(queue, stop=stop, limit=min(limit * 8, 1024), max_seconds=max_seconds / 8)
    nonzero = (seed_unobserved_nonzero_pages(queue, stop=stop, limit=min(limit * 8, 1024), max_seconds=max_seconds / 8)
        if accepted_scan['complete'] else {'complete': False, 'checked': 0, 'queued': 0,
                                          'blocked': 'waiting_accepted_batch_scan'})
    scan = seed_overflow_retained(queue, stop=stop, limit=limit, max_seconds=max_seconds / 2)
    lanes = []
    if scan['complete']:
        lanes.append([(dict(row), 'overflow_pending') for row in queue.db.execute(
            "SELECT * FROM cnyes_history_retained_work WHERE state='overflow_pending' ORDER BY request_url LIMIT ?",
            (limit,)).fetchall()])
    lanes.append([(dict(row), 'pending') for row in queue.db.execute(
        "SELECT w.* FROM cnyes_history_retained_work w LEFT JOIN cnyes_history_local_inventory_scopes i "
        "ON i.scope_url=w.scope_url WHERE w.state='pending' AND (i.scope_url IS NULL OR i.scan_state='ready') "
        "ORDER BY w.request_url LIMIT ?",
        (limit,)).fetchall()])
    retry_filter = '' if scan['complete'] else " AND r.original_state='pending'"
    retries = queue.db.execute('SELECT w.*,r.original_state AS retry_original_state '
        'FROM cnyes_history_retained_local_retries r CROSS JOIN cnyes_history_retained_work w '
        "ON w.request_url=r.request_url AND w.state='local_retry' "
        'LEFT JOIN cnyes_history_local_inventory_scopes i ON i.scope_url=w.scope_url '
        "WHERE r.not_before_epoch<=? AND r.error_code!='accepted_batch_scan_pending' "
        "AND (i.scope_url IS NULL OR i.scan_state='ready')" + retry_filter +
        ' ORDER BY r.not_before_epoch,r.request_url LIMIT ?',
        (time.time(), limit)).fetchall()
    lanes.append([(dict(row), row['retry_original_state']) for row in retries])
    if accepted_scan['complete']:
        waiting = queue.db.execute('SELECT w.*,r.original_state AS retry_original_state '
            'FROM cnyes_history_retained_local_retries r CROSS JOIN cnyes_history_retained_work w '
            "ON w.request_url=r.request_url AND w.state='local_retry' "
            'LEFT JOIN cnyes_history_local_inventory_scopes i ON i.scope_url=w.scope_url '
            "WHERE r.error_code='accepted_batch_scan_pending' AND (i.scope_url IS NULL OR i.scan_state='ready')"
            + retry_filter + ' ORDER BY r.request_url LIMIT ?', (limit,)).fetchall()
        lanes.append([(dict(row), row['retry_original_state']) for row in waiting])
    # Rotate among new overflow, normal and due retry work so a single bad
    # file or a continuously replenished lane cannot starve other evidence.
    offset = getattr(queue, '_cnyes_history_lane_offset', 0) % len(lanes)
    queue._cnyes_history_lane_offset = (offset + 1) % len(lanes)
    positions = [0] * len(lanes)
    rows = []
    while len(rows) < limit:
        added = False
        for index in ((offset + step) % len(lanes) for step in range(len(lanes))):
            if positions[index] < len(lanes[index]) and len(rows) < limit:
                rows.append(lanes[index][positions[index]])
                positions[index] += 1
                added = True
        if not added:
            break
    local_io_deferrals = source_scope_deferrals = 0
    for work, original_state in rows:
        if time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
            break
        source = queue.registry.sources.get('cnyes')
        if not source or not source.allowed or not source.cnyes_apis:
            break
        if wordpress_base(work['request_url']) not in source.cnyes_apis:
            _defer_retained_work(queue, work, original_state, 'source_endpoint_unavailable')
            source_scope_deferrals += 1
            done += 1
            continue
        sha = work['response_sha256']
        state, outcome = 'retained_unavailable', {'source_bodies_reaccepted': False, 'publisher_requests': 0}
        proof = queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=? '
            'AND source_bulk_fingerprint=? AND authorization_fingerprint=? AND evidence_contract_version=?',
            (work['scope_url'], cnyes_bulk_fingerprint(source), source.authorization_fingerprint, CONTRACT_VERSION)).fetchone()
        current = queue.db.execute('SELECT 1 FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
            'ON o.observation_sha256=h.observation_sha256 WHERE h.scope_url=? AND h.page=? '
            'AND o.evidence_contract_version=? AND o.source_bulk_fingerprint=? AND o.authorization_fingerprint=?',
            (work['scope_url'], work['page'], CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint)).fetchone()
        if proof and proof[0] in COMPLETE_STATES or current:
            with queue.db:
                queue.db.execute('UPDATE cnyes_history_retained_work SET state=?,outcome_json=? WHERE request_url=?',
                                 ('forward_evidence_available', encoded(outcome), work['request_url']))
                queue.db.execute('DELETE FROM cnyes_history_retained_local_retries WHERE request_url=?',
                                 (work['request_url'],))
                _release_complete_local_inventory_fence(queue, work['scope_url'], source)
            done += 1
            continue
        local_error, errors = None, []
        candidates = [(path, path.with_suffix('.parquet.receipt.json'), kind, None)
                      for path, kind in _retained_envelopes(queue.root, sha)]
        candidates.extend((row['parquet_path'], row['receipt_path'], 'accepted_body_batch', row['parquet_path'])
            for row in queue.db.execute('SELECT parquet_path,receipt_path FROM cnyes_history_accepted_batch_candidates '
                'WHERE request_url=? AND response_sha256=? ORDER BY parquet_path', (work['request_url'], sha)))
        if not candidates:
            errors.append('invalid_retained_response_sha256')
        for path, receipt_path, artifact_kind, accepted_relative in candidates:
            try:
                if accepted_relative is not None:
                    from downloader.news_fulltext_queue import replay_source_path
                    accepted = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?',
                                                (accepted_relative,)).fetchone()
                    if not accepted or accepted[0] != receipt_path:
                        raise ValueError('accepted_batch_not_committed')
                    path = replay_source_path(queue.root, accepted_relative)
                    receipt_path = replay_source_path(queue.root, receipt_path)
                    receipt = json.loads(receipt_path.read_bytes())
                    batch = pq.ParquetFile(path)
                    if (type(receipt.get('contract_version')) is not int
                            or receipt['contract_version'] != NEWS_CONTRACT_VERSION
                            or receipt.get('parquet_path') != accepted_relative or receipt.get('publish') is not False
                            or type(receipt.get('rows')) is not int or receipt['rows'] != batch.metadata.num_rows
                            or sha256_file(path) != receipt.get('sha256')):
                        raise ValueError('invalid_accepted_batch_receipt')
                    columns = ['provider', 'source_request_url', 'raw_response_body', 'response_sha256',
                               'captured_at_utc', 'authorization_fingerprint', 'publish']
                    if not set(columns) <= set(batch.schema_arrow.names):
                        raise ValueError('accepted_batch_original_metadata_missing')
                    original = None
                    for chunk in batch.iter_batches(batch_size=16, columns=columns, use_threads=False):
                        original = next((row for row in chunk.to_pylist() if row.get('provider') == source.provider
                            and row.get('source_request_url') == work['request_url']
                            and row.get('raw_response_body') is not None and row.get('response_sha256') == sha), None)
                        if original is not None:
                            break
                    if original is None:
                        raise ValueError('accepted_batch_matching_original_missing')
                    original = {**original, 'request_url': original['source_request_url']}
                else:
                    if not path.is_file() or not receipt_path.is_file():
                        continue
                    receipt = json.loads(receipt_path.read_bytes())
                    records = pq.read_table(path).to_pylist()
                    if (sha256_file(path) != receipt['sha256'] or receipt['response_sha256'] != sha
                            or receipt.get('publish') is not False or len(records) != 1):
                        raise ValueError('invalid_original_envelope_receipt')
                    original = records[0]
                raw = original['raw_response_body']
                raw = raw.encode() if isinstance(raw, str) else raw
                if original['request_url'] != work['request_url']:
                    raise ValueError('retained_original_request_differs_not_relabelled')
                if hashlib.sha256(raw).hexdigest() != sha or original.get('publish') is not False:
                    raise ValueError('retained_original_bytes_or_private_scope_invalid')
                if original['authorization_fingerprint'] != source.authorization_fingerprint:
                    raise ValueError('retained_original_authorization_mismatch')
                captured = datetime.fromisoformat(original['captured_at_utc'])
                if captured.tzinfo is None or captured.utcoffset() is None:
                    raise ValueError('retained_original_capture_clock_invalid')
                response = SimpleNamespace(url=original['request_url'], body=raw,
                                           headers={'content-type': 'application/json'})
                info, posts = cnyes_pagination(response)
                if any(type(post.get('payment')) is not int or post['payment'] != 0 for post in posts):
                    raise ValueError('restricted_retained_envelope_not_reused')
                if (info['total'] != work['expected_total'] or info['ids_sha256'] != work['expected_ids_sha256']
                        or info['page'] != work['page'] or info['scope'] != work['scope_url']):
                    raise ValueError('retained_source_page_identity_mismatch')
                with queue.db:
                    record_page(queue, response, info, posts, source, {
                        'kind': artifact_kind, 'parquet_path': accepted_relative or str(path.relative_to(queue.root)),
                        'receipt_path': accepted[0] if accepted_relative is not None else str(receipt_path.relative_to(queue.root))},
                        observed_at=original['captured_at_utc'], origin='verified_retained_source_response')
                state, outcome['response_sha256'] = 'restored', sha
                break
            except FileNotFoundError:
                errors.append('accepted_batch_file_missing')
            except (OSError, MemoryError) as error:
                local_error = error
            except (ValueError, KeyError, TypeError, OverflowError, NewsError, pa.ArrowException) as error:
                errors.append(str(error))
        if state != 'restored' and local_error is not None:
            code = 'local_memory_unavailable' if isinstance(local_error, MemoryError) else 'local_io_unavailable'
            _defer_retained_work(queue, work, original_state, code)
            local_io_deferrals += 1
            done += 1
            continue
        if (state != 'restored' and not accepted_scan['complete'] and work['expected_total'] > 0
                and _retained_envelopes(queue.root, sha)):
            _defer_retained_work(queue, work, original_state, 'accepted_batch_scan_pending',
                                 candidate_errors=errors)
            done += 1
            continue
        if state != 'restored' and errors:
            # Identical bytes do not make two publisher requests identical.
            mismatch = 'retained_original_request_differs_not_relabelled'
            state = 'retained_request_mismatch' if all(error == mismatch for error in errors) else 'retained_invalid'
            outcome['error'] = errors[0] if state == 'retained_request_mismatch' else next(
                error for error in errors if error != mismatch)
            if len(errors) > 1:
                outcome['candidate_errors'] = errors
        # A missing raw envelope is not empty data. Schedule one canonical
        # successful page check for this effective date/page; preserve failed
        # jobs, source/endpoint cooldowns, leases, old hashes and real clocks.
        with queue.db:
            if (state in ('retained_unavailable', 'retained_invalid', 'retained_request_mismatch')
                    and original_state != 'overflow_pending'
                    and not queue.db.execute('SELECT 1 FROM cnyes_history_local_inventory_scopes WHERE scope_url=?',
                                             (work['scope_url'],)).fetchone()):
                key = scope_key(work['request_url'])
                recheck_marker = 'cnyes_history_evidence_recheck_v1:' + digest((*key, work['page']))
                if not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (recheck_marker,)).fetchone():
                    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (work['request_url'],)).fetchone()
                    if (job and job['error_code'] is None and job['attempts'] == 0
                            and job['superseded_by_windows_json'] is None):
                        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (recheck_marker, encoded({
                            'prior_job': dict(job), 'reason': 'missing_ID_and_publication_day_evidence',
                            'changed_at_utc': now(), 'source_failures_or_deadlines_reset': False})))
                        queue.db.execute('UPDATE discovery SET due=min(due,?),etag=NULL,last_modified=NULL WHERE url=?',
                                         (time.time(), work['request_url']))
                        outcome['one_successful_page_evidence_recheck_scheduled'] = True
            queue.db.execute('UPDATE cnyes_history_retained_work SET state=?,outcome_json=? WHERE request_url=?',
                             (state, encoded(outcome), work['request_url']))
            queue.db.execute('DELETE FROM cnyes_history_retained_local_retries WHERE request_url=?',
                             (work['request_url'],))
            _release_complete_local_inventory_fence(queue, work['scope_url'], source)
        done += 1
    repair_budget = max(0, max_seconds - (time.monotonic() - started))
    repair = repair_incomplete_scopes(queue, stop=stop, limit=limit, max_seconds=repair_budget)
    repair_pending = bool(queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone())
    repair_pending = repair_pending or bool(repair.get('unfinished_slice'))
    source = queue.registry.sources.get('cnyes')
    source_available = bool(source and source.allowed and source.cnyes_apis)
    if not repair_pending and repair_budget <= 0 and source_available:
        # The slice never started and therefore has no cursor. A bounded
        # index seek distinguishes real pending work from an idle owner.
        repair_pending = bool(queue.db.execute('SELECT 1 FROM cnyes_history_scope_heads WHERE provider=? '
            'AND evidence_contract_version=? AND source_bulk_fingerprint=? AND authorization_fingerprint=? '
            'AND state IN (?,?) LIMIT 1',
            (source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
             *REPAIR_STATES)).fetchone())
    pending = queue.db.execute("SELECT 1 FROM cnyes_history_retained_work WHERE state='pending' LIMIT 1").fetchone()
    if scan['complete'] and not pending:
        pending = queue.db.execute("SELECT 1 FROM cnyes_history_retained_work WHERE state='overflow_pending' LIMIT 1").fetchone()
    if accepted_scan['complete'] and not pending:
        pending = queue.db.execute('SELECT 1 FROM cnyes_history_retained_local_retries r '
            "JOIN cnyes_history_retained_work w ON w.request_url=r.request_url AND w.state='local_retry' "
            "WHERE r.error_code='accepted_batch_scan_pending'" + retry_filter + ' LIMIT 1').fetchone()
    next_retry = queue.db.execute('SELECT min(r.not_before_epoch) FROM cnyes_history_retained_local_retries r '
        'CROSS JOIN cnyes_history_retained_work w ON w.request_url=r.request_url AND w.state=\'local_retry\' '
        "WHERE r.error_code!='accepted_batch_scan_pending'" + retry_filter).fetchone()[0]
    accepted_retry = accepted_scan.get('next_retry_epoch')
    if accepted_retry is not None:
        next_retry = min(next_retry, accepted_retry) if next_retry is not None else accepted_retry
    ready_retry = next_retry is not None and next_retry <= time.time()
    exhausted = (not source_available or
                 (not pending and not ready_retry and not repair_pending
                  and (scan['complete'] or scan['blocked'] == 'invalid_overflow_scan_checkpoint')
                  and (legacy['complete'] or legacy['blocked'] == 'invalid_legacy_mismatch_checkpoint')
                  and (unavailable['complete'] or unavailable['blocked'] == 'invalid_legacy_unavailable_checkpoint')
                  and (empty['complete'] or empty['blocked'] == 'invalid_legacy_empty_scope_checkpoint')
                  and (accepted_scan['complete'] or accepted_scan['blocked'] == 'local_retry_wait')
                  and (nonzero['complete'] or nonzero['blocked'] == 'waiting_accepted_batch_scan')))
    return {'processed': done, 'exhausted': exhausted, 'evidence_repair': repair,
            'repair_sweep_pending': repair_pending,
            'overflow_retained_scan': scan, 'legacy_mismatch_reconciliation': legacy,
            'legacy_unavailable_reconciliation': unavailable, 'unobserved_empty_scope_scan': empty,
            'accepted_body_batch_scan': accepted_scan, 'unobserved_nonzero_page_scan': nonzero,
            'local_io_deferrals': local_io_deferrals,
            'source_scope_deferrals': source_scope_deferrals,
            'next_local_retry_epoch': next_retry if source_available else None,
            'wall_seconds': time.monotonic() - started}
