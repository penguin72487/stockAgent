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
import time
from types import SimpleNamespace
from urllib.parse import parse_qs

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_bulk import (
    CNYES_QUERY_DAY_OFFSET_SECONDS, cnyes_pagination, cnyes_scope, cnyes_url, wordpress_base,
)
from downloader.news_fulltext_policy import NewsError, cnyes_bulk_fingerprint


CONTRACT_VERSION = 1
COMPLETE_STATES = ('list_complete', 'list_complete_by_page_ID_equivalence', 'partition_list_complete')
REPAIR_STATES = ('waiting_page_evidence', 'inconsistent_source_snapshot')
REPAIR_CURSOR = 'cnyes_history_incomplete_scope_cursor_v1'
SCOPE_KEY_CACHE_SIZE = 4096
SCOPE_KEY_CACHE_MAX_URL_CHARS = 2048


def now():
    return datetime.now(timezone.utc).isoformat()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


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


def initialize(queue):
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
    ''')
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
    share_matching_page_IDs(queue, info['scope'], sha, info, source, stamp)


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


def repair_scope_pages(queue, scope, source):
    """Repair missing/currently contradictory evidence, not completed history.

    An old successful job's annual revisit is not a list-completeness proof.
    Prefer the existing retained-envelope worker; otherwise wake only that
    original successful page. Refusals, active/due jobs and split parents keep
    their own schedules. Per-page evidence markers bound unchanged conflicts.
    """
    result = {'retained_pages_queued': 0, 'page_checks_scheduled': 0}
    key = scope_key(scope)
    if (not key or not source.allowed or key[0] not in source.cnyes_apis
            or source.acquisition_mode == 'finite' or source.known_url_only):
        return result
    coverage = queue.db.execute('SELECT * FROM bulk_api_coverage WHERE base_url=?', (scope,)).fetchone()
    if (not coverage or not 1 <= coverage['total_pages'] <= 30
            or queue.db.execute('SELECT 1 FROM bulk_window_splits WHERE scope_url=?', (scope,)).fetchone()):
        return result
    observations = {row['page']: (row['observation_sha256'], json.loads(row['observation_json']))
        for row in queue.db.execute('SELECT h.page,o.observation_sha256,o.observation_json '
            'FROM cnyes_history_page_heads h JOIN cnyes_history_page_observations o '
            'ON o.observation_sha256=h.observation_sha256 WHERE h.scope_url=? AND h.page<=?',
            (scope, coverage['total_pages']))}
    query = parse_qs(scope.split('?', 1)[1])
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
        if not prior:
            work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
            if work and work['state'] == 'pending':
                continue
            legacy = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=?',
                                      (scope, page)).fetchone()
            sha = job['response_sha256']
            if (not work and legacy and sha and job['last_checked_at_utc']
                    and (queue.root / 'quarantine' / (sha + '.parquet')).is_file()
                    and (queue.root / 'quarantine' / (sha + '.parquet.receipt.json')).is_file()):
                queue.db.execute('INSERT INTO cnyes_history_retained_work '
                    '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
                    'VALUES (?,?,?,?,?,?,?)', (url, scope, page, sha, legacy['total'], legacy['ids_sha256'],
                                               legacy['observed_at_utc']))
                result['retained_pages_queued'] += 1
                continue
        evidence = digest((scope, page, coverage['total_articles'], coverage['total_pages'],
            CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
            prior[0] if prior else ('missing_page', job['last_checked_at_utc'], job['response_sha256'])))
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
    scopes = queue.db.execute('SELECT scope_url FROM cnyes_history_scope_heads WHERE provider=? '
        'AND evidence_contract_version=? AND source_bulk_fingerprint=? AND authorization_fingerprint=? '
        'AND state IN (?,?) AND scope_url>? ORDER BY scope_url LIMIT ?',
        (source.provider, CONTRACT_VERSION, cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
         *REPAIR_STATES, cursor[0] if cursor else '', limit)).fetchall()
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
    return {'contract_version': CONTRACT_VERSION, 'scope_states': states, 'retained_metadata_work': work,
            'current_source_contract_scope_states': current,
            'retired_gap_receipts': db.execute('SELECT count(*) FROM cnyes_history_gap_retirements').fetchone()[0],
            'list_coverage_is_not_body_coverage': True, 'all_history_complete': False, 'finish_eta_utc': None}


def backfill(queue, *, stop=None, limit=8, max_seconds=.2):
    """Bounded normal-owner metadata work; no second HTTP owner or body write."""
    started, done = time.monotonic(), 0
    rows = queue.db.execute("SELECT * FROM cnyes_history_retained_work WHERE state='pending' ORDER BY request_url LIMIT ?",
                            (limit,)).fetchall()
    for work in rows:
        if time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
            break
        source = queue.registry.sources.get('cnyes')
        if not source or not source.allowed or wordpress_base(work['request_url']) not in source.cnyes_apis:
            break
        sha = work['response_sha256']
        path = queue.root / 'quarantine' / (sha + '.parquet')
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
            done += 1
            continue
        if path.is_file() and path.with_suffix('.parquet.receipt.json').is_file():
            try:
                receipt = json.loads(path.with_suffix('.parquet.receipt.json').read_bytes())
                records = pq.read_table(path).to_pylist()
                if (sha256_file(path) != receipt['sha256'] or receipt['response_sha256'] != sha
                        or receipt['publish'] or len(records) != 1):
                    raise ValueError('invalid_original_envelope_receipt')
                original = records[0]
                raw = original['raw_response_body']
                raw = raw.encode() if isinstance(raw, str) else raw
                if original['request_url'] != work['request_url']:
                    raise ValueError('retained_original_request_differs_not_relabelled')
                if hashlib.sha256(raw).hexdigest() != sha or original['publish']:
                    raise ValueError('retained_original_bytes_or_private_scope_invalid')
                if original['authorization_fingerprint'] != source.authorization_fingerprint:
                    raise ValueError('retained_original_authorization_mismatch')
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
                        'kind': 'canonical_quarantine', 'parquet_path': str(path.relative_to(queue.root)),
                        'receipt_path': str(path.with_suffix('.parquet.receipt.json').relative_to(queue.root))},
                        observed_at=original['captured_at_utc'], origin='verified_retained_source_response')
                state, outcome['response_sha256'] = 'restored', sha
            except (ValueError, KeyError, OSError, NewsError) as error:
                state, outcome['error'] = 'retained_invalid', str(error)
        # A missing raw envelope is not empty data. Schedule one canonical
        # successful page check for this effective date/page; preserve failed
        # jobs, source/endpoint cooldowns, leases, old hashes and real clocks.
        with queue.db:
            if state in ('retained_unavailable', 'retained_invalid'):
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
        done += 1
    repair = repair_incomplete_scopes(queue, stop=stop, limit=limit,
                                     max_seconds=max(0, max_seconds - (time.monotonic() - started)))
    exhausted = queue.db.execute("SELECT 1 FROM cnyes_history_retained_work WHERE state='pending' LIMIT 1").fetchone() is None
    return {'processed': done, 'exhausted': exhausted, 'evidence_repair': repair,
            'wall_seconds': time.monotonic() - started}
