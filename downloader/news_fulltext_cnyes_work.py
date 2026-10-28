"""Bounded scheduling hints for missing Cnyes bodies; never alias acceptance.

A supported mobile/AMP/desktop variant with an existing primary head is a lower-value body
candidate, not an interchangeable observation. Keep every URL, body, clock,
attribution, refusal, retry deadline and lease. Indexed fairness still services
candidate views; only source parsing and immutable receipts can accept them.
Missing desktop targets are independent, explicitly unverified lookup work.
"""
from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
import hashlib
import json
import math
import re
import time
from urllib.parse import urlsplit

from downloader.news_fulltext_publishers import cnyes_supported_native_alias_view


CONTRACT_VERSION = 4
LOOKUP_DISCOVERY_VERSION = 1
PROGRESS_VERSION = 1
REQUEST_SCOPE_PROGRESS_VERSION = 1
PENDING_SOURCE_RECHECKS_VERSION = 1
MAX_REQUEST_SCOPE_URLS = 2048
STATES = ('pending', 'retry', 'retry_cooldown')
ACTIVE_STATES = (*STATES, 'inflight')
CURSOR = 'cnyes_body_candidate_cursor_v1'
AMP_CURSOR = 'cnyes_amp_candidate_cursor_v1'
DESKTOP_CURSOR = 'cnyes_desktop_variant_candidate_cursor_v1'


def native_lookup_url(url):
    """A lookup in an existing public namespace, NOT an observed alias."""
    parts = urlsplit(url)
    match = re.fullmatch(r'/news/id/([1-9][0-9]*)', parts.path)
    if (parts.scheme != 'https' or parts.netloc != 'amp-news.cnyes.com'
            or not match or parts.query or parts.fragment):
        return None
    return 'https://news.cnyes.com/news/id/' + match[1]


def canonical_candidate_id(url):
    lookup = native_lookup_url(url)
    if lookup is not None:
        return hashlib.sha256(lookup.encode()).digest()
    parts = urlsplit(url)
    match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
    if not match or not cnyes_supported_native_alias_view(parts):
        return None
    return hashlib.sha256(('https://news.cnyes.com/news/id/' + match[1]).encode()).digest()


def initialize(queue):
    columns = {row[1] for row in queue.db.execute('PRAGMA table_info(articles)')}
    if 'cnyes_canonical_candidate_id' not in columns:
        queue.db.execute('ALTER TABLE articles ADD COLUMN cnyes_canonical_candidate_id BLOB')
    if 'cnyes_known_body_candidate' not in columns:
        queue.db.execute('ALTER TABLE articles ADD COLUMN cnyes_known_body_candidate INTEGER NOT NULL DEFAULT 0 CHECK(cnyes_known_body_candidate IN (0,1))')
    queue.db.executescript("""
        CREATE TABLE IF NOT EXISTS cnyes_native_lookup_candidates(
            parent_article_id BLOB PRIMARY KEY, candidate_article_id BLOB NOT NULL UNIQUE,
            parent_url TEXT NOT NULL, candidate_url TEXT NOT NULL,
            discovered_at_utc TEXT NOT NULL, data_fingerprint TEXT NOT NULL,
            authorization_fingerprint TEXT NOT NULL, discovery_contract_version INTEGER NOT NULL,
            basis TEXT NOT NULL CHECK(basis='unverified_same_id_public_lookup_not_alias'));
        CREATE INDEX IF NOT EXISTS cnyes_candidate_target ON articles(cnyes_canonical_candidate_id)
            WHERE cnyes_canonical_candidate_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS dispatch_cnyes_body_value ON articles(
            provider,state,cnyes_known_body_candidate,metadata_order_day DESC,due)
            WHERE provider='cnyes' AND body_sha256 IS NULL AND state IN ('pending','retry','retry_cooldown');
        CREATE TRIGGER IF NOT EXISTS cnyes_candidate_body_update AFTER UPDATE OF body_sha256 ON articles
        WHEN NEW.provider='cnyes' AND (OLD.body_sha256 IS NULL)!=(NEW.body_sha256 IS NULL) BEGIN
            UPDATE articles SET cnyes_known_body_candidate=(NEW.body_sha256 IS NOT NULL)
            WHERE cnyes_canonical_candidate_id=NEW.id;
        END;
        CREATE TRIGGER IF NOT EXISTS cnyes_candidate_body_delete AFTER DELETE ON articles
        WHEN OLD.provider='cnyes' AND OLD.body_sha256 IS NOT NULL BEGIN
            UPDATE articles SET cnyes_known_body_candidate=0 WHERE cnyes_canonical_candidate_id=OLD.id;
        END;
    """)
    with queue.db:
        queue.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('cnyes_body_value_dispatch_contract',?)",
                         (str(CONTRACT_VERSION),))
        queue.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('cnyes_native_lookup_discovery_contract',?)",
                         (str(LOOKUP_DISCOVERY_VERSION),))
    initialize_progress(queue)


def initialize_progress(queue):
    """Seed only active missing heads once; then exact transactional O(8) reads.

    A hint is not a body, equivalence proof, or completion. State transitions,
    primary-body availability and rollback update the same small aggregate;
    heartbeat/page views never scan historical articles or raw files.
    """
    existed = queue.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cnyes_body_work_metrics'").fetchone()
    queue.db.executescript("""
        CREATE TABLE IF NOT EXISTS cnyes_body_work_metrics(
            state TEXT NOT NULL, candidate INTEGER NOT NULL CHECK(candidate IN (0,1)),
            urls INTEGER NOT NULL CHECK(urls>=0), PRIMARY KEY(state,candidate)
        ) WITHOUT ROWID;
        CREATE TRIGGER IF NOT EXISTS cnyes_work_insert AFTER INSERT ON articles
        WHEN NEW.provider='cnyes' AND NEW.body_sha256 IS NULL
            AND NEW.state IN ('pending','retry','retry_cooldown','inflight') BEGIN
            INSERT INTO cnyes_body_work_metrics VALUES (NEW.state,NEW.cnyes_known_body_candidate,1)
            ON CONFLICT(state,candidate) DO UPDATE SET urls=urls+1;
        END;
        CREATE TRIGGER IF NOT EXISTS cnyes_work_delete AFTER DELETE ON articles
        WHEN OLD.provider='cnyes' AND OLD.body_sha256 IS NULL
            AND OLD.state IN ('pending','retry','retry_cooldown','inflight') BEGIN
            UPDATE cnyes_body_work_metrics SET urls=urls-1
            WHERE state=OLD.state AND candidate=OLD.cnyes_known_body_candidate;
        END;
        CREATE TRIGGER IF NOT EXISTS cnyes_work_update
        AFTER UPDATE OF provider,state,body_sha256,cnyes_known_body_candidate ON articles
        WHEN OLD.provider!=NEW.provider OR OLD.state!=NEW.state
            OR OLD.cnyes_known_body_candidate!=NEW.cnyes_known_body_candidate
            OR (OLD.body_sha256 IS NULL)!=(NEW.body_sha256 IS NULL) BEGIN
            UPDATE cnyes_body_work_metrics SET urls=urls-1
            WHERE state=OLD.state AND candidate=OLD.cnyes_known_body_candidate
                AND OLD.provider='cnyes' AND OLD.body_sha256 IS NULL
                AND OLD.state IN ('pending','retry','retry_cooldown','inflight');
            INSERT INTO cnyes_body_work_metrics SELECT NEW.state,NEW.cnyes_known_body_candidate,1
            WHERE NEW.provider='cnyes' AND NEW.body_sha256 IS NULL
                AND NEW.state IN ('pending','retry','retry_cooldown','inflight')
            ON CONFLICT(state,candidate) DO UPDATE SET urls=urls+1;
        END;
    """)
    with queue.db:
        marker = queue.db.execute("SELECT value FROM queue_settings WHERE key='cnyes_body_work_metrics_version'").fetchone()
        if not existed or not marker or marker[0] != str(PROGRESS_VERSION):
            queue.db.execute('DELETE FROM cnyes_body_work_metrics')
            queue.db.execute("INSERT INTO cnyes_body_work_metrics "
                "SELECT state,cnyes_known_body_candidate,count(*) FROM articles INDEXED BY dispatch_cnyes_body_value "
                "WHERE provider='cnyes' AND body_sha256 IS NULL AND state IN ('pending','retry','retry_cooldown') "
                "GROUP BY state,cnyes_known_body_candidate")
            queue.db.execute("INSERT INTO cnyes_body_work_metrics "
                "SELECT state,cnyes_known_body_candidate,count(*) FROM articles INDEXED BY dispatch "
                "WHERE provider='cnyes' AND body_sha256 IS NULL AND state='inflight' "
                "GROUP BY state,cnyes_known_body_candidate")
            queue.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('cnyes_body_work_metrics_version',?)",
                             (str(PROGRESS_VERSION),))


def progress(db):
    rows = [dict(row) for row in db.execute('SELECT state,candidate,urls FROM cnyes_body_work_metrics WHERE urls>0 ORDER BY state,candidate')]
    known = sum(row['urls'] for row in rows if row['candidate'] == 1)
    missing = sum(row['urls'] for row in rows if row['candidate'] == 0)
    return {'contract_version': PROGRESS_VERSION, 'active_missing_body_urls': known + missing,
            'existing_primary_body_candidates': known, 'no_known_primary_body_urls': missing,
            'states': rows, 'candidate_views_accepted_by_hint': False,
            'candidate_body_equivalence_verified': False,
            'scope': 'active missing-body URL heads; no-known-primary includes unclassified views, not unique stories'}


def request_scope_progress(db, work, control, *, now_epoch):
    """Join a small missing frontier to the owner's actual HTTP scope gates.

    Never scan retained bodies or change dispatch/permission. Large frontiers,
    absent controls and inconsistent counters stay unknown rather than treating
    desktop/API ingestion speed as an AMP recovery rate. The regular constant-
    size ``progress`` read remains unchanged.
    """
    result = {'contract_version': REQUEST_SCOPE_PROGRESS_VERSION, 'available': False,
        'active_missing_body_urls': None, 'source_paused_missing_body_urls': None,
        'other_missing_body_urls': None, 'policy_paused_missing_body_urls': None,
        'gateway_paused_missing_body_urls': None, 'next_source_probe_at_utc': None,
        'pending_source_rechecks': {'contract_version': PENDING_SOURCE_RECHECKS_VERSION,
            'available': False, 'unverified_missing_body_urls': None,
            'due_missing_body_urls': None, 'next_probe_at_utc': None},
        'source_recovery_verified': False}
    total = work.get('active_missing_body_urls')
    if (type(total) is not int or not 0 <= total <= MAX_REQUEST_SCOPE_URLS
            or not isinstance(control, dict) or type(now_epoch) not in {int, float}
            or not math.isfinite(now_epoch)):
        return result
    policies = control.get('temporarily_paused_robots_origins')
    gateways = control.get('temporarily_paused_gateway_hosts')
    for gates in (policies, gateways):
        if (not isinstance(gates, dict) or len(gates) > 32
                or any(not isinstance(key, str) or not 0 < len(key) <= 320
                    or type(deadline) not in {int, float} or not math.isfinite(deadline)
                    or not 0 <= deadline <= 253402214400 for key, deadline in gates.items())):
            return result
    unresolved_policies = control.get('unverified_robots_origins')
    unresolved_gateways = control.get('unverified_gateway_hosts')
    overflow = control.get('unverified_robots_origins_overflow')
    rechecks_available = (overflow is False and all(
        isinstance(gates, dict) and len(gates) <= 32
        and all(isinstance(key, str) and 0 < len(key) <= 320
                and type(deadline) in {int, float} and math.isfinite(deadline)
                and 0 <= deadline <= 253402214400
                for key, deadline in gates.items())
        for gates in (unresolved_policies, unresolved_gateways)))
    if rechecks_available:
        rechecks_available = all(
            key in unresolved and unresolved[key] >= deadline
            for active, unresolved in ((policies, unresolved_policies),
                                       (gateways, unresolved_gateways))
            for key, deadline in active.items() if deadline > now_epoch)
    # These existing partial indexes exclude the millions of retained heads.
    rows = db.execute("SELECT url,state FROM articles INDEXED BY dispatch_cnyes_body_value "
        "WHERE provider='cnyes' AND body_sha256 IS NULL "
        "AND state IN ('pending','retry','retry_cooldown') LIMIT ?", (total + 1,)).fetchall()
    expected = Counter()
    for row in work['states']:
        expected[row['state']] += row['urls']
    if expected['inflight']:
        rows.extend(db.execute("SELECT url,state FROM articles INDEXED BY dispatch "
            "WHERE provider='cnyes' AND state='inflight' AND body_sha256 IS NULL LIMIT ?",
            (expected['inflight'] + 1,)).fetchall())
    if len(rows) != total or Counter(row['state'] for row in rows) != expected:
        return result
    paused = policy_count = gateway_count = rechecks = due_rechecks = 0
    deadlines = []
    recheck_deadlines = []
    for row in rows:
        if not isinstance(row['url'], str) or len(row['url']) > 4096:
            return result
        try:
            parts = urlsplit(row['url'])
            if (parts.scheme not in {'http', 'https'} or not parts.hostname
                    or parts.username is not None or parts.password is not None
                    or parts.port is not None and not 1 <= parts.port <= 65535):
                return result
        except ValueError:
            return result
        # Exact same origin construction as NewsHTTP._robots; paths/queries
        # never become public status fields or equivalence assertions.
        policy = policies.get(f'{parts.scheme}://{parts.netloc}', 0)
        gateway = gateways.get(parts.hostname, 0)
        deadline = max(policy, gateway)
        # An in-flight request is not prevented by a later scheduler gate,
        # but it can still be checking unresolved negative evidence.
        if row['state'] != 'inflight':
            policy_count += policy > now_epoch
            gateway_count += gateway > now_epoch
            if deadline > now_epoch:
                paused += 1
                deadlines.append(deadline)
        if rechecks_available:
            origin = f'{parts.scheme}://{parts.netloc}'
            if origin in unresolved_policies or parts.hostname in unresolved_gateways:
                recheck = max(deadline, unresolved_policies.get(origin, 0),
                              unresolved_gateways.get(parts.hostname, 0))
                rechecks += 1
                due_rechecks += recheck <= now_epoch
                recheck_deadlines.append(recheck)
    recheck_progress = ({'contract_version': PENDING_SOURCE_RECHECKS_VERSION,
        'available': True, 'unverified_missing_body_urls': rechecks,
        'due_missing_body_urls': due_rechecks,
        'next_probe_at_utc': datetime.fromtimestamp(min(recheck_deadlines), UTC).isoformat()
            if recheck_deadlines else None} if rechecks_available else result['pending_source_rechecks'])
    return {**result, 'available': True, 'active_missing_body_urls': total,
        'source_paused_missing_body_urls': paused, 'other_missing_body_urls': total - paused,
        'policy_paused_missing_body_urls': policy_count,
        'gateway_paused_missing_body_urls': gateway_count,
        'pending_source_rechecks': recheck_progress,
        'next_source_probe_at_utc': datetime.fromtimestamp(min(deadlines), UTC).isoformat() if deadlines else None}


def admit_native_lookup(queue, row):
    """Register missing lookup work in the SAME queue and transaction.

    No parent metadata/body/clock is copied; no request or acceptance occurs.
    Refusals, leases, existing target states, finite cohorts and source rights
    cannot be reset by discovery. HTTP still owns robots/pacing/cooldowns.
    """
    from downloader.news_fulltext_policy import NewsError
    target = native_lookup_url(row['url']) if row['provider'] == 'cnyes' else None
    if target is None:
        return False
    parent = queue.db.execute('SELECT state,error_code,body_sha256 FROM articles WHERE id=?', (row['id'],)).fetchone()
    if (not parent or parent['state'] not in STATES or parent['body_sha256']
            or parent['error_code'] not in {None, 'http_502', 'http_504', 'host_gateway_cooldown', 'transport_error'}):
        return False
    try:
        source, other = queue.registry.require(row['url']), queue.registry.require(target)
    except NewsError:
        return False
    if (source.provider != 'cnyes' or other.provider != source.provider
            or source.acquisition_mode != 'continuous' or other.acquisition_mode != 'continuous'
            or source.data_fingerprint != other.data_fingerprint
            or source.authorization_fingerprint != other.authorization_fingerprint
            or not source.article_path_pattern
            or not re.search(source.article_path_pattern, urlsplit(target).path)):
        return False
    candidate_id = hashlib.sha256(target.encode()).digest()
    if queue.db.execute('SELECT 1 FROM articles WHERE id=?', (candidate_id,)).fetchone():
        return False
    # Caller owns the transaction, just like metadata registration/classify.
    # add_articles does not commit, reset deadlines, or create an origin link.
    queue.add_articles([{'url': target}])
    candidate = queue.db.execute('SELECT first_seen_utc FROM articles WHERE id=?', (candidate_id,)).fetchone()
    queue.db.execute('INSERT INTO cnyes_native_lookup_candidates VALUES (?,?,?,?,?,?,?,?,?)',
                     (row['id'], candidate_id, row['url'], target, candidate[0], source.data_fingerprint,
                      source.authorization_fingerprint, LOOKUP_DISCOVERY_VERSION,
                      'unverified_same_id_public_lookup_not_alias'))
    queue.counter('cnyes_native_lookup_candidates_added', 1)
    return True


def classify(queue, row):
    if row['provider'] != 'cnyes':
        return False
    target = canonical_candidate_id(row['url'])
    if target is None:
        return False
    other = queue.db.execute('SELECT body_sha256 FROM articles WHERE id=? AND provider=\'cnyes\'',
                             (target,)).fetchone()
    known = bool(other and other[0])
    # Reobserving an unchanged metadata partition must not rewrite its candidate
    # indexes. The primary body may have arrived since the last observation, so
    # still compare both fields against the current source state in this update.
    queue.db.execute('UPDATE articles SET cnyes_canonical_candidate_id=?,cnyes_known_body_candidate=? '
                     'WHERE id=? AND (cnyes_canonical_candidate_id IS NOT ? OR cnyes_known_body_candidate!=?)',
                     (target, known, row['id'], target, known))
    return known


def _valid_cursors(value):
    return (isinstance(value, dict) and set(value) == set(STATES)
            and all(type(v) is int and 0 <= v <= 2**63 - 1 for v in value.values()))


def backfill(queue, *, limit=256, max_seconds=0.04, stop=None):
    if type(limit) is not int or not 1 <= limit <= 2048 or not 0.001 <= max_seconds <= 0.1:
        raise ValueError('bounded_cnyes_candidate_slice_required')
    from downloader.news_fulltext_urls import _read_checkpoint
    started = time.monotonic()
    checked = known = added = 0
    with queue.metrics.measure('cnyes_body_candidate_backfill'), queue.db:
        for host, checkpoint in (('amp-news.cnyes.com', AMP_CURSOR), ('m.cnyes.com', CURSOR),
                                 ('news.cnyes.com', DESKTOP_CURSOR)):
            cursors = _read_checkpoint(queue, checkpoint, {state: 0 for state in STATES}, _valid_cursors)
            scanned = 0
            for state in STATES:
                if checked >= limit or time.monotonic() - started >= max_seconds or stop and stop.is_set():
                    break
                rows = queue.db.execute('SELECT rowid AS candidate_rowid,id,url,provider FROM articles INDEXED BY host_state '
                    'WHERE host=? AND state=? AND rowid>? '
                    + ("AND substr(url,-6)='?exp=a' " if host == 'news.cnyes.com' else '')
                    + 'ORDER BY rowid LIMIT ?',
                    (host, state, cursors[state], limit - checked)).fetchall()
                for row in rows:
                    if time.monotonic() - started >= max_seconds or stop and stop.is_set():
                        break
                    known += classify(queue, row)
                    if host == 'amp-news.cnyes.com':
                        added += admit_native_lookup(queue, row)
                    checked += 1
                    scanned += 1
                    cursors[state] = row['candidate_rowid']
            if scanned:
                queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (checkpoint, json.dumps(cursors)))
        if checked:
            queue.counter('cnyes_candidate_heads_classified', checked)
            queue.counter('cnyes_existing_body_candidates_observed', known)
    return {'checked_heads': checked, 'existing_body_candidates': known,
            'native_lookup_candidates_added': added, 'publisher_requests': 0,
            'source_observations_merged': False, 'candidate_body_equivalence_verified': False}


def maintenance_delay(result):
    # Imports classify immediately and claims lazily classify older state
    # transitions. An idle background seek cannot delay actual HTTP dispatch.
    # A new owner always seeks immediately from its durable cursor.
    return 0.25 if result['checked_heads'] else 5.0


def statement(*, exclude_parser_repairs=False):
    return ('SELECT * FROM articles INDEXED BY dispatch_cnyes_body_value '
        "WHERE provider=? AND provider='cnyes' AND state=? AND due<=? "
        "AND cnyes_known_body_candidate=? AND body_sha256 IS NULL "
        "AND state IN ('pending','retry','retry_cooldown') "
        + ("AND (error_code IS NULL OR error_code NOT IN ('body_not_found','article_attribution_not_verified')) "
           if exclude_parser_repairs else '')
        + 'ORDER BY metadata_order_day DESC LIMIT 1')


def candidate(queue, state, now, prefer_known, *, exclude_parser_repairs=False, excluded_hosts=(), excluded_origins=()):
    from downloader.news_fulltext_host_circuit import host_filtered_query
    for known in ((1, 0) if prefer_known else (0, 1)):
        # Newly imported or state-changing mobile work may precede the cursor.
        # At most eight indexed lookups/classifications; never a hot full scan.
        for _ in range(8):
            query, parameters = host_filtered_query(statement(exclude_parser_repairs=exclude_parser_repairs),
                                                   ('cnyes', state, now, known), excluded_hosts, excluded_origins=excluded_origins)
            row = queue.db.execute(query, parameters).fetchone()
            if row is None:
                break
            if not known and row['cnyes_canonical_candidate_id'] is None and classify(queue, row):
                continue
            return row
    return None
