"""Publisher-declared public Anue index, distinct from bulk article bodies.

The category API has an observed thirty-page limit. The separately published
all-list advertises and serves later pages, but contains summaries, not bodies.
Use one list per actual overflowing source date; preserve the original gaps
and use the existing article queue for every newly discovered public ID.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import html
import json
import math
import re
import sqlite3
import time
from urllib.parse import parse_qs, urlencode, urlsplit

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_policy import NewsError


CONTRACT_VERSION = 1
INDEX_DATA_CONTRACT_VERSION = 2
MANIFEST_SCHEDULE_CONTRACT_VERSION = 1
MANIFEST_PHASE_CONTRACT_VERSION = 1
FAILURE_SCOPE_CONTRACT_VERSION = 1
FAILURE_DEADLINE_CONTRACT_VERSION = 1
FAILURE_DIAGNOSTIC_CONTRACT_VERSION = 2
TOTAL_RECONCILIATION_CONTRACT_VERSION = 1
ENDPOINT = 'https://api.cnyes.com/media/api/v1/newslist/all'
MAXIMUM_PAGE = 100_000  # Engineering bound, not an official pagination limit.
MANIFEST_PRIORITY_BAND = -10_000_000
FIELDS = frozenset({
    'abTesting', 'categoryId', 'columnists', 'coverSrc', 'hasCoverPhoto',
    'isCategoryHeadline', 'isIndex', 'newsId', 'payment', 'publishAt', 'summary', 'title',
})


def request_failure_scope_key(source, request_url=None):
    key = 'cnyes_index_request_failure_scope_v1:' + source.cnyes_index_data_fingerprint
    return key if request_url is None else key + ':' + hashlib.sha256(request_url.encode()).hexdigest()


def request_failure_scope_guard(source, *, requested_url=None):
    """An exact derived annotation may isolate this one metadata error.

    The original circuit and source deadline remain authoritative. Missing,
    malformed, changed or older read-only annotations retain the original
    endpoint-wide guard; HTTP/robots/schema/pagination failures never qualify.
    No new table, second writer or source-permission decision is introduced.
    """
    target = 'discovery.url' if requested_url is None else '?'
    parameters = [] if requested_url is None else [requested_url]
    return (" AND (circuit.request_url=" + target + " OR NOT EXISTS ("
        "SELECT 1 FROM queue_settings AS failure_scope WHERE failure_scope.key=? "
        "AND circuit.error_code='cnyes_index_unverified_metadata' "
        "AND CASE WHEN json_valid(failure_scope.value) THEN ("
        "json_type(failure_scope.value,'$.contract_version')='integer' AND "
        "json_extract(failure_scope.value,'$.contract_version')=1 AND "
        "json_extract(failure_scope.value,'$.scope')='request' AND "
        "json_type(failure_scope.value,'$.permission_granted')='false' AND "
        "json_type(failure_scope.value,'$.preserves_source_validation')='true' AND "
        "json_extract(failure_scope.value,'$.provider')=circuit.provider AND "
        "json_extract(failure_scope.value,'$.endpoint')=circuit.endpoint AND "
        "json_extract(failure_scope.value,'$.data_fingerprint')=circuit.data_fingerprint AND "
        "json_extract(failure_scope.value,'$.request_url')=circuit.request_url AND "
        "json_extract(failure_scope.value,'$.error_code')=circuit.error_code AND "
        "json_extract(failure_scope.value,'$.not_before')=circuit.not_before"
        ") ELSE 0 END))", [*parameters, request_failure_scope_key(source)])


def _request_failure_scope_fields(queue, source, circuit):
    """Only this page's own deadline can prove a request-local failure."""
    if (not source.allowed or not source.cnyes_index_api or circuit['provider'] != 'cnyes'
            or circuit['endpoint'] != ENDPOINT
            or circuit['data_fingerprint'] not in compatible_fingerprints(source)
            or circuit['error_code'] != 'cnyes_index_unverified_metadata'
            or type(circuit['not_before']) not in (int, float)
            or not math.isfinite(circuit['not_before'])):
        return None
    try:
        validate_request(circuit['request_url'], source)
    except (NewsError, TypeError, AttributeError, ValueError):
        return None
    job = queue.db.execute('SELECT kind,provider,due,attempts,error_code,last_checked_at_utc '
        'FROM discovery WHERE url=?', (circuit['request_url'],)).fetchone()
    if (not job or job['kind'] != 'index' or job['provider'] != source.provider
            or job['error_code'] != circuit['error_code'] or type(job['attempts']) is not int
            or job['attempts'] < 1 or type(job['due']) not in (int, float)
            or not math.isfinite(job['due'])):
        return None
    try:
        checked = datetime.fromisoformat(job['last_checked_at_utc'])
        if (checked.utcoffset() is None or checked.timestamp() > job['due']
                or not 0 <= circuit['not_before'] - job['due'] <= 1):
            return None
    except (ValueError, TypeError, OverflowError):
        return None
    value = {key: circuit[key] for key in
        ('provider', 'endpoint', 'data_fingerprint', 'request_url', 'error_code', 'not_before')}
    value.update(contract_version=FAILURE_SCOPE_CONTRACT_VERSION, scope='request',
        permission_granted=False, preserves_source_validation=True,
        basis='canonical_failed_page_metadata_validation_not_http_or_endpoint_schema')
    return value


def record_request_failure_scope(queue, source, circuit, *, metadata_diagnostic=None):
    """Keep the exact endpoint witness plus bounded per-failed-page lineage.

    Scope provenance is not metadata/body acceptance or a source permission.
    Original jobs, source clocks and deadlines are never changed here.
    """
    value = _request_failure_scope_fields(queue, source, circuit)
    if value is None:
        return False
    key = request_failure_scope_key(source)
    previous = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()
    if previous is None and circuit['data_fingerprint'] != source.cnyes_index_data_fingerprint:
        # The current alias reads the exact first clock/typed evidence. Its
        # original key/value, job and circuit remain untouched.
        previous = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            ('cnyes_index_request_failure_scope_v1:' + circuit['data_fingerprint'],)).fetchone()
    if previous:
        try:
            original = json.loads(previous[0])
            if isinstance(original, dict) and all(
                    type(original.get(k)) is type(v) and original[k] == v for k, v in value.items()):
                value = original  # Preserve first observation and typed field evidence at startup.
        except (ValueError, TypeError):
            pass
    if 'recorded_at_utc' not in value:
        value['recorded_at_utc'] = clock()
    if metadata_diagnostic is not None:
        # A private, bounded leaf; never merge it into scope/permission fields.
        value['metadata_failure_diagnostic'] = metadata_diagnostic
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':'))
    for target_key in (key, request_failure_scope_key(source, circuit['request_url'])):
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?) ON CONFLICT(key) DO UPDATE '
            'SET value=excluded.value WHERE queue_settings.value IS NOT excluded.value', (target_key, encoded))
    return True


def _proved_request_scope(queue, source, value):
    if not isinstance(value, dict):
        return False
    try:
        circuit = {k: value[k] for k in
            ('provider', 'endpoint', 'data_fingerprint', 'request_url', 'error_code', 'not_before')}
        fields = _request_failure_scope_fields(queue, source, circuit)
        if fields is None or not all(type(value.get(k)) is type(v) and value[k] == v for k, v in fields.items()):
            return False
        recorded = datetime.fromisoformat(value['recorded_at_utc'])
        checked = datetime.fromisoformat(queue.db.execute('SELECT last_checked_at_utc FROM discovery WHERE url=?',
            (value['request_url'],)).fetchone()[0])
        return recorded.utcoffset() is not None and checked <= recorded and recorded.timestamp() <= value['not_before']
    except (KeyError, NewsError, ValueError, TypeError, AttributeError, OverflowError):
        return False


def reconcile_inherited_metadata_deadline(queue, source):
    """Repair only proved metadata -> transport max() deadline donation.

    Keep all per-job retry clocks and every remaining global deadline. Unknown,
    refusal, rate-limit, server and malformed provenance keep the old circuit.
    The original circuit and every included cause survive in an immutable
    queue-settings journal before the one canonical writer changes its union.
    """
    original = queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns WHERE provider=? AND endpoint=?',
                               (source.provider, ENDPOINT)).fetchone()
    if not original:
        return False
    circuit = dict(original)
    if circuit['error_code'] != 'transport_error' or circuit['data_fingerprint'] != source.cnyes_index_data_fingerprint:
        return False
    try:
        entry = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                                 (request_failure_scope_key(source),)).fetchone()
        donor = json.loads(entry[0]) if entry else None
        if (not _proved_request_scope(queue, source, donor)
                or circuit['not_before'] != donor['not_before'] or circuit['request_url'] == donor['request_url']):
            return False
        validate_request(circuit['request_url'], source)
        failed = queue.db.execute('SELECT * FROM discovery WHERE url=?', (circuit['request_url'],)).fetchone()
        if (not failed or failed['kind'] != 'index' or failed['provider'] != source.provider
                or failed['error_code'] != 'transport_error' or type(failed['attempts']) is not int
                or failed['attempts'] < 1 or type(failed['due']) not in (int, float)
                or not math.isfinite(failed['due']) or not failed['due'] < circuit['not_before']):
            return False
        checked = datetime.fromisoformat(failed['last_checked_at_utc'])
        if (checked.utcoffset() is None or checked < datetime.fromisoformat(donor['recorded_at_utc'])
                or checked.timestamp() > failed['due']):
            return False
        # A legacy exact donor is promoted without changing its first clock.
        # Other request-local exclusions require their own matching witness.
        rows = queue.db.execute('SELECT * FROM discovery WHERE provider=? AND kind=\'index\' '
            'AND substr(url,1,instr(url||\'?\',\'?\')-1)=? '
            'AND error_code IS NOT NULL AND due>? AND url!=? ORDER BY due DESC LIMIT 129',
            (source.provider, ENDPOINT, time.time(), donor['request_url'])).fetchall()
        if len(rows) > 128:
            return False  # Never lower a floor from a truncated cause set.
        global_deadline, included, excluded = failed['due'], [dict(failed)], [donor]
        for row in rows:
            validate_request(row['url'], source)
            if type(row['due']) not in (int, float) or not math.isfinite(row['due']):
                return False
            witness = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                (request_failure_scope_key(source, row['url']),)).fetchone()
            scope = json.loads(witness[0]) if witness else None
            if row['error_code'] == 'cnyes_index_unverified_metadata' and _proved_request_scope(queue, source, scope):
                excluded.append(scope)
            else:
                global_deadline = max(global_deadline, row['due'])
                included.append(dict(row))
        # Cover the original adjacent time.time() calls without shortening a
        # genuine remaining source deadline by a subsecond scheduling margin.
        global_deadline += 1
        if global_deadline >= circuit['not_before']:
            return False
    except (NewsError, KeyError, ValueError, TypeError, AttributeError, OverflowError):
        return False
    value = {'contract_version': FAILURE_DEADLINE_CONTRACT_VERSION,
        'original_circuit': circuit, 'request_scoped_metadata_causes': excluded,
        'remaining_global_causes': included, 'corrected_endpoint_not_before': global_deadline,
        'original_job_deadlines_preserved': True, 'source_validation_preserved': True,
        'permission_granted': False, 'reconciled_at_utc': clock()}
    identity = hashlib.sha256(json.dumps(circuit, sort_keys=True).encode()).hexdigest()
    key = 'cnyes_index_deadline_reconciliation_v1:' + identity
    if queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (key,)).fetchone():
        return False  # A changed/restored old context requires independent audit.
    queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
                     (key, json.dumps(value, sort_keys=True, separators=(',', ':'))))
    record_request_failure_scope(queue, source, donor)
    queue.db.execute('UPDATE bulk_endpoint_cooldowns SET not_before=? WHERE provider=? AND endpoint=?',
                     (global_deadline, source.provider, ENDPOINT))
    queue.metrics.count(source.provider, 'public_index_inherited_metadata_deadline_reconciliations')
    return True


def clock():
    return datetime.now(timezone.utc).isoformat()


def reviewed_index(entry, provider, authorization, *, data_version=INDEX_DATA_CONTRACT_VERSION):
    """Keep this metadata ABI independent of every existing body/cache ABI."""
    config = entry.get('cnyes_public_index')
    if config is None:
        return '', ''
    if type(data_version) is not int or data_version not in (1, INDEX_DATA_CONTRACT_VERSION):
        raise ValueError('unreviewed Cnyes public index data contract')
    if not isinstance(config, dict):
        raise ValueError('invalid reviewed Cnyes public index')
    navigation = config.get('publisher_navigation', {})
    pages = config.get('observed_pages', [])
    if (provider != 'cnyes' or config.get('contract_version') != CONTRACT_VERSION
            or config.get('endpoint') != ENDPOINT or config.get('observed_page_size') != 30
            or config.get('seed_scope') != 'actual_single_source_day_category_gaps'
            or not isinstance(navigation, dict)
            or not re.fullmatch(r'https://news[.]cnyes[.]com/_next/static/chunks/[A-Za-z0-9._-]+[.]js', navigation.get('url',''))
            or not re.fullmatch(r'[0-9a-f]{64}', navigation.get('response_sha256',''))
            or not isinstance(pages, list) or len(pages) != 3
            or [row.get('page') for row in pages if isinstance(row, dict)] != [1, 2, 31]
            or any(type(row.get('page')) is not int or not re.fullmatch(r'[0-9a-f]{64}', row.get('response_sha256','')) for row in pages)):
        raise ValueError('unreviewed Cnyes public index endpoint/navigation/pagination')
    payload = {'version':CONTRACT_VERSION, 'config':config, 'authorization':authorization,
               'purpose':'private_public_index_metadata_not_article_or_training_acceptance'}
    if data_version == 2:
        # The reviewed endpoint/scope stays v1. Only list-title admission changes:
        # native strings may be empty; article identity/body validation does not.
        payload['optional_native_string_title_metadata_contract_version'] = 2
    return ENDPOINT, hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def compatible_fingerprints(source):
    """Read the exact stricter v1 ABI; never relabel or generalize old evidence.

    Registry derives the (current, previous) pair from the same reviewed config
    and authorization. Binding the first member to the current fingerprint keeps
    changed/foreign parser contracts out even after dataclass replacement.
    """
    current = source.cnyes_index_data_fingerprint
    pair = getattr(source, 'cnyes_index_compatibility', ())
    if (isinstance(pair, tuple) and len(pair) == 2 and pair[0] == current
            and pair[1] != current and isinstance(pair[1], str)
            and re.fullmatch(r'[0-9a-f]{64}', pair[1])):
        return current, pair[1]
    return (current,)


def source_gate_progress(db, source, control, *, now_epoch):
    """Observe the all-index gates without changing requests or their evidence.

    Reuse the dispatcher's exact page-vs-endpoint proof: a validated one-page
    metadata failure must not freeze its healthy neighbours. An expired
    endpoint, host or robots failure is still unverified until a real success.
    """
    result = {'contract_version': 1, 'state': 'unknown', 'next_probe_at_utc': None,
              'source_recovery_requires_success': True}
    if (not source or not source.allowed or source.cnyes_index_api != ENDPOINT
            or type(now_epoch) not in (int, float) or not math.isfinite(now_epoch)):
        return result
    versions = compatible_fingerprints(source)
    guard, parameters = request_failure_scope_guard(source, requested_url='')
    row = db.execute('SELECT circuit.not_before FROM bulk_endpoint_cooldowns AS circuit '
                     'WHERE circuit.provider=? AND circuit.endpoint=? AND circuit.data_fingerprint IN ('
                     + ','.join('?' for _ in versions) + ')' + guard,
                     (source.provider, ENDPOINT, *versions, *parameters)).fetchone()
    waiting, rechecking, unknown = [], False, False

    def deadline(value):
        return type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 253402214400

    if row is not None:
        if not deadline(row[0]):
            unknown = True
        elif row[0] > now_epoch:
            waiting.append(row[0])
        else:
            rechecking = True

    parts = urlsplit(ENDPOINT)
    pairs = ((parts.hostname, 'temporarily_paused_gateway_hosts', 'unverified_gateway_hosts', False),
             (parts.scheme + '://' + parts.netloc, 'temporarily_paused_robots_origins',
              'unverified_robots_origins', True))
    if not isinstance(control, dict):
        unknown = True
    else:
        if control.get('unverified_robots_origins_overflow') is not False:
            unknown = True
        for target, active_name, unresolved_name, allow_unknown in pairs:
            active, unresolved = control.get(active_name), control.get(unresolved_name)
            if (not isinstance(active, dict) or not isinstance(unresolved, dict)
                    or len(active) > 32 or len(unresolved) > 32):
                unknown = True
                continue
            if target in active:
                value = active[target]
                if not deadline(value):
                    unknown = True
                elif value > now_epoch:
                    waiting.append(value)
                if target not in unresolved:
                    unknown = True
            if target in unresolved:
                value = unresolved[target]
                if value is None and allow_unknown:
                    rechecking = True
                elif not deadline(value):
                    unknown = True
                elif value <= now_epoch:
                    rechecking = True
                elif target not in active or not deadline(active[target]) or active[target] != value:
                    unknown = True
    if waiting:
        result.update(state='waiting', next_probe_at_utc=datetime.fromtimestamp(max(waiting), timezone.utc).isoformat())
    elif rechecking:
        result['state'] = 'rechecking'
    elif not unknown:
        result['state'] = 'clear'
    return result


def compatible_head_guard(source):
    """A page contributes once: current head wins over its stricter old head."""
    versions = compatible_fingerprints(source)
    if len(versions) == 1:
        return 'h.index_fingerprint=?', versions
    current, previous = versions
    return ("(h.index_fingerprint=? OR (h.index_fingerprint=? AND NOT EXISTS ("
            "SELECT 1 FROM cnyes_public_index_heads AS newer WHERE newer.scope_url=h.scope_url "
            "AND newer.page=h.page AND newer.authorization_fingerprint=h.authorization_fingerprint "
            "AND newer.index_fingerprint=?)))", (current, previous, current))


def is_index_task(task):
    return task['kind'] == 'index' and task['url'].split('?',1)[0] == ENDPOINT


def request_url(source, day, page=1):
    if (getattr(source, 'cnyes_index_api', '') != ENDPOINT or type(day) is not int
            or type(page) is not int or not 1 <= page <= MAXIMUM_PAGE):
        raise ValueError('unregistered or unbounded public Cnyes index request')
    low = day*86400 - 28800
    return ENDPOINT+'?'+urlencode({'limit':30, 'page':page, 'startAt':low, 'endAt':low+86399})


def discovery_priority(day, page=1):
    """Observe every actual gap date before deep metadata-only pagination.

    This ordering applies only inside the existing index lane. The shared
    selector still serves current feeds and efficient body batches first, and
    its article fairness remains unchanged. A first page reveals source-reported
    work, not unique articles, accepted bodies, or a complete historical scope.
    """
    if type(day) is not int or type(page) is not int or not 1 <= page <= MAXIMUM_PAGE:
        raise ValueError('invalid bounded Cnyes index priority')
    return -day * 100 + (MANIFEST_PRIORITY_BAND if page == 1 else 0)


def validate_request(url, source):
    query = parse_qs(urlsplit(url).query, keep_blank_values=True)
    try:
        if (getattr(source, 'cnyes_index_api', '') != ENDPOINT or url.split('?',1)[0] != ENDPOINT
                or set(query) != {'limit','page','startAt','endAt'} or any(len(v) != 1 for v in query.values())
                or query['limit'] != ['30']):
            raise ValueError('unregistered parameters')
        page, low, high = (int(query[k][0]) for k in ('page','startAt','endAt'))
        day = (low+28800)//86400
        first = int(datetime(1999,1,1,tzinfo=timezone.utc).timestamp())-28800
        today = int(time.time())+86400
        if (low < first or high > today or low != day*86400-28800 or high != low+86399
                or not 1 <= page <= MAXIMUM_PAGE or url != request_url(source,day,page)):
            raise ValueError('unbounded or noncanonical date/page')
        return day, page, low, high
    except (ValueError, KeyError, TypeError) as error:
        raise NewsError('cnyes_index_request_unregistered') from error


def initialize(queue):
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS cnyes_public_index_observations(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,response_sha256 TEXT NOT NULL,
            index_fingerprint TEXT NOT NULL,authorization_fingerprint TEXT NOT NULL,
            total INTEGER NOT NULL,pages INTEGER NOT NULL,ids_sha256 TEXT NOT NULL,
            metadata_rows INTEGER NOT NULL,public_ids INTEGER NOT NULL,
            evidence_path TEXT NOT NULL,source_observed_at_utc TEXT NOT NULL,
            PRIMARY KEY(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_public_index_heads(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,response_sha256 TEXT NOT NULL,
            index_fingerprint TEXT NOT NULL,authorization_fingerprint TEXT NOT NULL,
            PRIMARY KEY(scope_url,page,index_fingerprint,authorization_fingerprint)
        ) WITHOUT ROWID;
    ''')
    from downloader.news_fulltext_cnyes_revision import initialize as initialize_revision
    initialize_revision(queue)
    source = queue.registry.sources.get('cnyes')
    if source and source.cnyes_index_api:
        with queue.db:
            for row in queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns WHERE provider=? AND endpoint=?',
                                       (source.provider, ENDPOINT)).fetchall():
                record_request_failure_scope(queue, source, dict(row))
                reconcile_inherited_metadata_deadline(queue, source)
            # A prior process may have stopped on an already mixed total
            # before reconciliation was introduced. One indexed startup pass
            # recovers only these scopes; healthy dates and due/error states
            # are left untouched. Continuing repairs are transaction-backed.
            if source.allowed:
                guard, parameters = compatible_head_guard(source)
                mixed = queue.db.execute('SELECT o.scope_url FROM cnyes_public_index_heads h '
                    'JOIN cnyes_public_index_observations o '
                    'USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
                    'WHERE ' + guard + ' AND h.authorization_fingerprint=? GROUP BY o.scope_url '
                    'HAVING min(o.total)!=max(o.total) OR min(o.pages)!=max(o.pages)',
                    (*parameters, source.authorization_fingerprint)).fetchall()
                for row in mixed:
                    _reconcile_index_totals(queue, source, row['scope_url'], 1, changed=True)
                    from downloader.news_fulltext_cnyes_revision import _round, start_round
                    if not _round(queue, source, row['scope_url']):
                        start_round(queue, source, row['scope_url'], reason='startup_mixed_totals')
                from downloader.news_fulltext_cnyes_revision import migrate_historical_revisions
                migrate_historical_revisions(queue, source)


def gap_dates(db, source):
    """The same real source-day set owns scheduling and its phase denominator."""
    days = set()
    from downloader.news_fulltext_cnyes_history import scope_key
    for row in db.execute("SELECT scope_url FROM bulk_window_gaps WHERE provider=?", (source.provider,)):
        key = scope_key(row[0])
        if key and key[0] in source.cnyes_apis and key[1] == key[2]:
            days.add(key[1])
    return days


def seed_gap_dates(queue, source):
    if not getattr(source,'cnyes_index_api','') or not source.allowed:
        return
    # Existing category work changes only its actual gap table. A bounded
    # once/minute read avoids scanning 1,518+ rows on every owner cycle.
    epoch = time.monotonic()
    if epoch < getattr(queue,'_cnyes_index_seed_after',0):
        return
    days = gap_dates(queue.db, source)
    # Existing first-page jobs migrate only their ordering. In particular, do
    # not reset successful deadlines, source errors, attempts, ETags, raw hashes
    # or clocks just because the scheduling policy changed. Subsequent seeds
    # have no changed rows; unrelated/foreign work at a colliding URL is retained.
    queue.db.executemany("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'index',?,?) "
                        "ON CONFLICT(url) DO UPDATE SET priority=excluded.priority "
                        "WHERE discovery.provider=excluded.provider AND discovery.kind='index' "
                        "AND discovery.priority!=excluded.priority",
                        ((request_url(source,day), source.provider, discovery_priority(day))
                         for day in sorted(days,reverse=True)))
    queue._cnyes_index_seed_after = epoch+60
    queue.metrics.count(source.provider,'public_index_seed_dates_examined',len(days))


def _metadata_failure(response, source, post, posts, number, low, high):
    """Explain a rejected row, never coerce source clocks or permission flags.

    The already-fetched response moves to the queue owner only on failure.
    No healthy-page clock/hash, source request, writer or acceptance is added.
    A later unreviewed field anywhere in the page forbids retaining raw bytes.
    """
    from downloader.news_fulltext_diagnostics import FailureResponse
    reasons = []
    for field, valid, reason in (
        ('publishAt', type(post.get('publishAt')) is int, 'publishAt_type'),
        ('categoryId', type(post.get('categoryId')) is int, 'categoryId_type'),
        ('payment', type(post.get('payment')) is int, 'payment_type'),
        ('title', isinstance(post.get('title'), str), 'title_type'),
    ):
        if not valid:
            reasons.append(reason)
        elif field == 'publishAt' and not low <= post[field] <= high:
            reasons.append('publishAt_outside_request')
        elif field == 'categoryId' and post[field] <= 0:
            reasons.append('categoryId_nonpositive')
        elif field == 'payment' and post[field] not in {0, 1}:
            reasons.append('payment_outside_free_paid_flags')
    checked = {}
    for field in ('publishAt', 'categoryId', 'payment', 'title'):
        value = post.get(field)
        checked[field] = {'present': field in post, 'json_type': type(value).__name__}
        if field == 'title' and isinstance(value, str):
            checked[field]['character_count'] = len(value)
        elif field != 'title' and (type(value) in (int, bool) or value is None):
            checked[field]['value'] = value
    error = NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600)
    error.cnyes_index_failure_details = {
        'contract_version': FAILURE_DIAGNOSTIC_CONTRACT_VERSION,
        'item_number': number, 'news_id': post['newsId'], 'violations': reasons,
        'request_low_epoch': low, 'request_high_epoch': high, 'checked_fields': checked,
    }
    error.cnyes_index_metadata_raw_allowed = all(not set(row) - FIELDS for row in posts)
    error.failure_response = FailureResponse(response, source.provider, source.cnyes_index_data_fingerprint,
                                             response.url, clock())
    return error


def parse_index(response, source):
    day, page, low, high = validate_request(response.url,source)
    if response.status != 200:
        raise NewsError('cnyes_index_response_not_200',retry=True,delay=60)
    from downloader.news_fulltext_bulk import cnyes_pagination
    info, posts = cnyes_pagination(response,maximum_page=MAXIMUM_PAGE)
    if info['pages'] > MAXIMUM_PAGE:
        raise NewsError('cnyes_index_reported_pages_over_engineering_bound')
    links, seen = [], set()
    for number, post in enumerate(posts):
        # This adapter never retains an unreviewed body field. A schema change
        # must be reviewed instead of accidentally archiving paid full text.
        if set(post)-FIELDS:
            raise NewsError('cnyes_index_unreviewed_metadata_fields',retry=True,delay=3600)
        if (type(post.get('publishAt')) is not int or not low <= post['publishAt'] <= high
                or type(post.get('categoryId')) is not int or post['categoryId'] <= 0
                or type(post.get('payment')) is not int or post['payment'] not in {0,1}
                or not isinstance(post.get('title'),str)):
            raise _metadata_failure(response, source, post, posts, number, low, high)
        identifier = post['newsId']
        if identifier in seen:
            continue
        seen.add(identifier)
        if post['payment'] == 0:
            # This is the existing publisher permalink contract using a real
            # source ID, never an invented ID or a guessed historical value.
            links.append(('https://news.cnyes.com/news/id/'+str(identifier),
                          html.unescape(post['title']),
                          datetime.fromtimestamp(post['publishAt'],timezone.utc).isoformat()))
    info.update(source_day=day,public_ids=len(links),metadata_rows=len(posts),
                blank_title_metadata_rows=sum(not post['title'].strip() for post in posts),
                article_bodies_accepted=0,source_total_is_not_unique_story_count=True)
    return info, posts, links


def _total_reconciliation_key(source, scope):
    identity = (scope, source.cnyes_index_data_fingerprint, source.authorization_fingerprint)
    return 'cnyes_index_total_reconciliation_v1:' + hashlib.sha256(
        json.dumps(identity, separators=(',', ':')).encode()).hexdigest()


def _index_head(queue, source, scope, page):
    guard, parameters = compatible_head_guard(source)
    return queue.db.execute('SELECT o.* FROM cnyes_public_index_heads h '
        'JOIN cnyes_public_index_observations o '
        'USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
        'WHERE h.scope_url=? AND h.page=? AND ' + guard + ' AND h.authorization_fingerprint=?',
        (scope, page, *parameters, source.authorization_fingerprint)).fetchone()


def _schedule_index_total_recheck(queue, source, head, round_id):
    """Recheck a proved conflicting successful page without changing failures.

    A successful but still conflicting result may recur. Fence each attempt by
    its actual successful check clock and apply the source discovery cadence
    with a capped exponential delay, retained across process restarts.
    """
    try:
        from downloader.news_fulltext_bulk import cnyes_scope
        day = (int(parse_qs(urlsplit(head['scope_url']).query)['startAt'][0]) + 28800) // 86400
        url = request_url(source, day, head['page'])
        validate_request(url, source)
        if cnyes_scope(url) != head['scope_url']:
            return False
    except (KeyError, IndexError, TypeError, ValueError, NewsError):
        return False
    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()
    now = time.time()
    if (not job or job['kind'] != 'index' or job['provider'] != source.provider
            or job['error_code'] is not None or job['attempts'] != 0
            or job['superseded_by_windows_json'] is not None
            or type(job['due']) not in (int, float) or not math.isfinite(job['due']) or job['due'] <= now
            or job['response_sha256'] != head['response_sha256']
            or job['parser_fingerprint'] != head['index_fingerprint']):
        return False
    identity = (url, source.cnyes_index_data_fingerprint, source.authorization_fingerprint, round_id)
    key = 'cnyes_index_total_recheck_v1:' + hashlib.sha256(
        json.dumps(identity, separators=(',', ':')).encode()).hexdigest()
    existing = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()
    try:
        checked = datetime.fromisoformat(job['last_checked_at_utc'])
        if checked.utcoffset() is None:
            return False
        previous = json.loads(existing[0]) if existing else None
        if existing:
            if (type(previous) is not dict or type(previous['contract_version']) is not int
                    or previous['contract_version'] != TOTAL_RECONCILIATION_CONTRACT_VERSION
                    or previous['url'] != url or previous['scope_url'] != head['scope_url']
                    or previous['round_id'] != round_id
                    or previous['source_failures_or_deadlines_reset'] is not False
                    or previous['article_heads_requeued'] is not False or type(previous['attempts']) is not int
                    or previous['attempts'] < 1
                    or checked <= datetime.fromisoformat(previous['successful_check_at_utc'])):
                return False
            attempts = previous['attempts'] + 1
            delay = source.discovery_interval_seconds * 2 ** min(attempts - 2, 4)
        else:
            attempts, delay = 1, 0
        due = max(now, checked.timestamp() + delay)
        if due >= job['due']:
            return False
    except (KeyError, ValueError, TypeError, OverflowError):
        return False
    proof = {'contract_version': TOTAL_RECONCILIATION_CONTRACT_VERSION,
        'url': url, 'scope_url': head['scope_url'], 'round_id': round_id, 'attempts': attempts,
        'successful_check_at_utc': job['last_checked_at_utc'], 'scheduled_at_utc': clock(),
        'next_due_epoch': due, 'prior_job': dict(job),
        'source_failures_or_deadlines_reset': False, 'article_heads_requeued': False}
    encoded = json.dumps(proof, sort_keys=True, separators=(',', ':'))
    history_key = key + ':' + hashlib.sha256(encoded.encode()).hexdigest()
    queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)', (history_key, encoded))
    queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (key, encoded))
    queue.db.execute('UPDATE discovery SET due=?,etag=NULL,last_modified=NULL WHERE url=?', (due, url))
    queue.metrics.count(source.provider, 'public_index_total_rechecks_scheduled')
    return True


def _reconcile_index_totals(queue, source, scope, accepted_page, *, changed=False):
    """Continue an activated scope repair without scanning healthy histories."""
    if not source.allowed or source.cnyes_index_api != ENDPOINT:
        return
    # The revision round covers both total and membership changes. Running the
    # older total-only scheduler alongside it would create a second retry
    # budget and could prematurely override the round's backoff.
    from downloader.news_fulltext_cnyes_revision import _round
    if _round(queue, source, scope):
        return
    first = _index_head(queue, source, scope, 1)
    current = first if accepted_page == 1 else _index_head(queue, source, scope, accepted_page)
    if not first or not current:
        return
    inconsistent = (current['total'], current['pages']) != (first['total'], first['pages'])
    key = _total_reconciliation_key(source, scope)
    if changed or inconsistent:
        started = clock()
        identity = (scope, source.cnyes_index_data_fingerprint, source.authorization_fingerprint,
                    first['response_sha256'], current['response_sha256'], started)
        state = {'contract_version': TOTAL_RECONCILIATION_CONTRACT_VERSION, 'scope_url': scope,
                 'round_id': hashlib.sha256(json.dumps(identity, separators=(',', ':')).encode()).hexdigest(),
                 'started_at_utc': started}
        queue.db.execute('INSERT OR IGNORE INTO queue_settings VALUES (?,?)',
                         (key, json.dumps(state, sort_keys=True, separators=(',', ':'))))
    active = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()
    if not active:
        return
    try:
        state = json.loads(active[0])
        if (type(state) is not dict or type(state['contract_version']) is not int
                or state['contract_version'] != TOTAL_RECONCILIATION_CONTRACT_VERSION
                or state['scope_url'] != scope or not re.fullmatch(r'[a-f0-9]{64}', state['round_id'])
                or datetime.fromisoformat(state['started_at_utc']).utcoffset() is None):
            return
    except (KeyError, ValueError, TypeError):
        return
    if inconsistent and accepted_page != 1:
        _schedule_index_total_recheck(queue, source, first, state['round_id'])
        return
    guard, parameters = compatible_head_guard(source)
    stale = queue.db.execute('SELECT o.* FROM cnyes_public_index_heads h '
        'JOIN cnyes_public_index_observations o '
        'USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
        'WHERE h.scope_url=? AND h.page>1 AND ' + guard
        + ' AND h.authorization_fingerprint=? AND (o.total!=? OR o.pages!=? OR h.page>?) ORDER BY h.page',
        (scope, *parameters, source.authorization_fingerprint,
         first['total'], first['pages'], first['pages']))
    found = False
    for head in stale:
        found = True
        # The latest first page does not authorize a request beyond its last
        # page. Recheck that first page instead and preserve older heads; a
        # concurrent response may have observed a different source version.
        target = first if head['page'] > first['pages'] else head
        if _schedule_index_total_recheck(queue, source, target, state['round_id']):
            break
    if not found:
        queue.db.execute('DELETE FROM queue_settings WHERE key=? AND value=?', (key, active[0]))


def accept_index(queue, task, response, links, source):
    day, page, _, _ = validate_request(task['url'],source)
    if response.url != task['url']:
        raise NewsError('cnyes_index_request_response_scope_mismatch',retry=True,delay=60)
    fingerprint = source.cnyes_index_data_fingerprint
    from downloader.news_fulltext_cnyes_revision import accepted as accepted_revision, stale_dispatch
    if response.status == 304:
        # A conditional response cannot supply lost list evidence or add URLs.
        from downloader.news_fulltext_bulk import cnyes_scope
        if task.get('parser_fingerprint') not in compatible_fingerprints(source) or not queue.db.execute(
                'SELECT 1 FROM cnyes_public_index_observations WHERE response_sha256=? AND index_fingerprint=? '
                'AND scope_url=? AND page=? AND authorization_fingerprint=?',
                (task.get('response_sha256'),task['parser_fingerprint'],cnyes_scope(task['url']),page,source.authorization_fingerprint)).fetchone():
            raise NewsError('cnyes_index_not_modified_without_evidence',retry=True,delay=60)
        with queue.db:
            if stale_dispatch(queue, source, task):
                queue.metrics.count(source.provider, 'public_index_stale_revision_responses')
                return 0
            queue.db.execute('UPDATE discovery SET due=?,last_checked_at_utc=? WHERE url=?',
                             (time.time()+365*86400,clock(),task['url']))
            current = _index_head(queue, source, cnyes_scope(task['url']), page)
            if (current and current['response_sha256'] == task.get('response_sha256')
                    and current['index_fingerprint'] == task.get('parser_fingerprint')):
                accepted_revision(queue, source, task, current, current)
                _reconcile_index_totals(queue, source, current['scope_url'], page)
        return 0
    info, posts, actual_links = parse_index(response,source)
    if links != actual_links:
        raise NewsError('cnyes_index_worker_metadata_mismatch',retry=True,delay=60)
    sha = hashlib.sha256(response.body).hexdigest()
    versions = compatible_fingerprints(source)
    prior_page = _index_head(queue, source, info['scope'], page)
    guard, parameters = compatible_head_guard(source)
    previous = queue.db.execute('SELECT o.page FROM cnyes_public_index_heads h '
        'JOIN cnyes_public_index_observations o USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
        'WHERE h.scope_url=? AND o.total=? AND o.ids_sha256=? AND h.page!=? AND '
        + guard + ' AND h.authorization_fingerprint=? LIMIT 1',
        (info['scope'],info['total'],info['ids_sha256'],info['page'],*parameters,source.authorization_fingerprint)).fetchone()
    from downloader.news_fulltext_cnyes_revision import _round
    # An identical page from an older publisher version is not evidence that
    # the current page is stuck. During a changed-list round a page may move;
    # duplicates among the current heads must still be resolved before finish.
    if (posts and previous and not _round(queue, source, info['scope'])
            and not (prior_page and (prior_page['total'], prior_page['pages'], prior_page['ids_sha256'])
                     != (info['total'], info['pages'], info['ids_sha256']))):
        raise NewsError('cnyes_index_pagination_no_progress',retry=True,delay=3600)
    context_sha = hashlib.sha256(response.url.encode()).hexdigest()
    relative = 'list_metadata/cnyes/'+fingerprint+'/'+context_sha+'-'+sha+'.parquet'
    target = queue.root/relative
    receipt = queue.root/(relative+'.receipt.json')
    if not target.exists():
        atomic_write_parquet(target,table_from_records([{
            'request_url':response.url,'response_sha256':sha,'raw_response_body':response.body,
            'source_observed_at_utc':clock(),'index_fingerprint':fingerprint,
            'authorization_fingerprint':source.authorization_fingerprint,
            'metadata_only':True,'accepted_body_count':0,'publish':False,
        }]),compression='zstd',compression_level=9,durable=True)
    # Recover a crash between atomic Parquet and receipt commits from exact
    # original bytes, request context, authorization and metadata-only flags.
    # Never rewrite the original file or its observation clock.
    from pyarrow import parquet as pq
    retained = pq.read_table(target).to_pylist()
    if len(retained) != 1 or any(retained[0].get(key) != value for key,value in {
            'request_url':response.url,'response_sha256':sha,'raw_response_body':response.body,
            'index_fingerprint':fingerprint,'authorization_fingerprint':source.authorization_fingerprint,
            'metadata_only':True,'accepted_body_count':0,'publish':False}.items()):
        raise NewsError('cnyes_index_original_evidence_unverified',retry=True,delay=60)
    receipt_data = {'parquet_path':relative,'sha256':sha256_file(target),'response_sha256':sha,
        'index_fingerprint':fingerprint,'metadata_rows':len(posts),'accepted_body_count':0,
        'index_data_contract_version':INDEX_DATA_CONTRACT_VERSION,
        'blank_title_metadata_rows':info['blank_title_metadata_rows'],
        'metadata_only':True,'publish':False}
    if receipt.exists():
        original_receipt = json.loads(receipt.read_bytes())
        if any(original_receipt.get(key) != value for key,value in receipt_data.items()):
            raise NewsError('cnyes_index_original_receipt_unverified',retry=True,delay=60)
    else:
        atomic_write_json(receipt,receipt_data,durable=True)
    observed = clock()
    with queue.db:
        before = _index_head(queue, source, info['scope'], page)
        queue.add_articles([{'url':url,'title':title,'date':date} for url,title,date in actual_links])
        queue.db.execute('INSERT OR IGNORE INTO cnyes_public_index_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (info['scope'],info['page'],sha,fingerprint,source.authorization_fingerprint,info['total'],info['pages'],
             info['ids_sha256'],len(posts),len(actual_links),relative,observed))
        if stale_dispatch(queue, source, task):
            # Preserve exact source bytes/IDs, but an old in-flight request may
            # neither overwrite the new head nor postpone its pending retry.
            queue.metrics.count(source.provider, 'public_index_stale_revision_responses')
            return 0
        queue.db.execute('INSERT OR REPLACE INTO cnyes_public_index_heads VALUES (?,?,?,?,?)',
            (info['scope'],info['page'],sha,fingerprint,source.authorization_fingerprint))
        if info['page'] < info['pages']:
            url = request_url(source,info['source_day'],info['page']+1)
            queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider,priority) VALUES (?,'index',?,?)",
                             (url,source.provider,discovery_priority(info['source_day'],info['page']+1)))
        queue.db.execute('UPDATE discovery SET due=?,etag=?,last_modified=?,response_sha256=?,parser_fingerprint=?,'
                         'last_checked_at_utc=?,attempts=0,error_code=NULL WHERE url=?',
            (time.time()+365*86400,response.headers.get('etag'),response.headers.get('last-modified'),sha,
             fingerprint,observed,task['url']))
        current = _index_head(queue, source, info['scope'], page)
        accepted_revision(queue, source, task, before, current)
        first_changed = bool(page == 1 and before and (before['total'], before['pages']) !=
                             (info['total'], info['pages']))
        _reconcile_index_totals(queue, source, info['scope'], page, changed=first_changed)
        # A successful neighboring page cannot heal this page's failed data.
        # The exact same guard is used for dispatch and retirement. A scoped
        # failure retires only after its own page has been accepted; unknown or
        # endpoint-wide failures retain their existing recovery semantics.
        scope_guard, scope_parameters = request_failure_scope_guard(source, requested_url=task['url'])
        versions = compatible_fingerprints(source)
        queue.db.execute('DELETE FROM bulk_endpoint_cooldowns AS circuit '
                         'WHERE provider=? AND endpoint=? AND data_fingerprint IN ('
                         + ','.join('?' for _ in versions) + ')' + scope_guard,
                         (source.provider,ENDPOINT,*versions,*scope_parameters))
    queue.metrics.count(source.provider,'public_index_pages_verified')
    queue.metrics.count(source.provider,'public_index_metadata_rows_verified',len(posts))
    queue.metrics.count(source.provider,'public_index_public_article_ids',len(actual_links))
    if info['blank_title_metadata_rows']:
        queue.metrics.count(source.provider,'public_index_blank_title_metadata_rows',info['blank_title_metadata_rows'])
    return 0  # Not an article-body acceptance, even when new URLs were added.


def manifest_phase(db, source, rows, revision_scopes=frozenset()):
    """Only actual overflowing source dates, never the whole article history.

    Each denominator comes from its own accepted first page under the current
    metadata/authorization ABI. Missing first pages or changed source totals
    suppress totals instead of extrapolating observed days to unseen days.
    """
    from downloader.news_fulltext_bulk import cnyes_scope
    scopes = {cnyes_scope(request_url(source, day)) for day in gap_dates(db, source)}
    selected = [row for row in rows if row[0] in scopes]
    firsts = sum(row[8] is not None and row[9] is not None for row in selected)
    pages, metadata = sum(row[1] for row in selected), sum(row[6] for row in selected)
    changed = sum(row[2] != row[3] or row[4] != row[5] for row in selected)
    # A round without a current head is already accounted for by its missing
    # first page; keep the public revision count within observed dates.
    reconciling = len({row[0] for row in selected} & revision_scopes)
    known = bool(scopes) and firsts == len(scopes) and not changed and not (scopes & revision_scopes)
    total_pages = sum(row[9] for row in selected) if known else None
    total_rows = sum(row[8] for row in selected) if known else None
    known = known and total_pages >= pages and total_rows >= metadata
    if not known:
        total_pages = total_rows = None
    return {'contract_version': MANIFEST_PHASE_CONTRACT_VERSION,
        'scope': 'actual_category_overflow_source_dates',
        'scope_fingerprint': hashlib.sha256(json.dumps(sorted(scopes)).encode()).hexdigest(),
        'planned_source_dates': len(scopes), 'first_pages_observed': firsts,
        'first_pages_remaining': len(scopes) - firsts, 'totals_known': known,
        'source_reported_total_pages': total_pages, 'source_reported_total_metadata_rows': total_rows,
        'verified_pages': pages, 'observed_metadata_rows': metadata,
        'remaining_pages': total_pages - pages if known else None,
        'progress_ratio': pages / total_pages if known and total_pages else None,
        'dates_with_changed_source_total': changed,
        'dates_reconciling_page_membership': reconciling,
        'phase_complete': bool(known and pages == total_pages and metadata == total_rows),
        'metadata_only': True, 'body_completion_claimed': False,
        'all_history_complete': False}, scopes


def manifest_last_page_gate(db, source, phase, rows, scopes, *, now_epoch):
    """Observe the sole missing page's job without blocking any dispatch.

    Only when one page remains, use an indexed lookup on its candidate date.
    Distinct, in-range pages with exactly one missing member determine its
    page number; otherwise report unknown rather than guessing.
    """
    if not phase['totals_known'] or phase['remaining_pages'] != 1:
        return None
    result = {'contract_version': 1, 'state': 'unknown', 'next_probe_at_utc': None,
              'recovery_requires_success': True}
    if (not source.allowed or type(now_epoch) not in (int, float) or not math.isfinite(now_epoch)):
        return result
    selected = [row for row in rows if row[0] in scopes]
    incomplete = []
    for row in selected:
        count, total = row[1], row[9]
        if (type(count) is not int or type(total) is not int
                or not 1 <= count <= total <= MAXIMUM_PAGE):
            return result
        if count != total:
            incomplete.append(row)
    if len(incomplete) != 1 or incomplete[0][9] - incomplete[0][1] != 1:
        return result
    row = incomplete[0]
    guard, parameters = compatible_head_guard(source)
    page_counts = db.execute('SELECT count(*),min(h.page),max(h.page),sum(h.page) '
        'FROM cnyes_public_index_heads h CROSS JOIN cnyes_public_index_observations o '
        'WHERE h.scope_url=? AND ' + guard + ' AND h.authorization_fingerprint=? '
        'AND o.scope_url=h.scope_url AND o.page=h.page AND o.response_sha256=h.response_sha256 '
        'AND o.index_fingerprint=h.index_fingerprint AND o.authorization_fingerprint=h.authorization_fingerprint',
        (row[0], *parameters, source.authorization_fingerprint)).fetchone()
    if (not page_counts or page_counts[0] != row[1] or page_counts[1] != 1
            or type(page_counts[2]) is not int or page_counts[2] > row[9]
            or type(page_counts[3]) is not int):
        return result
    missing = row[9] * (row[9] + 1) // 2 - page_counts[3]
    if not 2 <= missing <= row[9]:
        return result
    try:
        from downloader.news_fulltext_bulk import cnyes_scope
        query = parse_qs(urlsplit(row[0]).query, keep_blank_values=True)
        low = int(query['startAt'][0])
        day = (low + 28800) // 86400
        url = request_url(source, day, missing)
        validate_request(url, source)
        if cnyes_scope(url) != row[0]:
            return result
    except (KeyError, IndexError, ValueError, TypeError, NewsError):
        return result
    if db.execute('SELECT 1 FROM cnyes_public_index_heads h WHERE h.scope_url=? AND h.page=? AND '
                  + guard + ' AND h.authorization_fingerprint=? LIMIT 1',
                  (row[0], missing, *parameters, source.authorization_fingerprint)).fetchone():
        return result
    job = db.execute('SELECT kind,provider,due,attempts,error_code,superseded_by_windows_json '
                     'FROM discovery WHERE url=?', (url,)).fetchone()
    if not job:
        return result
    kind, provider, due, attempts, error, superseded = job
    if (kind != 'index' or provider != source.provider or superseded is not None
            or type(due) not in (int, float) or not math.isfinite(due) or not 0 <= due <= 253402214400
            or type(attempts) is not int or attempts < 0
            or not ((attempts == 0 and error is None)
                    or (attempts > 0 and isinstance(error, str) and error))):
        return result
    versions = compatible_fingerprints(source)
    circuit = db.execute('SELECT not_before FROM bulk_endpoint_cooldowns WHERE provider=? AND endpoint=? '
        'AND request_url=? AND data_fingerprint IN (' + ','.join('?' for _ in versions) + ')',
        (source.provider, ENDPOINT, url, *versions)).fetchone()
    if circuit:
        deadline = circuit[0]
        if (type(deadline) not in (int, float) or not math.isfinite(deadline)
                or not 0 <= deadline <= 253402214400):
            return result
        due = max(due, deadline)
    if due > now_epoch:
        result.update(state='waiting', next_probe_at_utc=datetime.fromtimestamp(due, timezone.utc).isoformat())
    else:
        result['state'] = 'rechecking' if attempts or circuit else 'ready'
    return result


def manifest_partial_page_gate(db, source, phase, rows, scopes, *, now_epoch):
    """Report proved request-local failures in the unfinished manifest only.

    This is an ETA observation, never a dispatch gate.  Request-specific
    witnesses survive when another page replaces the endpoint circuit.  Bound
    the indexed scan and retain unknown when a relevant witness is ambiguous;
    absent witnesses are not proof that every remaining page can succeed.
    """
    if not phase['totals_known'] or phase['remaining_pages'] <= 1:
        return None
    result = {'contract_version': 1, 'state': 'unknown', 'failed_pages': None,
              'next_probe_at_utc': None, 'recovery_requires_success': True,
              'scope': 'observed_request_local_failures_only'}
    if (not source.allowed or type(now_epoch) not in (int, float) or not math.isfinite(now_epoch)):
        return result
    prefix = request_failure_scope_key(source) + ':'
    versions = compatible_fingerprints(source)
    entries = db.execute("SELECT key,CASE WHEN typeof(value)='text' AND length(value)<=8192 THEN value END "
                         'FROM queue_settings WHERE key>=? AND key<? LIMIT 129',
                         (prefix, prefix + '\uffff')).fetchall()
    if len(entries) > 128:
        return result
    # Older queues can carry only the endpoint's original scope witness.  Do
    # not infer that its page recovered merely because the later per-page key
    # is absent; reuse the exact current endpoint witness if it still matches.
    current = db.execute('SELECT request_url FROM bulk_endpoint_cooldowns WHERE provider=? AND endpoint=? '
                         'AND error_code=? AND data_fingerprint IN (' + ','.join('?' for _ in versions) + ')',
                         (source.provider, ENDPOINT, 'cnyes_index_unverified_metadata', *versions)).fetchone()
    if current is not None:
        url = current[0]
        if not isinstance(url, str):
            return result
        key = request_failure_scope_key(source, url)
        if key not in {entry[0] for entry in entries}:
            original = db.execute("SELECT CASE WHEN typeof(value)='text' AND length(value)<=8192 THEN value END "
                                  'FROM queue_settings WHERE key=?',
                                  (request_failure_scope_key(source),)).fetchone()
            if original is None:
                return result
            entries.append((key, original[0]))
    selected = {row[0]: row for row in rows if row[0] in scopes}
    guard, parameters = compatible_head_guard(source)
    failed, waiting = 0, []
    from downloader.news_fulltext_bulk import cnyes_scope
    for key, raw in entries:
        try:
            if not isinstance(key, str) or len(key) != len(prefix) + 64 or not isinstance(raw, str):
                return result
            value = json.loads(raw)
            if not isinstance(value, dict):
                return result
            url = value['request_url']
            if not isinstance(url, str) or request_failure_scope_key(source, url) != key:
                return result
            _, page, _, _ = validate_request(url, source)
            scope = cnyes_scope(url)
            if scope not in selected or page > selected[scope][9]:
                continue
            # A previously failed page that is now a verified head no longer
            # prevents this phase from finishing; keep its evidence untouched.
            if db.execute('SELECT 1 FROM cnyes_public_index_heads h WHERE h.scope_url=? AND h.page=? AND '
                          + guard + ' AND h.authorization_fingerprint=? LIMIT 1',
                          (scope, page, *parameters, source.authorization_fingerprint)).fetchone():
                continue
            deadline = value['not_before']
            if (type(value.get('contract_version')) is not int or value['contract_version'] != 1
                    or value.get('scope') != 'request' or value.get('permission_granted') is not False
                    or value.get('preserves_source_validation') is not True
                    or value.get('provider') != source.provider or value.get('endpoint') != ENDPOINT
                    or value.get('data_fingerprint') not in versions
                    or value.get('error_code') != 'cnyes_index_unverified_metadata'
                    or type(deadline) not in (int, float) or not math.isfinite(deadline)
                    or not 0 <= deadline <= 253402214400):
                return result
            job = db.execute('SELECT kind,provider,due,attempts,error_code,last_checked_at_utc,'
                             'superseded_by_windows_json FROM discovery WHERE url=?', (url,)).fetchone()
            if not job:
                return result
            kind, provider, due, attempts, error, checked_raw, superseded = job
            checked, recorded = datetime.fromisoformat(checked_raw), datetime.fromisoformat(value['recorded_at_utc'])
            if (kind != 'index' or provider != source.provider or superseded is not None
                    or type(due) not in (int, float) or not math.isfinite(due)
                    or not 0 <= due <= 253402214400 or type(attempts) is not int or attempts < 1
                    or error != value['error_code'] or not 0 <= deadline - due <= 1
                    or checked.utcoffset() is None or recorded.utcoffset() is None
                    or checked.timestamp() > due or checked > recorded or recorded.timestamp() > deadline):
                return result
            failed += 1
            if deadline > now_epoch:
                waiting.append(deadline)
        except (KeyError, NewsError, TypeError, ValueError, OverflowError, OSError):
            return result
    if failed > phase['remaining_pages']:
        return result
    result['failed_pages'] = failed
    if waiting:
        result.update(state='waiting', next_probe_at_utc=datetime.fromtimestamp(max(waiting), timezone.utc).isoformat())
    else:
        result['state'] = 'rechecking' if failed else 'none_observed'
    return result


def manifest_forecast(db, source, phase, scopes, *, owner_start_epoch, now_epoch, activity):
    """Recent durable page heads / whole sixty-second wall bins, including zero.

    This is a conditional metadata-phase scenario, not a statistical confidence
    interval, publisher quota, unique-story count, or final full-text deadline.
    Current-owner wall time prevents borrowing a previous owner's faster work.
    No queue writes, request, retained raw read, or page-view sampling occurs.
    """
    result = {'contract_version': MANIFEST_PHASE_CONTRACT_VERSION, 'state': 'warming',
        'sample_seconds': None, 'sample_count': None, 'verified_pages': None,
        'pages_per_second': None, 'fastest_seconds': None, 'middle_seconds': None,
        'slowest_seconds': None, 'middle_finish_at_utc': None,
        'observed_at_utc': datetime.fromtimestamp(now_epoch, timezone.utc).isoformat(),
        'scenario_not_confidence_interval': True, 'metadata_only': True,
        'all_history_complete': False}
    if not phase['totals_known']:
        return {**result, 'state': 'unknown_denominator'}
    if phase['phase_complete']:
        return {**result, 'state': 'phase_complete'}
    if phase['remaining_pages'] == 0:
        return {**result, 'state': 'unavailable'}
    if not source.allowed or activity not in {'running', 'idle', 'waiting', 'inventorying'}:
        return {**result, 'state': 'inactive'}
    gate = phase.get('last_page_gate')
    if gate is not None and gate['state'] != 'ready':
        return {**result, 'state': {'waiting': 'last_page_waiting',
            'rechecking': 'last_page_rechecking', 'unknown': 'last_page_scope_unknown'}[gate['state']]}
    partial = phase.get('partial_page_gate')
    if partial is not None and partial['state'] != 'none_observed':
        return {**result, 'state': {'waiting': 'partial_page_waiting',
            'rechecking': 'partial_page_rechecking', 'unknown': 'partial_page_scope_unknown'}[partial['state']]}
    if (type(owner_start_epoch) not in {int, float} or not math.isfinite(owner_start_epoch)
            or not math.isfinite(now_epoch) or owner_start_epoch > now_epoch):
        return result
    bin_count = min(5, int((now_epoch - owner_start_epoch) // 60))
    if bin_count < 3:
        return result
    start = now_epoch - bin_count * 60
    bins = [0] * bin_count
    low = datetime.fromtimestamp(start, timezone.utc).isoformat()
    high = datetime.fromtimestamp(now_epoch, timezone.utc).isoformat()
    head_guard, head_parameters = compatible_head_guard(source)
    for scope, observed in db.execute('SELECT o.scope_url,o.source_observed_at_utc '
            'FROM cnyes_public_index_heads h JOIN cnyes_public_index_observations o '
            'USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
            'WHERE ' + head_guard + ' AND h.authorization_fingerprint=? '
            'AND o.source_observed_at_utc>=? AND o.source_observed_at_utc<=?',
            (*head_parameters, source.authorization_fingerprint, low, high)):
        if scope not in scopes:
            continue
        try:
            stamp = datetime.fromisoformat(observed)
            if stamp.tzinfo is None:
                return {**result, 'state': 'unavailable'}
            seconds = stamp.timestamp() - start
            if not 0 <= seconds <= bin_count * 60:
                return {**result, 'state': 'unavailable'}
        except (ValueError, TypeError, OverflowError):
            return {**result, 'state': 'unavailable'}
        bins[min(bin_count - 1, int(seconds // 60))] += 1
    total = sum(bins)
    result.update(sample_seconds=bin_count * 60, sample_count=bin_count, verified_pages=total,
        window_page_counts=bins)
    if bins[-1] == 0:
        return {**result, 'state': 'no_recent_progress'}
    if total < 10:
        return result
    rate = total / result['sample_seconds']
    remaining = phase['remaining_pages']
    middle = remaining / rate
    result.update(state='recent_measured', pages_per_second=rate,
        fastest_seconds=remaining * 60 / max(bins), middle_seconds=middle,
        slowest_seconds=remaining * 60 / min(bins) if min(bins) else None,
        middle_finish_at_utc=datetime.fromtimestamp(now_epoch + middle, timezone.utc).isoformat())
    return result


class PublicIndexRowsCache:
    """Reuse the index aggregation only while the SQLite view is unchanged.

    This is an owner-local read cache, not an acquisition checkpoint. An open
    transaction can contain changes later rolled back without decreasing
    ``total_changes``; never cache or reuse results inside one. The data and
    schema versions also fence commits by other connections and DDL. Live gap
    membership, forecasts and clocks are still recomputed on every status.
    """

    def __init__(self):
        self._db = self._key = self._rows = None

    def _clear(self):
        self._db = self._key = self._rows = None

    @staticmethod
    def _revision(db):
        return (db.total_changes,
                db.execute('PRAGMA main.data_version').fetchone()[0],
                db.execute('PRAGMA main.schema_version').fetchone()[0],
                db.execute('PRAGMA temp.schema_version').fetchone()[0],
                db.row_factory)

    def fetch(self, db, sql, parameters):
        # Connection wrappers used by diagnostics may expose only execute();
        # attached databases also have independent data_version counters.  In
        # either case execute the original query instead of guessing freshness.
        if (type(db) is not sqlite3.Connection or db.in_transaction or db.text_factory is not str
                or not (db.row_factory is None or db.row_factory is sqlite3.Row)):
            self._clear()
            return db.execute(sql, parameters).fetchall()
        if any(row[1] not in ('main', 'temp') for row in db.execute('PRAGMA database_list')):
            self._clear()
            return db.execute(sql, parameters).fetchall()
        before = self._revision(db)
        key = (before, sql, tuple(parameters))
        if db is self._db and key == self._key:
            return self._rows
        rows = db.execute(sql, parameters).fetchall()
        if not db.in_transaction and before == self._revision(db):
            self._db, self._key, self._rows = db, key, rows
        else:
            self._clear()
        return rows


def progress(db, source, *, owner_start_epoch=None, now_epoch=None, activity='inactive', rows_cache=None):
    if not source or not getattr(source,'cnyes_index_api',''):
        return {'enabled':False,'article_bodies_accepted_by_index':0}
    head_guard, head_parameters = compatible_head_guard(source)
    sql = ('SELECT o.scope_url,count(*),min(o.total),max(o.total),min(o.pages),max(o.pages),'
        'sum(o.metadata_rows),sum(o.public_ids),max(CASE WHEN o.page=1 THEN o.total END),'
        'max(CASE WHEN o.page=1 THEN o.pages END) FROM cnyes_public_index_heads h JOIN cnyes_public_index_observations o '
        'USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
        'WHERE ' + head_guard + ' AND h.authorization_fingerprint=? GROUP BY o.scope_url')
    parameters = (*head_parameters, source.authorization_fingerprint)
    rows = (db.execute(sql, parameters).fetchall() if rows_cache is None
            else rows_cache.fetch(db, sql, parameters))
    from downloader.news_fulltext_cnyes_revision import active_scopes
    revisions = active_scopes(db, source)
    phase, scopes = manifest_phase(db, source, rows, revisions)
    effective_now = time.time() if now_epoch is None else now_epoch
    last_page_gate = manifest_last_page_gate(db, source, phase, rows, scopes, now_epoch=effective_now)
    if last_page_gate is not None:
        phase['last_page_gate'] = last_page_gate
    partial_page_gate = manifest_partial_page_gate(db, source, phase, rows, scopes, now_epoch=effective_now)
    if partial_page_gate is not None:
        phase['partial_page_gate'] = partial_page_gate
    phase['forecast'] = manifest_forecast(db, source, phase, scopes,
        owner_start_epoch=owner_start_epoch, now_epoch=effective_now,
        activity=activity)
    return {'contract_version':CONTRACT_VERSION,'enabled':True,'manifest_phase':phase,
        'index_data_contract_version':INDEX_DATA_CONTRACT_VERSION,
        'manifest_schedule_contract_version':MANIFEST_SCHEDULE_CONTRACT_VERSION,'source_dates_observed':len(rows),
        'verified_pages':sum(row[1] for row in rows),'metadata_rows':sum(row[6] for row in rows),
        'public_ID_row_observations':sum(row[7] for row in rows),
        'dates_all_reported_pages_observed':sum(row[0] not in revisions and row[2]==row[3] and row[4]==row[5] and row[1]==row[5] for row in rows),
        'dates_with_changed_source_total':sum(row[2]!=row[3] or row[4]!=row[5] for row in rows),
        'dates_reconciling_page_membership':sum(row[0] in revisions for row in rows),
        'article_bodies_accepted_by_index':0,'all_history_complete':False,'finish_eta_utc':None,
        'source_total_is_not_unique_story_count':True,'category_gaps_preserved':True}
