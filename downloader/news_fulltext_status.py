"""Bounded heartbeat views, not deletion of acquisition diagnostic evidence.

Public readers never query SQLite. The canonical owner publishes a bounded
sample and exact grouped counts; full gap rows and calibration windows retain
their existing authority and recovery contracts.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import math
import sqlite3


STATUS_PROJECTION_CONTRACT_VERSION = 3
STATUS_QUERY_TIMING_CONTRACT_VERSION = 1
BULK_ERROR_COUNT_CONTRACT_VERSION = 1
DISCOVERY_SUMMARY_CONTRACT_VERSION = 1
ARTICLE_BODY_STATE_COUNTER_CONTRACT_VERSION = 1
BULK_COVERAGE_SUMMARY_CONTRACT_VERSION = 1
CAPACITY_STATUS_HISTORY_LIMIT = 64
BULK_GAP_STATUS_SAMPLE_LIMIT = 64
STATUS_VALUE_COPY_CONTRACT_VERSION = 1

_JSON_SCALARS = frozenset((type(None), bool, int, float, str))
_NOT_COPIED = object()


def cnyes_local_replay_retries(db, source, focus):
    """Owner snapshot of persisted local-I/O retry records, not article progress.

    These three tables have different grains. A record can remain after its
    head or authorization changes, so its deadline is only a local lower bound,
    not proof that the owner will dispatch or that the publisher recovered.
    Missing/old tables and an absent diagnostic scope remain unknown.
    """
    names = ('cached_bulk_local_retries', 'retained_metadata_local_retries',
             'retained_failure_local_retries')
    entries = {'api_pages': None, 'metadata_batches': None, 'diagnostic_originals': None}
    if source is None:
        return {'contract_version': 1, 'current_source_contract_only': True,
                'retry_records_are_article_counts': False, 'entries': entries}
    try:
        present = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' "
                                               "AND name IN (?,?,?)", names)}
    except sqlite3.Error:
        present = set()
    from downloader.news_fulltext_policy import cnyes_bulk_fingerprint

    requests = (
        ('api_pages', names[0], 'source_fingerprint=?', (cnyes_bulk_fingerprint(source),)),
        ('metadata_batches', names[1], 'source_fingerprint=?', (source.data_fingerprint,)),
        ('diagnostic_originals', names[2], 'cohort_id=? AND parser_fingerprint=?',
         (getattr(focus, 'id', None), source.parser_repair_fingerprint)),
    )
    for name, table, predicate, parameters in requests:
        if table not in present or any(value is None for value in parameters):
            continue
        try:
            count, due = db.execute(f'SELECT count(*),min(not_before_epoch) FROM {table} WHERE {predicate}',
                                    parameters).fetchone()
            if type(count) is not int or count < 0 or (count == 0) != (due is None):
                continue
            if due is not None:
                if type(due) not in (int, float) or not math.isfinite(due) or due < 0:
                    continue
                due = datetime.fromtimestamp(due, timezone.utc).isoformat()
            entries[name] = {'records': count, 'earliest_not_before_utc': due}
        except (sqlite3.Error, ValueError, OverflowError, OSError):
            # Corrupt or incompatible optional telemetry cannot certify zero
            # and must not prevent the owner from reporting other work.
            continue
    return {'contract_version': 1, 'current_source_contract_only': True,
            'retry_records_are_article_counts': False, 'entries': entries}


def cnyes_metadata_resource_waits(db, source):
    """Snapshot local batch-size observations, not pending article counts."""
    if source is None:
        return None
    from downloader.news_fulltext_metadata_replay import METADATA_REPLAY_MAX_BYTES

    try:
        records, above_bound, maximum = db.execute('''SELECT count(*),
            coalesce(sum(required_bytes>?),0),max(required_bytes)
            FROM retained_metadata_resource_waits WHERE source_fingerprint=?''',
            (METADATA_REPLAY_MAX_BYTES, source.data_fingerprint)).fetchone()
        if (type(records) is not int or type(above_bound) is not int
                or not 0 <= above_bound <= records <= 2**53 - 1
                or (records == 0 and maximum is not None)
                or (records > 0 and (type(maximum) is not int or not 1 <= maximum <= 2**53 - 1))):
            return None
        return {'contract_version': 1, 'current_source_contract_only': True,
                'records_are_article_counts': False, 'records': records,
                'above_current_bound_records': above_bound,
                'current_bound_bytes': METADATA_REPLAY_MAX_BYTES,
                'max_observed_required_bytes': maximum}
    except sqlite3.Error:
        # Old snapshots, absent tables, or corrupt optional telemetry are
        # unknown. Never break the owner or publish them as a proven zero.
        return None


def cnyes_metadata_large_replay(db, source, job):
    """Report only the current source's local proof assignment and retry records."""
    if source is None:
        return None
    try:
        records, due, unknown, unchanged = db.execute('''SELECT count(*),min(not_before_epoch),
                coalesce(sum(CASE WHEN last_error='worker_progress_unknown_timeout' THEN 1 ELSE 0 END),0),
                coalesce(sum(CASE WHEN last_error='worker_progress_unchanged_timeout' THEN 1 ELSE 0 END),0)
            FROM retained_metadata_large_retries WHERE source_fingerprint=?''',
            (source.data_fingerprint,)).fetchone()
        if (type(records) is not int or not 0 <= records <= 2**53 - 1
                or (records == 0) != (due is None)
                or any(type(value) is not int or not 0 <= value <= records for value in (unknown, unchanged))
                or unknown + unchanged > records):
            return None
        if due is not None:
            if type(due) not in (int, float) or not math.isfinite(due) or due < 0:
                return None
            due = datetime.fromtimestamp(due, timezone.utc).isoformat()
        assigned = isinstance(job, dict) and job.get('fingerprint') == source.data_fingerprint
        return {'contract_version': 1, 'current_source_contract_only': True,
                'retry_records_are_article_completion': False, 'assigned': assigned,
                'retry_records': records, 'earliest_not_before_utc': due,
                'retry_causes_contract_version': 1,
                'retry_cause_counts': {'progress_unknown_timeout': unknown,
                                       'counters_unchanged_timeout': unchanged,
                                       'other': records - unknown - unchanged}}
    except (sqlite3.Error, OverflowError, ValueError, OSError):
        return None


def cnyes_metadata_index_progress(db, source):
    """Snapshot one source-version baseline scan, never article completeness."""
    if source is None:
        return None
    try:
        row = db.execute('''SELECT state,rows_seen,registered_heads,dirty_processed,cursor_resets,updated_at_utc
            FROM retained_metadata_incremental_scans WHERE source_fingerprint=?''',
            (source.data_fingerprint,)).fetchone()
        if not row or row[0] not in ('scanning', 'draining', 'complete'):
            return None
        if any(type(value) is not int or not 0 <= value <= 2**53 - 1 for value in row[1:5]):
            return None
        if not isinstance(row[5], str) or not row[5]:
            return None
        invalid = db.execute('''SELECT count(*) FROM retained_metadata_incremental_invalid
            WHERE source_fingerprint=?''', (source.data_fingerprint,)).fetchone()[0]
        if type(invalid) is not int or not 0 <= invalid <= 2**53 - 1:
            return None
        return {'contract_version': 1, 'current_source_contract_only': True,
                'counts_are_article_completion': False, 'state': row[0], 'rows_seen': row[1],
                'registered_heads': row[2], 'dirty_processed': row[3], 'cursor_resets': row[4],
                'invalid_heads_observed': invalid, 'updated_at_utc': row[5]}
    except sqlite3.Error:
        return None


def _bulk_endpoint(column):
    return (f"CASE WHEN instr({column},'?')=0 THEN {column} "
            f"ELSE substr({column},1,instr({column},'?')-1) END")


BULK_COVERAGE_SCAN_SQL = f"""
    WITH coverage AS (
        SELECT c.*, {_bulk_endpoint('c.base_url')} AS endpoint,
               coalesce(json_array_length(s.child_urls_json),0)>0 AS superseded
        FROM bulk_api_coverage c LEFT JOIN bulk_window_splits s ON s.scope_url=c.base_url
    )
    SELECT provider,endpoint AS base_url,count(*) AS coverage_scopes,
           sum(superseded) AS superseded_scopes,sum(NOT superseded) AS leaf_scopes,
           sum(CASE WHEN NOT superseded THEN total_articles ELSE 0 END) AS total_articles,
           sum(CASE WHEN NOT superseded THEN total_pages ELSE 0 END) AS total_pages,
           max(observed_at_utc) AS observed_at_utc
    FROM coverage GROUP BY provider,endpoint ORDER BY provider,endpoint
"""
BULK_COVERAGE_SUMMARY_SQL = """SELECT provider,base_url,coverage_scopes,superseded_scopes,
    leaf_scopes,total_articles,total_pages,observed_at_utc
    FROM bulk_endpoint_metrics ORDER BY provider,base_url"""


def scan_bulk_coverage(db):
    """Exact pre-migration projection, also the independent comparison oracle."""
    return db.execute(BULK_COVERAGE_SCAN_SQL).fetchall()


def bulk_coverage_rows(db):
    """Read this transaction's endpoint totals without revisiting every scope."""
    import sqlite3
    try:
        return db.execute(BULK_COVERAGE_SUMMARY_SQL).fetchall()
    except sqlite3.OperationalError as error:
        if str(error) != 'no such table: bulk_endpoint_metrics':
            raise
        # Old read-only snapshots stay usable; readers never migrate a writer.
        return scan_bulk_coverage(db)


def initialize_bulk_coverage_summary(db):
    """Source writes and endpoint totals commit or roll back together.

    Coverage uses canonical UPSERT, never delete/reinsert REPLACE. Splits alter
    only a coverage row's leaf contribution; their own provider/clock are not
    part of the original projection. A narrow source index maintains the exact
    maximum clock when the latest observation moves or disappears. All source
    rows remain authoritative and unchanged by this crash-atomic migration.
    """
    table = '''CREATE TABLE IF NOT EXISTS bulk_endpoint_metrics(
        provider TEXT NOT NULL,base_url TEXT NOT NULL,
        coverage_scopes INTEGER NOT NULL CHECK(coverage_scopes>=0),
        superseded_scopes INTEGER NOT NULL CHECK(superseded_scopes>=0),
        leaf_scopes INTEGER NOT NULL CHECK(leaf_scopes>=0),
        total_articles INTEGER NOT NULL,total_pages INTEGER NOT NULL,
        observed_at_utc TEXT NOT NULL,PRIMARY KEY(provider,base_url),
        CHECK(coverage_scopes=superseded_scopes+leaf_scopes)
    ) WITHOUT ROWID'''

    def flag(ref):
        return ('coalesce((SELECT json_array_length(child_urls_json) FROM bulk_window_splits '
                f'WHERE scope_url={ref}.base_url),0)>0')

    def remove(ref):
        endpoint = _bulk_endpoint(ref+'.base_url')
        return f'''UPDATE bulk_endpoint_metrics SET coverage_scopes=coverage_scopes-1,
            superseded_scopes=superseded_scopes-({flag(ref)}),
            leaf_scopes=leaf_scopes-(NOT ({flag(ref)})),
            total_articles=total_articles-CASE WHEN NOT ({flag(ref)}) THEN {ref}.total_articles ELSE 0 END,
            total_pages=total_pages-CASE WHEN NOT ({flag(ref)}) THEN {ref}.total_pages ELSE 0 END,
            observed_at_utc=CASE WHEN observed_at_utc={ref}.observed_at_utc THEN coalesce(
                (SELECT max(observed_at_utc) FROM bulk_api_coverage
                 WHERE provider={ref}.provider AND {_bulk_endpoint('base_url')}=({endpoint})), '')
                ELSE observed_at_utc END
            WHERE provider={ref}.provider AND base_url=({endpoint});'''

    def add(ref):
        return f'''INSERT INTO bulk_endpoint_metrics
            SELECT {ref}.provider,{_bulk_endpoint(ref+'.base_url')},1,superseded,NOT superseded,
                CASE WHEN NOT superseded THEN {ref}.total_articles ELSE 0 END,
                CASE WHEN NOT superseded THEN {ref}.total_pages ELSE 0 END,{ref}.observed_at_utc
            FROM (SELECT ({flag(ref)}) AS superseded) WHERE 1
            ON CONFLICT(provider,base_url) DO UPDATE SET
                coverage_scopes=coverage_scopes+excluded.coverage_scopes,
                superseded_scopes=superseded_scopes+excluded.superseded_scopes,
                leaf_scopes=leaf_scopes+excluded.leaf_scopes,
                total_articles=total_articles+excluded.total_articles,total_pages=total_pages+excluded.total_pages,
                observed_at_utc=max(observed_at_utc,excluded.observed_at_utc);'''

    def cleanup(ref):
        return (f'DELETE FROM bulk_endpoint_metrics WHERE provider={ref}.provider '
                f'AND base_url=({_bulk_endpoint(ref+".base_url")}) AND coverage_scopes=0;')

    def split_delta(ref, previous, current, condition='1'):
        delta = f'(({current})-({previous}))'
        # Coverage's provider owns the contribution, even if a retained split
        # has an unrelated provider field. Match the original join exactly.
        return f'''UPDATE bulk_endpoint_metrics SET
            superseded_scopes=superseded_scopes+{delta},leaf_scopes=leaf_scopes-{delta},
            total_articles=total_articles-{delta}*(SELECT total_articles FROM bulk_api_coverage WHERE base_url={ref}.scope_url),
            total_pages=total_pages-{delta}*(SELECT total_pages FROM bulk_api_coverage WHERE base_url={ref}.scope_url)
            WHERE ({condition}) AND {delta}!=0
                AND provider=(SELECT provider FROM bulk_api_coverage WHERE base_url={ref}.scope_url)
                AND base_url=({_bulk_endpoint(ref+'.scope_url')});'''

    old_flag = 'coalesce(json_array_length(OLD.child_urls_json),0)>0'
    new_flag = 'coalesce(json_array_length(NEW.child_urls_json),0)>0'
    triggers = {
        'bulk_coverage_summary_insert': f'AFTER INSERT ON bulk_api_coverage BEGIN {add("NEW")} END',
        'bulk_coverage_summary_delete': f'AFTER DELETE ON bulk_api_coverage BEGIN {remove("OLD")} {cleanup("OLD")} END',
        'bulk_coverage_summary_update': f'''AFTER UPDATE OF base_url,provider,total_articles,total_pages,observed_at_utc
            ON bulk_api_coverage WHEN OLD.base_url IS NOT NEW.base_url OR OLD.provider IS NOT NEW.provider
                OR OLD.total_articles IS NOT NEW.total_articles OR OLD.total_pages IS NOT NEW.total_pages
                OR OLD.observed_at_utc IS NOT NEW.observed_at_utc
            BEGIN {remove('OLD')} {add('NEW')} {cleanup('OLD')} END''',
        'bulk_split_summary_insert': f'AFTER INSERT ON bulk_window_splits BEGIN {split_delta("NEW","0",new_flag)} END',
        'bulk_split_summary_delete': f'AFTER DELETE ON bulk_window_splits BEGIN {split_delta("OLD",old_flag,"0")} END',
        'bulk_split_summary_update': f'''AFTER UPDATE OF scope_url,child_urls_json ON bulk_window_splits
            WHEN OLD.scope_url IS NOT NEW.scope_url OR OLD.child_urls_json IS NOT NEW.child_urls_json BEGIN
            {split_delta('NEW',old_flag,new_flag,'OLD.scope_url=NEW.scope_url')}
            {split_delta('OLD',old_flag,'0','OLD.scope_url!=NEW.scope_url')}
            {split_delta('NEW','0',new_flag,'OLD.scope_url!=NEW.scope_url')} END''',
    }
    names = {'bulk_endpoint_metrics','bulk_coverage_endpoint_observed',*triggers}
    present = {row[0] for row in db.execute('SELECT name FROM sqlite_master WHERE name IN ('
        + ','.join('?' for _ in names)+')',tuple(names))}
    marker = db.execute("SELECT value FROM queue_settings WHERE key='bulk_coverage_summary_version'").fetchone()
    if present==names and marker and marker[0]==str(BULK_COVERAGE_SUMMARY_CONTRACT_VERSION):
        return
    with db:
        # execute(), not executescript(): DDL, seed, index and marker must be
        # in one transaction even for a fresh autocommit connection.
        db.execute('SAVEPOINT news_bulk_coverage_migration')
        db.execute(table)
        db.execute('DROP INDEX IF EXISTS bulk_coverage_endpoint_observed')
        db.execute('CREATE INDEX bulk_coverage_endpoint_observed ON bulk_api_coverage('
            f'provider,({_bulk_endpoint("base_url")}),observed_at_utc DESC)')
        for name,definition in triggers.items():
            db.execute('DROP TRIGGER IF EXISTS '+name)
            db.execute('CREATE TRIGGER '+name+' '+definition)
        db.execute('DELETE FROM bulk_endpoint_metrics')
        db.execute('INSERT INTO bulk_endpoint_metrics '+BULK_COVERAGE_SCAN_SQL)
        db.execute("INSERT INTO queue_settings VALUES ('bulk_coverage_summary_version',?) "
            'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            (str(BULK_COVERAGE_SUMMARY_CONTRACT_VERSION),))
        db.execute('RELEASE news_bulk_coverage_migration')


def copy_status_value(value, memo=None):
    """Detach JSON containers without dispatching deepcopy for every scalar.

    Preserve values, aliasing and cycles. Non-JSON values and subclasses use
    ordinary deepcopy with the same memo; this never serializes/coerces a
    number, cuts history or returns a controller-owned mutable container.
    """
    kind = type(value)
    if kind in _JSON_SCALARS:
        return value
    if kind not in (dict, list):
        return deepcopy(value, memo)
    if memo is None:
        memo = {}
    prior = memo.get(id(value), _NOT_COPIED)
    if prior is not _NOT_COPIED:
        return prior
    result = {} if kind is dict else []
    memo[id(value)] = result
    if kind is dict:
        for key, item in value.items():
            result[key if type(key) is str else deepcopy(key, memo)] = (
                item if type(item) in _JSON_SCALARS else copy_status_value(item, memo))
    else:
        result.extend(item if type(item) in _JSON_SCALARS else copy_status_value(item, memo) for item in value)
    return result


def initialize_article_body_states(db):
    """Extend the existing state counters, not a second frontier or hot scan.

    A URL's latest check state and retained body are independent dimensions.
    Seed their joint counts once under the canonical owner's lock. The column,
    triggers, original state/byte totals and marker commit (or roll back)
    together; later source mutations update both dimensions transactionally.
    """
    columns = {row[1] for row in db.execute('PRAGMA table_info(state_metrics)')}
    marker = db.execute("SELECT value FROM queue_settings WHERE key='article_body_state_counter_version'").fetchone()
    triggers = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' "
        "AND name IN ('metrics_insert','metrics_update','metrics_delete')")}
    if ('stored_body_urls' in columns and marker and marker[0] == str(ARTICLE_BODY_STATE_COUNTER_CONTRACT_VERSION)
            and triggers == {'metrics_insert', 'metrics_update', 'metrics_delete'}):
        return
    with db:
        # sqlite3 does not implicitly BEGIN for DDL. A savepoint makes even a
        # fresh connection's ALTER/trigger replacement/seed crash-atomic.
        db.execute('SAVEPOINT news_body_state_migration')
        if 'stored_body_urls' not in columns:
            db.execute('ALTER TABLE state_metrics ADD COLUMN stored_body_urls INTEGER NOT NULL DEFAULT 0 '
                       'CHECK(stored_body_urls>=0 AND stored_body_urls<=articles)')
        for name in ('metrics_insert', 'metrics_update', 'metrics_delete'):
            db.execute(f'DROP TRIGGER IF EXISTS {name}')
        db.execute('''CREATE TRIGGER metrics_insert AFTER INSERT ON articles BEGIN
            INSERT INTO state_metrics(provider,state,articles,body_bytes,stored_body_urls)
                VALUES (NEW.provider,NEW.state,1,coalesce(NEW.body_bytes,0),NEW.body_sha256 IS NOT NULL)
            ON CONFLICT(provider,state) DO UPDATE SET articles=articles+1,
                body_bytes=body_bytes+excluded.body_bytes,stored_body_urls=stored_body_urls+excluded.stored_body_urls;
            END''')
        db.execute('''CREATE TRIGGER metrics_update AFTER UPDATE OF provider,state,body_bytes,body_sha256 ON articles
            WHEN OLD.provider IS NOT NEW.provider OR OLD.state IS NOT NEW.state
                OR OLD.body_bytes IS NOT NEW.body_bytes OR (OLD.body_sha256 IS NULL)!=(NEW.body_sha256 IS NULL) BEGIN
            UPDATE state_metrics SET articles=articles-1,body_bytes=body_bytes-coalesce(OLD.body_bytes,0),
                stored_body_urls=stored_body_urls-(OLD.body_sha256 IS NOT NULL)
                WHERE provider=OLD.provider AND state=OLD.state;
            INSERT INTO state_metrics(provider,state,articles,body_bytes,stored_body_urls)
                VALUES (NEW.provider,NEW.state,1,coalesce(NEW.body_bytes,0),NEW.body_sha256 IS NOT NULL)
            ON CONFLICT(provider,state) DO UPDATE SET articles=articles+1,
                body_bytes=body_bytes+excluded.body_bytes,stored_body_urls=stored_body_urls+excluded.stored_body_urls;
            END''')
        db.execute('''CREATE TRIGGER metrics_delete AFTER DELETE ON articles BEGIN
            UPDATE state_metrics SET articles=articles-1,body_bytes=body_bytes-coalesce(OLD.body_bytes,0),
                stored_body_urls=stored_body_urls-(OLD.body_sha256 IS NOT NULL)
                WHERE provider=OLD.provider AND state=OLD.state;
            END''')
        db.execute('DELETE FROM state_metrics')
        db.execute('''INSERT INTO state_metrics(provider,state,articles,body_bytes,stored_body_urls)
            SELECT provider,state,count(*),coalesce(sum(body_bytes),0),sum(body_sha256 IS NOT NULL)
            FROM articles GROUP BY provider,state''')
        db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('article_body_state_counter_version',?)",
                   (str(ARTICLE_BODY_STATE_COUNTER_CONTRACT_VERSION),))
        db.execute('RELEASE news_body_state_migration')


def initialize_discovery_summary(db):
    """Exact transactional counts and deadline groups, not a timed cache.

    Static error/archive counts need no clock. Runnable counts group identical
    real deadlines, then range-seek the display clock; future deadlines and NULL
    remain distinct. Triggers share each canonical source mutation's commit or
    rollback. A missing derived generation is seeded once from original rows.
    """
    existed = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_static_counts'").fetchone()
    deadlines_existed = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_deadline_counts'").fetchone()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS discovery_static_counts(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            active_errors INTEGER NOT NULL CHECK(active_errors>=0),
            archived_rows INTEGER NOT NULL CHECK(archived_rows>=0),
            archived_errors INTEGER NOT NULL CHECK(archived_errors>=0)
        );
        CREATE TABLE IF NOT EXISTS discovery_deadline_counts(
            due REAL NOT NULL,body_capable INTEGER NOT NULL CHECK(body_capable IN (0,1)),
            rows INTEGER NOT NULL CHECK(rows>=0),PRIMARY KEY(due,body_capable)
        ) WITHOUT ROWID;
    ''')
    kinds = "('wordpress_posts','cnyes_news','rss_fulltext')"
    def delta(ref, sign):
        static = f'''UPDATE discovery_static_counts SET
            active_errors=active_errors+({sign})*({ref}.superseded_by_windows_json IS NULL AND {ref}.error_code IS NOT NULL),
            archived_rows=archived_rows+({sign})*({ref}.superseded_by_windows_json IS NOT NULL),
            archived_errors=archived_errors+({sign})*({ref}.superseded_by_windows_json IS NOT NULL AND {ref}.error_code IS NOT NULL)
            WHERE singleton=1;'''
        if sign == 1:
            return static + f'''INSERT INTO discovery_deadline_counts
                SELECT {ref}.due,({ref}.kind IN {kinds}),1
                WHERE {ref}.superseded_by_windows_json IS NULL AND {ref}.due IS NOT NULL
                ON CONFLICT(due,body_capable) DO UPDATE SET rows=rows+1;'''
        return static + f'''UPDATE discovery_deadline_counts SET rows=rows-1
            WHERE due={ref}.due AND body_capable=({ref}.kind IN {kinds})
                AND {ref}.superseded_by_windows_json IS NULL AND {ref}.due IS NOT NULL;
            DELETE FROM discovery_deadline_counts WHERE rows=0 AND due={ref}.due
                AND body_capable=({ref}.kind IN {kinds});'''
    db.executescript(f'''
        CREATE TRIGGER IF NOT EXISTS discovery_summary_insert AFTER INSERT ON discovery BEGIN
            {delta('NEW', 1)} END;
        CREATE TRIGGER IF NOT EXISTS discovery_summary_delete AFTER DELETE ON discovery BEGIN
            {delta('OLD', -1)} END;
        CREATE TRIGGER IF NOT EXISTS discovery_summary_update
        AFTER UPDATE OF due,kind,error_code,superseded_by_windows_json ON discovery
        WHEN OLD.due IS NOT NEW.due OR OLD.kind IS NOT NEW.kind
          OR OLD.error_code IS NOT NEW.error_code
          OR OLD.superseded_by_windows_json IS NOT NEW.superseded_by_windows_json BEGIN
            {delta('OLD', -1)} {delta('NEW', 1)} END;
    ''')
    with db:
        marker = db.execute("SELECT value FROM queue_settings WHERE key='discovery_summary_version'").fetchone()
        row = db.execute('SELECT singleton FROM discovery_static_counts WHERE singleton=1').fetchone()
        if not existed or not deadlines_existed or not marker or marker[0] != str(DISCOVERY_SUMMARY_CONTRACT_VERSION) or row is None:
            db.execute('DELETE FROM discovery_static_counts')
            db.execute('''INSERT INTO discovery_static_counts SELECT 1,
                coalesce(sum(superseded_by_windows_json IS NULL AND error_code IS NOT NULL),0),
                coalesce(sum(superseded_by_windows_json IS NOT NULL),0),
                coalesce(sum(superseded_by_windows_json IS NOT NULL AND error_code IS NOT NULL),0)
                FROM discovery''')
            db.execute('DELETE FROM discovery_deadline_counts')
            db.execute(f'''INSERT INTO discovery_deadline_counts
                SELECT due,(kind IN {kinds}),count(*) FROM discovery
                WHERE superseded_by_windows_json IS NULL AND due IS NOT NULL GROUP BY 1,2''')
            db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('discovery_summary_version',?)",
                       (str(DISCOVERY_SUMMARY_CONTRACT_VERSION),))


def discovery_summary(db, now):
    """One exact range read on this transaction's source-derived generation."""
    import sqlite3
    try:
        row = db.execute('''SELECT coalesce(sum(d.rows),0),s.active_errors,s.archived_rows,s.archived_errors,
            coalesce(sum(d.rows*d.body_capable),0) FROM discovery_static_counts s
            LEFT JOIN discovery_deadline_counts d ON d.due<=? WHERE s.singleton=1 GROUP BY s.singleton''', (now,)).fetchone()
    except sqlite3.OperationalError as error:
        if str(error) not in {'no such table: discovery_static_counts', 'no such table: discovery_deadline_counts'}:
            raise
        # Read-only pre-migration snapshots never initialize a source writer.
        return tuple(db.execute('''SELECT coalesce(sum(superseded_by_windows_json IS NULL AND due<=?),0),
            coalesce(sum(superseded_by_windows_json IS NULL AND error_code IS NOT NULL),0),
            coalesce(sum(superseded_by_windows_json IS NOT NULL),0),
            coalesce(sum(superseded_by_windows_json IS NOT NULL AND error_code IS NOT NULL),0),
            coalesce(sum(kind IN ('wordpress_posts','cnyes_news','rss_fulltext') AND superseded_by_windows_json IS NULL AND due<=?),0)
            FROM discovery''', (now,now)).fetchone())
    if row is None or any(type(v) is not int or v < 0 for v in row):
        raise ValueError('invalid_discovery_summary_generation')
    return tuple(row)


def initialize_bulk_error_count(db):
    """Exact derived row count, maintained in each source-row transaction.

    Canonical writers delete a request's prior diagnostics and insert its new
    observations, or delete an independently proven retired subset. No source
    row, source deadline or diagnostic receipt is rewritten here. A missing
    table/marker rebuilds once, not on every heartbeat.
    """
    existed = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                         "AND name='bulk_item_error_metrics'").fetchone()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS bulk_item_error_metrics(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), rows INTEGER NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS bulk_item_error_count_insert
        AFTER INSERT ON bulk_item_errors BEGIN
            UPDATE bulk_item_error_metrics SET rows=rows+1 WHERE singleton=1;
        END;
        CREATE TRIGGER IF NOT EXISTS bulk_item_error_count_delete
        AFTER DELETE ON bulk_item_errors BEGIN
            UPDATE bulk_item_error_metrics SET rows=rows-1 WHERE singleton=1;
        END;
    """)
    with db:
        marker = db.execute("SELECT value FROM queue_settings WHERE key='bulk_item_error_count_version'").fetchone()
        row = db.execute('SELECT rows FROM bulk_item_error_metrics WHERE singleton=1').fetchone()
        if not existed or not marker or marker[0] != str(BULK_ERROR_COUNT_CONTRACT_VERSION) or row is None:
            db.execute('INSERT OR REPLACE INTO bulk_item_error_metrics '
                       'SELECT 1,count(*) FROM bulk_item_errors')
            db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('bulk_item_error_count_version',?)",
                       (str(BULK_ERROR_COUNT_CONTRACT_VERSION),))


def bulk_item_error_count(db):
    """Preserve old read-only snapshots; never initialize a writer in a view."""
    import sqlite3
    try:
        row = db.execute('SELECT rows FROM bulk_item_error_metrics WHERE singleton=1').fetchone()
    except sqlite3.OperationalError as error:
        if str(error) != 'no such table: bulk_item_error_metrics':
            raise
        # Canonical read-only benchmark on an owner predating this migration.
        # New owners initialize the trigger-backed metric before collection.
        return db.execute('SELECT count(*) FROM bulk_item_errors').fetchone()[0]
    if row is None or type(row[0]) is not int or row[0] < 0:
        raise ValueError('invalid_bulk_item_error_count_metric')
    return row[0]


def summarize_bulk_window_gaps(db):
    """One grouped SQL pass; never build every historical gap as Python rows."""
    groups = [dict(row) for row in db.execute(
        'SELECT provider,error_code,count(*) AS scopes,min(observed_at_utc) AS first_observed_at_utc,'
        'max(observed_at_utc) AS latest_observed_at_utc FROM bulk_window_gaps '
        'GROUP BY provider,error_code ORDER BY provider,error_code')]
    samples = [dict(row) for row in db.execute(
        'SELECT * FROM bulk_window_gaps ORDER BY scope_url LIMIT ?', (BULK_GAP_STATUS_SAMPLE_LIMIT,))]
    total = sum(row['scopes'] for row in groups)
    return samples, {
        'contract_version': 1, 'total_gaps': total, 'groups': groups,
        'sample_limit': BULK_GAP_STATUS_SAMPLE_LIMIT, 'sample_rows': len(samples),
        'sample_truncated': total > len(samples), 'sample_order': 'scope_url',
        'all_gap_details_preserved': True, 'detail_authority': 'SQLite bulk_window_gaps',
        'historical_completeness_verified': False,
    }
