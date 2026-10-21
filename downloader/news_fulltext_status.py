"""Bounded heartbeat views, not deletion of acquisition diagnostic evidence.

Public readers never query SQLite. The canonical owner publishes a bounded
sample and exact grouped counts; full gap rows and calibration windows retain
their existing authority and recovery contracts.
"""
from __future__ import annotations


STATUS_PROJECTION_CONTRACT_VERSION = 3
STATUS_QUERY_TIMING_CONTRACT_VERSION = 1
BULK_ERROR_COUNT_CONTRACT_VERSION = 1
DISCOVERY_SUMMARY_CONTRACT_VERSION = 1
ARTICLE_BODY_STATE_COUNTER_CONTRACT_VERSION = 1
CAPACITY_STATUS_HISTORY_LIMIT = 64
BULK_GAP_STATUS_SAMPLE_LIMIT = 64


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
