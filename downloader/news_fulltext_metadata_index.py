"""Incremental current-head index for retained mobile metadata repairs.

The canonical acceptance transaction registers new candidates, while one
resumable host-index scan reconciles heads accepted before this contract.
Neither path requests a publisher or changes an article, HTTP clock or receipt.
"""
from __future__ import annotations

import json
import re
from urllib.parse import urlsplit


METADATA_INCREMENTAL_INDEX_VERSION = 1
METADATA_INDEX_SCAN_ROWS = 1024
METADATA_INDEX_DIRTY_ROWS = 64
METADATA_INDEX_IDLE_SECONDS = 1
MOBILE_HOST = 'm.cnyes.com'


def initialize_metadata_index(db):
    """Create additive state before any acceptance or receipt recovery."""
    db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_replays(
        source_fingerprint TEXT NOT NULL, article_id BLOB NOT NULL,
        original_path TEXT NOT NULL, original_body_sha256 TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending', error_code TEXT, checked_at_utc TEXT,
        PRIMARY KEY(source_fingerprint,article_id)) WITHOUT ROWID''')
    db.execute('CREATE INDEX IF NOT EXISTS retained_metadata_ready '
               'ON retained_metadata_replays(source_fingerprint,state,original_path,article_id)')
    db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_checkpoint_history(
        history_id INTEGER PRIMARY KEY, source_fingerprint TEXT NOT NULL, article_id BLOB NOT NULL,
        original_path TEXT NOT NULL, original_body_sha256 TEXT NOT NULL, state TEXT NOT NULL,
        error_code TEXT, checked_at_utc TEXT, archived_at_utc TEXT NOT NULL, reason TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_incremental_scans(
        source_fingerprint TEXT PRIMARY KEY, state TEXT NOT NULL, cursor_state TEXT,
        cursor_rowid INTEGER NOT NULL, rows_seen INTEGER NOT NULL, registered_heads INTEGER NOT NULL,
        dirty_processed INTEGER NOT NULL, schema_version INTEGER NOT NULL, cursor_resets INTEGER NOT NULL,
        started_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL
    ) WITHOUT ROWID''')
    columns = {row[1] for row in db.execute('PRAGMA table_info(retained_metadata_incremental_scans)')}
    if 'schema_version' not in columns:
        db.execute('ALTER TABLE retained_metadata_incremental_scans ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 0')
    if 'cursor_resets' not in columns:
        db.execute('ALTER TABLE retained_metadata_incremental_scans ADD COLUMN cursor_resets INTEGER NOT NULL DEFAULT 0')
    db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_incremental_current(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), source_fingerprint TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_incremental_dirty(
        article_id BLOB PRIMARY KEY, generation INTEGER NOT NULL) WITHOUT ROWID''')
    db.execute('''CREATE TABLE IF NOT EXISTS retained_metadata_incremental_invalid(
        source_fingerprint TEXT NOT NULL, article_id BLOB NOT NULL, reason TEXT NOT NULL,
        body_sha256 TEXT NOT NULL, observed_at_utc TEXT NOT NULL,
        PRIMARY KEY(source_fingerprint,article_id)) WITHOUT ROWID''')
    # While a legacy baseline is scanned in (state,rowid) index order, a head
    # can move behind that cursor. Record those changes in the same article
    # transaction. Once the finite migration is drained these triggers are
    # inert; normal new captures use the acceptance hook below.
    active = '''EXISTS(SELECT 1 FROM retained_metadata_incremental_current c
        JOIN retained_metadata_incremental_scans s ON s.source_fingerprint=c.source_fingerprint
        WHERE c.singleton=1 AND s.state IN ('scanning','draining'))'''
    db.execute(f'''CREATE TRIGGER IF NOT EXISTS retained_metadata_incremental_article_update
        AFTER UPDATE OF state,body_sha256,parquet_path ON articles
        WHEN NEW.host='{MOBILE_HOST}' AND (NEW.body_sha256 IS NOT NULL OR OLD.body_sha256 IS NOT NULL) AND
          (OLD.state IS NOT NEW.state OR OLD.body_sha256 IS NOT NEW.body_sha256
           OR OLD.parquet_path IS NOT NEW.parquet_path) AND {active}
        BEGIN
          INSERT INTO retained_metadata_incremental_dirty VALUES(NEW.id,1)
          ON CONFLICT(article_id) DO UPDATE SET generation=generation+1;
        END''')
    db.execute(f'''CREATE TRIGGER IF NOT EXISTS retained_metadata_incremental_article_insert
        AFTER INSERT ON articles WHEN NEW.host='{MOBILE_HOST}' AND NEW.body_sha256 IS NOT NULL AND {active}
        BEGIN
          INSERT INTO retained_metadata_incremental_dirty VALUES(NEW.id,1)
          ON CONFLICT(article_id) DO UPDATE SET generation=generation+1;
        END''')


def inherit_unchanged_chart_checkpoints(db, fingerprint, previous, *, article_id=None):
    """Reuse exact prior capture proofs after the chart-only parser change.

    ``previous`` must be reconstructed from the complete current source
    policy with only the chart ABI removed. A bounded scanner supplies an
    article ID; the small initial cohort may use the same predicate in bulk.
    Do not inherit old batch-wide mismatch failures: a peer may have caused
    the failure and each head now requires its own verified outcome.
    """
    if not isinstance(previous, str) or not re.fullmatch(r'[0-9a-f]{64}', previous) or previous == fingerprint:
        return 0
    key_filter = '' if article_id is None else ' AND current.article_id=?'
    params = (fingerprint, previous) if article_id is None else (fingerprint, previous, article_id)
    result = db.execute('''UPDATE retained_metadata_replays AS current
        SET state=previous.state,error_code=previous.error_code,checked_at_utc=previous.checked_at_utc
        FROM retained_metadata_replays AS previous
        WHERE current.source_fingerprint=? AND current.state='pending'
            AND previous.source_fingerprint=? AND previous.article_id=current.article_id
            AND previous.original_path=current.original_path
            AND previous.original_body_sha256=current.original_body_sha256
            AND (previous.state IN ('current_metadata_complete','not_mobile_html_candidate')
                OR (previous.state='failed_validation' AND previous.error_code IS NOT 'body_not_found'
                    AND previous.error_code IS NOT 'retained_metadata_batch_head_not_unique'))'''
        + key_filter, params)
    return result.rowcount


def register_metadata_head(db, fingerprint, article_id, path, body_sha256, *, reason, now,
                           prior_fingerprint=''):
    """Index one current head; preserve any prior checkpoint before replacing it.

    The caller owns the surrounding transaction and must already have proved
    that this is the current article head. Re-registering the same head never
    reopens a completed or failed checkpoint.
    """
    old = db.execute('''SELECT original_path,original_body_sha256,state,error_code,checked_at_utc
        FROM retained_metadata_replays WHERE source_fingerprint=? AND article_id=?''',
        (fingerprint, article_id)).fetchone()
    if old and (old[0], old[1]) == (path, body_sha256):
        if old[2] == 'pending':
            inherit_unchanged_chart_checkpoints(db, fingerprint, prior_fingerprint, article_id=article_id)
        return False
    if old:
        db.execute('''INSERT INTO retained_metadata_checkpoint_history
            (source_fingerprint,article_id,original_path,original_body_sha256,state,error_code,
             checked_at_utc,archived_at_utc,reason) VALUES (?,?,?,?,?,?,?,?,?)''',
            (fingerprint, article_id, *old, now, reason))
        db.execute('''UPDATE retained_metadata_replays SET original_path=?,original_body_sha256=?,
            state='pending',error_code=NULL,checked_at_utc=NULL WHERE source_fingerprint=? AND article_id=?''',
            (path, body_sha256, fingerprint, article_id))
    else:
        db.execute('''INSERT INTO retained_metadata_replays
            (source_fingerprint,article_id,original_path,original_body_sha256) VALUES (?,?,?,?)''',
            (fingerprint, article_id, path, body_sha256))
    inherit_unchanged_chart_checkpoints(db, fingerprint, prior_fingerprint, article_id=article_id)
    return True


def register_accepted_metadata_head(db, source, record, path, *, now):
    """Register only a newly accepted mobile response with missing metadata."""
    if source is None or not source.allowed or record.get('provider') != 'cnyes':
        return False
    try:
        if (urlsplit(record.get('requested_url') or '').hostname != MOBILE_HOST
                or urlsplit(record.get('final_url') or '').hostname != MOBILE_HOST):
            return False
    except (TypeError, ValueError):
        return False
    article_id = bytes.fromhex(record['article_id'])
    # A successful canonical acceptance provides a new durable path. Clear an
    # earlier observed broken head even when this version needs no replay.
    db.execute('DELETE FROM retained_metadata_incremental_invalid WHERE source_fingerprint=? AND article_id=?',
               (source.data_fingerprint, article_id))
    try:
        authors = json.loads(record.get('authors_json') or '[]')
    except (TypeError, ValueError):
        # Malformed metadata remains a local proof task, never a reason to
        # reject or rewrite the original accepted response.
        authors = None
    if (record.get('published_at_raw') and authors
            and record.get('body_completeness') != 'article_container_unverified'):
        return False
    return register_metadata_head(db, source.data_fingerprint, article_id,
                                  path, record['body_sha256'], reason='acceptance',
                                  now=now() if callable(now) else now)


def reconcile_metadata_head(db, fingerprint, article_id, path, digest, *, reason, now, prior_fingerprint=''):
    """Register a valid head or retain evidence of a missing original path."""
    if digest and not path:
        db.execute('''INSERT INTO retained_metadata_incremental_invalid
            (source_fingerprint,article_id,reason,body_sha256,observed_at_utc)
            VALUES (?,?,'missing_parquet_path',?,?) ON CONFLICT(source_fingerprint,article_id)
            DO UPDATE SET reason=excluded.reason,body_sha256=excluded.body_sha256,
                          observed_at_utc=excluded.observed_at_utc''',
            (fingerprint, article_id, digest, now))
        return False
    db.execute('DELETE FROM retained_metadata_incremental_invalid WHERE source_fingerprint=? AND article_id=?',
               (fingerprint, article_id))
    if path and digest:
        return register_metadata_head(db, fingerprint, article_id, path, digest, reason=reason, now=now,
                                      prior_fingerprint=prior_fingerprint)
    return False


def begin_metadata_index_scan(db, fingerprint, *, initial_cohort_seeded, now):
    """Begin one legacy reconciliation, or finish an already seeded cohort."""
    current = db.execute('SELECT source_fingerprint FROM retained_metadata_incremental_current WHERE singleton=1').fetchone()
    changed_source = bool(current and current[0] != fingerprint)
    if not current or changed_source:
        db.execute('''INSERT INTO retained_metadata_incremental_current VALUES (1,?)
            ON CONFLICT(singleton) DO UPDATE SET source_fingerprint=excluded.source_fingerprint''', (fingerprint,))
    existing = db.execute('SELECT 1 FROM retained_metadata_incremental_scans WHERE source_fingerprint=?',
                          (fingerprint,)).fetchone()
    if existing:
        if changed_source:
            # The index may have been inactive while another source version
            # accepted heads; a previous cursor cannot cover that interval.
            db.execute('''UPDATE retained_metadata_incremental_scans SET state='scanning',cursor_state=NULL,
                cursor_rowid=0,schema_version=?,cursor_resets=cursor_resets+1,updated_at_utc=?
                WHERE source_fingerprint=?''', (db.execute('PRAGMA schema_version').fetchone()[0], now, fingerprint))
        return
    old_dirty = db.execute('SELECT 1 FROM retained_metadata_incremental_dirty LIMIT 1').fetchone()
    state = ('draining' if old_dirty else 'complete') if initial_cohort_seeded else 'scanning'
    schema_version = db.execute('PRAGMA schema_version').fetchone()[0]
    db.execute('''INSERT OR IGNORE INTO retained_metadata_incremental_scans
        (source_fingerprint,state,cursor_state,cursor_rowid,rows_seen,registered_heads,dirty_processed,
         schema_version,cursor_resets,started_at_utc,updated_at_utc)
        VALUES (?,?,NULL,0,0,0,0,?,0,?,?)''', (fingerprint, state, schema_version, now, now))


def advance_metadata_index_scan(db, fingerprint, *, now, scan_rows=METADATA_INDEX_SCAN_ROWS,
                                dirty_rows=METADATA_INDEX_DIRTY_ROWS, prior_fingerprint=''):
    """Advance a finite, restart-safe scan and its concurrent-change journal."""
    if (type(scan_rows) is not int or not 1 <= scan_rows <= METADATA_INDEX_SCAN_ROWS
            or type(dirty_rows) is not int or not 1 <= dirty_rows <= METADATA_INDEX_DIRTY_ROWS):
        raise ValueError('unbounded metadata index slice')
    result = {'rows_scanned': 0, 'heads_registered': 0, 'dirty_processed': 0}
    with db:
        scan = db.execute('''SELECT state,cursor_state,cursor_rowid,schema_version FROM retained_metadata_incremental_scans
            WHERE source_fingerprint=?''', (fingerprint,)).fetchone()
        if not scan or scan[0] == 'complete':
            return result
        state, cursor_state, cursor_rowid, previous_schema = scan
        schema_version = db.execute('PRAGMA schema_version').fetchone()[0]
        reset = int(schema_version != previous_schema)
        if reset:
            # VACUUM or a schema rebuild can renumber a non-INTEGER-PK rowid.
            # A restart of this finite scan is safe and preserves its journal.
            state, cursor_state, cursor_rowid = 'scanning', None, 0
        dirty = db.execute('''SELECT d.article_id,d.generation,a.host,a.parquet_path,a.body_sha256
            FROM retained_metadata_incremental_dirty d LEFT JOIN articles a ON a.id=d.article_id
            ORDER BY d.article_id LIMIT ?''', (dirty_rows,)).fetchall()
        for article_id, generation, host, path, digest in dirty:
            if host == MOBILE_HOST:
                result['heads_registered'] += reconcile_metadata_head(
                    db, fingerprint, article_id, path, digest, reason='concurrent_change', now=now,
                    prior_fingerprint=prior_fingerprint)
            else:
                db.execute('DELETE FROM retained_metadata_incremental_invalid WHERE source_fingerprint=? AND article_id=?',
                           (fingerprint, article_id))
            db.execute('DELETE FROM retained_metadata_incremental_dirty WHERE article_id=? AND generation=?',
                       (article_id, generation))
        result['dirty_processed'] = len(dirty)
        if state == 'scanning':
            if cursor_state is None:
                first = db.execute('''SELECT state FROM articles INDEXED BY host_state
                    WHERE host=? ORDER BY state LIMIT 1''', (MOBILE_HOST,)).fetchone()
                cursor_state, cursor_rowid = (first[0], 0) if first else (None, 0)
            if cursor_state is not None:
                rows = db.execute('''SELECT a.rowid,a.id,a.parquet_path,a.body_sha256,
                        r.original_path,r.original_body_sha256,i.article_id,r.state FROM articles a INDEXED BY host_state
                    LEFT JOIN retained_metadata_replays r ON r.source_fingerprint=? AND r.article_id=a.id
                    LEFT JOIN retained_metadata_incremental_invalid i
                      ON i.source_fingerprint=? AND i.article_id=a.id
                    WHERE a.host=? AND a.state=? AND a.rowid>?
                    ORDER BY a.rowid LIMIT ?''',
                    (fingerprint, fingerprint, MOBILE_HOST, cursor_state, cursor_rowid, scan_rows)).fetchall()
                for rowid, article_id, path, digest, prior_path, prior_digest, invalid_id, prior_state in rows:
                    if digest and not path:
                        reconcile_metadata_head(db, fingerprint, article_id, path, digest,
                                                reason='legacy_scan', now=now)
                    elif invalid_id is not None or (path and digest and (path, digest) != (prior_path, prior_digest)):
                        result['heads_registered'] += reconcile_metadata_head(
                            db, fingerprint, article_id, path, digest, reason='legacy_scan', now=now,
                            prior_fingerprint=prior_fingerprint)
                    elif path and digest and prior_state == 'pending':
                        # An interrupted or older scan may already have
                        # registered this exact head without inheriting proof.
                        inherit_unchanged_chart_checkpoints(db, fingerprint, prior_fingerprint,
                                                            article_id=article_id)
                result['rows_scanned'] = len(rows)
                if rows:
                    cursor_rowid = rows[-1][0]
                if len(rows) < scan_rows:
                    next_state = db.execute('''SELECT state FROM articles INDEXED BY host_state
                        WHERE host=? AND state>? ORDER BY state LIMIT 1''', (MOBILE_HOST, cursor_state)).fetchone()
                    cursor_state, cursor_rowid = (next_state[0], 0) if next_state else (None, 0)
            if cursor_state is None:
                state = 'draining'
        if state == 'draining' and not db.execute('SELECT 1 FROM retained_metadata_incremental_dirty LIMIT 1').fetchone():
            state = 'complete'
        db.execute('''UPDATE retained_metadata_incremental_scans SET state=?,cursor_state=?,cursor_rowid=?,
            rows_seen=rows_seen+?,registered_heads=registered_heads+?,dirty_processed=dirty_processed+?,
            schema_version=?,cursor_resets=cursor_resets+?,updated_at_utc=? WHERE source_fingerprint=?''',
            (state, cursor_state, cursor_rowid, result['rows_scanned'], result['heads_registered'],
             result['dirty_processed'], schema_version, reset, now, fingerprint))
    return result
