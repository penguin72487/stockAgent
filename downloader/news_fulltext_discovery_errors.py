"""Resumable, transactional rollup of last observed discovery errors.

The original discovery rows remain authoritative. A rowid cursor builds the
derived view in small owner-controlled slices; triggers account for mutations
already behind that cursor. No URL or source response enters the rollup.
"""
from __future__ import annotations

from collections import Counter
import math
import sqlite3
import time


CONTRACT_VERSION = 1
_TRIGGERS = ('discovery_error_insert', 'discovery_error_delete', 'discovery_error_update')
_KINDS = frozenset({'feed', 'metadata_feed', 'index', 'sitemap', 'rss_fulltext',
                    'wordpress_posts', 'cnyes_news'})
_ERRORS = {
    'discovery_http_401': 'http_401', 'discovery_http_403': 'http_403',
    'discovery_http_404': 'http_404', 'discovery_http_410': 'http_410',
    'discovery_http_429': 'http_429', 'discovery_http_451': 'http_451',
    'transport_error': 'transport', 'unregistered_bulk_api': 'unregistered_endpoint',
    'invalid_feed': 'invalid_feed',
    'rss_invalid_xml_body_requires_html': 'invalid_feed_body',
    'rss_xml_body_no_usable_identity': 'invalid_feed_identity',
    'bulk_response_contains_restricted_text': 'restricted_content',
    'bulk_all_items_unusable': 'unusable_items',
    'bulk_pagination_no_progress': 'pagination_no_progress',
    'bulk_pagination_totals_invalid': 'pagination_invalid',
}


def _covered(ref: str) -> str:
    return (f"{ref}.error_code IS NOT NULL AND {ref}.superseded_by_windows_json IS NULL "
            f"AND EXISTS (SELECT 1 FROM discovery_error_scan WHERE singleton=1 AND "
            f"(state='ready' OR (state='building' AND high_rowid IS NOT NULL AND {ref}.rowid<=high_rowid)))")


def initialize_discovery_errors(db: sqlite3.Connection) -> None:
    """Install a fresh derived generation without scanning original rows.

    Missing generation components cause a new incremental build. This is
    called by the exclusive queue owner during initialization, with no active
    caller transaction; it never edits discovery or its retry deadlines.
    """
    found = {row[0] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE name IN (?,?,?,?,?)",
        ('discovery_error_scan', 'discovery_error_groups', *_TRIGGERS))}
    if len(found) == 5:
        try:
            row = db.execute('SELECT contract_version,state,high_rowid,scanned_rows '
                             'FROM discovery_error_scan WHERE singleton=1').fetchone()
            db.execute('SELECT provider,kind,error_code,rows FROM discovery_error_groups LIMIT 0')
            if (row is not None and row[0] == CONTRACT_VERSION and row[1] in {'building', 'ready'}
                    and (row[2] is None or type(row[2]) is int)
                    and type(row[3]) is int and row[3] >= 0):
                return
        except sqlite3.DatabaseError:
            pass
    if db.in_transaction:
        raise RuntimeError('discovery_error_initialization_requires_no_transaction')
    db.executescript('''
        DROP TRIGGER IF EXISTS discovery_error_insert;
        DROP TRIGGER IF EXISTS discovery_error_delete;
        DROP TRIGGER IF EXISTS discovery_error_update;
        DROP TABLE IF EXISTS discovery_error_groups;
        DROP TABLE IF EXISTS discovery_error_scan;
        CREATE TABLE discovery_error_scan(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), contract_version INTEGER NOT NULL,
            state TEXT NOT NULL, high_rowid INTEGER, scanned_rows INTEGER NOT NULL CHECK(scanned_rows>=0)
        );
        CREATE TABLE discovery_error_groups(
            provider TEXT NOT NULL, kind TEXT NOT NULL, error_code TEXT NOT NULL,
            rows INTEGER NOT NULL CHECK(rows>=0), PRIMARY KEY(provider,kind,error_code)
        ) WITHOUT ROWID;
        INSERT INTO discovery_error_scan VALUES(1,1,'building',NULL,0);
    ''')
    old, new = _covered('OLD'), _covered('NEW')
    # A missing old group means the derived state is unreliable. Reset it to
    # unknown/building instead of preventing the source transaction.
    reset = f'''UPDATE discovery_error_scan SET state='repair'
        WHERE singleton=1 AND ({old}) AND NOT EXISTS
        (SELECT 1 FROM discovery_error_groups WHERE provider=OLD.provider AND kind=OLD.kind AND error_code=OLD.error_code AND rows>0);
        DELETE FROM discovery_error_groups WHERE (SELECT state FROM discovery_error_scan WHERE singleton=1)='repair';
        UPDATE discovery_error_scan SET state='building',high_rowid=NULL,scanned_rows=0
        WHERE singleton=1 AND state='repair';'''
    remove = f'''UPDATE discovery_error_groups SET rows=rows-1
        WHERE provider=OLD.provider AND kind=OLD.kind AND error_code=OLD.error_code AND ({old});
        DELETE FROM discovery_error_groups WHERE provider=OLD.provider AND kind=OLD.kind
        AND error_code=OLD.error_code AND rows=0;'''
    add = f'''INSERT INTO discovery_error_groups(provider,kind,error_code,rows)
        SELECT NEW.provider,NEW.kind,NEW.error_code,1 WHERE ({new})
        ON CONFLICT(provider,kind,error_code) DO UPDATE SET rows=rows+1;'''
    db.executescript(f'''
        CREATE TRIGGER discovery_error_insert AFTER INSERT ON discovery WHEN ({new}) BEGIN {add} END;
        CREATE TRIGGER discovery_error_delete AFTER DELETE ON discovery WHEN ({old}) BEGIN {reset} {remove} END;
        CREATE TRIGGER discovery_error_update
        AFTER UPDATE OF provider,kind,error_code,superseded_by_windows_json,rowid,_rowid_,oid ON discovery
        WHEN (OLD.rowid IS NOT NEW.rowid OR OLD.provider IS NOT NEW.provider OR OLD.kind IS NOT NEW.kind
              OR OLD.error_code IS NOT NEW.error_code OR OLD.superseded_by_windows_json IS NOT NEW.superseded_by_windows_json)
             AND (({old}) OR ({new})) BEGIN {reset} {remove} {add} END;
    ''')


def advance_discovery_errors(db: sqlite3.Connection, *, limit: int = 2048,
                             max_seconds: float = .05, expected_active_errors: int | None = None) -> dict:
    """Advance one resumable slice atomically with any outer transaction."""
    if type(limit) is not int or limit <= 0 or not isinstance(max_seconds, (float, int)) or not math.isfinite(max_seconds) or max_seconds <= 0:
        raise ValueError('invalid_discovery_error_slice')
    db.execute('SAVEPOINT discovery_error_slice')
    try:
        row = db.execute('SELECT contract_version,state,high_rowid,scanned_rows '
                         'FROM discovery_error_scan WHERE singleton=1').fetchone()
        if row is None or row[0] != CONTRACT_VERSION or row[1] not in {'building', 'ready'}:
            raise ValueError('invalid_discovery_error_scan')
        state, high, scanned = row[1:]
        if state == 'ready' and expected_active_errors is not None:
            total = db.execute('SELECT coalesce(sum(rows),0) FROM discovery_error_groups').fetchone()[0]
            if type(expected_active_errors) is not int or expected_active_errors < 0:
                raise ValueError('invalid_discovery_error_expected_count')
            if total != expected_active_errors:
                db.execute('DELETE FROM discovery_error_groups')
                db.execute("UPDATE discovery_error_scan SET state='building',high_rowid=NULL,scanned_rows=0 WHERE singleton=1")
                state, high, scanned = 'building', None, 0
        if state == 'building':
            statement = ('SELECT rowid,provider,kind,error_code,superseded_by_windows_json FROM discovery ' +
                         ('' if high is None else 'WHERE rowid>? ') + 'ORDER BY rowid LIMIT ?')
            params = (limit,) if high is None else (high, limit)
            cursor = db.execute(statement, params)
            counts = Counter()
            visited, last, exhausted = 0, high, True
            started = time.monotonic()
            for item in cursor:
                last, provider, kind, error, archived = item
                visited += 1
                if error is not None and archived is None:
                    counts[(provider, kind, error)] += 1
                if visited % 32 == 0 and time.monotonic() - started >= max_seconds:
                    exhausted = False
                    break
            cursor.close()
            db.executemany('INSERT INTO discovery_error_groups VALUES (?,?,?,?) '
                           'ON CONFLICT(provider,kind,error_code) DO UPDATE SET rows=rows+excluded.rows',
                           ((*key, count) for key, count in counts.items()))
            ready = exhausted and visited < limit
            db.execute('UPDATE discovery_error_scan SET high_rowid=?,scanned_rows=?,state=? WHERE singleton=1',
                       (last, scanned + visited, 'ready' if ready else 'building'))
        db.execute('RELEASE discovery_error_slice')
    except BaseException:
        db.execute('ROLLBACK TO discovery_error_slice')
        db.execute('RELEASE discovery_error_slice')
        raise
    return discovery_errors(db)


def reset_discovery_errors(db: sqlite3.Connection) -> None:
    """Discard only a damaged derived generation, preserving the source queue."""
    db.execute('SAVEPOINT discovery_error_reset')
    try:
        db.execute('DELETE FROM discovery_error_groups')
        updated = db.execute("UPDATE discovery_error_scan SET state='building',high_rowid=NULL,scanned_rows=0 WHERE singleton=1")
        if updated.rowcount != 1:
            raise ValueError('invalid_discovery_error_scan')
        db.execute('RELEASE discovery_error_reset')
    except BaseException:
        db.execute('ROLLBACK TO discovery_error_reset')
        db.execute('RELEASE discovery_error_reset')
        raise


def discovery_errors(db: sqlite3.Connection, *, expected_active_errors: int | None = None) -> dict:
    """Return a compact, safe snapshot; a partial generation is never zero."""
    unavailable = {'contract_version': CONTRACT_VERSION, 'state': 'unavailable',
                   'scanned_rows': None, 'active_errors': None, 'providers': None}
    try:
        triggers = db.execute("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name IN (?,?,?)", _TRIGGERS).fetchone()[0]
        if triggers != len(_TRIGGERS):
            return unavailable
        row = db.execute('SELECT contract_version,state,scanned_rows FROM discovery_error_scan WHERE singleton=1').fetchone()
        if (row is None or row[0] != CONTRACT_VERSION or row[1] not in {'building', 'ready'}
                or type(row[2]) is not int or row[2] < 0):
            return unavailable
        if row[1] == 'building':
            return {**unavailable, 'state': 'building', 'scanned_rows': row[2]}
        providers = {}
        total = 0
        for provider, kind, code, count in db.execute('SELECT provider,kind,error_code,rows FROM discovery_error_groups'):
            if not isinstance(provider, str) or not isinstance(kind, str) or not isinstance(code, str) or type(count) is not int or count <= 0:
                return unavailable
            item = providers.setdefault(provider, {'active_errors': 0, 'kinds': {}, 'reasons': {}})
            kind_key = kind if kind in _KINDS else 'other'
            reason = _ERRORS.get(code, 'http_other' if code.startswith('discovery_http_') else 'other')
            item['active_errors'] += count
            item['kinds'][kind_key] = item['kinds'].get(kind_key, 0) + count
            item['reasons'][reason] = item['reasons'].get(reason, 0) + count
            total += count
        if expected_active_errors is not None and total != expected_active_errors:
            return unavailable
        return {'contract_version': CONTRACT_VERSION, 'state': 'ready', 'scanned_rows': row[2],
                'active_errors': total, 'providers': providers}
    except sqlite3.DatabaseError:
        return unavailable
