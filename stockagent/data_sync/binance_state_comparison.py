"""Compare downloader state generations without executing their contents."""
from __future__ import annotations

from contextlib import closing
import sqlite3

from stockagent.data_sync.desync_snapshots import SnapshotError

EXPECTED = ('object_key', 'etag', 'status', 'sha256', 'row_count', 'output_path',
            'downloaded_at_utc', 'error')


def compare_databases(original: bytes, newer: bytes) -> dict:
    """Check durable object identities and completion regressions in memory."""
    try:
        with closing(sqlite3.connect(':memory:')) as db:
            db.execute('PRAGMA trusted_schema=OFF')
            db.deserialize(newer)
            db.execute("ATTACH DATABASE ':memory:' AS old")
            db.deserialize(original, name='old')
            for schema in ('main', 'old'):
                tables = [row[0] for row in db.execute(
                    f"SELECT name FROM {schema}.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
                columns = tuple(row[1] for row in db.execute(f'PRAGMA {schema}.table_info(archive_objects)'))
                if tables != ['archive_objects'] or columns != EXPECTED:
                    raise SnapshotError('Binance state schema is not the enrolled original')
                if db.execute(f'SELECT COUNT(*) FROM {schema}.archive_objects WHERE object_key IS NULL').fetchone()[0]:
                    raise SnapshotError('Binance state has a null object identity')
            scalar = lambda query: db.execute(query).fetchone()[0]
            old = scalar('SELECT COUNT(*) FROM old.archive_objects')
            new = scalar('SELECT COUNT(*) FROM main.archive_objects')
            missing = scalar('SELECT COUNT(*) FROM old.archive_objects o LEFT JOIN main.archive_objects n '
                             'ON o.object_key=n.object_key WHERE n.object_key IS NULL')
            added = scalar('SELECT COUNT(*) FROM main.archive_objects n LEFT JOIN old.archive_objects o '
                           'ON o.object_key=n.object_key WHERE o.object_key IS NULL')
            regressions = scalar("SELECT COUNT(*) FROM old.archive_objects o JOIN main.archive_objects n "
                                 "ON o.object_key=n.object_key WHERE o.status='complete' "
                                 "AND n.status!='complete' AND o.etag=n.etag")
            hash_conflicts = scalar('SELECT COUNT(*) FROM old.archive_objects o JOIN main.archive_objects n '
                'ON o.object_key=n.object_key WHERE o.etag=n.etag AND o.sha256 IS NOT NULL '
                'AND n.sha256 IS NOT NULL AND o.sha256!=n.sha256')
            return {'old_rows': old, 'new_rows': new, 'old_only_rows': missing,
                    'new_only_rows': added, 'completed_regressions_same_etag': regressions,
                    'hash_conflicts_same_etag': hash_conflicts}
    except sqlite3.Error as error:
        raise SnapshotError('Binance state database cannot be opened or compared') from error
