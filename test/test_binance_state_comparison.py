import sqlite3

import pytest

from stockagent.data_sync.binance_state_comparison import compare_databases
from stockagent.data_sync.desync_snapshots import SnapshotError


def database(rows):
    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE archive_objects (object_key TEXT PRIMARY KEY, etag TEXT NOT NULL, '
               'status TEXT NOT NULL, sha256 TEXT, row_count INTEGER, output_path TEXT, '
               'downloaded_at_utc TEXT, error TEXT)')
    db.executemany('INSERT INTO archive_objects (object_key,etag,status,sha256) VALUES (?,?,?,?)', rows)
    result = db.serialize()
    db.close()
    return result


def test_binance_state_detects_missing_rows_same_etag_regressions_and_hash_conflicts():
    old = database([('a', 'v1', 'complete', 'one'), ('b', 'v1', 'complete', 'two'),
                    ('c', 'v1', 'complete', 'three')])
    new = database([('a', 'v1', 'failed', 'one'), ('b', 'v1', 'complete', 'changed'),
                    ('d', 'v2', 'complete', 'four')])
    result = compare_databases(old, new)
    assert result == {'old_rows': 3, 'new_rows': 3, 'old_only_rows': 1, 'new_only_rows': 1,
                      'completed_regressions_same_etag': 1, 'hash_conflicts_same_etag': 1}


def test_binance_state_does_not_claim_unrecognized_sqlite_is_equivalent():
    other = sqlite3.connect(':memory:')
    other.execute('CREATE TABLE unknown (id TEXT)')
    content = other.serialize()
    other.close()
    with pytest.raises(SnapshotError, match='schema'):
        compare_databases(content, content)
