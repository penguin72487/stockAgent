"""Complete catalog reuse must retain the full publisher's invalidation gates."""
from pathlib import Path

import duckdb
import pytest

from scripts import compact_openbb_l1 as c
from test_openbb_l1_compaction import _publish_tasks, _sec_grouped_args


def test_unchanged_publication_keeps_inode_and_never_rebinds_or_copies(tmp_path, monkeypatch):
    _publish_tasks(tmp_path, ['aa', 'bb'], endpoint=c.SCHEMA_GROUPED_ENDPOINT)
    args = _sec_grouped_args(tmp_path)
    assert c.run(args) == 0
    path = tmp_path/'openbb_l1.duckdb'
    original = path.read_bytes(), c._view_file_signature(path)
    connect = c.duckdb.connect

    class ReadOnly:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, sql, *parameters):
            assert not sql.startswith('DESCRIBE'), 'unchanged physical proof must avoid rebinding'
            return self.connection.execute(sql, *parameters)

        def close(self):
            self.connection.close()

    def readonly(database, *positional, **kwargs):
        assert kwargs.get('read_only'), 'unchanged view publication must not open a writer'
        return ReadOnly(connect(database, *positional, **kwargs))

    def forbidden(*args, **kwargs):
        raise AssertionError('unchanged catalog must not be copied')

    monkeypatch.setattr(c.duckdb, 'connect', readonly)
    monkeypatch.setattr(c.shutil, 'copy2', forbidden)
    assert c.run(args) == 0
    assert (path.read_bytes(), c._view_file_signature(path)) == original


@pytest.mark.parametrize('column,value', [
    ('rows', '999'), ('source_files', '999'), ('updated_at_utc', "'forged-clock'"),
    ('query_reason', "'forged-reason'"),
])
def test_complete_catalog_fields_are_repaired_instead_of_reused(tmp_path, column, value):
    _publish_tasks(tmp_path, ['aa', 'bb'], endpoint=c.SCHEMA_GROUPED_ENDPOINT)
    args = _sec_grouped_args(tmp_path)
    assert c.run(args) == 0
    path = tmp_path/'openbb_l1.duckdb'
    with duckdb.connect(str(path), read_only=True) as database:
        expected = database.execute('SELECT * FROM l1_catalog').fetchall()
    with duckdb.connect(str(path)) as database:
        database.execute(f'UPDATE l1_catalog SET {column}={value}')
    identity = c._view_file_signature(path)
    assert c.run(args) == 0
    assert c._view_file_signature(path) != identity
    with duckdb.connect(str(path), read_only=True) as database:
        assert database.execute('SELECT * FROM l1_catalog').fetchall() == expected


def test_catalog_replacement_during_noop_proof_aborts(tmp_path, monkeypatch):
    _publish_tasks(tmp_path, ['aa'], endpoint=c.SCHEMA_GROUPED_ENDPOINT)
    args = _sec_grouped_args(tmp_path)
    assert c.run(args) == 0
    original = c._reusable_view_catalog
    calls = 0

    def changed(path, **kwargs):
        nonlocal calls
        calls += 1
        result = original(path, **kwargs)
        if calls == 2:
            import os
            before = path.stat()
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns+1))
        return result

    monkeypatch.setattr(c, '_reusable_view_catalog', changed)
    with pytest.raises(RuntimeError, match='catalog changed'):
        c.run(args)
