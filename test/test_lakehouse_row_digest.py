"""Streaming must preserve the existing canonical JSON digest byte for byte."""
import math

import pytest

pytest.importorskip('temporalio', reason='use the locked lakehouse control role')
from pathlib import Path
from types import SimpleNamespace
from scripts.benchmark_lakehouse_row_digest import (
    batches_as_rows, canonical_row_digest, engineering_reference_limits, paged_metadata_rows,
)
from stockagent.runtime_identity import identity_sha256


@pytest.mark.parametrize('rows', [[], [(1,)], [('中文\n"\\', None, True, False, -0.0, 1e-20)],
    [('a', {'z': 3, 'a': [1, 2]}, 100000000000000000000)], [('x'*1000000,)]])
def test_exact_original_canonical_rows_digest(rows):
    assert canonical_row_digest(iter(rows)) == (len(rows), identity_sha256(rows))


def test_nonfinite_values_still_rejected():
    with pytest.raises(ValueError):
        canonical_row_digest([(math.nan,)])


def test_bounded_cursor_never_fetches_all():
    class Cursor:
        calls = 0
        def fetchmany(self, size):
            assert size == 16
            self.calls += 1
            return [(self.calls,)] if self.calls < 3 else []
    cursor = Cursor()
    assert list(batches_as_rows(cursor)) == [(1,), (2,)]
    assert cursor.calls == 3


def test_semantic_reference_cannot_change_production_limits(monkeypatch):
    monkeypatch.setattr(Path, 'read_text', lambda *a, **kw: '0::/system.slice/stockagent-storage-lifecycle.service\n')
    from scripts import benchmark_lakehouse_row_digest as probe
    monkeypatch.setattr(probe.subprocess, 'run', lambda *a, **kw: pytest.fail('production property write'))
    with pytest.raises(ValueError, match='production cgroup'):
        engineering_reference_limits()


def test_export_rechecks_guard_after_catalog_owner_releases(tmp_path, monkeypatch):
    from stockagent.control import lakehouse
    state = tmp_path / 'state'; state.mkdir()
    guards, attempts, waits = [], [], []
    monkeypatch.setattr(lakehouse, 'guard', lambda c: guards.append(True))
    class Conn:
        def execute(self, _sql): return self
        def fetchone(self): return (8,)
        def close(self): pass
    monkeypatch.setattr(lakehouse, 'connect', lambda *a, **kw: Conn())
    original = lakehouse.fcntl.flock
    def busy_twice(fd, operation):
        attempts.append(operation)
        if len(attempts) <= 2:
            raise BlockingIOError('busy')
        return original(fd, operation)
    monkeypatch.setattr(lakehouse.fcntl, 'flock', busy_twice)
    result = lakehouse.export({'state_root': str(state)}, 7, on_owner_busy=lambda: waits.append(True) or True)
    assert result['state'] == 'catalog_advanced' and result['snapshot_id'] == 8
    assert len(attempts) == 3 and len(waits) == 2 and len(guards) == 2


def test_export_owner_wait_can_stop_without_touching_data(tmp_path, monkeypatch):
    from stockagent.control import lakehouse
    state = tmp_path / 'state'; state.mkdir()
    monkeypatch.setattr(lakehouse, 'guard', lambda c: None)
    monkeypatch.setattr(lakehouse.fcntl, 'flock', lambda *args: (_ for _ in ()).throw(BlockingIOError('busy')))
    with pytest.raises(lakehouse.CatalogOwnerBusy):
        lakehouse.export({'state_root': str(state)}, 7, on_owner_busy=lambda: False)
    assert not (state / 'export-7.json').exists()


@pytest.mark.parametrize('bounds', [(0, 1), (1, 0), (-1, 2)])
def test_zero_or_negative_pages_rejected(bounds):
    with pytest.raises(ValueError, match='positive bounds'):
        list(paged_metadata_rows(None, maximum_bytes=bounds[0], maximum_rows=bounds[1]))


@pytest.mark.parametrize('mode', ['paged', 'single_pass', 'memory_short', 'observation_failure', 'missing_helper'])
@pytest.mark.parametrize('variant', ['unique', 'duplicate', 'null', 'negative', 'empty', 'reordered', 'collated'])
def test_real_sql_pages_preserve_complete_original_order_and_hash(variant, mode, monkeypatch):
    import duckdb
    from stockagent import remote_build
    def profile(*args):
        if mode == 'missing_helper':
            raise ImportError('optional memory observer absent from an older restore capsule')
        if mode == 'observation_failure':
            raise ValueError('unavailable cgroup observation')
        return {'limits': {'memory_headroom_bytes': 64*1024**3 if mode == 'single_pass' else 0}}
    monkeypatch.setattr(remote_build, 'observe_node', profile)
    connection = duckdb.connect()
    try:
        connection.execute("SET memory_limit='-1'")
        connection.execute("ATTACH ':memory:' AS lake")
        fields = ('sha256 VARCHAR,relative VARCHAR' if variant == 'reordered' else
                  'relative VARCHAR COLLATE nocase,sha256 VARCHAR COLLATE nocase' if variant == 'collated' else
                  'relative VARCHAR,sha256 VARCHAR')
        connection.execute('CREATE TABLE lake.source_metadata('+fields+',bytes BIGINT,content_utf8 VARCHAR)')
        rows = [('heads/a', str(i), len(('中文'+str(i)).encode()), '中文'+str(i)) for i in range(12)]
        if variant == 'duplicate': rows.append(('heads/a', '1', 7, '不同內容'))
        elif variant == 'null': rows.append((None, None, None, None))
        elif variant == 'negative': rows.append(('heads/b', '0', -1, 'raw'))
        elif variant == 'empty': rows = []
        elif variant == 'reordered': rows.extend([('a', 'z', 1, 'a'), ('z', 'a', 1, 'z')])
        elif variant == 'collated': rows.append(('HEADS/A', '1', 1, 'case-insensitive ordering tie'))
        if rows:
            connection.executemany('INSERT INTO lake.source_metadata VALUES (?,?,?,?)', rows)
        original = connection.execute('SELECT * FROM lake.source_metadata ORDER BY ALL').fetchall()
        queries = []
        class TrackedConnection:
            def execute(self, query, *args):
                queries.append(query)
                connection.execute(query, *args)
                return self
            def __getattr__(self, name):
                return getattr(connection, name)
        observed = list(paged_metadata_rows(TrackedConnection(), maximum_bytes=16, maximum_rows=2,
                                           single_pass=mode != 'paged'))
        assert observed == original
        assert canonical_row_digest(observed) == (len(original), identity_sha256(original))
        if variant == 'unique':
            pages = sum('WHERE relative' in query for query in queries)
            assert (pages == 0) if mode == 'single_pass' else (pages > 1)
    finally:
        connection.close()


@pytest.mark.parametrize('sql,available,expected', [
    ('16.0 EiB', 64*1024**3, True), ('16.0 GiB', 64*1024**3, True),
    ('512.0 MiB', 64*1024**3, False), ('16.0 EiB', 8*1024**3, False),
    ('invalid', 64*1024**3, False), (None, 64*1024**3, False),
    ('16.0 EiB', None, False)])
def test_metadata_single_pass_needs_current_sql_and_host_memory(sql, available, expected):
    from stockagent.control import lakehouse
    assert lakehouse._metadata_sort_fits_memory(1024**3, 100, sql, available) is expected


@pytest.mark.parametrize('table', ['dataset_releases', 'source_objects', 'catalog_captures', 'source_metadata'])
def test_shared_export_and_restore_digest_preserves_actual_sql_rows(table):
    import duckdb
    from stockagent.control import lakehouse
    connection = duckdb.connect()
    try:
        connection.execute("ATTACH ':memory:' AS lake")
        if table == 'source_metadata':
            connection.execute('CREATE TABLE lake.source_metadata(relative VARCHAR,sha256 VARCHAR,bytes BIGINT,content_utf8 VARCHAR)')
            connection.executemany('INSERT INTO lake.source_metadata VALUES (?,?,?,?)', [
                ('heads/a', 'b', 7, '完整中文'), ('heads/a', 'a', 2, 'raw'), ('heads/z', 'c', 3, 'end')])
        else:
            connection.execute('CREATE TABLE lake.'+table+'(name VARCHAR,number DOUBLE,active BOOLEAN)')
            connection.executemany('INSERT INTO lake.'+table+' VALUES (?,?,?)', [
                ('中文', -0.0, True), ('raw', 1e-20, False), (None, None, None)])
        original = identity_sha256(connection.execute('SELECT * FROM lake.'+table+' ORDER BY ALL').fetchall())
        assert lakehouse._registered_table_digest(connection, table) == original
        with pytest.raises(ValueError, match='unexpected'):
            lakehouse._registered_table_digest(connection, 'unregistered')
    finally:
        connection.close()


@pytest.fixture
def registry_digest_oracle():
    import duckdb
    connection = duckdb.connect()
    connection.execute("ATTACH ':memory:' AS lake")
    names = ('dataset_releases', 'source_objects', 'catalog_captures')
    for name in names:
        connection.execute('CREATE TABLE lake.'+name+'(name VARCHAR,value DOUBLE,active BOOLEAN)')
        connection.executemany('INSERT INTO lake.'+name+' VALUES (?,?,?)', [('raw', -0.0, True), ('中文', 1e-10, False)])
    connection.execute('CREATE TABLE lake.source_metadata(relative VARCHAR,sha256 VARCHAR,bytes BIGINT,content_utf8 VARCHAR)')
    connection.execute("INSERT INTO lake.source_metadata VALUES ('heads/a','sha',3,'raw')")
    expected = {name: identity_sha256(connection.execute('SELECT * FROM lake.'+name+' ORDER BY ALL').fetchall())
                for name in (*names, 'source_metadata')}

    class Registry:
        def execute(self, sql, *args):
            if 'ducklake_snapshots' in sql:
                return SimpleNamespace(fetchone=lambda: (7,))
            if sql.startswith(('SET ', 'LOAD ', 'ATTACH ')):
                return self
            return connection.execute(sql, *args)
        def __getattr__(self, name):
            return getattr(connection, name)
    yield Registry(), expected
    connection.close()


def test_original_export_path_uses_every_shared_digest_before_any_publication(
        tmp_path, monkeypatch, registry_digest_oracle):
    from stockagent.control import lakehouse
    from stockagent.data_sync import catalog_incremental
    registry, expected = registry_digest_oracle
    original, observed = lakehouse._registered_table_digest, {}
    def record(connection, name):
        observed[name] = original(connection, name)
        return observed[name]
    class BeforePayloadPublication(Exception):
        pass
    def forbid_copy(*a, **kw):
        raise BeforePayloadPublication()
    state, root = tmp_path/'state', tmp_path/'lake'
    state.mkdir(); root.mkdir()
    monkeypatch.setattr(lakehouse, 'guard', lambda c: None)
    monkeypatch.setattr(lakehouse, 'connect', lambda *a, **kw: registry)
    monkeypatch.setattr(lakehouse, '_registered_table_digest', record)
    monkeypatch.setattr(catalog_incremental, 'copy_catalog_data', forbid_copy)
    with pytest.raises(BeforePayloadPublication):
        lakehouse.export({'state_root': str(state), 'lake_root': str(root)}, 7)
    assert observed == expected


@pytest.mark.parametrize('damage', ['none', 'row_hash', 'snapshot'])
def test_original_restore_validator_uses_every_shared_digest_and_retains_rejection(
        tmp_path, monkeypatch, registry_digest_oracle, damage):
    import duckdb
    from downloader.artifact_io import atomic_write_json
    from stockagent.control import lakehouse, recovery
    registry, expected = registry_digest_oracle
    root = tmp_path/'source'; root.mkdir()
    recorded = dict(expected)
    if damage == 'row_hash': recorded['source_metadata'] = '0'*64
    snapshot = 8 if damage == 'snapshot' else 7
    atomic_write_json(root/'catalog-state.json', {'snapshot_id': snapshot, 'table_row_sha256': recorded})
    monkeypatch.setattr(lakehouse, 'verify', lambda p: {'context': {
        'kind': 'ducklake_catalog_and_data', 'ducklake_snapshot_id': snapshot}})
    monkeypatch.setattr(duckdb, 'connect', lambda: registry)
    monkeypatch.setattr(recovery, 'restore_control', lambda *a, validator, **kw:
        validator({'dbname': 'private_test', 'host': 'localhost', 'port': 1, 'user': 'test'}))
    original, observed = lakehouse._registered_table_digest, {}
    def record(connection, name):
        observed[name] = original(connection, name)
        return observed[name]
    monkeypatch.setattr(lakehouse, '_registered_table_digest', record)
    def run():
        return lakehouse.restore_lake_delivery(root, tmp_path/'scratch', extensions=tmp_path, pg_bin=tmp_path)
    if damage == 'none':
        result = run()
        assert result['ducklake_all_registered_rows_verified'] and result['ducklake_snapshot_id'] == 7
    else:
        with pytest.raises(ValueError, match='version/rows differ'):
            run()
    assert observed == expected
