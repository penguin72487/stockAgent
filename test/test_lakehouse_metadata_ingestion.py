"""New metadata is streamed without bypassing validation of known values."""
import hashlib
import fcntl
from pathlib import Path
import threading

import pytest

pa = pytest.importorskip('pyarrow')
duckdb = pytest.importorskip('duckdb')
from stockagent.control import lakehouse
from stockagent.runtime_identity import identity_sha256


@pytest.fixture
def captured(tmp_path):
    files = []
    for number in range(12):
        raw = ('原始值 '+str(number)+'\n').encode()
        relative = 'manifests/'+str(number)+'.json'
        path = tmp_path / relative
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(raw)
        files.append({'relative': relative, 'sha256': hashlib.sha256(raw).hexdigest(),
                      'bytes': len(raw), 'role': 'cold_metadata'})
    return {'cold_root': str(tmp_path)}, files


def test_known_values_still_read_and_validated_without_retaining_payload(captured, monkeypatch):
    c, files = captured
    known = {(row['relative'], row['sha256']) for row in files[:10]}
    read = lakehouse._metadata_value
    observed = []
    def counted(config, row):
        observed.append(row['relative'])
        return read(config, row)
    monkeypatch.setattr(lakehouse, '_metadata_value', counted)
    incoming = lakehouse._new_metadata_descriptors(c, files, known)
    assert incoming == files[10:] and incoming[0] is files[10]
    assert observed == [row['relative'] for row in files]
    assert all('content_utf8' not in row for row in files)


def test_parallel_metadata_validation_overlaps_reads_and_keeps_original_order(captured, monkeypatch):
    c, files = captured
    known = {(row['relative'], row['sha256']) for row in files[:4]}
    serial = lakehouse._new_metadata_descriptors(c, files, known)
    original = lakehouse._metadata_value
    barrier = threading.Barrier(4, timeout=5)
    lock = threading.Lock()
    active = 0
    peak = 0

    def concurrent(config, row):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            if row in files[:4]:
                barrier.wait()
            return original(config, row)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(lakehouse, '_metadata_value', concurrent)
    assert lakehouse._new_metadata_descriptors(c, files, known, workers=4) == serial
    assert peak == 4
    assert all('content_utf8' not in row for row in files)


@pytest.mark.parametrize('kind', ['wrong_sha', 'wrong_length', 'invalid_utf8'])
def test_parallel_validation_does_not_skip_corrupt_known_metadata(captured, kind):
    c, files = captured
    if kind == 'wrong_sha':
        files[2]['sha256'] = '0' * 64
    elif kind == 'wrong_length':
        files[2]['bytes'] += 1
    else:
        raw = b'\xff'
        (Path(c['cold_root']) / files[2]['relative']).write_bytes(raw)
        files[2].update(bytes=1, sha256=hashlib.sha256(raw).hexdigest())
    known = {(row['relative'], row['sha256']) for row in files}
    with pytest.raises((ValueError, UnicodeDecodeError)):
        lakehouse._new_metadata_descriptors(c, files, known, workers=4)


@pytest.mark.parametrize('workers', [0, -1, 5, True, 1.5])
def test_metadata_validation_rejects_invalid_worker_count(captured, workers):
    c, files = captured
    with pytest.raises(ValueError, match='metadata verification workers'):
        lakehouse._new_metadata_descriptors(c, files, set(), workers=workers)


def test_real_catalog_lock_contention_has_a_distinct_busy_reason(tmp_path, monkeypatch):
    monkeypatch.setattr(lakehouse, 'guard', lambda _config: None)
    with (tmp_path / 'catalog-owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(lakehouse.CatalogOwnerBusy, match='catalog owner is busy'):
            lakehouse.register({'state_root': str(tmp_path)})


@pytest.mark.parametrize('kind', ['wrong_sha', 'wrong_length', 'invalid_utf8'])
def test_existing_key_does_not_bypass_corruption(captured, kind):
    c, files = captured
    if kind == 'wrong_sha':
        files[0]['sha256'] = '0'*64
    elif kind == 'wrong_length':
        files[0]['bytes'] += 1
    else:
        raw = b'\xff'
        (Path(c['cold_root'])/files[0]['relative']).write_bytes(raw)
        files[0].update(bytes=1, sha256=hashlib.sha256(raw).hexdigest())
    known = {(row['relative'], row['sha256']) for row in files}
    with pytest.raises((ValueError, UnicodeDecodeError)):
        lakehouse._new_metadata_descriptors(c, files, known)


def test_captured_head_bytes_are_not_replaced_by_moving_latest(captured):
    c, files = captured
    first = files[0]
    path = Path(c['cold_root']) / first['relative']
    original = path.read_text()
    first['captured_bytes_utf8'] = original
    path.write_text('newer genuine head')
    selected = lakehouse._new_metadata_descriptors(c, [first], set())
    assert lakehouse._metadata_arrow_reader(c, selected).read_all().to_pylist()[0]['content_utf8'] == original
    assert path.read_text() == 'newer genuine head'


def test_actual_arrow_batches_are_bounded_and_lazy(captured, monkeypatch):
    c, files = captured
    observed = []
    read = lakehouse._metadata_value
    def counted(config, row):
        observed.append(row['relative'])
        return read(config, row)
    monkeypatch.setattr(lakehouse, '_metadata_value', counted)
    reader = lakehouse._metadata_arrow_reader(c, files, maximum_bytes=1000, maximum_rows=2)
    assert observed == []
    first = reader.read_next_batch()
    assert len(first) == 2 and len(observed) == 2
    batches = [first, *reader]
    assert all(len(batch) <= 2 for batch in batches)
    assert sum(len(batch) for batch in batches) == len(files)


def test_byte_bound_and_one_indivisible_oversized_row(captured):
    c, files = captured
    limit = files[0]['bytes']+files[1]['bytes']-1
    batches = list(lakehouse._metadata_arrow_reader(c, files, maximum_bytes=limit))
    assert all(sum(batch['bytes'].to_pylist()) <= limit for batch in batches)
    assert all(len(batch) == 1 for batch in lakehouse._metadata_arrow_reader(c, files, maximum_bytes=1))


@pytest.mark.parametrize('bounds', [(0, 1), (1, 0), (-1, 2)])
def test_nonpositive_arrow_bounds_rejected(captured, bounds):
    c, files = captured
    with pytest.raises(ValueError, match='positive batch'):
        lakehouse._metadata_arrow_reader(c, files, maximum_bytes=bounds[0], maximum_rows=bounds[1])


@pytest.mark.parametrize('known_count', [0, 5, 12])
def test_full_sql_merge_and_canonical_rows_match_original(captured, known_count):
    c, files = captured
    con = duckdb.connect()
    try:
        con.execute("ATTACH ':memory:' AS lake")
        con.execute('CREATE TABLE lake.source_metadata(relative VARCHAR,sha256 VARCHAR,bytes BIGINT,content_utf8 VARCHAR)')
        original = [lakehouse._metadata_value(c, row) for row in files]
        initial = original[:known_count]
        if initial:
            con.register('seed', pa.Table.from_pylist(initial))
            con.execute('INSERT INTO lake.source_metadata SELECT * FROM seed')
        keys = set(con.execute('SELECT relative,sha256 FROM lake.source_metadata').fetchall())
        incoming = lakehouse._new_metadata_descriptors(c, files, keys)
        con.register('incoming_metadata', lakehouse._metadata_arrow_reader(c, incoming, maximum_rows=2))
        con.execute('BEGIN')
        con.execute('INSERT INTO lake.source_metadata SELECT i.* FROM incoming_metadata i '
                    'WHERE NOT EXISTS (SELECT 1 FROM lake.source_metadata o '
                    'WHERE o.relative=i.relative AND o.sha256=i.sha256)')
        con.execute('COMMIT')
        rows = con.execute('SELECT * FROM lake.source_metadata ORDER BY ALL').fetchall()
        con.register('original', pa.Table.from_pylist(original))
        expected = con.execute('SELECT * FROM original ORDER BY ALL').fetchall()
        assert rows == expected
        assert lakehouse._registered_table_digest(con, 'source_metadata') == identity_sha256(expected)
    finally:
        con.close()


def test_late_source_mutation_aborts_entire_transaction(captured):
    c, files = captured
    incoming = lakehouse._new_metadata_descriptors(c, files, set())
    (Path(c['cold_root'])/files[-1]['relative']).write_bytes(b'changed after admission')
    con = duckdb.connect()
    try:
        con.execute('CREATE TABLE metadata(relative VARCHAR,sha256 VARCHAR,bytes BIGINT,content_utf8 VARCHAR)')
        con.register('incoming', lakehouse._metadata_arrow_reader(c, incoming, maximum_rows=1))
        con.execute('BEGIN')
        with pytest.raises(duckdb.Error, match='source metadata changed'):
            con.execute('INSERT INTO metadata SELECT * FROM incoming')
        con.execute('ROLLBACK')
        assert con.execute('SELECT count(*) FROM metadata').fetchone()[0] == 0
    finally:
        con.close()


def test_registration_comparator_refuses_unrelated_hash_or_source_scope_changes():
    from scripts.benchmark_lakehouse_metadata_ingestion import validate_scope
    reference = 'def register():\n return 1\ndef guard():\n return 2\n'
    candidate = ('def register():\n return 3\ndef guard():\n return 2\n'
                 'def _metadata_value():\n return 4\n')
    validate_scope(reference, candidate)
    with pytest.raises(ValueError, match='unrelated source'):
        validate_scope(reference, candidate.replace('return 2', 'return 5'))

