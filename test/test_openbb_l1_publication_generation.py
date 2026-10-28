"""Generation reuse retains complete publication, source and reader fences."""
import json
import sqlite3

import duckdb
import pytest

from scripts import compact_openbb_l1 as c
from scripts.openbb_l1_view_publication_proof import KEY
from test_openbb_l1_compaction import _publish_tasks, _sec_grouped_args


def _seed(root):
    _publish_tasks(root, ['aa', 'bb'], endpoint=c.SCHEMA_GROUPED_ENDPOINT)
    args = [*_sec_grouped_args(root), '--incremental-view-publication']
    assert c.run(args) == 0
    return args, root/'openbb_l1.duckdb', root/'_state/openbb_archive.sqlite3'


def _audit(root):
    return json.loads((root/'_state/l1_compaction_attempt_latest.json').read_text())['view_publication_proof']


def test_warm_generation_opens_no_published_duckdb_and_never_rewrites(tmp_path, monkeypatch):
    args, path, manifest = _seed(tmp_path)
    before = c._view_file_signature(path), path.read_bytes()
    connect = c.duckdb.connect
    opened = []

    def checked(database, *positional, **kwargs):
        opened.append(str(database))
        assert str(database) == ':memory:'
        return connect(database, *positional, **kwargs)

    monkeypatch.setattr(c.duckdb, 'connect', checked)
    assert c.run(args) == 0
    assert opened == [':memory:']
    assert (c._view_file_signature(path), path.read_bytes()) == before
    assert _audit(tmp_path)['state'] == 'reused_complete_publication'
    assert _audit(tmp_path)['proofs_written'] == 0


@pytest.mark.parametrize('mutation', ['sql', 'totals', 'proof', 'contract', 'age', 'force'])
def test_publication_generation_invalidates_every_changed_boundary(tmp_path, mutation):
    args, path, manifest = _seed(tmp_path)
    with duckdb.connect(str(path), read_only=True) as database:
        expected = database.execute('SELECT * FROM l1_catalog').fetchall()
        name = expected[0][1]
        expected_sql = database.execute('SELECT sql FROM duckdb_views() WHERE view_name=?', [name]).fetchone()[0]
    if mutation == 'sql':
        with duckdb.connect(str(path)) as database:
            database.execute(f'CREATE OR REPLACE VIEW {name} AS SELECT 999 AS forged')
    elif mutation == 'totals':
        with duckdb.connect(str(path)) as database:
            database.execute('UPDATE l1_catalog SET rows=999')
    elif mutation == 'force':
        args = [*args, '--force-full-view-publication']
    else:
        with sqlite3.connect(manifest) as connection:
            if mutation == 'proof':
                connection.execute('UPDATE archive_meta SET value=? WHERE key=?', ('{}', KEY))
            else:
                import hashlib
                envelope = json.loads(connection.execute('SELECT value FROM archive_meta WHERE key=?', (KEY,)).fetchone()[0])
                body = json.loads(envelope['body'])
                if mutation == 'age':body[3] = 0
                else:body[2]['duckdb'] = 'changed-reader'
                envelope['body'] = json.dumps(body)
                envelope['sha256'] = hashlib.sha256(envelope['body'].encode()).hexdigest()
                connection.execute('UPDATE archive_meta SET value=? WHERE key=?', (json.dumps(envelope), KEY))
    assert c.run(args) == 0
    assert _audit(tmp_path)['state'] == 'complete_validation_recorded'
    assert _audit(tmp_path)['proofs_written'] == 1
    with duckdb.connect(str(path), read_only=True) as database:
        assert database.execute('SELECT * FROM l1_catalog').fetchall() == expected
        assert database.execute('SELECT sql FROM duckdb_views() WHERE view_name=?', [name]).fetchone()[0] == expected_sql


def test_generation_proof_does_not_hide_source_mutation(tmp_path, monkeypatch):
    args, path, manifest = _seed(tmp_path)
    check = c._verify_view_sources
    calls = 0

    def changed(signatures):
        nonlocal calls
        calls += 1
        if calls == 2:
            source = next(iter(signatures))
            import os
            value = source.stat()
            os.utime(source, ns=(value.st_atime_ns, value.st_mtime_ns+1))
        return check(signatures)

    monkeypatch.setattr(c, '_verify_view_sources', changed)
    with pytest.raises(RuntimeError, match='source changed'):
        c.run(args)
    assert calls == 2


def test_generation_proof_does_not_hide_reader_policy_drift(tmp_path, monkeypatch):
    args, path, manifest = _seed(tmp_path)
    original = c._schema_group_reader_policy
    calls = 0

    def changed(database):
        nonlocal calls
        calls += 1
        policy = original(database)
        if calls > 1:policy = {**policy, 'changed': True}
        return policy

    monkeypatch.setattr(c, '_schema_group_reader_policy', changed)
    with pytest.raises(RuntimeError, match='reader policy changed'):
        c.run(args)


def test_force_publication_requires_incremental_flag():
    with pytest.raises(SystemExit):
        c.parse_args(['--force-full-view-publication'])
