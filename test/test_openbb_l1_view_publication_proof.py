import json
import sqlite3

import pytest

from scripts.openbb_l1_view_publication_proof import INTERVAL_NS, KEY, ViewPublicationProof, identity


@pytest.fixture
def proof(tmp_path):
    connection = sqlite3.connect(':memory:')
    connection.execute('CREATE TABLE archive_meta(key TEXT PRIMARY KEY,value TEXT)')
    path = tmp_path/'catalog.duckdb'
    path.write_bytes(b'fixture; caller must separately validate SQL before recording')
    witness = ViewPublicationProof(connection,path,{'reader':'fixed'},now_ns=100)
    rows = [('e','view','published',None,1,1,1,2,3,4,'clock','a'*64,'b'*64)]
    witness.record(rows,expected_identity=identity(path))
    return connection,path,witness,rows


@pytest.mark.parametrize('mutation',['file','replace','contract','expired','future','envelope','body','missing','wal'])
def test_view_proof_requires_every_identity_and_integrity_boundary(proof, mutation):
    connection,path,witness,rows = proof
    if mutation=='file':path.write_bytes(b'changed')
    elif mutation=='replace':
        old = path.with_suffix('.new')
        old.write_bytes(path.read_bytes())
        old.replace(path)
    elif mutation=='contract':witness.contract={'reader':'changed'}
    elif mutation=='expired':witness.now_ns += INTERVAL_NS
    elif mutation=='future':witness.now_ns -= 1
    elif mutation=='missing':connection.execute('DELETE FROM archive_meta')
    elif mutation=='wal':path.with_name(path.name+'.wal').write_bytes(b'new SQL not in the witnessed main inode')
    elif mutation=='envelope':connection.execute('UPDATE archive_meta SET value=?',(json.dumps({'bad':True}),))
    else:
        envelope = json.loads(connection.execute('SELECT value FROM archive_meta').fetchone()[0])
        envelope['body'] += ' '
        connection.execute('UPDATE archive_meta SET value=?',(json.dumps(envelope),))
    assert witness.read() is None


def test_valid_view_proof_does_no_writes_and_keeps_exact_catalog_rows(proof):
    connection,path,witness,rows = proof
    before = connection.total_changes
    assert witness.read()==(identity(path),rows)
    assert connection.total_changes==before


def test_recording_catalog_drift_never_mints_a_new_proof(proof):
    connection,path,witness,rows = proof
    old = connection.execute('SELECT value FROM archive_meta').fetchone()[0]
    before = identity(path)
    path.write_bytes(b'changed')
    with pytest.raises(RuntimeError,match='changed after complete validation'):
        witness.record(rows,expected_identity=before)
    assert connection.execute('SELECT value FROM archive_meta').fetchone()[0]==old
