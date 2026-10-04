"""Actual PostgreSQL concurrency/failure acceptance in disposable, isolated schemas."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

import pytest

from stockagent.control.contracts import WorkSpec
from stockagent.control.postgres import ControlStore, LeaseLost
from stockagent.control.worker import execute_one
from stockagent.runtime_identity import identity_sha256


class PrivateDSN(str):
    def __repr__(self):
        return '<private control-test DSN>'


@pytest.fixture
def db():
    dsn = os.environ.get('CONTROL_PLANE_TEST_DSN')
    if not dsn:
        pytest.skip('explicit control-role PostgreSQL integration environment is not selected')
    dsn = PrivateDSN(dsn)
    schema = 'control_test_' + uuid.uuid4().hex
    with ControlStore(dsn, schema=schema) as store:
        store.initialize()
        yield store, dsn, schema
        from psycopg import sql
        store.connection.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


def work(key='check', **values):
    return WorkSpec(key, {'receipt_sha256':'a'*64,'source_sha256':'b'*64}, memory_bytes=100, **values)


def node(store, name='node-a', *, slots=2, memory=200, scratch=100):
    store.register_node(name, cpu_slots=slots, memory_bytes=memory, scratch_bytes=scratch,
                        observation={'scope':'synthetic integration capacity; not market observations'})


def proof(claim):
    return {'receipt_sha256':claim.spec.inputs['receipt_sha256'],
            'source_sha256':claim.spec.inputs['source_sha256'], 'source_files_verified':True}


def test_duplicate_submit_is_idempotent_but_new_source_is_rejected(db):
    store, _, _ = db
    spec = work()
    assert store.submit(spec)['attempt'] == 0
    assert store.submit(spec)['attempt'] == 0
    different = replace(spec, inputs={'receipt_sha256':'a'*64,'source_sha256':'c'*64})
    with pytest.raises(ValueError, match='incompatible'): store.submit(different)
    assert len(store.snapshot()['jobs']) == 1


@pytest.mark.parametrize('version', [None, 2])
def test_each_deployed_reader_rejects_missing_or_unknown_stored_contract(db, version):
    store, dsn, schema = db
    store.submit(work())
    store.connection.execute('DELETE FROM contract_version')
    if version is not None:
        store.connection.execute('INSERT INTO contract_version VALUES (%s)', (version,))
    with pytest.raises(ValueError, match='stored control contract'):
        with ControlStore(dsn, schema=schema) as deployed:
            node(deployed, 'must-not-write')
    # Failing admission does not relabel/delete work or write a node row.
    assert store.connection.execute('SELECT state,attempt FROM jobs').fetchall() == [{'state':'pending','attempt':0}]
    assert store.connection.execute('SELECT node_id FROM nodes').fetchall() == []


def test_existing_work_without_version_table_is_not_a_fresh_catalog(db):
    store, dsn, schema = db
    store.submit(work())
    store.connection.execute('DROP TABLE contract_version')
    with pytest.raises(ValueError, match='missing stored control contract'):
        with ControlStore(dsn, schema=schema) as deployed:
            deployed.initialize()
    assert store.connection.execute('SELECT state,attempt FROM jobs').fetchall() == [{'state':'pending','attempt':0}]
    assert store.connection.execute('SELECT to_regclass(%s) AS version_table',
                                    (schema+'.contract_version',)).fetchone()['version_table'] is None


@pytest.mark.parametrize('changed_version', [None, 2, 'drop_table'])
@pytest.mark.parametrize('operation', ['node','submit','claim','expire','heartbeat','finish','fail','snapshot','initialize'])
def test_existing_connection_rechecks_contract_before_every_operation(db,changed_version,operation):
    store,_,_=db;node(store);store.submit(work())
    claim=store.claim('node-a','original',lease_seconds=30)
    before=store.connection.execute('SELECT state,attempt,result FROM jobs').fetchall()
    if changed_version=='drop_table':store.connection.execute('DROP TABLE contract_version')
    elif changed_version is None:store.connection.execute('DELETE FROM contract_version')
    else:store.connection.execute('UPDATE contract_version SET version=%s',(changed_version,))
    operations={'node':lambda:node(store,'must-not-write'),'submit':lambda:store.submit(work('must-not-write')),
        'claim':lambda:store.claim('node-a','must-not-write'),'expire':store.expire_leases,
        'heartbeat':lambda:store.heartbeat(claim),'finish':lambda:store.finish(claim,proof(claim)),
        'fail':lambda:store.fail(claim,'must-not-write'),'snapshot':store.snapshot,'initialize':store.initialize}
    with pytest.raises(ValueError,match='stored control contract'):operations[operation]()
    assert store.connection.execute('SELECT state,attempt,result FROM jobs').fetchall()==before
    assert store.connection.execute('SELECT node_id FROM nodes').fetchall()==[{'node_id':'node-a'}]


def test_contract_update_waits_until_the_admitted_operation_finishes(db):
    import psycopg
    from psycopg import sql

    store,dsn,schema=db;store.submit(work())
    with psycopg.connect(dsn,autocommit=True) as migration:
        pid=migration.execute('SELECT pg_backend_pid()').fetchone()[0]
        statement=sql.SQL('UPDATE {}.contract_version SET version=2').format(sql.Identifier(schema))
        with ThreadPoolExecutor(max_workers=1) as workers:
            with store._transaction():
                pending=workers.submit(migration.execute,statement)
                deadline=time.monotonic()+3
                waiting=False
                while time.monotonic()<deadline:
                    waiting=store.connection.execute('''SELECT EXISTS (
                        SELECT 1 FROM pg_locks WHERE pid=%s AND NOT granted) AS waiting''',(pid,)).fetchone()['waiting']
                    if waiting:break
                    time.sleep(0.01)
                assert waiting and not pending.done()
                store.connection.execute("UPDATE jobs SET priority=priority+1 WHERE key='check'")
            pending.result(timeout=3)
        # The old connected worker cannot write again after the migration commit.
        with pytest.raises(ValueError,match='stored control contract'):store.submit(work('after-version-change'))
        assert store.connection.execute("SELECT priority FROM jobs WHERE key='check'").fetchone()=={'priority':1}


def test_dependency_claims_follow_actual_completed_proof(db):
    store, _, _ = db; node(store)
    store.submit(work('parent'))
    store.submit(work('dependent', priority=100, dependencies=('parent',)))
    first = store.claim('node-a','worker-1')
    assert first.spec.key == 'parent'
    assert store.claim('node-a','worker-2') is None
    with pytest.raises(ValueError, match='evidence'): store.finish(first, {'source_files_verified':True})
    store.finish(first, proof(first))
    assert store.claim('node-a','worker-2').spec.key == 'dependent'


def test_missing_dependency_rolls_back_new_job_and_cannot_form_cycle(db):
    store, _, _ = db
    with pytest.raises(ValueError, match='dependency'): store.submit(work(dependencies=('absent',)))
    assert store.snapshot()['jobs'] == []


def test_multiple_nodes_cannot_claim_the_same_work(db):
    store, dsn, schema = db
    for name in ['node-a','node-b','node-c','node-d']: node(store,name)
    store.submit(work())
    def take(index):
        with ControlStore(dsn, schema=schema) as peer:
            return peer.claim(f'node-{chr(97+index%4)}', f'worker-{index}')
    with ThreadPoolExecutor(max_workers=8) as workers:
        claims = list(workers.map(take, range(16)))
    assert len([r for r in claims if r is not None]) == 1
    assert len(store.snapshot()['attempts']) == 1


def test_resource_admission_serializes_concurrent_workers_on_one_node(db):
    store, dsn, schema = db; node(store,slots=2,memory=200)
    for index in range(10): store.submit(work(f'check-{index}'))
    def take(index):
        with ControlStore(dsn, schema=schema) as peer:
            return peer.claim('node-a',f'worker-{index}')
    with ThreadPoolExecutor(max_workers=8) as workers:
        claims = [r for r in workers.map(take,range(12)) if r is not None]
    assert len(claims) == 2
    store.finish(claims[0],proof(claims[0]))
    assert store.claim('node-a','worker-next') is not None


def test_expired_owner_cannot_heartbeat_complete_or_fail_after_reassignment(db):
    store, dsn, schema = db; node(store)
    store.submit(work())
    old = store.claim('node-a','old-worker',lease_seconds=1)
    # Force a past SERVER lease in the isolated test schema; no fake market timestamps.
    store.connection.execute("UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE key=%s",(old.spec.key,))
    with ControlStore(dsn,schema=schema) as successor:
        new = successor.claim('node-a','new-worker')
    assert new.attempt == 2 and new.token != old.token
    for operation in [lambda:store.heartbeat(old),lambda:store.finish(old,proof(old)),lambda:store.fail(old,'old error')]:
        with pytest.raises(LeaseLost): operation()
    store.finish(new,proof(new))
    assert [r['state'] for r in store.snapshot()['attempts']] == ['expired','succeeded']


def test_server_clock_is_rechecked_after_waiting_for_a_row_lock(db):
    store, dsn, schema = db; node(store); store.submit(work())
    claim = store.claim('node-a','owner',lease_seconds=1)
    with ThreadPoolExecutor(max_workers=1) as workers:
        with ControlStore(dsn,schema=schema) as blocker:
            with blocker.connection.transaction():
                blocker.connection.execute('SELECT 1 FROM jobs WHERE key=%s FOR UPDATE',(claim.spec.key,))
                pending = workers.submit(store.finish,claim,proof(claim))
                time.sleep(1.1)
        with pytest.raises(LeaseLost): pending.result(timeout=4)


def test_retry_delay_and_attempt_limit_preserve_failed_attempts(db):
    store, _, _ = db; node(store); store.submit(work())
    first = store.claim('node-a','first')
    assert store.fail(first,'bounded original failure',retry_delay_seconds=30) == 'pending'
    assert store.claim('node-a','too-early') is None
    store.connection.execute("UPDATE jobs SET ready_at=clock_timestamp() WHERE key='check'")
    second = store.claim('node-a','second')
    assert store.fail(second,'second failure',retry_delay_seconds=0) == 'failed'
    assert store.claim('node-a','third') is None
    attempts = store.snapshot()['attempts']
    assert len(attempts)==2 and all(r['state']=='failed' for r in attempts)
    assert attempts[0]['result']['reason']=='bounded original failure'


def test_stale_node_or_insufficient_memory_disk_rejects_admission(db):
    store, _, _ = db; node(store,memory=50,scratch=0); store.submit(work(scratch_bytes=1))
    assert store.claim('node-a','worker') is None
    store.connection.execute("UPDATE nodes SET expires_at=clock_timestamp()-interval '1 second'")
    with pytest.raises(ValueError,match='fresh'): store.claim('node-a','worker')


def test_aging_allows_waiting_low_priority_work_to_progress(db):
    store, _, _ = db; node(store)
    store.submit(work('old',priority=0)); store.submit(work('new',priority=100))
    store.connection.execute("UPDATE jobs SET ready_at=clock_timestamp()-interval '101 minutes' WHERE key='old'")
    assert store.claim('node-a','worker').spec.key=='old'


def code_release(tmp_path):
    root=tmp_path/'frozen';root.mkdir();(root/'example.py').write_text('verified original code\n')
    files={'example.py':hashlib.sha256((root/'example.py').read_bytes()).hexdigest()}
    source_sha=identity_sha256(files)
    receipt=tmp_path/'release.json'
    receipt.write_text(json.dumps({'schema_version':1,'state':'built','code':{'files':files,'source_sha256':source_sha},
                                    'source_bundle':{'files':files,'sha256':'a'*64}}))
    return root,receipt,{'receipt_sha256':hashlib.sha256(receipt.read_bytes()).hexdigest(),'source_sha256':source_sha}


def test_worker_calls_canonical_verifier_and_keeps_attempt_receipt(db,tmp_path):
    store,dsn,schema=db
    root,receipt,inputs=code_release(tmp_path);store.submit(WorkSpec('verify',inputs))
    result=execute_one(dsn,schema=schema,node_id='actual-node',worker_id='actual-worker',root=root,
                       receipt=receipt,output=tmp_path/'receipts')
    assert result['state']=='succeeded' and result['result']['source_files_verified'] is True
    assert store.snapshot()['jobs'][0]['state']=='succeeded'
    assert len(list((tmp_path/'receipts').glob('*.json')))==1
    assert not result['result']['data_publication']


def test_worker_rejects_mutated_code_and_does_not_rewrite_it(db,tmp_path):
    store,dsn,schema=db
    root,receipt,inputs=code_release(tmp_path);store.submit(WorkSpec('verify',inputs,max_attempts=1))
    (root/'example.py').write_text('changed by another writer\n')
    result=execute_one(dsn,schema=schema,node_id='actual-node',worker_id='actual-worker',root=root,
                       receipt=receipt,output=tmp_path/'receipts')
    assert result['state']=='attempt_failed' and result['next_work_state']=='failed'
    assert (root/'example.py').read_text()=='changed by another writer\n'
    assert store.snapshot()['jobs'][0]['result']['reason']=='ValueError'
