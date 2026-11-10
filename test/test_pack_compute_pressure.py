"""Emergency recycling may discard only an exact, fenced disposable pair."""
import io
import hashlib
import json
import os
from contextlib import nullcontext
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

from stockagent.data_sync import pack_compute_cache as cache_module
from stockagent.data_sync import pack_compute_pressure as pressure
from stockagent.data_sync import pack_compute_pressure_control as control
from stockagent.data_sync import packed_verification as worker
from test_packed_verification import pack_bytes


@pytest.fixture
def prepared(tmp_path):
    owner = cache_module.ComputeCache(tmp_path/'cache', worker, 1024**3)
    payload, item, _rows = pack_bytes({'one': b'original data'})
    proof = owner.store(io.BytesIO(payload), item)
    expected = {key: proof[key] for key in ('object_sha256', 'object_bytes', 'worker_sha256',
                                             'cache_worker_sha256', 'computed_at_unix')}
    expected.update(source_verified=True, source_retirement=False, source_generation=[1, 2, 3],
                    source_manifest_sha256='a'*64, source_snapshot_id='fixed-snapshot')
    return owner, item, expected


def test_pressure_reclaims_only_the_locked_exact_pair_with_a_journal(prepared):
    owner, item, expected = prepared
    expected['source_snapshot_id'] = 'local-tw-index-futures-20261104T233329973305168Z-l0-penguin-fixed'
    journal = []
    result = pressure.retire_cached_pair(owner, item, expected, references=lambda *_: False, record=journal.append)
    assert result['removed'] is True and result['source_retirement'] is False and result['nas_acceptance'] is False
    assert [row['state'] for row in journal] == ['prepared', 'removed']
    assert not (owner.root/(item['sha256']+'.zip')).exists()
    assert not (owner.root/(item['sha256']+'.json')).exists()
    assert (owner.root/(item['sha256']+'.lock')).is_file()


def test_pressure_preserves_busy_cache_and_active_key_lock(prepared):
    owner, item, expected = prepared
    result = pressure.retire_cached_pair(owner, item, expected, references=lambda *_: True, record=lambda _: pytest.fail('busy'))
    assert result['state'] == 'busy' and (owner.root/(item['sha256']+'.zip')).exists()
    with owner.lock(item):
        with pytest.raises(BlockingIOError):
            pressure.retire_cached_pair(owner, item, expected, references=lambda *_: False, record=lambda _: None)


def test_pressure_reserve_recovery_records_exact_object_and_keeps_source_scope(prepared, monkeypatch):
    owner, item, expected = prepared
    payload = owner.root/(item['sha256']+'.zip')
    monkeypatch.setattr(pressure.shutil, 'disk_usage', lambda _: SimpleNamespace(
        free=0 if payload.exists() else 2*1024**3))
    request = {'plan':'a'*32, 'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
               'reserve_bytes':1024**3, 'target_free_bytes':2*1024**3,
               'candidates':[{'item':item, 'expected':expected}]}
    result = pressure.relieve(owner, request, references=lambda *_: False)
    assert result['state'] == 'target_reached' and len(result['removed']) == 1
    assert result['source_retirement'] is False and result['nas_acceptance'] is False
    journal = json.loads((owner.root/'.pressure-journal'/(request['plan']+'-'+item['sha256']+'.json')).read_text())
    assert journal['state'] == 'removed' and journal['object_sha256'] == item['sha256']


def test_pressure_rejects_wrong_boot_or_unbounded_plan(prepared):
    owner, item, expected = prepared
    request = {'plan':'a'*32, 'boot_id':'wrong', 'reserve_bytes':1024**3,
               'target_free_bytes':2*1024**3, 'candidates':[{'item':item, 'expected':expected}]}
    with pytest.raises(ValueError, match='boot changed'):
        pressure.relieve(owner, request, references=lambda *_: False)
    assert (owner.root/(item['sha256']+'.zip')).exists()


def test_fixed_remote_program_reclaims_a_private_fixture_without_touching_source(tmp_path):
    repo = tmp_path/'repo'
    cache_root = repo/'service_data'/'pack_compute_cache'
    owner = cache_module.ComputeCache(cache_root, worker, 1024**3)
    payload, item, _ = pack_bytes({'one': b'original remains elsewhere'})
    original = tmp_path/'original.zip'
    original.write_bytes(payload)
    proof = owner.store(io.BytesIO(payload), item)
    expected = {key: proof[key] for key in ('object_sha256', 'object_bytes', 'worker_sha256',
                                             'cache_worker_sha256', 'computed_at_unix')}
    expected.update(source_verified=True, source_retirement=False, source_generation=[1, 2, 3],
                    source_manifest_sha256='a'*64, source_snapshot_id='fixed-snapshot')
    reserve = shutil.disk_usage(cache_root).free + 1024**3
    request = {'plan':'a'*32, 'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
               'reserve_bytes':reserve, 'target_free_bytes':reserve,
               'candidates':[{'item':item, 'expected':expected}]}
    bindir = tmp_path/'bin'
    bindir.mkdir()
    lsof = bindir/'lsof'
    lsof.write_text('#!/bin/sh\nexit 1\n')
    lsof.chmod(0o755)
    env = {**os.environ, 'PATH': str(bindir) + os.pathsep + os.environ.get('PATH', '')}
    process = subprocess.run([sys.executable, '-c', control._remote_program(str(repo), reserve)],
        input=json.dumps(request), text=True, capture_output=True, timeout=30, env=env)
    assert process.returncode == 0, process.stderr
    result = json.loads(process.stdout)
    assert len(result['removed']) == 1 and result['source_retirement'] is False
    assert original.read_bytes() == payload
    assert not (cache_root/(item['sha256']+'.zip')).exists()


@pytest.mark.parametrize('code,stdout,stderr,expected', [
    (0, '123\n', '', True), (1, '', '', False), (2, '', 'denied', None),
    (1, '', 'incomplete process census', None),
])
def test_process_reference_census_fails_closed(monkeypatch, code, stdout, stderr, expected):
    monkeypatch.setattr(pressure.subprocess, 'run', lambda *a, **kw:
        SimpleNamespace(returncode=code, stdout=stdout, stderr=stderr))
    if expected is None:
        with pytest.raises(RuntimeError, match='census unavailable'):
            pressure._referenced(Path('/tmp/payload'), None)
    else:
        assert pressure._referenced(Path('/tmp/payload'), None) is expected


def test_controller_proves_d_generation_and_persists_result(tmp_path, prepared, monkeypatch):
    _owner, item, proof = prepared
    cold = tmp_path/'cold'
    source = cold/'objects'/'packs'/item['sha256'][:2]/(item['sha256']+'.zip')
    source.parent.mkdir(parents=True)
    source.write_bytes((_owner.root/(item['sha256']+'.zip')).read_bytes())
    owner_lock = cold/'.local-state'/'locks'/'publish-retention-global.lock'
    owner_lock.parent.mkdir(parents=True)
    owner_lock.write_bytes(b'')
    info = source.lstat()
    proof.update(source_generation=list(cache_module.signature(info)), inventory_bound=False,
                 acceptance=False, source_retirement=False)
    completed, current = {item['sha256']:proof}, {item['sha256']:info}
    state = tmp_path/'state'
    state.mkdir(mode=0o700)
    monkeypatch.setitem(control.SCOPES, str(cold), 'test-dataset')
    monkeypatch.setattr(control, '_outside_opening', lambda: True)
    monkeypatch.setattr(control, '_check_d_primary_mount', lambda *_: None)
    monkeypatch.setattr(control, '_try_publication_lock', lambda *a, **k: nullcontext())
    monkeypatch.setattr(control, '_fixed_release', lambda *a: {'snapshot_id':'fixed-snapshot',
        'manifest_sha256':'a'*64, 'operation_directory':'/fixed', 'objects':{item['sha256']:item['bytes']}})
    monkeypatch.setattr(control, 'hash_many', lambda paths: {path:hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})
    reply = {'state':'target_reached', 'source_retirement':False, 'nas_acceptance':False,
             'removed':[{'object_sha256':item['sha256'], 'object_bytes':item['bytes'],
                         'released_allocated_bytes':4096}]}
    monkeypatch.setattr(control.subprocess, 'run', lambda *a, **k: SimpleNamespace(stdout=json.dumps(reply)))
    result = control.relieve({'cold_root':str(cold), 'dataset':'test-dataset'}, current, completed,
        {'ssh_profile':'profile', 'remote_spool_reserve_bytes':1024**3},
        SimpleNamespace(boot_id='test', repo='/remote'), state, available=0)
    assert result == reply and source.exists()
    receipt_path = next(path for path in state.glob('pressure-*.json') if len(path.stem) == len('pressure-') + 32)
    receipt = json.loads(receipt_path.read_bytes())
    assert receipt['state'] == 'recorded' and receipt['source_retirement'] is False
    marker = json.loads((state/'pressure-test-dataset.status.json').read_bytes())
    assert marker['state'] == 'recorded'


def test_controller_rejects_changed_source_before_remote_mutation(tmp_path, prepared, monkeypatch):
    _owner, item, proof = prepared
    cold = tmp_path/'cold'
    source = cold/'objects'/'packs'/item['sha256'][:2]/(item['sha256']+'.zip')
    source.parent.mkdir(parents=True)
    source.write_bytes(b'x'*item['bytes'])
    lock = cold/'.local-state'/'locks'/'publish-retention-global.lock'
    lock.parent.mkdir(parents=True)
    lock.write_bytes(b'')
    info = source.lstat()
    proof.update(source_generation=list(cache_module.signature(info)), inventory_bound=False,
                 acceptance=False, source_retirement=False)
    source.write_bytes(b'y'*item['bytes'])
    state = tmp_path/'state'
    state.mkdir(mode=0o700)
    monkeypatch.setitem(control.SCOPES, str(cold), 'test-dataset')
    monkeypatch.setattr(control, '_outside_opening', lambda: True)
    monkeypatch.setattr(control, '_check_d_primary_mount', lambda *_: None)
    monkeypatch.setattr(control, '_try_publication_lock', lambda *a, **k: nullcontext())
    monkeypatch.setattr(control, '_fixed_release', lambda *a: {'snapshot_id':'fixed-snapshot',
        'manifest_sha256':'a'*64, 'operation_directory':'/fixed', 'objects':{item['sha256']:item['bytes']}})
    monkeypatch.setattr(control.subprocess, 'run', lambda *a, **k: pytest.fail('remote mutation started'))
    with pytest.raises(ValueError, match='D source changed'):
        control.relieve({'cold_root':str(cold), 'dataset':'test-dataset'}, {item['sha256']:info},
            {item['sha256']:proof}, {'ssh_profile':'profile', 'remote_spool_reserve_bytes':1024**3},
            SimpleNamespace(boot_id='test', repo='/remote'), state, available=0)


@pytest.mark.parametrize('mismatch', ['unreferenced', 'corrupted'])
def test_controller_refuses_cached_proof_without_fixed_d_bytes(tmp_path, prepared, monkeypatch, mismatch):
    owner, item, proof = prepared
    cold = tmp_path/'cold'
    source = cold/'objects'/'packs'/item['sha256'][:2]/(item['sha256']+'.zip')
    source.parent.mkdir(parents=True)
    source.write_bytes((owner.root/(item['sha256']+'.zip')).read_bytes() if mismatch == 'unreferenced'
                       else b'x'*item['bytes'])
    lock = cold/'.local-state'/'locks'/'publish-retention-global.lock'
    lock.parent.mkdir(parents=True)
    lock.write_bytes(b'')
    info = source.lstat()
    proof.update(source_generation=list(cache_module.signature(info)), inventory_bound=False,
                 acceptance=False, source_retirement=False, cache_hit=True)
    state = tmp_path/'state'
    state.mkdir(mode=0o700)
    monkeypatch.setitem(control.SCOPES, str(cold), 'test-dataset')
    monkeypatch.setattr(control, '_outside_opening', lambda: True)
    monkeypatch.setattr(control, '_check_d_primary_mount', lambda *_: None)
    monkeypatch.setattr(control, '_try_publication_lock', lambda *a, **k: nullcontext())
    monkeypatch.setattr(control, '_fixed_release', lambda *a: {'snapshot_id':'fixed-snapshot',
        'manifest_sha256':'a'*64, 'operation_directory':'/fixed',
        'objects':({} if mismatch == 'unreferenced' else {item['sha256']:item['bytes']})})
    monkeypatch.setattr(control, 'hash_many', lambda paths: {path:hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})
    monkeypatch.setattr(control.subprocess, 'run', lambda *a, **k: pytest.fail('remote mutation started'))
    args = ({'cold_root':str(cold), 'dataset':'test-dataset'}, {item['sha256']:info},
            {item['sha256']:proof}, {'ssh_profile':'profile', 'remote_spool_reserve_bytes':1024**3},
            SimpleNamespace(boot_id='test', repo='/remote'), state)
    if mismatch == 'unreferenced':
        assert control.relieve(*args, available=0)['state'] == 'no_proven_candidates'
    else:
        with pytest.raises(ValueError, match='full SHA differs'):
            control.relieve(*args, available=0)
    assert (owner.root/(item['sha256']+'.zip')).is_file()


def test_unknown_previous_remote_outcome_stops_before_source_or_remote(tmp_path, monkeypatch):
    cold = tmp_path/'cold'
    state = tmp_path/'state'
    cold.mkdir(); state.mkdir()
    (state/'pressure-test-dataset.status.json').write_text(json.dumps({'state':'outcome_unknown'}))
    monkeypatch.setitem(control.SCOPES, str(cold), 'test-dataset')
    monkeypatch.setattr(control, '_outside_opening', lambda: True)
    monkeypatch.setattr(control, '_check_d_primary_mount', lambda *_: pytest.fail('must not proceed'))
    result = control.relieve({'cold_root':str(cold), 'dataset':'test-dataset'}, {}, {},
        {'remote_spool_reserve_bytes':1024**3}, SimpleNamespace(boot_id='boot'), state, available=0)
    assert result == {'state':'blocked_unknown_prior_result', 'removed':[]}


@pytest.mark.parametrize('payload, expected', [('{"state":"requested"}', 'requested'),
    ('{"state":"not_required"}', 'not_required'), ('{"state":"unexpected"}', 'failed'),
    ('not-json', 'failed')])
def test_scan_retry_uses_existing_endpoint_and_fails_closed(monkeypatch, payload, expected):
    calls = []
    def run(command, **_kwargs):
        calls.append(command)
        return SimpleNamespace(stdout=payload)
    monkeypatch.setattr(control.subprocess, 'run', run)
    assert control.retry_vast_syncthing_scan({'ssh_profile':'profile'}) == expected
    assert calls[0][:5] == ['ssh', '-T', '-F', 'profile', 'stockagent-vast']
    assert 'stockagent-packed' in calls[0][-1] and '/rest/db/' in calls[0][-1]


@pytest.mark.parametrize('defect', ['none', 'receipt_not_verified', 'different_manifest', 'different_plan'])
def test_fixed_release_requires_matched_independent_reconstruction(tmp_path, monkeypatch, defect):
    root = tmp_path/'control'
    operation = root/'operations'/'fixed-operation'
    (operation/'recovery').mkdir(parents=True)
    digest = 'b'*64
    current = {'contract':'all_catalog_incremental_migration_v1', 'state':'verified',
               'operation_directory':str(operation), 'plan_sha256':'c'*64}
    recovery = operation/'recovery'/'test-dataset.json'
    row = {'state':'verified', 'snapshot_id':'fixed-snapshot', 'manifest_sha256':'a'*64,
           'recovery_receipt':str(recovery)}
    report = {'contract':current['contract'], 'state':'verified', 'plan_sha256':current['plan_sha256'],
              'datasets':{'test-dataset':row}}
    proof = {'contract':current['contract'], 'dataset':'test-dataset',
             'snapshot_id':'fixed-snapshot', 'manifest_sha256':'a'*64,
             'full_original_file_reconstruction_verified':True}
    if defect == 'receipt_not_verified': proof['full_original_file_reconstruction_verified'] = False
    if defect == 'different_manifest': proof['manifest_sha256'] = 'd'*64
    if defect == 'different_plan': report['plan_sha256'] = 'd'*64
    (root/'current.json').write_text(json.dumps(current))
    (operation/'progress.json').write_text(json.dumps(report))
    recovery.write_text(json.dumps(proof))
    monkeypatch.setattr(control, 'VERSION_CONTROL', root)
    monkeypatch.setattr(control, 'resolve_packed_snapshot_id', lambda *_a, **_kw: SimpleNamespace(
        manifest_sha256='a'*64, manifest={'archive':{'objects':[{'kind':'pack','sha256':digest,
        'relpath':f'objects/packs/{digest[:2]}/{digest}.zip','bytes':123}]}}))
    if defect == 'none':
        assert control._fixed_release({'dataset':'test-dataset'}, tmp_path/'cold')['objects'] == {digest:123}
    else:
        with pytest.raises(ValueError):
            control._fixed_release({'dataset':'test-dataset'}, tmp_path/'cold')


@pytest.mark.parametrize('defect', ['source', 'identity', 'proof', 'payload', 'journal'])
def test_pressure_refuses_missing_proofs_or_unsafe_names(prepared, tmp_path, defect):
    owner, item, expected = prepared
    path = owner.root/(item['sha256']+'.zip')
    control = owner.root/(item['sha256']+'.json')
    record = lambda _: None
    if defect == 'source':
        expected['source_verified'] = False
    elif defect == 'identity':
        expected['object_sha256'] = '0'*64
    elif defect == 'proof':
        proof = json.loads(control.read_text())
        proof['worker_sha256'] = '0'*64
        control.write_text(json.dumps(proof))
    elif defect == 'payload':
        moved = tmp_path/'moved'
        path.rename(moved)
        path.symlink_to(moved)
    else:
        def record(_):
            raise OSError('journal unavailable')
    with pytest.raises((ValueError, OSError)):
        pressure.retire_cached_pair(owner, item, expected, references=lambda *_: False, record=record)
    assert os.path.lexists(path) and control.exists()
