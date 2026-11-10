import fcntl
import json

import pytest

from scripts import run_v3_reader_activation_queue as queue
from stockagent.data_sync import dedup_stream_candidate as candidate, packed_dedup_plan as plan
from stockagent.data_sync import packed_snapshots as packed


@pytest.fixture
def ready_candidate(tmp_path, monkeypatch):
    source, cold, workspace, reports = (tmp_path/name for name in ('source', 'cold', 'workspace', 'reports'))
    source.mkdir()
    workspace.mkdir()
    reports.mkdir()
    (source/'a').write_bytes(b'repeated')
    (source/'b').write_bytes(b'repeated')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='penguin')
    expected = old.manifest_sha256
    stage = workspace/'migration/stream-source-repack-v1/fixture'/expected
    index = plan.build_plan(cold, 'fixture', expected, reports)['index_path']
    candidate.prepare(cold, old, stage, index)
    candidate.prepare_stream_adoption(cold, stage, 'fixture', expected)
    status = workspace/'migration/source-local-dedup-candidate-v1/fixture.status.json'
    status.parent.mkdir(parents=True)
    status.write_text(json.dumps({'contract': 'stockagent.dedup_candidate_queue.v1', 'dataset': 'fixture',
        'expected_manifest_sha256': expected, 'state': 'v3_handed_off_waiting_activation',
        'publishable': False, 'retirement_authorized': False}))
    (workspace/'migration/source-local-exact-repack').mkdir()
    monkeypatch.setattr(queue.source_repack_bloat, 'auto_holds', lambda _w: {('fixture', expected)})
    policy = {'enabled': True, 'remote_reader_manifest_sha256': 'a'*64,
              'remote_wrapper_sha256': 'b'*64, 'probe_dataset': 'fixture'}
    proof = {'state': 'ready', 'reader_manifest_sha256': 'a'*64, 'wrapper_sha256': 'b'*64,
             'schema_version': 3, 'payload_fetched': False, 'payload_verified': False}
    return workspace, cold, stage, old, policy, proof


def test_activation_only_enables_exact_sealed_candidate_after_reader_probe(ready_candidate):
    workspace, cold, stage, old, policy, proof = ready_candidate
    preview = queue.run(workspace, cold, policy, remote=lambda _p: proof)
    assert preview['eligible_count'] == 1 and preview['activated_count'] == 0
    assert not (stage/'alias-install-activation.json').exists()
    result = queue.run(workspace, cold, policy, apply=True, remote=lambda _p: proof)
    assert result['activated_count'] == 1 and result['remote_reader_observed']
    marker = json.loads((stage/'alias-install-activation.json').read_text())
    assert marker['old_manifest_sha256'] == old.manifest_sha256 and marker['remote_reader_accepted'] is False
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert result['D_head_changed'] is False and result['independent_D_readback'] is False


def test_unavailable_reader_does_not_enable_candidate(ready_candidate):
    workspace, cold, stage, old, policy, _proof = ready_candidate
    result = queue.run(workspace, cold, policy, apply=True, remote=lambda _p: (_ for _ in ()).throw(OSError('offline')))
    assert result['state'] == 'waiting_reader' and result['activated_count'] == 0
    assert not (stage/'alias-install-activation.json').exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_marker_can_progress_while_unrelated_global_retirement_owner_is_busy(ready_candidate):
    workspace, cold, stage, old, policy, proof = ready_candidate
    lock = cold/'.local-state/locks/publish-retention-global.lock'
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open('a+') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = queue.run(workspace, cold, policy, apply=True, remote=lambda _p: proof)
    assert result['activated_count'] == 1
    assert (stage/'alias-install-activation.json').is_file()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_other_publisher_head_prevents_automatic_activation(ready_candidate):
    workspace, cold, stage, _old, policy, proof = ready_candidate
    (cold/'heads/fixture/other.json').write_text(json.dumps({'dataset': 'fixture', 'node_id': 'other',
                                                            'manifest_sha256': 'c'*64}))
    result = queue.run(workspace, cold, policy, apply=True, remote=lambda _p: proof)
    assert result['activated_count'] == 0 and result['blocked_count'] == 1
    assert not (stage/'alias-install-activation.json').exists()


def test_already_cleaned_generation_is_not_reactivated_and_mismatches_still_hold(ready_candidate):
    workspace, cold, stage, old, policy, proof = ready_candidate
    queue.run(workspace, cold, policy, apply=True, remote=lambda _p: proof)
    prepared = json.loads((stage/'prepared.json').read_text())
    replacement = 'c'*64
    install = {'contract': queue.stream_repack.CONTRACT+'.install', 'state': 'head_pending',
               'dataset': 'fixture', 'old_manifest_sha256': old.manifest_sha256,
               'new_manifest_sha256': replacement, 'snapshot_id': 'new-snapshot',
               'prepared_sha256': prepared['identity_sha256']}
    cleanup = {'contract': queue.stream_repack.CONTRACT+'.cleanup', 'state': 'completed',
               **{key: install[key] for key in ('dataset', 'old_manifest_sha256',
                                                  'new_manifest_sha256', 'prepared_sha256')}}
    (stage/'install.json').write_text(json.dumps(install))
    (stage/'cleanup.json').write_text(json.dumps(cleanup))
    receipt_path = workspace/'migration/source-local-exact-repack/fixture'/(replacement+'.json')
    receipt_path.parent.mkdir()
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': replacement,
               'new_snapshot_id': 'new-snapshot', 'packed_format': 'content-alias-v3',
               'E_stage_cleanup': {'state': 'completed'}}
    receipt_path.write_text(json.dumps(receipt))
    # Model the real E cleanup: metadata remains, but the packed payload does not.
    (stage/prepared['packages'][0]['name']).unlink()
    assert queue.pending_candidates(workspace) == []
    assert queue.run(workspace, cold, policy, apply=True,
                     remote=lambda _p: pytest.fail('completed generation needs no remote probe'))['state'] == 'idle'
    receipt['new_manifest_sha256'] = 'd'*64
    receipt_path.write_text(json.dumps(receipt))
    assert queue.pending_candidates(workspace) == [('fixture', old.manifest_sha256)]
