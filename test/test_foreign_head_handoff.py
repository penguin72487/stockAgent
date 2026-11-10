import json
from contextlib import contextmanager
from pathlib import Path

import pytest

from stockagent.data_sync import foreign_head_handoff as handoff, packed_snapshots as packed, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_retired_peer_generation_matches_source_enrollment():
    from scripts import retire_lab203_syncthing as retired, run_stream_packed_repack as stream_cli
    assert handoff._RETIRED_LAB203_DATASET == retired.LAB203_DATASET == stream_cli._RETIRED_LAB203_DATASET
    assert handoff._RETIRED_LAB203_MANIFEST == stream_cli._RETIRED_LAB203_MANIFEST
    assert handoff._RETIRED_LAB203_SNAPSHOT == retired.LAB203_SNAPSHOT_ID


def setup_case(tmp_path, monkeypatch, *, publisher='vastai1T'):
    source, cold, materialized, state = (tmp_path/name for name in ('source', 'cold', 'materialized', 'state'))
    source.mkdir()
    materialized.mkdir()
    state.mkdir()
    (state/'operation.lock').touch()
    (state/'state.json').write_text(json.dumps({'mode': 'index-only', 'hydrating': {}, 'retained_payloads': {}}))
    (source/'a').write_bytes(b'fixed-data')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id=publisher)
    stage = tmp_path/'stage'
    stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='penguin')
    new = packed.resolve_latest_packed(cold, 'fixture')
    req = {'dataset': 'fixture', 'old_snapshot_id': old.manifest['snapshot_id'],
           'old_manifest_sha256': old.manifest_sha256, 'new_snapshot_id': new.manifest['snapshot_id'],
           'new_manifest_sha256': new.manifest_sha256}
    if publisher == 'lab203':
        req['old_publisher'] = 'lab203'
        monkeypatch.setattr(handoff, '_RETIRED_LAB203_DATASET', 'fixture')
        monkeypatch.setattr(handoff, '_RETIRED_LAB203_MANIFEST', old.manifest_sha256)
        monkeypatch.setattr(handoff, '_RETIRED_LAB203_SNAPSHOT', old.manifest['snapshot_id'])
    args = (cold, materialized, state, req)
    options = {'sync_ready': lambda: True, 'references': lambda _paths: []}
    return args, options, old, new


def test_only_old_head_moves_and_exact_new_version_remains(tmp_path, monkeypatch):
    args, options, old, new = setup_case(tmp_path, monkeypatch)
    cold, _materialized, state, req = args
    old_head = cold/'heads/fixture/vastai1T.json'
    assert handoff.withdraw(*args, **options)['state'] == 'ready'
    assert old_head.exists()
    result = handoff.withdraw(*args, apply=True, **options)
    assert result['state'] == 'handed_off' and not result['D_old_objects_retired']
    assert not old_head.exists() and old.manifest_path.exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == new.manifest_sha256
    assert (state/'foreign-head-handoffs/fixture'/(req['old_manifest_sha256']+'.head.json')).is_file()
    assert handoff.withdraw(*args, apply=True, **options)['state'] == 'handed_off'


def test_fixed_retired_peer_uses_original_handoff_and_remote_guards(tmp_path, monkeypatch):
    import hashlib
    from stockagent.data_sync import authoritative_head_handoff as authority, materialized_cache

    args, options, old, new = setup_case(tmp_path, monkeypatch, publisher='lab203')
    cold, remote_materialized, state, req = args
    assert handoff.withdraw(*args, **options)['state'] == 'ready'
    with pytest.raises(SnapshotError, match='fixed enrolled generation'):
        handoff._request({**req, 'old_manifest_sha256': 'a'*64})
    with pytest.raises(SnapshotError, match='pinned'):
        handoff.withdraw(*args, **{**options, 'pins': lambda _root: {old.manifest['snapshot_id']}})
    assert old.head_path.exists()
    workspace, local_materialized = tmp_path/'workspace', tmp_path/'local-materialized'
    workspace.mkdir()
    local_materialized.mkdir()
    digest = hashlib.sha256(old.head_path.read_bytes()).hexdigest()
    authority.commit(workspace, cold, local_materialized, req, peer_head_sha=digest, notify=lambda _r: None)
    assert not old.head_path.exists()
    assert old.manifest['snapshot_id'] in materialized_cache._pinned_snapshot_ids(local_materialized)
    ack = handoff.confirm(cold, remote_materialized, state, req, head_sha=digest, **options)
    assert authority.finalize(workspace, cold, local_materialized, req, ack)['state'] == 'completed'
    assert old.manifest_path.exists() and new.manifest_path.exists()
    assert old.manifest['snapshot_id'] not in materialized_cache._pinned_snapshot_ids(local_materialized)


def test_failed_transport_can_restore_exact_remote_head_idempotently(tmp_path, monkeypatch):
    args, options, old, _new = setup_case(tmp_path, monkeypatch)
    cold, _materialized, state, request = args
    handoff.withdraw(*args, apply=True, **options)
    assert handoff.restore(cold, state, request)['state'] == 'restored'
    assert old.head_path.is_file()
    assert handoff.restore(cold, state, request)['state'] == 'restored'


@pytest.mark.parametrize('block', ['pin', 'lease', 'materialized', 'reader', 'sync'])
def test_active_or_unobservable_old_source_never_moves_head(tmp_path, monkeypatch, block):
    args, options, old, _new = setup_case(tmp_path, monkeypatch)
    cold, materialized, _state, _req = args
    old_id = old.manifest['snapshot_id']
    if block == 'pin':
        path = materialized/'old.pin.json'
        path.write_text(json.dumps({'manifest': {'snapshot_id': old_id}}))
    elif block == 'lease':
        path = materialized/'.cache-state/leases/fixture'/(old_id+'.json')
        path.parent.mkdir(parents=True)
        path.touch()
    elif block == 'materialized':
        (materialized/'fixture'/old_id).mkdir(parents=True)
    elif block == 'reader':
        options['references'] = lambda _paths: ['running-user']
    else:
        options['sync_ready'] = lambda: False
    with pytest.raises(SnapshotError):
        handoff.withdraw(*args, apply=True, **options)
    assert (cold/'heads/fixture/vastai1T.json').exists()


@pytest.mark.parametrize('after_capture', [False, True])
def test_handoff_resumes_before_or_after_atomic_capture(tmp_path, monkeypatch, after_capture):
    args, options, _old, _new = setup_case(tmp_path, monkeypatch)
    rename = handoff.os.rename
    def interrupted(source, destination):
        if after_capture:
            rename(source, destination)
        raise OSError('interrupted')
    monkeypatch.setattr(handoff.os, 'rename', interrupted)
    with pytest.raises(OSError, match='interrupted'):
        handoff.withdraw(*args, apply=True, **options)
    monkeypatch.setattr(handoff.os, 'rename', rename)
    assert handoff.withdraw(*args, apply=True, **options)['state'] == 'handed_off'
    assert not (args[0]/'heads/fixture/vastai1T.json').exists()


def test_changed_successor_is_never_accepted(tmp_path, monkeypatch):
    args, options, old, _new = setup_case(tmp_path, monkeypatch)
    request = {**args[3], 'new_manifest_sha256': 'a'*64}
    with pytest.raises(SnapshotError):
        handoff.withdraw(*args[:3], request, apply=True, **options)
    assert old.head_path.exists()


def test_queue_requires_fixed_source_receipt_before_contacting_remote(tmp_path, monkeypatch):
    from scripts import run_foreign_head_handoff_queue as queue
    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir()
    workspace.mkdir()
    (source/'a').write_bytes(b'fixed-data')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='vastai1T')
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='penguin')
    new = packed.resolve_latest_packed(cold, 'fixture')
    root = workspace/'migration/source-local-exact-repack'
    receipt_dir = root/'fixture'
    receipt_dir.mkdir(parents=True)
    receipt_path = receipt_dir/(new.manifest_sha256+'.json')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
        'dataset': 'fixture', 'old_manifest_sha256': old.manifest_sha256,
        'new_manifest_sha256': new.manifest_sha256, 'new_snapshot_id': new.manifest['snapshot_id'],
        'old_release_removed': False}
    receipt_path.write_text(json.dumps(receipt))
    (root/'retirement-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.source_local_repack_queue.v1.retirement-status', 'entries': [
            {'mode': 'direct', 'dataset': 'fixture', 'old_manifest_sha256': old.manifest_sha256,
             'reason': 'retirement_current_head'}]}))
    calls = []
    @contextmanager
    def transport(_helper, request):
        calls.append(request)
        yield object()
    assert queue.run(workspace, cold, apply=False, transport=transport)['attempts'][0]['state'] == 'ready'
    assert not (root/'foreign-head-handoff-status.json').exists()
    receipt['old_manifest_sha256'] = 'a'*64
    receipt_path.write_text(json.dumps(receipt))
    calls.clear()
    result = queue.run(workspace, cold, apply=True, transport=transport)
    assert result['attempts'][0]['state'] == 'waiting_retry' and calls == []
    assert (cold/'heads/fixture/vastai1T.json').exists()


def test_queue_uses_original_remote_handoff_for_fixed_retired_lab203(tmp_path, monkeypatch):
    from scripts import run_foreign_head_handoff_queue as queue, run_stream_packed_repack as stream_cli

    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir()
    workspace.mkdir()
    (source/'a').write_bytes(b'fixed-data')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='lab203')
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='penguin')
    new = packed.resolve_latest_packed(cold, 'fixture')
    prepared = stream_repack._prepared_metadata(stage, 'fixture', old.manifest_sha256)
    for module in (stream_cli, handoff):
        monkeypatch.setattr(module, '_RETIRED_LAB203_DATASET', 'fixture')
        monkeypatch.setattr(module, '_RETIRED_LAB203_MANIFEST', old.manifest_sha256)
    monkeypatch.setattr(handoff, '_RETIRED_LAB203_SNAPSHOT', old.manifest['snapshot_id'])
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_PREPARED', prepared['identity_sha256'])
    topology = {'ready': True}
    monkeypatch.setattr(stream_cli, '_retired_lab203_successor_ready',
                        lambda *_args, **_kwargs: topology['ready'])
    root = workspace/'migration/source-local-exact-repack'
    receipt_dir = root/'fixture'
    receipt_dir.mkdir(parents=True)
    (receipt_dir/(new.manifest_sha256+'.json')).write_text(json.dumps({
        'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
        'dataset': 'fixture', 'old_manifest_sha256': old.manifest_sha256,
        'new_manifest_sha256': new.manifest_sha256, 'new_snapshot_id': new.manifest['snapshot_id'],
        'old_release_removed': False}))
    row = {'mode': 'direct', 'dataset': 'fixture', 'old_manifest_sha256': old.manifest_sha256,
           'reason': 'retirement_current_head'}
    (root/'retirement-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.source_local_repack_queue.v1.retirement-status', 'entries': [row]}))
    calls = []
    @contextmanager
    def transport(_helper, request):
        calls.append(request)
        yield object()
    assert queue.run(workspace, cold, apply=False, transport=transport)['attempts'][0]['state'] == 'ready'
    assert calls[0]['old_publisher'] == 'lab203'
    topology['ready'] = False
    calls.clear()
    assert queue.run(workspace, cold, apply=False, transport=transport)['attempts'][0]['state'] == 'waiting_retry'
    assert calls == [] and old.head_path.exists()


def test_authority_holds_exact_old_pin_until_peer_confirms(tmp_path, monkeypatch):
    import hashlib
    from stockagent.data_sync import authoritative_head_handoff as authority, materialized_cache
    args, _options, old, new = setup_case(tmp_path, monkeypatch)
    cold, materialized, _state, req = args
    workspace = tmp_path/'workspace'
    workspace.mkdir()
    notified = []
    digest = hashlib.sha256(old.head_path.read_bytes()).hexdigest()
    result = authority.commit(workspace, cold, materialized, req, peer_head_sha=digest, notify=notified.append)
    assert result['state'] == 'moved' and notified == [req]
    assert not old.head_path.exists() and old.manifest['snapshot_id'] in materialized_cache._pinned_snapshot_ids(materialized)
    ack = {'contract': handoff.CONTRACT, 'request': req, 'state': 'peer_confirmed',
           'head_sha256': digest, 'source_payload_deleted': False, 'D_old_objects_retired': False}
    assert authority.finalize(workspace, cold, materialized, req, ack)['state'] == 'completed'
    assert old.manifest_path.exists()
    assert old.manifest['snapshot_id'] not in materialized_cache._pinned_snapshot_ids(materialized)
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == new.manifest_sha256


def test_failed_peer_confirmation_restores_head_and_retry_can_finish(tmp_path, monkeypatch):
    import hashlib
    from stockagent.data_sync import authoritative_head_handoff as authority, materialized_cache
    args, _options, old, _new = setup_case(tmp_path, monkeypatch)
    cold, materialized, _state, req = args
    workspace = tmp_path/'workspace'
    workspace.mkdir()
    notified = []
    digest = hashlib.sha256(old.head_path.read_bytes()).hexdigest()
    def broken_notify(item):
        notified.append(item)
        if len(notified) == 1:
            raise OSError('scan unavailable')
    with pytest.raises(OSError, match='scan unavailable'):
        authority.commit(workspace, cold, materialized, req, peer_head_sha=digest, notify=broken_notify)
    assert old.head_path.is_file() and old.manifest['snapshot_id'] not in materialized_cache._pinned_snapshot_ids(materialized)
    assert authority.commit(workspace, cold, materialized, req, peer_head_sha=digest, notify=notified.append)['state'] == 'moved'
    ack = {'contract': handoff.CONTRACT, 'request': req, 'state': 'peer_confirmed',
           'head_sha256': digest, 'source_payload_deleted': False, 'D_old_objects_retired': False}
    assert authority.finalize(workspace, cold, materialized, req, ack)['state'] == 'completed'
    stage = workspace/'migration/authoritative-head-handoffs/fixture'/req['old_manifest_sha256']
    assert len(list((stage/'history').glob('*.json'))) == 1


def test_interrupted_source_capture_is_recovered_before_pin_release(tmp_path, monkeypatch):
    import hashlib
    from stockagent.data_sync import authoritative_head_handoff as authority, materialized_cache
    args, _options, old, _new = setup_case(tmp_path, monkeypatch)
    cold, materialized, _state, req = args
    workspace = tmp_path/'workspace'
    workspace.mkdir()
    original = authority.rollback
    monkeypatch.setattr(authority, 'rollback', lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError('crash')))
    digest = hashlib.sha256(old.head_path.read_bytes()).hexdigest()
    with pytest.raises(OSError, match='crash'):
        authority.commit(workspace, cold, materialized, req, peer_head_sha=digest,
                         notify=lambda _req: (_ for _ in ()).throw(OSError('lost owner')))
    assert not old.head_path.exists() and old.manifest['snapshot_id'] in materialized_cache._pinned_snapshot_ids(materialized)
    monkeypatch.setattr(authority, 'rollback', original)
    assert authority.rollback(workspace, cold, materialized, req, notify=lambda _req: None)['state'] == 'rolled_back'
    assert old.head_path.exists() and old.manifest['snapshot_id'] not in materialized_cache._pinned_snapshot_ids(materialized)


def test_receive_only_peer_confirms_only_after_old_head_disappears(tmp_path, monkeypatch):
    import hashlib
    args, options, old, _new = setup_case(tmp_path, monkeypatch)
    cold, materialized, state, req = args
    digest = hashlib.sha256(old.head_path.read_bytes()).hexdigest()
    with pytest.raises(SnapshotError, match='still present'):
        handoff.confirm(cold, materialized, state, req, head_sha=digest, **options)
    old.head_path.unlink()
    result = handoff.confirm(cold, materialized, state, req, head_sha=digest, **options)
    assert result['state'] == 'peer_confirmed' and result['head_sha256'] == digest


def test_scheduled_recovery_retains_old_pin_until_exact_peer_confirmation(tmp_path, monkeypatch):
    import hashlib
    from scripts import run_foreign_head_handoff_queue as queue
    from stockagent.data_sync import authoritative_head_handoff as authority, materialized_cache
    args, _options, old, _new = setup_case(tmp_path, monkeypatch)
    cold, materialized, _state, request = args
    workspace = tmp_path/'workspace'
    workspace.mkdir()
    digest = hashlib.sha256(old.head_path.read_bytes()).hexdigest()
    authority.commit(workspace, cold, materialized, request, peer_head_sha=digest, notify=lambda _r: None)
    notifications = []
    def pending(*_args):
        raise SnapshotError('peer has not confirmed')
    result = queue.recover_interrupted(workspace, cold, confirm_peer=pending, materialized=materialized,
                                       notify_peer=notifications.append)
    assert result[0]['state'] == 'waiting_peer' and notifications == [request]
    assert not old.head_path.exists()
    assert old.manifest['snapshot_id'] in materialized_cache._pinned_snapshot_ids(materialized)
    assert old.manifest_path.exists()
    def confirmed(_helper, req, sha):
        return {'contract': handoff.CONTRACT, 'request': req, 'state': 'peer_confirmed',
                'head_sha256': sha, 'source_payload_deleted': False, 'D_old_objects_retired': False}
    result = queue.recover_interrupted(workspace, cold, confirm_peer=confirmed, materialized=materialized,
                                       notify_peer=notifications.append)
    assert result[0]['state'] == 'completed'
    assert old.manifest['snapshot_id'] not in materialized_cache._pinned_snapshot_ids(materialized)
    assert old.manifest_path.exists()  # The separate retirement owner remains responsible.


def test_busy_global_publication_owner_defers_once_instead_of_probing_every_candidate(tmp_path, monkeypatch):
    from scripts import run_foreign_head_handoff_queue as queue
    workspace, cold = tmp_path/'workspace', tmp_path/'cold'
    root = workspace/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'retirement-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.source_local_repack_queue.v1.retirement-status',
        'entries': [{'mode': 'direct', 'dataset': f'fixture-{i}', 'old_manifest_sha256': 'a'*64,
                     'reason': 'retirement_current_head'} for i in range(4)]}))
    calls = []
    @contextmanager
    def busy(path, **_kwargs):
        calls.append(path.name)
        raise packed.PackedPublicationBusy('publication owner busy: publish-retention-global.lock')
        yield
    monkeypatch.setattr(packed, '_try_publication_lock', busy)
    result = queue.run(workspace, cold, apply=False)
    assert result['state'] == 'waiting_owner' and len(result['attempts']) == 1
    assert calls == ['publish-retention-global.lock']
