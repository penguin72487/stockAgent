from dataclasses import replace
import json
import os
import time

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import (
    MANUAL_WSL_CAPTURE_CONTRACT, MANUAL_OFFLINE_SIMULATION_CONTRACT,
    MANUAL_TRANSFER_QUARANTINE_CONTRACT,
    MANUAL_MINUTE_DERIVED_VIEW_CONTRACT,
    load_legacy_specs, prepare_archive,
    publish_archive, source_plan, verify_cold_archive,
)


def example(tmp_path):
    root = tmp_path / 'repo/artifacts'
    source = root / 'replays/old-result'
    source.mkdir(parents=True)
    file = source / 'unique-result.bin'
    file.write_bytes(b'unique offline research output')
    stamp = time.time() - 2 * 86400
    os.utime(file, (stamp, stamp))
    row = {'dataset': 'legacy-wsl-example', 'relative_root': 'replays/old-result',
           'minimum_stable_days': 7, 'manual_capture_min_stable_hours': 12,
           'archive_only': True, 'compression': 'gzip-1-csv-over-8m',
           'stage_root': str(tmp_path / 'stage'), 'capture_contract': MANUAL_WSL_CAPTURE_CONTRACT}
    catalog = tmp_path / 'catalog.json'
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    return root, source, row, catalog


def test_manual_offline_capture_preserves_unique_bytes_and_cannot_run_automatically(tmp_path):
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='explicit manual capture'):
        source_plan(source, spec)
    packed = tmp_path / 'packed'
    release = publish_archive(spec, root, packed, repo_root=root.parent, manual_capture=True)
    proof = verify_cold_archive(spec, packed)
    assert proof['snapshot_id'] == release['snapshot_id']
    assert proof['decoded_originals_verified'] and proof['manifest']['capture_contract'] == MANUAL_WSL_CAPTURE_CONTRACT
    assert proof['manifest']['files'][0]['original_sha256']
    assert (source / 'unique-result.bin').read_bytes() == b'unique offline research output'


def test_retired_transport_preserves_original_namespace_and_has_a_distinct_physical_role(tmp_path):
    from scripts.retire_local_offline_artifacts import source_layout,dataset_prefix,TRANSFER_QUARANTINE_ORIGIN
    root,source,row,catalog=example(tmp_path)
    relative='stockagent-transfer-quarantine/2026-08-20_ssh_transport'
    target=root/relative;target.parent.mkdir();source.rename(target)
    row.update(relative_root=relative,capture_contract=MANUAL_TRANSFER_QUARANTINE_CONTRACT)
    catalog.write_text(json.dumps({'schema_version':1,'authority_node_id':'penguin','archives':[row]}))
    spec=load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError,match='explicit manual capture'):
        source_plan(target,spec)
    packed=tmp_path/'packed'
    published=publish_archive(spec,root,packed,repo_root=root.parent,manual_capture=True)
    proof=verify_cold_archive(spec,packed)
    assert proof['decoded_originals_verified'] and proof['snapshot_id']==published['snapshot_id']
    assert proof['manifest']['relative_root']==relative
    assert proof['manifest']['capture_contract']==MANUAL_TRANSFER_QUARANTINE_CONTRACT
    assert (target/'unique-result.bin').read_bytes()==b'unique offline research output'
    assert str(source_layout(TRANSFER_QUARANTINE_ORIGIN)[0])=='/srv'
    assert dataset_prefix(TRANSFER_QUARANTINE_ORIGIN)=='legacy-wsl-retired-transfer-'


@pytest.mark.parametrize('relative',[
    'stockagent-live/data_tw_public','stockagent-packed/objects',
    'stockagent-transfer-quarantine','stockagent-transfer-quarantine/arbitrary',
    'stockagent-transfer-quarantine/2026-08-20_ssh_transport/child'])
def test_retired_transport_contract_rejects_other_service_source_roots(tmp_path,relative):
    root,source,row,catalog=example(tmp_path)
    row.update(relative_root=relative,capture_contract=MANUAL_TRANSFER_QUARANTINE_CONTRACT)
    catalog.write_text(json.dumps({'schema_version':1,'authority_node_id':'penguin','archives':[row]}))
    with pytest.raises(SnapshotError):load_legacy_specs(catalog)


@pytest.mark.parametrize('name', ['research_dataset_developing_v5', 'research_dataset_schema2_volume_bug_20260807'])
def test_retired_minute_view_is_separate_exact_preservation_not_source_promotion(tmp_path, name):
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, row, catalog = example(tmp_path)
    relative = 'data_tw_minute/'+name
    target = root.parent/relative
    target.parent.mkdir()
    source.rename(target)
    row.update(relative_root=relative, capture_contract=MANUAL_MINUTE_DERIVED_VIEW_CONTRACT)
    catalog.write_text(json.dumps({'schema_version':1, 'authority_node_id':'penguin', 'archives':[row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='explicit manual capture'):
        source_plan(target, spec)
    packed = tmp_path/'packed'
    published = publish_archive(spec, root.parent, packed, repo_root=root.parent, manual_capture=True)
    proof = verify_cold_archive(spec, packed)
    assert proof['decoded_originals_verified'] and proof['snapshot_id']==published['snapshot_id']
    assert proof['manifest']['relative_root']==relative
    assert proof['manifest']['capture_contract']==MANUAL_MINUTE_DERIVED_VIEW_CONTRACT
    assert published['source_files']==1 and target.is_dir()
    assert cleanup.dataset_prefix(cleanup.MINUTE_DERIVED_VIEW_ORIGIN)=='legacy-wsl-minute-derived-'


@pytest.mark.parametrize('relative', ['data_tw_minute/research_dataset', 'data_tw_minute/shioaji_1m',
    'data_tw_minute', 'data_openBB/history', 'data_tw_minute/research_dataset_developing_v5/child'])
def test_retired_minute_view_cannot_include_current_raw_or_arbitrary_data_roots(tmp_path, relative):
    root, source, row, catalog = example(tmp_path)
    row.update(relative_root=relative, capture_contract=MANUAL_MINUTE_DERIVED_VIEW_CONTRACT)
    catalog.write_text(json.dumps({'schema_version':1, 'authority_node_id':'penguin', 'archives':[row]}))
    with pytest.raises(SnapshotError):
        load_legacy_specs(catalog)


@pytest.mark.parametrize('reference,protected', [('workspace', False), ('data-scope', True), ('selected-view', True)])
def test_minute_view_process_domain_keeps_real_data_ancestors_not_generic_workspace(tmp_path, reference, protected):
    import subprocess
    import sys
    from stockagent.data_sync.legacy_artifact_archive import legacy_process_scope
    from stockagent.data_sync.artifact_maintenance import artifact_process_references
    root, original, row, catalog = example(tmp_path)
    relative = 'data_tw_minute/research_dataset_developing_v5'
    source = root.parent/relative
    source.parent.mkdir()
    original.rename(source)
    row.update(relative_root=relative, capture_contract=MANUAL_MINUTE_DERIVED_VIEW_CONTRACT)
    catalog.write_text(json.dumps({'schema_version':1,'authority_node_id':'penguin','archives':[row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    argument = {'workspace': root.parent, 'data-scope': source.parent, 'selected-view': source}[reference]
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)',
                              '--scan-root', str(argument)], cwd=root.parent)
    try:
        scope = legacy_process_scope(spec, root.parent)
        assert scope == source.parent
        refs = artifact_process_references(source, scope)
        assert any(f'pid={child.pid}:' in ref for ref in refs) is protected
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_minute_view_named_cli_selection_uses_only_fixed_paths_without_source_argv(tmp_path, monkeypatch):
    import sys
    from scripts import retire_local_offline_artifacts as cleanup
    seen=[]
    monkeypatch.setattr(cleanup.socket, 'gethostname', lambda: 'penguin')
    def fake_inventory(roots, output, existing, **options):
        seen.append((roots, options))
        return {'items': []}
    monkeypatch.setattr(cleanup, 'inventory', fake_inventory)
    monkeypatch.setattr(sys, 'argv', ['retire_local_offline_artifacts.py','inventory','--minute-derived-view',
                                    '--minute-view-name','developing-v5','--inventory',str(tmp_path/'inventory.json')])
    assert cleanup.main()==0
    assert seen==[(['data_tw_minute/research_dataset_developing_v5'],
                   {'hot_mirror':False,'transfer_quarantine':False,'minute_derived_view':True})]


@pytest.mark.parametrize('changed', [False, True])
def test_publication_owner_excludes_complete_c_and_d_reads_but_keeps_source_guard(tmp_path, monkeypatch, changed):
    from contextlib import contextmanager
    import fcntl
    import stockagent.data_sync.legacy_artifact_archive as archive
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    prepare_archive(spec, root, manual_capture=True)
    owner_path = tmp_path/'common.lock';events=[]
    def held():
        with owner_path.open('a') as probe:
            try:fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:return True
            fcntl.flock(probe,fcntl.LOCK_UN)
            return False
    @contextmanager
    def owner():
        with owner_path.open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            if changed:
                path=source/'unique-result.bin';before=path.stat()
                path.write_bytes(b'newly changed source')
                os.utime(path,ns=(before.st_atime_ns,before.st_mtime_ns))
            yield
    real_c, real_publish, real_d = archive.verify_archive_directory, archive.publish_packed_snapshot, archive.verify_packed_snapshot
    def check_c(*a, **kw):
        events.append(('original-read',held()));return real_c(*a,**kw)
    def publish(*a, **kw):
        events.append(('publish',held()));return real_publish(*a,**kw)
    def check_d(*a, **kw):
        events.append(('cold-read',held()));return real_d(*a,**kw)
    monkeypatch.setattr(archive,'verify_archive_directory',check_c)
    monkeypatch.setattr(archive,'publish_packed_snapshot',publish)
    monkeypatch.setattr(archive,'verify_packed_snapshot',check_d)
    cold=tmp_path/'packed'
    if changed:
        with pytest.raises(SnapshotError,match='source changed before cold commit'):
            publish_archive(spec,root,cold,repo_root=root.parent,manual_capture=True,publication_owner=owner)
        assert not list((cold/'heads').rglob('*.json'))
        assert source.exists()
    else:
        published=publish_archive(spec,root,cold,repo_root=root.parent,manual_capture=True,publication_owner=owner)
        assert events==[('original-read',False),('publish',True),('cold-read',False),('original-read',False)]
        assert verify_cold_archive(spec,cold)['manifest_sha256']==published['manifest_sha256']


@pytest.mark.parametrize('scope', ['live', 'runtime', '..', 'data_tw_public'])
def test_manual_contract_does_not_expand_to_live_ledgers_or_source_roots(tmp_path, scope):
    root, source, row, catalog = example(tmp_path)
    row['relative_root'] = scope + '/example'
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    with pytest.raises(SnapshotError):
        load_legacy_specs(catalog)


def test_manual_capture_checks_independent_configuration_consumers_before_encoding(tmp_path, monkeypatch):
    root, source, row, catalog = example(tmp_path)
    import stockagent.data_sync.remote_legacy_return as consumers
    monkeypatch.setattr(consumers, 'active_configuration_references', lambda *a: ['independent-training-config'])
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='in use'):
        prepare_archive(spec, root, manual_capture=True)
    assert not (spec.stage_root / spec.dataset).exists()


@pytest.mark.parametrize('operation', ['prepare', 'verify', 'publish'])
def test_separate_physical_source_always_uses_explicit_repository_consumers(tmp_path, monkeypatch, operation):
    import stockagent.data_sync.remote_legacy_return as consumers
    from stockagent.data_sync.legacy_artifact_archive import verify_archive_directory
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    repository = tmp_path/'actual-repository';repository.mkdir()
    if operation == 'verify':
        prepare_archive(spec, root, manual_capture=True)
    seen=[]
    def references(_source, repo):
        seen.append(repo)
        return ['configured-web-consumer'] if repo == repository else []
    monkeypatch.setattr(consumers, 'active_configuration_references', references)
    with pytest.raises(SnapshotError, match='in use'):
        if operation == 'prepare':
            prepare_archive(spec, root, manual_capture=True, repo_root=repository)
        elif operation == 'verify':
            verify_archive_directory(spec.stage_root/spec.dataset/'archive', source,
                spec=spec, manual_capture=True, artifact_root=root, repo_root=repository)
        else:
            publish_archive(spec, root, tmp_path/'packed', manual_capture=True, repo_root=repository)
    assert seen and all(repo == repository for repo in seen)
    assert source.is_dir()
    assert not (tmp_path/'packed/heads'/spec.dataset).exists()


@pytest.mark.parametrize('origin, physical', [('unknown', '/tmp/arbitrary'),
    ('retained-hot-mirror-preservation-v1', '/tmp/arbitrary')])
def test_hot_mirror_inventory_cannot_redirect_the_cleanup_source(tmp_path, origin, physical):
    from scripts import retire_local_offline_artifacts as cleanup
    inventory = tmp_path/'inventory.json';receipts=tmp_path/'receipts';receipts.mkdir()
    value={'schema_version':1,'contract':MANUAL_WSL_CAPTURE_CONTRACT,'authority_node_id':'penguin',
           'source_origin':origin,'artifact_root':physical,'items':[{'relative_root':'replays/example'}]}
    inventory.write_text(json.dumps(value))
    with pytest.raises(SnapshotError,match='source origin|physical source'):
        cleanup._apply(inventory, inventory.read_bytes(), receipts)
    assert not list(receipts.iterdir())


@pytest.mark.parametrize('origin', ['retained-transfer-quarantine-preservation-v1',
                                  'retained-minute-derived-view-preservation-v1'])
def test_separate_source_frozen_inventory_rejects_other_reviewed_artifact_scopes(tmp_path, origin):
    from scripts import retire_local_offline_artifacts as cleanup
    inventory = tmp_path/'inventory.json'
    receipts = tmp_path/'receipts'
    receipts.mkdir()
    value = {'schema_version':1, 'contract':MANUAL_WSL_CAPTURE_CONTRACT,
             'authority_node_id':'penguin', 'source_origin':origin,
             'artifact_root':str(cleanup.source_layout(origin)[0]),
             'items':[{'relative_root':'markets/unrelated-source'}]}
    inventory.write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match='unreviewed physical root'):
        cleanup._apply(inventory, inventory.read_bytes(), receipts)
    assert not list(receipts.iterdir())


def test_manifest_contract_change_cannot_be_used_as_recovery_proof(tmp_path):
    root, source, row, catalog = example(tmp_path)
    spec = load_legacy_specs(catalog)[row['dataset']]
    prepare_archive(spec, root, manual_capture=True)
    from stockagent.data_sync.legacy_artifact_archive import verify_archive_directory
    with pytest.raises(SnapshotError, match='identity differs'):
        verify_archive_directory(spec.stage_root / spec.dataset / 'archive', spec=replace(spec, capture_contract=None))


def test_explicit_offline_simulation_has_exact_recovery_and_remains_manual(tmp_path):
    root, source, row, catalog = example(tmp_path)
    relative = 'live/official-close-settlement-old-experiment'
    (root / 'live').mkdir()
    source.rename(root / relative)
    row.update(relative_root=relative, capture_contract=MANUAL_OFFLINE_SIMULATION_CONTRACT)
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='explicit manual capture'):
        prepare_archive(spec, root)
    release = publish_archive(spec, root, tmp_path / 'packed', repo_root=root.parent, manual_capture=True)
    proof = verify_cold_archive(spec, tmp_path / 'packed')
    assert proof['snapshot_id'] == release['snapshot_id'] and proof['decoded_originals_verified']
    assert proof['manifest']['capture_contract'] == MANUAL_OFFLINE_SIMULATION_CONTRACT


@pytest.mark.parametrize('relative', ['live/tw_day_trade_simulation', 'live/data_monitor',
    'live/shioaji_taifex_volatility_simulation', 'live/official-close-settlement-',
    'live/official-close-settlement-old/nested', 'markets/old-result'])
def test_offline_simulation_marker_never_admits_canonical_or_arbitrary_live_roots(tmp_path, relative):
    root, source, row, catalog = example(tmp_path)
    row.update(relative_root=relative, capture_contract=MANUAL_OFFLINE_SIMULATION_CONTRACT)
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    with pytest.raises(SnapshotError, match='excludes canonical'):
        load_legacy_specs(catalog)


def test_promoted_offline_simulation_stays_protected_when_a_service_uses_it(tmp_path, monkeypatch):
    root, source, row, catalog = example(tmp_path)
    relative = 'live/official-close-settlement-promoted'
    (root / 'live').mkdir()
    source.rename(root / relative)
    row.update(relative_root=relative, capture_contract=MANUAL_OFFLINE_SIMULATION_CONTRACT)
    catalog.write_text(json.dumps({'schema_version': 1, 'authority_node_id': 'penguin', 'archives': [row]}))
    import stockagent.data_sync.artifact_consumers as consumers
    monkeypatch.setattr(consumers, 'artifact_service_references',
                        lambda roots, repo: {str(p): ['promoted-live-state'] for p in roots})
    spec = load_legacy_specs(catalog)[row['dataset']]
    with pytest.raises(SnapshotError, match='in use'):
        prepare_archive(spec, root, manual_capture=True)
    assert (root / relative / 'unique-result.bin').is_file()


def test_existing_archive_retirement_binds_catalog_and_rejects_later_drift(tmp_path, monkeypatch):
    from scripts import retire_local_offline_artifacts as cleanup
    import hashlib
    root, source, row, catalog = example(tmp_path)
    row['stage_root'] = '/var/lib/stockagent-legacy-archive-stage'
    catalog.write_text(json.dumps({'schema_version':1, 'authority_node_id':'penguin', 'archives':[row]}))
    monkeypatch.setattr(cleanup, 'ROOT', root.parent)
    monkeypatch.setattr(cleanup, 'EXISTING_CATALOG', catalog)
    result = cleanup.inventory([], tmp_path / 'fixed-inventory.json', [row['dataset']])
    assert result['items'][0]['existing_dataset'] == row['dataset']
    assert result['existing_catalog_sha256'] == hashlib.sha256(catalog.read_bytes()).hexdigest()
    row['relative_root'] = 'replays/different-root'
    catalog.write_text(json.dumps({'schema_version':1, 'authority_node_id':'penguin', 'archives':[row]}))
    with pytest.raises(SnapshotError, match='catalog changed'):
        cleanup._apply(tmp_path / 'fixed-inventory.json', json.dumps(result).encode(), tmp_path / 'receipts')
    assert (source / 'unique-result.bin').is_file()


def test_phase_status_reports_failure_without_a_retirement_claim(tmp_path):
    from scripts.retire_local_offline_artifacts import phase
    with pytest.raises(SnapshotError):
        with phase(tmp_path, 'independent-D-original-recovery'):
            assert json.loads((tmp_path / 'phase.json').read_text())['state'] == 'running'
            raise SnapshotError('cold original corrupted')
    result = json.loads((tmp_path / 'phase.json').read_text())
    assert result['state'] == 'failed' and result['error_type'] == 'SnapshotError'
    assert result['elapsed_seconds'] >= 0
    assert 'retired' not in result and not (tmp_path / 'acceptance.json').exists()


@pytest.mark.parametrize('current_service', [False, True])
def test_previously_retired_root_recovered_for_service_is_retained(tmp_path, monkeypatch, current_service):
    import hashlib
    from scripts import retire_local_offline_artifacts as cleanup
    root, source, row, catalog = example(tmp_path)
    monkeypatch.setattr(cleanup, 'ROOT', root.parent)
    cfg = cleanup.RetentionConfig(tmp_path/'cold', tmp_path/'archive', tmp_path/'cache',
                                  tmp_path/'backup.json', tmp_path/'state', 'packed', ())
    monkeypatch.setattr(cleanup.RetentionConfig, 'load', lambda *a, **k: cfg)
    monkeypatch.setattr(cleanup, 'load_retirement_peer_names', lambda *a, **k: ())
    monkeypatch.setattr(cleanup, 'artifact_service_references',
                        lambda roots, repo: {str(p): ['execution-replay.source_files'] if current_service else [] for p in roots})
    relative = 'replays/old-result'
    fingerprint = 'fixed-observation'
    dataset = 'legacy-wsl-offline-' + hashlib.sha256((relative+fingerprint).encode()).hexdigest()[:24]
    receipts = tmp_path/'receipts'
    target = receipts/dataset
    target.mkdir(parents=True)
    prior = target/'acceptance.json'
    prior.write_text(json.dumps({'retired': True, 'dataset': dataset, 'relative_root': relative}))
    original_receipt = prior.read_bytes()
    inventory = tmp_path/'inventory.json'
    raw = json.dumps({'schema_version': 1, 'contract': MANUAL_WSL_CAPTURE_CONTRACT,
                      'authority_node_id': 'penguin', 'items': [{'relative_root': relative, 'fingerprint': fingerprint}]}).encode()
    inventory.write_bytes(raw)
    if current_service:
        result = cleanup._apply(inventory, raw, receipts)
        assert result['all_selected_retired'] is False
        assert result['items'][0]['retained_for_current_service'] is True
        assert result['items'][0]['service_references'] == ['execution-replay.source_files']
    else:
        with pytest.raises(SnapshotError, match='requires reconciliation'):
            cleanup._apply(inventory, raw, receipts)
    assert prior.read_bytes() == original_receipt
    assert (source/'unique-result.bin').read_bytes() == b'unique offline research output'
    assert not (target/'publication.json').exists()


@pytest.mark.parametrize('changed',[False,True])
def test_reuse_completed_publication_requires_fixed_identity_and_claims_no_recovery(tmp_path,changed):
    from scripts import retire_local_offline_artifacts as cleanup
    root,source,row,catalog=example(tmp_path)
    spec=load_legacy_specs(catalog)[row['dataset']]
    packed=tmp_path/'packed';target=tmp_path/'receipt';target.mkdir()
    published=publish_archive(spec,root,packed,repo_root=root.parent,manual_capture=True)
    assert cleanup.reusable_publication(target,spec,packed) is None
    if changed:published['manifest_sha256']='0'*64
    (target/'publication.json').write_text(json.dumps(published))
    if changed:
        with pytest.raises(SnapshotError,match='fixed D release'):
            cleanup.reusable_publication(target,spec,packed)
    else:
        reused=cleanup.reusable_publication(target,spec,packed)
        assert reused==published and 'cold_verified' not in reused
    assert source.is_dir()


def test_queued_cleanup_cannot_enter_the_existing_cohort_transaction(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import fcntl
    from scripts import retire_local_offline_artifacts as cleanup
    receipts=tmp_path/'receipts';receipts.mkdir()
    fixed=tmp_path/'inventory.json';fixed.write_text('{}')
    entered=[]
    monkeypatch.setattr(cleanup,'_apply',lambda *args:entered.append(True) or {'all_selected_retired':True})
    with (receipts/'owner.lock').open('a') as owner, ThreadPoolExecutor(max_workers=1) as pool:
        fcntl.flock(owner,fcntl.LOCK_EX)
        with pytest.raises(BlockingIOError):
            cleanup.apply(fixed,receipts)
        queued=pool.submit(cleanup.apply,fixed,receipts,wait_for_owner=True)
        time.sleep(0.05)
        assert not queued.done() and not entered
        fcntl.flock(owner,fcntl.LOCK_UN)
        assert queued.result(timeout=5)=={'all_selected_retired':True}
    assert entered==[True]


@pytest.mark.parametrize('ram_available',[False,True])
def test_recovery_scratch_selection_keeps_exact_decode_and_physical_capacity_gate(
        tmp_path,monkeypatch,ram_available):
    from types import SimpleNamespace
    from scripts import retire_local_offline_artifacts as cleanup
    import stockagent.data_sync.training_return as admission
    root,source,row,catalog=example(tmp_path)
    spec=load_legacy_specs(catalog)[row['dataset']]
    packed=tmp_path/'packed'
    published=publish_archive(spec,root,packed,repo_root=root.parent,manual_capture=True)
    target=tmp_path/'receipts';target.mkdir()
    monkeypatch.setattr(cleanup.shutil,'disk_usage',lambda path:SimpleNamespace(free=(64*1024**3 if ram_available else 0)))
    admitted=[]
    def reject(path,required):
        admitted.append((path,required))
        raise SnapshotError('actual physical backing drive lacks capacity')
    monkeypatch.setattr(admission,'admit_workspace',reject)
    if ram_available:
        proof=cleanup.verify_for_retirement(spec,packed,target)
        assert proof['decoded_originals_verified'] and proof['manifest_sha256']==published['manifest_sha256']
        assert not admitted
    else:
        with pytest.raises(SnapshotError,match='physical backing drive'):
            cleanup.verify_for_retirement(spec,packed,target)
        assert admitted[0][0]==target/'verification-scratch'
        assert admitted[0][1]>32*1024**3
    assert source.is_dir()
    assert not list(target.glob('verification-scratch/stockagent-legacy-verify-*'))


def test_inventory_drift_only_admits_removed_hardlinks(tmp_path,monkeypatch):
    from scripts import retire_local_offline_artifacts as cleanup
    root,source,row,catalog=example(tmp_path)
    monkeypatch.setattr(cleanup,'artifact_service_references',lambda roots,repo:{str(p):[] for p in roots})
    monkeypatch.setattr(cleanup,'active_configuration_references',lambda *a:[])
    neighbor=tmp_path/'other-hardlink'
    os.link(source/'unique-result.bin',neighbor)
    before=cleanup.observe(root,'replays/old-result')
    neighbor.unlink()
    after=cleanup.observe(root,'replays/old-result')
    assert cleanup.shared_link_observation_drift(before,after)==[
        {'path':'unique-result.bin','before_links':2,'current_links':1}]
    for position in (0,1,2,3,5):
        changed=json.loads(json.dumps(after));changed['observation_rows'][0]['signature'][position]+=1
        assert cleanup.shared_link_observation_drift(before,changed) is None
    no_link_change=json.loads(json.dumps(after));no_link_change['observation_rows'][0]['signature'][6]=2
    assert cleanup.shared_link_observation_drift(before,no_link_change) is None


@pytest.mark.parametrize('problem', [None, 'wait-failed', 'transport-changed',
                                   'expired-before', 'expired-during', 'consumer', 'source'])
def test_scan_wait_keeps_original_full_audit_and_all_final_gates(tmp_path, monkeypatch, problem):
    from datetime import datetime, timezone
    from scripts import retire_local_offline_artifacts as cleanup
    import stockagent.data_sync.legacy_artifact_retirement as retirement
    from test_legacy_artifact_retirement import _fixture
    import fcntl
    spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    options['manual_immediate'] = True
    current = {'ok': False, 'checked_at': datetime.now(timezone.utc).isoformat()}
    options['peer_probe'] = lambda: dict(current)
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan['blockers'] == ['cold-peer-not-converged']
    verified_at = plan['full_verification']['verified_at_epoch']
    if problem == 'expired-before':
        plan['full_verification']['verified_at_epoch'] = time.time() - 1501
    if problem == 'consumer':
        plan['blockers'].append('enabled-service-references-artifact')
    target = tmp_path/'receipt';target.mkdir()
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path/'common.lock')
    waits = []
    def wait(cfg, seconds):
        with cleanup.COMMON_OWNER.open('a') as probe:
            with pytest.raises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert 0 <= seconds <= 300
        waits.append(True)
        if problem == 'expired-during':
            monkeypatch.setattr(cleanup.time, 'time', lambda: verified_at + 1501)
        if problem == 'source':
            file = source/'checkpoint.pt';info=file.stat()
            file.write_bytes(b'X'*info.st_size)
            os.utime(file, ns=(info.st_atime_ns, info.st_mtime_ns))
        current['ok'] = problem not in ('wait-failed', 'transport-changed')
        returned = dict(current)
        if problem == 'transport-changed':
            returned['ok'] = True
        return returned
    monkeypatch.setattr(cleanup, '_wait_for_convergence', wait)
    monkeypatch.setattr(retirement, 'verify_packed_snapshot',
                        lambda *a, **kw: pytest.fail('full audit repeated while holding mutation owner'))
    if problem in ('wait-failed', 'transport-changed', 'consumer', 'source'):
        with pytest.raises(SnapshotError):
            cleanup.retire_after_convergence(spec, None, target, plan, options)
        assert source.exists() and hot.exists()
    else:
        result = cleanup.retire_after_convergence(spec, None, target, plan, options)
        if problem:
            assert result is None and source.exists() and hot.exists()
        else:
            assert result['deleted'] is True and not source.exists() and not hot.exists()
            assert plan['full_verification']['verified_at_epoch'] == verified_at
    assert bool(waits) is (problem not in ('consumer', 'expired-before'))


def test_batched_active_configuration_keeps_independent_jobs_and_unreadable_configs_protected(tmp_path):
    import subprocess,sys
    from stockagent.data_sync.remote_legacy_return import active_configuration_references_many
    roots=[tmp_path/'artifacts/replays/first',tmp_path/'artifacts/replays/second']
    for root in roots:root.mkdir(parents=True)
    config=tmp_path/'independent.yaml'
    config.write_text('checkpoint: artifacts/replays/first/model.pt\n')
    process=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)','--config',str(config)],cwd=tmp_path)
    try:
        result=active_configuration_references_many(roots,tmp_path)
        label='pid='+str(process.pid)+':active-config:'
        assert any(r.startswith(label) for r in result[str(roots[0])])
        assert not any(r.startswith(label) for r in result[str(roots[1])])
        config.write_text('checkpoint: [broken\n')
        result=active_configuration_references_many(roots,tmp_path)
        assert all(any(r.startswith('pid='+str(process.pid)+':active-config-unreadable:')
                       for r in result[str(root)]) for root in roots)
    finally:
        process.terminate();process.wait(timeout=5)


@pytest.mark.parametrize('corrupt',[False,True])
def test_complete_archive_publication_keeps_sha_gates_after_shared_link_removal(tmp_path,monkeypatch,corrupt):
    root,source,row,catalog=example(tmp_path)
    spec=load_legacy_specs(catalog)[row['dataset']]
    neighbor=tmp_path/'other-hardlink';os.link(source/'unique-result.bin',neighbor)
    prepare_archive(spec,root,manual_capture=True)
    manifest_path=spec.stage_root/spec.dataset/'archive/legacy_archive_manifest.json'
    preserved=manifest_path.read_bytes()
    neighbor.unlink()
    if corrupt:
        file=source/'unique-result.bin';info=file.stat();file.write_bytes(b'X'*info.st_size)
        os.utime(file,ns=(info.st_atime_ns,info.st_mtime_ns))
    import stockagent.data_sync.legacy_artifact_archive as canonical
    monkeypatch.setattr(canonical,'prepare_archive',lambda *a,**kw:pytest.fail('complete stage must keep its provenance'))
    if corrupt:
        with pytest.raises(SnapshotError,match='source differs'):
            publish_archive(spec,root,tmp_path/'cold',repo_root=root.parent,manual_capture=True)
        assert not (tmp_path/'cold/heads').exists()
    else:
        result=publish_archive(spec,root,tmp_path/'cold',repo_root=root.parent,manual_capture=True)
        assert verify_cold_archive(spec,tmp_path/'cold')['snapshot_id']==result['snapshot_id']
    assert manifest_path.read_bytes()==preserved
